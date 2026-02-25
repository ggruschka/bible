#!/usr/bin/env python3
"""
Query utilities for the Sagrada Biblia database.

Provides functions for common queries: verse lookup, chapter reading,
full-text search, footnote retrieval, and cross-reference lookup.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'bible.db')

_default_bid = None


def get_conn():
    """Get a database connection."""
    return sqlite3.connect(os.path.abspath(DB_PATH))


def default_bible_id():
    """Get the default Bible id (Bible with most verses)."""
    global _default_bid
    if _default_bid is None:
        conn = get_conn()
        row = conn.execute(
            "SELECT bible_id FROM verse GROUP BY bible_id ORDER BY COUNT(*) DESC LIMIT 1"
        ).fetchone()
        if row is None:
            row = conn.execute("SELECT id FROM bible ORDER BY id LIMIT 1").fetchone()
        conn.close()
        _default_bid = row[0] if row else 1
    return _default_bid


def resolve_book(conn, book):
    """Resolve a book identifier to its id and name.

    Accepts any of: slug ('juan'), OSIS id ('John'), Spanish abbreviation ('Jn'),
    English abbreviation ('John'), Spanish name ('Génesis'), or numeric id (1).

    Returns (book_id, name_es) or (None, None) if not found.
    """
    if isinstance(book, int):
        row = conn.execute(
            "SELECT id, name_es FROM book WHERE id = ?", (book,)).fetchone()
        return (row[0], row[1]) if row else (None, None)

    # Try exact match on slug, osis_id, abbreviations, then name
    row = conn.execute('''
        SELECT id, name_es FROM book
        WHERE slug = ? OR osis_id = ? OR abbrev_es = ? OR abbrev_en = ? OR name_es = ?
        LIMIT 1
    ''', (book, book, book, book, book)).fetchone()
    if row:
        return row[0], row[1]

    # Case-insensitive fallback
    b = book.lower()
    row = conn.execute('''
        SELECT id, name_es FROM book
        WHERE LOWER(slug) = ? OR LOWER(osis_id) = ? OR LOWER(abbrev_es) = ?
              OR LOWER(abbrev_en) = ? OR LOWER(name_es) = ?
        LIMIT 1
    ''', (b, b, b, b, b)).fetchone()
    return (row[0], row[1]) if row else (None, None)


def get_verse(book, chapter, verse, bible_id=None):
    """Get a single verse by book identifier, chapter, and verse number.

    Book can be: slug ('juan'), OSIS ('John'), abbreviation ('Jn'), name ('Génesis'), or id (1).
    """
    if bible_id is None:
        bible_id = default_bible_id()
    conn = get_conn()
    book_id, _ = resolve_book(conn, book)
    if book_id is None:
        conn.close()
        return None
    row = conn.execute('''
        SELECT v.text_clean, b.name_es, v.chapter_number, v.verse_number
        FROM verse v
        JOIN book b ON v.book_id = b.id
        WHERE v.book_id = ? AND v.chapter_number = ? AND v.verse_number = ?
              AND v.bible_id = ?
    ''', (book_id, chapter, verse, bible_id)).fetchone()
    conn.close()
    if row:
        return {
            'text': row[0],
            'book': row[1], 'chapter': row[2], 'verse': row[3]
        }
    return None


def get_chapter(book, chapter, bible_id=None):
    """Get all verses in a chapter."""
    if bible_id is None:
        bible_id = default_bible_id()
    conn = get_conn()
    book_id, book_name = resolve_book(conn, book)
    if book_id is None:
        conn.close()
        return []
    rows = conn.execute('''
        SELECT v.verse_number, v.text_clean
        FROM verse v
        WHERE v.book_id = ? AND v.chapter_number = ? AND v.bible_id = ?
        ORDER BY v.verse_number
    ''', (book_id, chapter, bible_id)).fetchall()
    conn.close()
    return [{'verse': r[0], 'text': r[1], 'book': book_name} for r in rows]


def search_text(query, bible_id=None, limit=20):
    """Full-text search across all verses."""
    if bible_id is None:
        bible_id = default_bible_id()
    conn = get_conn()
    rows = conn.execute('''
        SELECT b.abbrev_es, f.chapter_number, f.verse_number,
               snippet(verse_fts, 0, '>>>', '<<<', '...', 30)
        FROM verse_fts f
        JOIN book b ON f.book_id = b.id
        WHERE text_clean MATCH ? AND f.bible_id = ?
        LIMIT ?
    ''', (query, bible_id, limit)).fetchall()
    conn.close()
    return [{'ref': f'{r[0]} {r[1]}:{r[2]}', 'snippet': r[3]} for r in rows]


def get_footnotes(book, chapter, verse):
    """Get all footnotes for a specific verse (shared across all Bibles)."""
    conn = get_conn()
    book_id, _ = resolve_book(conn, book)
    if book_id is None:
        conn.close()
        return []
    rows = conn.execute('''
        SELECT fn.id, fn.text
        FROM footnote fn
        WHERE fn.book_id = ? AND fn.chapter_number = ?
              AND fn.verse_start <= ? AND (fn.verse_end >= ? OR fn.verse_end IS NULL AND fn.verse_start = ?)
        ORDER BY fn.verse_start, fn.id
    ''', (book_id, chapter, verse, verse, verse)).fetchall()
    conn.close()
    return [{'id': r[0], 'text': r[1]} for r in rows]


def get_cross_refs(book, chapter, verse, limit=20):
    """Get cross-references for a specific verse, ordered by relevance."""
    conn = get_conn()
    book_id, _ = resolve_book(conn, book)
    if book_id is None:
        conn.close()
        return []

    rows = conn.execute('''
        SELECT b.abbrev_es, cr.target_chapter, cr.target_verse, cr.votes
        FROM cross_reference cr
        JOIN book b ON cr.target_book_id = b.id
        WHERE cr.source_book_id = ? AND cr.source_chapter = ? AND cr.source_verse = ?
        ORDER BY cr.votes DESC
        LIMIT ?
    ''', (book_id, chapter, verse, limit)).fetchall()
    conn.close()
    return [{'ref': f'{r[0]} {r[1]}:{r[2]}', 'votes': r[3]} for r in rows]


def get_section_headings(book, chapter):
    """Get section headings for a chapter."""
    conn = get_conn()
    book_id, _ = resolve_book(conn, book)
    if book_id is None:
        conn.close()
        return []
    rows = conn.execute('''
        SELECT sh.before_verse, sh.heading_text, sh.heading_style
        FROM section_heading sh
        WHERE sh.book_id = ? AND sh.chapter_number = ?
        ORDER BY sh.before_verse
    ''', (book_id, chapter)).fetchall()
    conn.close()
    return [{'before_verse': r[0], 'text': r[1], 'style': r[2]} for r in rows]


# ─── CLI Demo ───

if __name__ == '__main__':
    print("=== Sagrada Biblia Database Query Demo ===\n")

    # Verse lookup — different identifier formats
    v = get_verse('Gen', 1, 1)               # OSIS id
    print(f"Gen 1:1 (OSIS 'Gen'): {v['text']}\n")

    v = get_verse('Jn', 3, 16)               # Spanish abbreviation
    print(f"Juan 3:16 (abbrev 'Jn'): {v['text']}\n")

    v = get_verse('Ps', 22, 1)               # OSIS 'Ps'
    print(f"Salmo 22:1 (OSIS 'Ps'): {v['text']}\n")

    v = get_verse(73, 1, 1)                  # numeric id
    print(f"Book 73, 1:1 (id=73): {v['text']}\n")

    # Chapter reading
    ch = get_chapter('Ps', 22)
    print(f"Salmo 22 (= Hebrew Psalm 23) — {len(ch)} verses:")
    for verse in ch:
        print(f"  {verse['verse']}. {verse['text']}")
    print()

    # Full-text search
    results = search_text('bienaventurado', limit=5)
    print(f"Search 'bienaventurado' — {len(results)} results:")
    for r in results:
        print(f"  {r['ref']}: {r['snippet']}")
    print()

    # Footnotes
    fns = get_footnotes('Gen', 1, 1)
    print(f"Footnotes for Gen 1:1 — {len(fns)} notes:")
    for fn in fns:
        print(f"  [{fn['id']}] {fn['text'][:150]}...")
    print()

    # Cross-references
    refs = get_cross_refs('John', 3, 16, limit=5)
    print(f"Cross-refs for John 3:16 — top 5:")
    for r in refs:
        print(f"  → {r['ref']} (votes: {r['votes']})")
    print()

    # Section headings
    headings = get_section_headings('Matt', 5)
    print(f"Section headings for Matt 5:")
    for h in headings:
        print(f"  Before v.{h['before_verse']}: {h['text']} [{h['style']}]")
