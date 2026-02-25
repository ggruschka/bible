#!/usr/bin/env python3
"""
Step 5: Validate the Sagrada Biblia database.

Runs comprehensive checks on data integrity, verse counts, footnote linkage,
cross-reference integrity, and FTS synchronization.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'bible.db')


def validate():
    """Run all validation checks."""
    conn = sqlite3.connect(os.path.abspath(DB_PATH))
    passed = 0
    failed = 0
    warnings = 0

    def check(name, condition, detail=""):
        nonlocal passed, failed
        status = "PASS" if condition else "FAIL"
        if condition:
            passed += 1
        else:
            failed += 1
        suffix = f" — {detail}" if detail else ""
        print(f"  [{status}] {name}{suffix}")

    def warn(name, detail=""):
        nonlocal warnings
        warnings += 1
        suffix = f" — {detail}" if detail else ""
        print(f"  [WARN] {name}{suffix}")

    # Get the primary bible ID (the one with actual verse data)
    bid = conn.execute(
        "SELECT bible_id FROM verse GROUP BY bible_id ORDER BY COUNT(*) DESC LIMIT 1"
    ).fetchone()
    if not bid:
        bid = conn.execute("SELECT id FROM bible ORDER BY id LIMIT 1").fetchone()
    if not bid:
        print("ERROR: No Bibles found in database")
        return False
    bid = bid[0]

    # ─── Structure Checks ───
    print("\n=== Structure ===")

    books = conn.execute("SELECT COUNT(*) FROM book").fetchone()[0]
    check("73 books", books == 73, f"got {books}")

    testaments = conn.execute("SELECT COUNT(*) FROM testament").fetchone()[0]
    check("2 testaments", testaments == 2)

    bibles = conn.execute("SELECT COUNT(*) FROM bible").fetchone()[0]
    check("At least 1 Bible", bibles >= 1, f"got {bibles}")

    chapters = conn.execute("SELECT COUNT(*) FROM chapter").fetchone()[0]
    check("Chapters created", chapters > 1300, f"got {chapters}")

    # ─── Verse Checks ───
    print("\n=== Verses ===")

    total_verses = conn.execute(
        "SELECT COUNT(*) FROM verse WHERE bible_id=?", (bid,)).fetchone()[0]
    check("Verse count >= 35000", total_verses >= 35000, f"got {total_verses}")

    # All books have verses
    empty_books = conn.execute('''
        SELECT b.name_es FROM book b
        LEFT JOIN verse v ON v.book_id = b.id AND v.bible_id = ?
        GROUP BY b.id HAVING COUNT(v.id) = 0
    ''', (bid,)).fetchall()
    check("All books have verses", len(empty_books) == 0,
          f"empty: {[r[0] for r in empty_books]}" if empty_books else "")

    # All chapters have verses
    empty_chapters = conn.execute('''
        SELECT b.name_es, c.number FROM chapter c
        JOIN book b ON c.book_id = b.id
        LEFT JOIN verse v ON v.chapter_id = c.id AND v.bible_id = ?
        GROUP BY c.id HAVING COUNT(v.id) = 0
    ''', (bid,)).fetchall()
    if empty_chapters:
        warn(f"{len(empty_chapters)} chapters with 0 verses",
             f"e.g. {empty_chapters[0][0]} ch.{empty_chapters[0][1]}")
    else:
        check("All chapters have verses", True)

    # Spot check specific verses
    gen11 = conn.execute('''
        SELECT text_clean FROM verse v JOIN book b ON v.book_id=b.id
        WHERE b.slug='genesis' AND v.chapter_number=1 AND v.verse_number=1 AND v.bible_id=?
    ''', (bid,)).fetchone()
    check("Gen 1:1 exists", gen11 is not None)
    if gen11:
        check("Gen 1:1 text correct",
              gen11[0].startswith("Al principio creó Dios"),
              f"got: {gen11[0][:50]}")

    # ─── Footnote Checks ───
    print("\n=== Footnotes ===")

    fn_count = conn.execute("SELECT COUNT(*) FROM footnote").fetchone()[0]
    check("Footnote count >= 9000", fn_count >= 9000, f"got {fn_count}")

    # Footnotes with valid book references
    orphan_fn = conn.execute('''
        SELECT COUNT(*) FROM footnote f
        LEFT JOIN book b ON f.book_id = b.id
        WHERE b.id IS NULL
    ''').fetchone()[0]
    check("All footnotes reference valid books", orphan_fn == 0, f"got {orphan_fn} orphans")

    # Empty footnotes
    empty_fn = conn.execute(
        "SELECT COUNT(*) FROM footnote WHERE text IS NULL OR text = ''").fetchone()[0]
    check("No empty footnotes", empty_fn == 0, f"got {empty_fn}")

    # ─── Cross-Reference Checks ───
    print("\n=== Cross-References ===")

    xref_count = conn.execute("SELECT COUNT(*) FROM cross_reference").fetchone()[0]
    check("Cross-refs >= 300000", xref_count >= 300000, f"got {xref_count}")

    # Gen 1:1 has cross-refs
    gen_id = conn.execute("SELECT id FROM book WHERE slug='genesis'").fetchone()[0]
    gen11_refs = conn.execute('''
        SELECT COUNT(*) FROM cross_reference
        WHERE source_book_id=? AND source_chapter=1 AND source_verse=1
    ''', (gen_id,)).fetchone()[0]
    check("Gen 1:1 has cross-refs", gen11_refs > 0, f"got {gen11_refs}")

    # Check Gen 1:1 → John 1:1 exists
    john_id = conn.execute("SELECT id FROM book WHERE slug='juan'").fetchone()[0]
    gen_john = conn.execute('''
        SELECT COUNT(*) FROM cross_reference
        WHERE source_book_id=? AND source_chapter=1 AND source_verse=1
              AND target_book_id=? AND target_chapter=1 AND target_verse=1
    ''', (gen_id, john_id)).fetchone()[0]
    check("Gen 1:1 → John 1:1 cross-ref exists", gen_john > 0)

    # Psalm numbering check: Hebrew Ps 23:1 cross-refs should be under LXX Ps 22
    salmo_id = conn.execute("SELECT id FROM book WHERE slug='salmo'").fetchone()[0]
    ps22_refs = conn.execute('''
        SELECT COUNT(*) FROM cross_reference
        WHERE source_book_id=? AND source_chapter=22
    ''', (salmo_id,)).fetchone()[0]
    check("LXX Psalm 22 has cross-refs (Hebrew 23)", ps22_refs > 0, f"got {ps22_refs}")

    # ─── FTS5 Checks ───
    print("\n=== FTS5 Search ===")

    all_verses = conn.execute("SELECT COUNT(*) FROM verse").fetchone()[0]
    fts_count = conn.execute("SELECT COUNT(*) FROM verse_fts").fetchone()[0]
    check("FTS5 in sync with verse table", fts_count == all_verses,
          f"fts={fts_count}, verses={all_verses}")

    # Test search
    search_result = conn.execute(
        "SELECT COUNT(*) FROM verse_fts WHERE text_clean MATCH 'pastor'"
    ).fetchone()[0]
    check("FTS5 search works", search_result > 0, f"'pastor' → {search_result} results")

    # ─── Bible Info ───
    print("\n=== Bibles ===")

    bibles_info = conn.execute("SELECT name, language, method FROM bible").fetchall()
    check("Bible table populated", len(bibles_info) >= 1, f"got {len(bibles_info)}")
    for b in bibles_info:
        print(f"    Bible: {b[0]} (lang={b[1]}, method={b[2]})")

    # ─── Import Log ───
    print("\n=== Import Log ===")

    logs = conn.execute(
        "SELECT step, status, records, message FROM import_log ORDER BY id"
    ).fetchall()
    for log in logs:
        print(f"    {log[0]}: {log[1]} ({log[2]} records) — {log[3]}")

    # ─── Summary ───
    print(f"\n{'='*60}")
    print(f"Validation complete: {passed} passed, {failed} failed, {warnings} warnings")
    print(f"{'='*60}")

    conn.close()
    return failed == 0


if __name__ == '__main__':
    success = validate()
    exit(0 if success else 1)
