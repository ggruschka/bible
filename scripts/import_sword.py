#!/usr/bin/env python3
"""
Import Bible text from a SWORD module into the database.

Reads the SpaPlatense SWORD module (Straubinger 1948) using pysword,
creates a Bible entry, and inserts all verses.

Requires: pysword (pip install pysword)
"""

import os
import re
import sqlite3
from datetime import datetime

try:
    from pysword.modules import SwordModules
except ImportError:
    print("ERROR: pysword is required. Install with: pip install pysword")
    raise SystemExit(1)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'bible.db')
SWORD_ZIP = os.path.join(os.path.dirname(__file__), '..', 'data', 'sword', 'SpaPlatense.zip')


def import_sword():
    """Import Bible text from the SWORD SpaPlatense module."""
    db_path = os.path.abspath(DB_PATH)
    sword_path = os.path.abspath(SWORD_ZIP)

    if not os.path.exists(sword_path):
        print(f"ERROR: SWORD module not found at {sword_path}")
        raise SystemExit(1)

    if not os.path.exists(db_path):
        print(f"ERROR: Database not found at {db_path}")
        print("Run create_db.py first.")
        raise SystemExit(1)

    # ─── Open SWORD module ───
    print(f"Opening SWORD module: {sword_path}")
    sword = SwordModules(paths=sword_path)
    modules = sword.parse_modules()
    module_name = list(modules.keys())[0]
    print(f"Module: {module_name}")

    bible_reader = sword.get_bible_from_module(module_name)
    structure = bible_reader.get_structure()

    # ─── Connect to DB ───
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # Log start
    started = datetime.now().isoformat()
    cur.execute(
        "INSERT INTO import_log (step, status, started_at) VALUES (?, ?, ?)",
        ('import_sword', 'started', started)
    )
    log_id = cur.lastrowid
    conn.commit()

    # ─── Create Bible entry ───
    cur.execute(
        """INSERT INTO bible (name, full_name, language, canon, parent_id, method, year, description)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ('Straubinger (SWORD)',
         'Sagrada Biblia Straubinger - SWORD SpaPlatense Module',
         'es', 'catholic', None, 'human', 1948,
         'Texto de la Biblia Platense (Straubinger 1948), extraído del módulo SWORD SpaPlatense')
    )
    bible_id = cur.lastrowid
    print(f"Created Bible entry: id={bible_id}, name='Straubinger (SWORD)'")

    # ─── Build OSIS → book_id map ───
    osis_map = {}
    for row in cur.execute("SELECT osis_id, book_id FROM osis_book_map WHERE book_id IS NOT NULL"):
        osis_map[row[0]] = row[1]

    # ─── Build chapter_id lookup ───
    chapter_ids = {}
    for row in cur.execute("SELECT id, book_id, number FROM chapter"):
        chapter_ids[(row[1], row[2])] = row[0]

    # ─── Import verses ───
    inserted = 0
    skipped_empty = 0
    skipped_no_book = 0
    books_imported = set()

    all_books = structure.get_books()
    for testament in ('ot', 'nt'):
        books = all_books.get(testament, [])
        for book in books:
            osis_id = book.osis_name
            book_id = osis_map.get(osis_id)

            if book_id is None:
                # Book not in our schema — skip silently
                skipped_no_book += 1
                continue

            for chapter_num in range(1, book.num_chapters + 1):
                chapter_id = chapter_ids.get((book_id, chapter_num))
                if chapter_id is None:
                    continue

                max_verse = book.chapter_lengths[chapter_num - 1]
                for verse_num in range(1, max_verse + 1):
                    try:
                        text = bible_reader.get(
                            books=book.name,
                            chapters=chapter_num,
                            verses=verse_num,
                            clean=True
                        )
                    except (KeyError, IndexError, TypeError):
                        text = None

                    if not text or not text.strip():
                        skipped_empty += 1
                        continue

                    # Clean up whitespace
                    text = re.sub(r'\s+', ' ', text.strip())

                    cur.execute(
                        """INSERT OR IGNORE INTO verse
                           (bible_id, chapter_id, book_id, chapter_number, verse_number, text_clean)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (bible_id, chapter_id, book_id, chapter_num, verse_num, text)
                    )

                    if cur.rowcount > 0:
                        inserted += 1
                        books_imported.add(book_id)

                if inserted % 1000 == 0 and inserted > 0:
                    conn.commit()

            if book_id in books_imported:
                book_name = cur.execute(
                    "SELECT name_es FROM book WHERE id=?", (book_id,)
                ).fetchone()[0]
                book_verses = cur.execute(
                    "SELECT COUNT(*) FROM verse WHERE bible_id=? AND book_id=?",
                    (bible_id, book_id)
                ).fetchone()[0]
                print(f"  {book_name}: {book_verses} verses")

    conn.commit()

    # ─── Log completion ───
    finished = datetime.now().isoformat()
    cur.execute(
        """UPDATE import_log SET status=?, records=?, message=?, finished_at=?
           WHERE id=?""",
        ('completed', inserted,
         f'{len(books_imported)} books, {inserted} verses, {skipped_empty} empty, {skipped_no_book} unmapped books',
         finished, log_id)
    )
    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"SWORD import complete!")
    print(f"  Bible ID:               {bible_id}")
    print(f"  Books imported:         {len(books_imported)}")
    print(f"  Verses imported:        {inserted}")
    print(f"  Skipped (empty):        {skipped_empty}")
    print(f"  Skipped (unmapped):     {skipped_no_book}")
    print(f"{'='*60}")


if __name__ == '__main__':
    import_sword()
