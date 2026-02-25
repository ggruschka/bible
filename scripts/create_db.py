#!/usr/bin/env python3
"""
Step 1: Create the Bible database and seed reference data.

Creates the SQLite database, applies schema, and populates:
- testament (2 rows)
- bible (2 rows: Straubinger 1948 + v28)
- book (73 rows: full Catholic canon)
- chapter (all chapters for all books)
- osis_book_map (standard OSIS IDs → book.id)
- psalm_number_map (LXX ↔ Hebrew psalm numbering)
- commentary_source (1 row: Straubinger)
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'bible.db')
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'schema.sql')


# ─── Book data: all 73 books of the Catholic canon ───
# (id, testament_id, name_es, name_en, abbrev_es, abbrev_en, slug, osis_id,
#  total_chapters, category, alt_name_es)
BOOKS = [
    # ── Pentateuch ──
    (1, 1, 'Génesis', 'Genesis', 'Gn', 'Gen', 'genesis', 'Gen', 50, 'pentateuch', None),
    (2, 1, 'Éxodo', 'Exodus', 'Ex', 'Exod', 'exodo', 'Exod', 40, 'pentateuch', None),
    (3, 1, 'Levítico', 'Leviticus', 'Lv', 'Lev', 'levitico', 'Lev', 27, 'pentateuch', None),
    (4, 1, 'Números', 'Numbers', 'Nm', 'Num', 'numeros', 'Num', 36, 'pentateuch', None),
    (5, 1, 'Deuteronomio', 'Deuteronomy', 'Dt', 'Deut', 'deuteronomio', 'Deut', 34, 'pentateuch', None),
    # ── History ──
    (6, 1, 'Josué', 'Joshua', 'Jos', 'Josh', 'josue', 'Josh', 24, 'history', None),
    (7, 1, 'Jueces', 'Judges', 'Jue', 'Judg', 'jueces', 'Judg', 21, 'history', None),
    (8, 1, 'Rut', 'Ruth', 'Rut', 'Ruth', 'rut', 'Ruth', 4, 'history', None),
    (9, 1, '1 Samuel', '1 Samuel', '1 Sam', '1Sam', '1-samuel', '1Sam', 31, 'history', 'I Reyes (1 Samuel)'),
    (10, 1, '2 Samuel', '2 Samuel', '2 Sam', '2Sam', '2-samuel', '2Sam', 24, 'history', 'II Reyes (2 Samuel)'),
    (11, 1, '1 Reyes', '1 Kings', '1 Rey', '1Kgs', '1-reyes', '1Kgs', 22, 'history', 'III Reyes (1 Reyes)'),
    (12, 1, '2 Reyes', '2 Kings', '2 Rey', '2Kgs', '2-reyes', '2Kgs', 25, 'history', 'IV Reyes (2 Reyes)'),
    (13, 1, '1 Crónicas', '1 Chronicles', '1 Cro', '1Chr', '1-cronicas', '1Chr', 29, 'history', None),
    (14, 1, '2 Crónicas', '2 Chronicles', '2 Cro', '2Chr', '2-cronicas', '2Chr', 36, 'history', None),
    (15, 1, 'Esdras', 'Ezra', 'Esd', 'Ezra', 'esdras', 'Ezra', 10, 'history', None),
    (16, 1, 'Nehemías', 'Nehemiah', 'Neh', 'Neh', 'nehemias', 'Neh', 13, 'history', None),
    (17, 1, 'Tobías', 'Tobit', 'Tob', 'Tob', 'tobias', 'Tob', 14, 'history', None),
    (18, 1, 'Judit', 'Judith', 'Jud', 'Jdt', 'judit', 'Jdt', 16, 'history', None),
    (19, 1, 'Ester', 'Esther', 'Est', 'Esth', 'ester', 'Esth', 16, 'history', None),
    (20, 1, '1 Macabeos', '1 Maccabees', '1 Mac', '1Macc', '1-macabeos', '1Macc', 16, 'history', None),
    (21, 1, '2 Macabeos', '2 Maccabees', '2 Mac', '2Macc', '2-macabeos', '2Macc', 15, 'history', None),
    # ── Wisdom ──
    (22, 1, 'Job', 'Job', 'Job', 'Job', 'job', 'Job', 42, 'wisdom', None),
    (23, 1, 'Salmos', 'Psalms', 'Sal', 'Ps', 'salmo', 'Ps', 150, 'wisdom', 'Los Salmos'),
    (24, 1, 'Proverbios', 'Proverbs', 'Prov', 'Prov', 'proverbios', 'Prov', 31, 'wisdom', None),
    (25, 1, 'Eclesiastés', 'Ecclesiastes', 'Ecle', 'Eccl', 'eclesiastes', 'Eccl', 12, 'wisdom', None),
    (26, 1, 'Cantar de los Cantares', 'Song of Solomon', 'Cant', 'Song', 'cantar', 'Song', 8, 'wisdom', None),
    (27, 1, 'Sabiduría', 'Wisdom', 'Sab', 'Wis', 'sabiduria', 'Wis', 19, 'wisdom', None),
    (28, 1, 'Eclesiástico', 'Sirach', 'Eclo', 'Sir', 'eclesiastico', 'Sir', 51, 'wisdom', 'Ben Sirá'),
    # ── Prophecy ──
    (29, 1, 'Isaías', 'Isaiah', 'Is', 'Isa', 'isaias', 'Isa', 66, 'prophecy', None),
    (30, 1, 'Jeremías', 'Jeremiah', 'Jer', 'Jer', 'jeremias', 'Jer', 52, 'prophecy', None),
    (31, 1, 'Lamentaciones', 'Lamentations', 'Lam', 'Lam', 'lamentaciones', 'Lam', 5, 'prophecy', None),
    (32, 1, 'Baruc', 'Baruch', 'Bar', 'Bar', 'baruc', 'Bar', 6, 'prophecy', None),
    (33, 1, 'Ezequiel', 'Ezekiel', 'Ez', 'Ezek', 'ezequiel', 'Ezek', 48, 'prophecy', None),
    (34, 1, 'Daniel', 'Daniel', 'Dan', 'Dan', 'daniel', 'Dan', 14, 'prophecy', None),
    (35, 1, 'Oseas', 'Hosea', 'Os', 'Hos', 'oseas', 'Hos', 14, 'prophecy', None),
    (36, 1, 'Joel', 'Joel', 'Joe', 'Joel', 'joel', 'Joel', 3, 'prophecy', None),
    (37, 1, 'Amós', 'Amos', 'Am', 'Amos', 'amos', 'Amos', 9, 'prophecy', None),
    (38, 1, 'Abdías', 'Obadiah', 'Abd', 'Obad', 'abdias', 'Obad', 1, 'prophecy', None),
    (39, 1, 'Jonás', 'Jonah', 'Jon', 'Jonah', 'jonas', 'Jonah', 4, 'prophecy', None),
    (40, 1, 'Miqueas', 'Micah', 'Miq', 'Mic', 'miqueas', 'Mic', 7, 'prophecy', None),
    (41, 1, 'Nahum', 'Nahum', 'Nah', 'Nah', 'nahum', 'Nah', 3, 'prophecy', None),
    (42, 1, 'Habacuc', 'Habakkuk', 'Hab', 'Hab', 'habacuc', 'Hab', 3, 'prophecy', None),
    (43, 1, 'Sofonías', 'Zephaniah', 'Sof', 'Zeph', 'sofonias', 'Zeph', 3, 'prophecy', None),
    (44, 1, 'Ageo', 'Haggai', 'Ag', 'Hag', 'ageo', 'Hag', 2, 'prophecy', None),
    (45, 1, 'Zacarías', 'Zechariah', 'Zac', 'Zech', 'zacarias', 'Zech', 14, 'prophecy', None),
    (46, 1, 'Malaquías', 'Malachi', 'Mal', 'Mal', 'malaquias', 'Mal', 4, 'prophecy', None),
    # ── Gospels ──
    (47, 2, 'Mateo', 'Matthew', 'Mt', 'Matt', 'mateo', 'Matt', 28, 'gospel', None),
    (48, 2, 'Marcos', 'Mark', 'Mc', 'Mark', 'marcos', 'Mark', 16, 'gospel', None),
    (49, 2, 'Lucas', 'Luke', 'Lc', 'Luke', 'lucas', 'Luke', 24, 'gospel', None),
    (50, 2, 'Juan', 'John', 'Jn', 'John', 'juan', 'John', 21, 'gospel', None),
    # ── Acts ──
    (51, 2, 'Hechos', 'Acts', 'Hch', 'Acts', 'hechos', 'Acts', 28, 'history', None),
    # ── Pauline Epistles ──
    (52, 2, 'Romanos', 'Romans', 'Rm', 'Rom', 'romanos', 'Rom', 16, 'epistle', None),
    (53, 2, '1 Corintios', '1 Corinthians', '1 Cor', '1Cor', '1-corintios', '1Cor', 16, 'epistle', None),
    (54, 2, '2 Corintios', '2 Corinthians', '2 Cor', '2Cor', '2-corintios', '2Cor', 13, 'epistle', None),
    (55, 2, 'Gálatas', 'Galatians', 'Gal', 'Gal', 'galatas', 'Gal', 6, 'epistle', None),
    (56, 2, 'Efesios', 'Ephesians', 'Ef', 'Eph', 'efesios', 'Eph', 6, 'epistle', None),
    (57, 2, 'Filipenses', 'Philippians', 'Filip', 'Phil', 'filipenses', 'Phil', 4, 'epistle', None),
    (58, 2, 'Colosenses', 'Colossians', 'Col', 'Col', 'colosenses', 'Col', 4, 'epistle', None),
    (59, 2, '1 Tesalonicenses', '1 Thessalonians', '1 Tes', '1Thess', '1-tesalonicenses', '1Thess', 5, 'epistle', None),
    (60, 2, '2 Tesalonicenses', '2 Thessalonians', '2 Tes', '2Thess', '2-tesalonicenses', '2Thess', 3, 'epistle', None),
    (61, 2, '1 Timoteo', '1 Timothy', '1 Tim', '1Tim', '1-timoteo', '1Tim', 6, 'epistle', None),
    (62, 2, '2 Timoteo', '2 Timothy', '2 Tim', '2Tim', '2-timoteo', '2Tim', 4, 'epistle', None),
    (63, 2, 'Tito', 'Titus', 'Tit', 'Titus', 'tito', 'Titus', 3, 'epistle', None),
    (64, 2, 'Filemón', 'Philemon', 'Filem', 'Phlm', 'filemon', 'Phlm', 1, 'epistle', None),
    (65, 2, 'Hebreos', 'Hebrews', 'Heb', 'Heb', 'hebreos', 'Heb', 13, 'epistle', None),
    # ── Catholic Epistles ──
    (66, 2, 'Santiago', 'James', 'St', 'Jas', 'santiago', 'Jas', 5, 'epistle', None),
    (67, 2, '1 Pedro', '1 Peter', '1 Pe', '1Pet', '1-pedro', '1Pet', 5, 'epistle', None),
    (68, 2, '2 Pedro', '2 Peter', '2 Pe', '2Pet', '2-pedro', '2Pet', 3, 'epistle', None),
    (69, 2, '1 Juan', '1 John', '1 Jn', '1John', '1-juan', '1John', 5, 'epistle', None),
    (70, 2, '2 Juan', '2 John', '2 Jn', '2John', '2-juan', '2John', 1, 'epistle', None),
    (71, 2, '3 Juan', '3 John', '3 Jn', '3John', '3-juan', '3John', 1, 'epistle', None),
    (72, 2, 'Judas', 'Jude', 'Jds', 'Jude', 'judas', 'Jude', 1, 'epistle', None),
    # ── Apocalypse ──
    (73, 2, 'Apocalipsis', 'Revelation', 'Apoc', 'Rev', 'apocalipsis', 'Rev', 22, 'apocalypse', None),
]

# Additional OSIS IDs used by Protestant Bibles (not in Catholic canon)
# These map to book_id=NULL so cross-ref import can skip them gracefully
PROTESTANT_ONLY_OSIS = []  # All 66 Protestant books are in the Catholic 73

# ─── Psalm numbering: LXX (Straubinger) ↔ Hebrew (Protestant) ───
# Format: (lxx_number, hebrew_number, lxx_display)
# Used to convert OpenBible.info Hebrew refs to our LXX numbering
PSALM_MAP = []

def _build_psalm_map():
    """Build the LXX ↔ Hebrew psalm number mapping."""
    m = []
    # Psalms 1-8: identical in both systems
    for n in range(1, 9):
        m.append((str(n), n, f'Salmo {n}'))
    # LXX 9a = Hebrew 9
    m.append(('9a', 9, 'Salmo 9 a'))
    # LXX 9b = Hebrew 10
    m.append(('9b', 10, 'Salmo 9 b (10)'))
    # LXX 10-112 = Hebrew 11-113 (offset by 1)
    for lxx in range(10, 113):
        heb = lxx + 1
        m.append((str(lxx), heb, f'Salmo {lxx} ({heb})'))
    # LXX 113a = Hebrew 114
    m.append(('113a', 114, 'Salmo 113 a (114)'))
    # LXX 113b = Hebrew 115
    m.append(('113b', 115, 'Salmo 113 b (115)'))
    # LXX 114 = Hebrew 116 (first half, verses 1-9)
    m.append(('114', 116, 'Salmo 114 (116, 1-9)'))
    # LXX 115 = Hebrew 116 (second half, verses 10-19)
    m.append(('115', 116, 'Salmo 115 (116, 10-19)'))
    # LXX 116-145 = Hebrew 117-146 (offset by 1)
    for lxx in range(116, 146):
        heb = lxx + 1
        m.append((str(lxx), heb, f'Salmo {lxx} ({heb})'))
    # LXX 146 = Hebrew 147 (first half, verses 1-11)
    m.append(('146', 147, 'Salmo 146 (147, 1-11)'))
    # LXX 147 = Hebrew 147 (second half, verses 12-20)
    # Note: both LXX 146 and 147 map to Hebrew 147
    m.append(('147', 147, 'Salmo 147'))
    # Psalms 148-150: identical
    for n in range(148, 151):
        m.append((str(n), n, f'Salmo {n}'))
    return m


def create_database():
    """Create the database, apply schema, and seed reference data."""
    db_path = os.path.abspath(DB_PATH)

    # Remove existing DB for clean creation
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed existing database: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # ─── Apply schema ───
    schema_path = os.path.abspath(SCHEMA_PATH)
    with open(schema_path, 'r') as f:
        schema_sql = f.read()
    cur.executescript(schema_sql)
    print("Schema applied.")

    # ─── Log start ───
    started = datetime.now().isoformat()
    cur.execute(
        "INSERT INTO import_log (step, status, started_at) VALUES (?, ?, ?)",
        ('create_db', 'started', started)
    )
    log_id = cur.lastrowid

    # ─── Testaments ───
    cur.executemany(
        "INSERT INTO testament (id, name_es, name_en, abbrev) VALUES (?, ?, ?, ?)",
        [
            (1, 'Antiguo Testamento', 'Old Testament', 'AT'),
            (2, 'Nuevo Testamento', 'New Testament', 'NT'),
        ]
    )
    print("Inserted 2 testaments.")

    # ─── Bible: Straubinger ───
    cur.execute(
        """INSERT INTO bible (name, full_name, language, canon, parent_id, method, year, description)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ('Straubinger', 'Sagrada Biblia - Mons. Dr. Juan Straubinger',
         'es', 'catholic', None, 'human', 1948,
         'Traducción directa de los textos primitivos por Mons. Dr. Juan Straubinger')
    )
    print("Inserted Straubinger Bible.")

    # ─── Bible: Straubinger v28 (Corrección 28, January 2024) ───
    cur.execute(
        """INSERT INTO bible (name, full_name, language, canon, parent_id, method, year, description)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        ('Straubinger v28', 'Sagrada Biblia Straubinger - Corrección 28',
         'es', 'catholic', 1, 'human', 2024,
         'Corrección 28 (Enero 2024) de la traducción Straubinger, revisada por Padre Jeromín León')
    )
    print("Inserted Straubinger v28 Bible.")

    # ─── Books (73) ───
    cur.executemany(
        """INSERT INTO book (id, testament_id, name_es, name_en, abbrev_es, abbrev_en,
                             slug, osis_id, total_chapters, category, alt_name_es)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        BOOKS
    )
    print(f"Inserted {len(BOOKS)} books.")

    # ─── Chapters ───
    chapter_count = 0
    for book in BOOKS:
        book_id, _, _, _, _, _, _, _, total_chapters, _, _ = book
        if total_chapters:
            for ch in range(1, total_chapters + 1):
                cur.execute(
                    "INSERT INTO chapter (book_id, number) VALUES (?, ?)",
                    (book_id, ch)
                )
                chapter_count += 1
    print(f"Inserted {chapter_count} chapters.")

    # ─── OSIS book map ───
    osis_rows = [(book[7], book[0]) for book in BOOKS]  # (osis_id, book_id)
    cur.executemany(
        "INSERT INTO osis_book_map (osis_id, book_id) VALUES (?, ?)",
        osis_rows
    )
    print(f"Inserted {len(osis_rows)} OSIS mappings.")

    # ─── Psalm number map ───
    psalm_map = _build_psalm_map()
    cur.executemany(
        "INSERT INTO psalm_number_map (lxx_number, hebrew_number, lxx_display) VALUES (?, ?, ?)",
        psalm_map
    )
    print(f"Inserted {len(psalm_map)} psalm number mappings.")

    # ─── Commentary source: Straubinger ───
    cur.execute(
        """INSERT INTO commentary_source (name, full_name, description, language, year)
           VALUES (?, ?, ?, ?, ?)""",
        ('Straubinger', 'Mons. Dr. Juan Straubinger',
         'Notas de la Sagrada Biblia, traducción directa de los textos primitivos',
         'es', 1948)
    )
    print("Inserted Straubinger commentary source.")

    # ─── Log completion ───
    finished = datetime.now().isoformat()
    total_records = 2 + 1 + len(BOOKS) + chapter_count + len(osis_rows) + len(psalm_map) + 1
    cur.execute(
        """UPDATE import_log SET status=?, records=?, message=?, finished_at=?
           WHERE id=?""",
        ('completed', total_records,
         f'{len(BOOKS)} books, {chapter_count} chapters, {len(psalm_map)} psalm maps',
         finished, log_id)
    )

    conn.commit()
    conn.close()
    print(f"\nDatabase created at: {db_path}")
    print(f"Total records seeded: {total_records}")


if __name__ == '__main__':
    create_database()
