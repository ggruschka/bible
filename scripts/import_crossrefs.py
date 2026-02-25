#!/usr/bin/env python3
"""
Step 4: Import cross-references from OpenBible.info.

Downloads and parses the cross-reference dataset (~340K entries),
maps OSIS book IDs to our book table, converts Protestant psalm numbering
(Hebrew) to LXX numbering, and inserts into cross_reference table.

Source: https://a.openbible.info/data/cross-references.zip (CC-BY license)
"""

import csv
import io
import os
import re
import sqlite3
import urllib.request
import zipfile
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'bible.db')
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
XREF_URL = 'https://a.openbible.info/data/cross-references.zip'
XREF_FILE = os.path.join(DATA_DIR, 'cross_references.txt')

SOURCE_DATASET = 'openbible'


def download_crossrefs():
    """Download cross-references if not already cached."""
    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(XREF_FILE):
        print(f"Using cached cross-references: {XREF_FILE}")
        return

    zip_path = os.path.join(DATA_DIR, 'cross-references.zip')
    print(f"Downloading cross-references from {XREF_URL}...")
    urllib.request.urlretrieve(XREF_URL, zip_path)

    print("Extracting...")
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Find the TSV file inside
        names = zf.namelist()
        tsv_name = [n for n in names if n.endswith('.txt') or n.endswith('.tsv')][0]
        with zf.open(tsv_name) as src, open(XREF_FILE, 'wb') as dst:
            dst.write(src.read())

    os.remove(zip_path)
    print(f"Saved to {XREF_FILE}")


def parse_osis_ref(ref):
    """Parse an OSIS reference like 'Gen.1.1' into (osis_book, chapter, verse).

    Also handles ranges like 'Gen.1.1-Gen.1.5' (returns start only).
    """
    # Take the first part of a range
    ref = ref.split('-')[0].strip()
    parts = ref.split('.')
    if len(parts) != 3:
        return None
    return parts[0], int(parts[1]), int(parts[2])


def hebrew_to_lxx_psalm(hebrew_num):
    """Convert Hebrew psalm numbering to LXX (Straubinger) numbering.

    Hebrew → LXX mapping:
    1-8 → 1-8 (same)
    9-10 → 9 (merged as 9a/9b in LXX)
    11-113 → 10-112 (shifted by 1)
    114-115 → 113 (merged as 113a/113b in LXX)
    116:1-9 → 114 (split in LXX)
    116:10-19 → 115 (split in LXX)
    117-146 → 116-145 (shifted by 1)
    147:1-11 → 146 (split in LXX)
    147:12-20 → 147 (split in LXX)
    148-150 → 148-150 (same)
    """
    if hebrew_num <= 8:
        return hebrew_num
    elif hebrew_num <= 10:
        return 9  # 9-10 → 9
    elif hebrew_num <= 113:
        return hebrew_num - 1  # 11→10, 12→11, ..., 113→112
    elif hebrew_num <= 115:
        return 113  # 114-115 → 113
    elif hebrew_num == 116:
        return 114  # simplified — 116 maps to 114/115
    elif hebrew_num <= 146:
        return hebrew_num - 1  # 117→116, ..., 146→145
    elif hebrew_num == 147:
        return 146  # simplified — 147 maps to 146/147
    else:
        return hebrew_num  # 148-150 same


def import_crossrefs():
    """Import cross-references into the database."""
    db_path = os.path.abspath(DB_PATH)

    # Download data
    download_crossrefs()

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # Log start
    started = datetime.now().isoformat()
    cur.execute(
        "INSERT INTO import_log (step, status, started_at) VALUES (?, ?, ?)",
        ('import_crossrefs', 'started', started)
    )
    log_id = cur.lastrowid
    conn.commit()

    # Build OSIS → book_id map
    osis_map = {}
    for row in cur.execute("SELECT osis_id, book_id FROM osis_book_map"):
        if row[1] is not None:
            osis_map[row[0]] = row[1]

    # Get psalm book_id
    psalm_book_id = cur.execute("SELECT id FROM book WHERE slug='salmo'").fetchone()[0]

    # Build Hebrew→LXX psalm map from our table
    psalm_map = {}
    for row in cur.execute("SELECT lxx_number, hebrew_number FROM psalm_number_map"):
        lxx = row[0]
        hebrew = row[1]
        # lxx_number can be "9a", "9b", "22", etc.
        # For cross-ref mapping, we just need the numeric part
        lxx_num = int(re.match(r'(\d+)', lxx).group(1))
        if hebrew not in psalm_map:
            psalm_map[hebrew] = lxx_num

    # Parse and import
    inserted = 0
    skipped_no_book = 0
    skipped_dup = 0
    total_lines = 0

    with open(XREF_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('From'):
                continue

            total_lines += 1
            parts = line.split('\t')
            if len(parts) < 2:
                continue

            source_ref = parts[0].strip()
            target_ref = parts[1].strip()
            votes = int(parts[2]) if len(parts) > 2 else None

            # Parse source
            src = parse_osis_ref(source_ref)
            if not src:
                continue
            src_osis, src_ch, src_v = src

            # Parse target
            tgt = parse_osis_ref(target_ref)
            if not tgt:
                continue
            tgt_osis, tgt_ch, tgt_v = tgt

            # Map OSIS book IDs to our book_id
            src_book_id = osis_map.get(src_osis)
            tgt_book_id = osis_map.get(tgt_osis)

            if src_book_id is None or tgt_book_id is None:
                skipped_no_book += 1
                continue

            # Convert psalm numbering (Hebrew → LXX)
            if src_book_id == psalm_book_id:
                src_ch = psalm_map.get(src_ch, hebrew_to_lxx_psalm(src_ch))
            if tgt_book_id == psalm_book_id:
                tgt_ch = psalm_map.get(tgt_ch, hebrew_to_lxx_psalm(tgt_ch))

            # Insert
            try:
                cur.execute(
                    """INSERT OR IGNORE INTO cross_reference
                       (source_book_id, source_chapter, source_verse,
                        target_book_id, target_chapter, target_verse,
                        votes, source_dataset)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (src_book_id, src_ch, src_v,
                     tgt_book_id, tgt_ch, tgt_v,
                     votes, SOURCE_DATASET)
                )
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped_dup += 1
            except sqlite3.IntegrityError:
                skipped_dup += 1

            if inserted % 50000 == 0 and inserted > 0:
                conn.commit()
                print(f"  {inserted} cross-references imported...")

    conn.commit()

    # Log completion
    finished = datetime.now().isoformat()
    cur.execute(
        """UPDATE import_log SET status=?, records=?, message=?, finished_at=?
           WHERE id=?""",
        ('completed', inserted,
         f'{total_lines} lines, {inserted} imported, {skipped_no_book} skipped (no book), {skipped_dup} skipped (dup)',
         finished, log_id)
    )
    conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print(f"Cross-reference import complete!")
    print(f"  Total lines parsed:     {total_lines}")
    print(f"  Cross-refs imported:    {inserted}")
    print(f"  Skipped (no book):      {skipped_no_book}")
    print(f"  Skipped (duplicate):    {skipped_dup}")
    print(f"{'='*60}")


if __name__ == '__main__':
    import_crossrefs()
