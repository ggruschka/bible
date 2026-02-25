# CLAUDE.md

## Project Overview

A SQLite database of the **Sagrada Biblia** (Straubinger Catholic Bible, Corrección 28). Goal: extract all verses, footnotes, headings, and cross-references from the source PDF into a clean relational database.

## Source Material

- `Biblia Straubinger v28.pdf` — 4,381-page source PDF (gitignored, on disk)
- `data/sword/SpaPlatense.zip` — SWORD module of the 1948 Straubinger text (gitignored, reference for validation)

## Database Schema: `db/schema.sql`

73-book Catholic canon. 13 tables.

Key design decisions:
- **Multi-Bible ready**: `verse` has `bible_id` FK. `bible` table tracks editions (Straubinger, KJV, Reina Valera, etc.) with `parent_id` for revisions and language derivatives.
- **LXX psalm numbering** (Straubinger uses Septuagint). `psalm_number_map` converts Hebrew↔LXX.
- **Address-based linking**: Cross-references, footnotes, and section headings all use `(book_id, chapter, verse)` tuples — shared across all Bibles.
- **Denormalized verse table**: `book_id` and `chapter_number` on `verse` avoid joins for common queries.
- **Shared footnotes**: Footnotes reference verses by address, not by Bible-specific `verse_id`. Straubinger's footnotes are visible when reading any Bible.
- **FTS5 full-text search** (diacritics-insensitive, `unicode61 remove_diacritics 2`)

## Scripts

```bash
python3 scripts/create_db.py        # Schema + seed 73 books, 1334 chapters, psalm maps
python3 scripts/validate_db.py      # Run DB integrity checks
python3 scripts/query_bible.py      # Query utilities
```

## No External Dependencies

Pure Python 3 stdlib (sqlite3, re, csv, json, urllib, zipfile). No pip packages needed.

## Previous Approach (abandoned)

Used Marker (GPU PDF-to-markdown converter) to generate markdown, then a 4-stage parsing pipeline. Abandoned because the markdown conversion has inherent corruption: interleaved verse/footnote text from PDF page breaks, truncated verses, stray markers. This corruption is not deterministically fixable by parsing.
