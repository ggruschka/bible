# Sagrada Biblia Database

A ready-to-use SQLite database of the **Sagrada Biblia** by Mons. Dr. Juan Straubinger (Corrección 28, January 2024) — the full 73-book Catholic canon with verse-level granularity, footnotes, cross-references, and full-text search.

## What's in the database

| Content | Count |
|---------|-------|
| Books | 73 (full Catholic canon incl. deuterocanonicals) |
| Chapters | 1,334 |
| Verses | 35,677 |
| Footnotes | 11,886 (linked to verses) |
| Cross-references | 344,795 |
| Section headings | 14,035 |

The database file `bible.db` is included in the repo. No build step needed — just query it.

## Schema

14 tables organized into four groups:

```
testament ─1:N─ book ─1:N─ chapter ─1:N─ verse ─N:1─ translation
                 │                    │     │
                 │        section_heading   verse_footnote ─N:1─ footnote
                 │
                 ├── cross_reference (address-based)
                 │
                 └── commentary (address-based)
```

See [`db/schema.sql`](db/schema.sql) for the full DDL.

## Query examples

### Python

```python
from scripts.query_bible import *

# Verse lookup — accepts OSIS, slug, abbreviation, name, or numeric id
get_verse('Gen', 1, 1)           # OSIS id
get_verse('genesis', 1, 1)       # slug
get_verse('Gn', 1, 1)            # Spanish abbreviation
get_verse('Génesis', 1, 1)       # Spanish name
get_verse(1, 1, 1)               # numeric book id
# → {'text': 'Al principio creó Dios el cielo y la tierra.', ...}

# Full chapter
get_chapter('Ps', 22)  # LXX Psalm 22 = Hebrew Psalm 23
# → [{'verse': 1, 'text': 'Salmo de David. Yahvé es mi pastor, nada me faltará.'}, ...]

# Full-text search (diacritics-insensitive)
search_text('bienaventurado')
# → [{'ref': 'Prov 8:34', 'snippet': '>>>Bienaventurado<<< el hombre que me oye...'}, ...]

# Footnotes
get_footnotes('Gen', 1, 1)
# → [{'id': 1, 'text': 'Al principio, es decir, cuando no existía aún nada...'}]

# Cross-references (sorted by community votes)
get_cross_refs('John', 3, 16, limit=5)
# → [{'ref': 'Rm 5:8', 'votes': 949}, {'ref': '1 Jn 4:9', 'votes': 674}, ...]
```

### Raw SQL

```sql
-- Get Juan 3:16
SELECT text_clean FROM verse v
JOIN book b ON v.book_id = b.id
WHERE b.slug = 'juan' AND v.chapter_number = 3 AND v.verse_number = 16;

-- Full-text search
SELECT b.abbrev_es, f.chapter_number, f.verse_number,
       snippet(verse_fts, 0, '>>>', '<<<', '...', 20)
FROM verse_fts f
JOIN book b ON f.book_id = b.id
WHERE text_clean MATCH 'misericordia';

-- Cross-references for a verse
SELECT tb.abbrev_es, cr.target_chapter, cr.target_verse, cr.votes
FROM cross_reference cr
JOIN book sb ON cr.source_book_id = sb.id
JOIN book tb ON cr.target_book_id = tb.id
WHERE sb.slug = 'genesis' AND cr.source_chapter = 1 AND cr.source_verse = 1
ORDER BY cr.votes DESC LIMIT 10;
```

## Psalm Numbering

The Straubinger edition uses **LXX/Vulgate numbering** (standard for Catholic Bibles). The `psalm_number_map` table maps between LXX and Hebrew numbering systems:

| LXX (Straubinger) | Hebrew (Protestant) | Content |
|---|---|---|
| Psalm 22 | Psalm 23 | "The Lord is my shepherd" |
| Psalm 50 | Psalm 51 | Miserere |
| Psalm 9 (a+b) | Psalms 9–10 | Merged in LXX |
| Psalm 113 (a+b) | Psalms 114–115 | Merged in LXX |

Cross-references from Protestant datasets are automatically converted to LXX numbering during import.

## Multi-Translation Support

The schema supports additional translations via the `translation` table. Cross-references are address-based and work across all translations without duplication.

```sql
-- Add a new translation
INSERT INTO translation (name, full_name, language, canon, method)
VALUES ('Straubinger-EN', 'Straubinger English Translation', 'en', 'catholic', 'ai');

-- Add translated verses
INSERT INTO verse (translation_id, chapter_id, book_id, chapter_number, verse_number, text_clean)
VALUES (2, 1, 1, 1, 1, 'In the beginning God created heaven and earth.');

-- Query all translations of a verse
SELECT t.name, v.text_clean
FROM verse v JOIN translation t ON v.translation_id = t.id
WHERE v.book_id = 1 AND v.chapter_number = 1 AND v.verse_number = 1;
```

## Project Structure

```
bible/
├── bible.db               # The database (ready to use)
├── db/
│   └── schema.sql         # Full DDL (14 tables)
└── scripts/
    ├── query_bible.py     # Query utilities
    ├── validate_db.py     # Database validation
    ├── create_db.py       # Build: schema + seed data
    ├── parse_verses.py    # Build: verse extraction
    ├── parse_footnotes.py # Build: footnote parsing
    └── import_crossrefs.py# Build: cross-reference import
```

## Data Sources

| Source | Records | License |
|--------|---------|---------|
| Sagrada Biblia - Mons. Dr. Juan Straubinger (Corrección 28) | 35,677 verses, 11,886 footnotes | Public domain (published 1948–1951) |
| [OpenBible.info Cross-References](https://www.openbible.info/labs/cross-references/) | 344,795 cross-refs | CC-BY |

## Requirements

- Python 3.8+ (only needed for `scripts/query_bible.py`)
- SQLite 3.35+ (for FTS5 support)
- No external Python dependencies
