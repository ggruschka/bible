# Multi-Bible Database

A SQLite database designed to hold **multiple Bibles** with shared cross-references, footnotes, and full-text search. The schema supports any number of Bibles across different canons (Catholic, Protestant, Orthodox) and languages.

## Current Data

| Content | Count |
|---------|-------|
| Bibles | 1 (Straubinger 1948 via SWORD) |
| Books | 78 (73 Catholic + 5 appendiceal) |
| Chapters | 1,362 |
| Verses | 35,791 |
| Cross-references | 344,795 |

`bible.db` is not included in the repo (too large with embeddings). Build it locally:

```bash
python scripts/create_db.py          # Schema + seed data
python scripts/import_sword.py       # Import verses (requires pysword)
python scripts/import_crossrefs.py   # Import cross-references
python scripts/embed_verses.py       # Optional: embeddings (requires GPU)
```

## Schema

24 tables in [`db/schema.sql`](db/schema.sql), plus optional embedding storage:

```
testament ─1:N─ book ─1:N─ chapter ─1:N─ verse ─N:1─ bible
                 │
                 ├── footnote
                 ├── cross_reference
                 ├── commentary
                 ├── section_heading
                 ├── parallel_passage
                 └── topic_verse
```

**Embedding storage** (choose one or both backends):

| Backend | Tables / Collections | Notes |
|---------|---------------------|-------|
| **sqlite-vec** (default) | 3 virtual tables (`verse_vec`, `chapter_vec`, `verse_vec_noctx`) + 4 regular tables | In-process, no server needed |
| **Qdrant** (optional) | 3 collections (`verses_ctx`, `verses_noctx`, `chapters`) | Docker server, HNSW indexes, on-disk vectors |

Both backends coexist. Use `--backend sqlite|qdrant` to select.

All annotations (footnotes, cross-references, section headings, commentary) reference verses by address `(book_id, chapter, verse)` — not by Bible-specific verse ID. This means annotations are automatically shared across all Bibles.

See [`db/schema.sql`](db/schema.sql) for the full DDL.

## Query Examples

### Python

```python
from scripts.query_bible import *

# Verse lookup — accepts OSIS, slug, abbreviation, name, or numeric id
get_verse('Gen', 1, 1)           # OSIS id
get_verse('genesis', 1, 1)       # slug
get_verse('Gn', 1, 1)            # Spanish abbreviation
get_verse(1, 1, 1)               # numeric book id
# -> {'text': 'Al principio creó Dios el cielo y la tierra.', ...}

# Full chapter
get_chapter('Ps', 22)  # LXX Psalm 22 = Hebrew Psalm 23
# -> [{'verse': 1, 'text': 'Salmo de David. Yahvé es mi pastor, nada me faltará.'}, ...]

# Full-text search (diacritics-insensitive)
search_text('bienaventurado')
# -> [{'ref': 'Prov 8:34', 'snippet': '>>>Bienaventurado<<< el hombre que me oye...'}, ...]

# Footnotes for a verse (shared across all Bibles)
get_footnotes('Gen', 1, 1)

# Cross-references (sorted by community votes)
get_cross_refs('John', 3, 16, limit=5)
# -> [{'ref': 'Rm 5:8', 'votes': 949}, {'ref': '1 Jn 4:9', 'votes': 674}, ...]
```

### Raw SQL

```sql
-- Get Juan 3:16
SELECT v.text_clean FROM verse v
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

## Multi-Bible Support

Each Bible is a row in the `bible` table. Verses are scoped by `bible_id`, while annotations are shared by address.

```sql
-- Add a new Bible
INSERT INTO bible (name, full_name, language, canon, method)
VALUES ('KJV', 'King James Version', 'en', 'protestant', 'human');

-- Add verses for it
INSERT INTO verse (bible_id, chapter_id, book_id, chapter_number, verse_number, text_clean)
VALUES (2, 1, 1, 1, 1, 'In the beginning God created the heaven and the earth.');

-- Query all Bibles for a verse
SELECT bi.name, v.text_clean
FROM verse v JOIN bible bi ON v.bible_id = bi.id
WHERE v.book_id = 1 AND v.chapter_number = 1 AND v.verse_number = 1;
```

## Semantic Search

BGE-M3 embeddings enable meaning-based verse search with three retrieval modes combined via hybrid scoring:

| Mode | What it captures | Storage |
|------|-----------------|---------|
| **Dense** (1024-dim) | Overall semantic meaning | ~144 MB |
| **Sparse** (learned token weights) | Keyword importance (neural BM25) | ~7 MB |
| **ColBERT** (per-token embeddings) | Fine-grained token-level matching | ~2.2 GB |

**Dual embedding modes:**
- **Context-aware** (late chunking): entire chapters fed to encoder, verses attend to full chapter context. Better for thematic/chapter-level search.
- **Context-free**: each verse encoded independently. Better for verse-to-verse similarity and cross-reference discovery (wider similarity spread, less same-chapter bias).

### Setup

```bash
pip install -r requirements-embeddings.txt   # FlagEmbedding + sqlite-vec + qdrant-client

# sqlite-vec backend (default, in-process)
python scripts/embed_verses.py --backend sqlite  # ~250s on RTX 5090 (both modes)

# Qdrant backend (optional, requires Docker)
docker run -d --name qdrant -p 6333:6333 -v $(pwd)/qdrant_data:/qdrant/storage qdrant/qdrant:latest
python scripts/embed_verses.py --backend qdrant --force
```

### Usage

```python
from scripts.semantic_search import *

# Find verses similar to a given verse (no model needed — reads from DB)
find_similar('Gen', 1, 1, top_k=10)

# Context-free mode: better for verse-to-verse matching
find_similar('Gen', 1, 1, top_k=10, use_context=False, exclude_same_chapter=True)

# Same with Qdrant backend
find_similar('Gen', 1, 1, top_k=10, backend='qdrant')

# Search by meaning (loads model lazily)
search_meaning('el amor de Dios', top_k=10)

# Chapter-level search
find_similar_chapters('Gen', 1, top_k=5)

# Two-stage: chapters first, then verses within
hierarchical_search('la resurrección de los muertos')

# Discover novel cross-references not in OpenBible.info
discover_crossrefs('John', 3, 16, top_k=50, use_context=False, exclude_same_chapter=True)

# Evaluate against high-vote cross-references
evaluate_quality(sample_size=500)
evaluate_quality(sample_size=500, backend='qdrant')  # compare backends
```

## Psalm Numbering

The database uses **LXX/Vulgate numbering** (standard for Catholic Bibles). The `psalm_number_map` table maps between LXX and Hebrew numbering:

| LXX | Hebrew | Content |
|-----|--------|---------|
| Psalm 22 | Psalm 23 | "The Lord is my shepherd" |
| Psalm 50 | Psalm 51 | Miserere |
| Psalm 9 (a+b) | Psalms 9-10 | Merged in LXX |
| Psalm 113 (a+b) | Psalms 114-115 | Merged in LXX |

Cross-references from Protestant datasets are automatically converted to LXX numbering during import.

## Project Structure

```
bible/
├── bible.db                      # Built locally (gitignored)
├── db/
│   └── schema.sql                # Full DDL (24 tables)
├── requirements-embeddings.txt   # Optional deps for semantic search
└── scripts/
    ├── create_db.py              # Schema + seed reference data
    ├── import_sword.py           # Import a SWORD module (requires pysword)
    ├── import_crossrefs.py       # Import cross-references from OpenBible.info
    ├── embed_verses.py           # BGE-M3 embeddings (--backend sqlite|qdrant, GPU)
    ├── semantic_search.py        # Hybrid semantic search + cross-ref discovery (dual backend)
    ├── validate_db.py            # Database validation checks
    └── query_bible.py            # Query utilities
```

## Data Sources

| Source | Records | License |
|--------|---------|---------|
| SWORD SpaPlatense module (Straubinger 1948) | 35,791 verses | Public domain |
| [OpenBible.info Cross-References](https://www.openbible.info/labs/cross-references/) | 344,795 cross-refs | CC-BY |

## Requirements

- Python 3.8+ (only needed for scripts)
- SQLite 3.35+ (for FTS5 support)
- No external Python dependencies (except `pysword` for SWORD imports)
- Optional: `pip install -r requirements-embeddings.txt` for semantic search (GPU recommended)
- Optional: Docker for Qdrant backend (`docker run -d --name qdrant -p 6333:6333 qdrant/qdrant:latest`)
