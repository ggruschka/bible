# CLAUDE.md

## Project Overview

A **multi-Bible** SQLite database. The schema supports N Bibles — KJV, Reina Valera, Orthodox, Straubinger, etc. Every design decision must account for this. Do NOT think only in terms of Straubinger.

The immediate goal is importing the Sagrada Biblia (Straubinger, Corrección 28) from its source PDF, but this is just the first Bible in a system designed for many.

## Key Concepts

### Bible vs Translation
The table is called `bible`, not `translation`. "Bible" = a specific edition/work (KJV, Straubinger, Reina Valera). "Translation" would imply converting to another language, which is a different operation. A Spanish KJV would be a new `bible` row with `language='es'` and `parent_id` pointing to the English KJV.

### Footnotes vs Commentaries
- **Footnotes** come FROM a specific Bible (e.g., Straubinger's inline notes). They target verses by address `(book_id, chapter, verse)` and are shared — visible when reading ANY Bible.
- **Commentaries** come from external works (e.g., Catena Aurea by St. Thomas Aquinas). They enrich the Bible text but are not tied to any specific Bible. Future feature.

### Address-Based Linking
Cross-references, footnotes, and section headings all use `(book_id, chapter, verse)` tuples — NOT Bible-specific `verse_id`. This means all annotations are shared across all Bibles without duplication.

## Source Material

- `Biblia Straubinger v28.pdf` — 4,381-page source PDF (gitignored, on disk). The target for extraction.
- `data/sword/SpaPlatense.zip` — SWORD module of the 1948 Straubinger text (gitignored). Already imported as reference. NOT a replacement for v28 — user wants the exact Corrección 28 text.

## Current State

- SWORD SpaPlatense imported as "Straubinger (SWORD)" — 35,791 verses
- 344,795 cross-references from OpenBible.info
- FTS5 full-text search working
- BGE-M3 dual-mode embeddings: context-aware (late chunking) + context-free (independent)
- Dual backend: Qdrant server (default) + sqlite-vec (fallback, requires --backend sqlite)
- Semantic search with hybrid scoring (dense + sparse → RRF fusion → ColBERT rescore)
- No footnotes, commentaries, or section headings imported yet
- PDF extraction approach for v28 still TBD

## Database Schema: `db/schema.sql`

78 books (73 Catholic canon + 5 appendiceal for multi-Bible support). 24 tables in schema.sql (+ 3 sqlite-vec virtual tables created at runtime by `embed_verses.py`). Optionally, 3 Qdrant collections (`verses_ctx`, `verses_noctx`, `chapters`) for vector search via Qdrant server.

Key design decisions:
- **Multi-Bible ready**: `verse` has `bible_id` FK. `bible` entries are created by import scripts, not pre-seeded.
- **78-book superset**: Book table covers Catholic, Protestant, and Orthodox canons. Appendiceal books (Prayer of Manasseh, 1-2 Esdras, Psalm 151, Laodiceans) are included for Bibles that use them.
- **LXX psalm numbering**: `psalm_number_map` converts Hebrew↔LXX for interop with Protestant data sources.
- **Address-based linking**: Footnotes, cross-references, section headings use `(book_id, chapter, verse)` — shared across all Bibles.
- **Denormalized verse table**: `book_id` and `chapter_number` on `verse` avoid joins for common queries.
- **FTS5 full-text search**: diacritics-insensitive (`unicode61 remove_diacritics 2`).

## Scripts

```bash
python3 scripts/create_db.py        # Schema + seed 78 books, chapters, psalm maps (no Bible entries)
python3 scripts/import_sword.py     # Import SWORD SpaPlatense Bible (requires pysword)
python3 scripts/import_crossrefs.py # Import cross-references from OpenBible.info
python3 scripts/embed_verses.py     # BGE-M3 embeddings (--backend sqlite|qdrant, requires GPU, optional deps)
python3 scripts/validate_db.py      # Run DB integrity checks
python3 scripts/query_bible.py      # Query utilities
python3 scripts/semantic_search.py  # Semantic search CLI demo (requires embeddings)
```

## No External Dependencies

Pure Python 3 stdlib (sqlite3, re, csv, json, urllib, zipfile). No pip packages needed for core functionality. `pysword` is required only for the SWORD import script.

### Optional: Embedding & Semantic Search

`pip install -r requirements-embeddings.txt` installs FlagEmbedding + sqlite-vec + qdrant-client. Requires GPU (CUDA) for reasonable embedding performance. Used by `embed_verses.py` and `semantic_search.py`.

### Optional: Qdrant Server

Qdrant provides an alternative vector search backend (real Rust engine, HNSW indexes, on-disk storage). Requires Docker:

```bash
docker run -d --name qdrant -p 6333:6333 -v $(pwd)/qdrant_data:/qdrant/storage qdrant/qdrant:latest
python scripts/embed_verses.py --backend qdrant --force
```

Both backends coexist. Use `--backend sqlite|qdrant` on CLI scripts, or `backend='sqlite'|'qdrant'` on public API functions. Default is `qdrant`.

## Previous Approach (abandoned)

Used Marker (GPU PDF-to-markdown converter) to generate markdown, then a 4-stage parsing pipeline. Abandoned because the markdown conversion has inherent corruption: interleaved verse/footnote text from PDF page breaks, truncated verses, stray markers. This corruption is not deterministically fixable by parsing.
