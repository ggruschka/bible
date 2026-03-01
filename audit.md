# Codebase Audit Report

**Project**: Sagrada Biblia (multi-Bible SQLite database)
**Date**: 2026-03-01
**Audited by**: Claude (deep-code-audit skill)
**Scope**: Full codebase audit (8 Python files, 1 SQL schema, ~4,650 lines)

---

## Summary

| Severity | Count |
|----------|-------|
| Critical | 1     |
| High     | 3     |
| Medium   | 5     |
| Low      | 8     |
| **Total**| **17**|

**Key Findings**: The codebase is clean and well-structured overall. The most significant issue is an XSS vulnerability in the Streamlit app where database text is rendered as raw HTML. Resource leaks from unclosed database connections on exception paths and overly broad exception handling are the main code quality concerns. Documentation has a stale default-backend claim that contradicts the code.

---

## Tech Stack & Scope

- **Languages**: Python 3.8+, SQL
- **Frameworks**: Streamlit, FlagEmbedding (BGE-M3), Qdrant, sqlite-vec
- **Total files scanned**: 10 (8 Python + 1 SQL schema + 1 Streamlit app)
- **Directories skipped**: `.git/`, `__pycache__/`, `qdrant_data/`, `data/`, `.claude/`
- **Codebase health**: Good. Pure stdlib for core, clean separation of concerns, no dead code worth mentioning. Main gaps are defensive coding patterns and documentation completeness.

---

## Critical Findings

### C-1: XSS via unsafe HTML rendering of database text
- **File**: `app.py:326`
- **Type**: Security (XSS)
- **Description**: Verse text from the database is directly concatenated into an HTML string and rendered with `st.markdown(chapter_html, unsafe_allow_html=True)`. The text is not HTML-escaped before insertion.
- **Impact**: If a Bible import ever contains malicious content (e.g., `<script>` tags in verse text), it would execute in user browsers. Attack surface is limited since data comes from controlled imports (SWORD modules), but the pattern is unsafe.
- **Suggested Fix**:
  ```python
  import html
  # In the verse rendering loop:
  text = html.escape(v['text'])
  text_parts.append(f"{sup} {text}{badges} ")
  ```

---

## High Findings

### H-1: Unclosed database connections on exception paths
- **File**: `app.py:58`, `app.py:76`, `app.py:146`, `scripts/query_bible.py:78`, `scripts/query_bible.py:103`
- **Type**: Resource Leak
- **Description**: Database connections are opened with `get_conn()` and closed explicitly at the end, but if an exception occurs between open and close, the connection leaks. This pattern exists in `load_books()`, `get_verse_count()`, `parse_ref()`, and all functions in `query_bible.py`.
- **Impact**: Connection pool exhaustion under sustained errors. SQLite handles this better than most databases (auto-closes on GC), but the pattern is brittle.
- **Suggested Fix**: Use try/finally or a context manager:
  ```python
  def get_verse(book, chapter, verse, bible_id=None):
      conn = get_conn()
      try:
          ...
      finally:
          conn.close()
  ```

### H-2: Overly broad exception handling masks real errors
- **File**: `scripts/semantic_search.py:71`, `scripts/import_sword.py:118`, `scripts/validate_db.py:166`
- **Type**: Error Handling
- **Description**: Multiple `except Exception:` and `except (ImportError, Exception):` clauses catch all exceptions indiscriminately. In `import_sword.py:118`, a bare `except Exception` around SWORD verse reading silently drops verses on any error — not just missing data but also real bugs.
- **Impact**: Silent data loss during imports. Real bugs masked as "not available" warnings. Difficult debugging.
- **Suggested Fix**: Catch specific exception types. For SWORD import, catch only `KeyError`/`IndexError` for missing data. For sqlite-vec loading, catch only `ImportError` and `sqlite3.OperationalError`.

### H-3: Unsafe int() conversion on external CSV data
- **File**: `scripts/import_crossrefs.py:63`, `scripts/import_crossrefs.py:158`
- **Type**: Input Validation
- **Description**: Chapter/verse numbers and vote counts from OpenBible.info CSV are converted with `int()` without error handling. If the CSV contains malformed data, the entire import crashes mid-transaction.
- **Impact**: Partial cross-reference import with no rollback. Script crash on malformed external data.
- **Suggested Fix**: Wrap in try/except ValueError, skip malformed rows with a warning.

---

## Medium Findings

### M-1: Non-thread-safe global caches in semantic_search.py
- **File**: `scripts/semantic_search.py:78-87`, `scripts/semantic_search.py:102-115`
- **Type**: Race Condition
- **Description**: `_qdrant_client` and `_model_cache` globals use a check-then-set pattern without synchronization. In Streamlit (which runs each request in a thread), two concurrent requests could both see `None` and create duplicate instances.
- **Impact**: Duplicate model loads (wasting ~10s + GPU memory) or duplicate Qdrant connections. Low probability but possible under concurrent use.
- **Suggested Fix**: Add `threading.Lock()` around the lazy initialization.

### M-2: Stale documentation — default backend claim contradicts code
- **File**: `CLAUDE.md:32`
- **Type**: Documentation
- **Description**: Line 32 says "Dual backend: sqlite-vec (default) + Qdrant server (optional)" but all code defaults to `backend='qdrant'` (semantic_search.py, embed_verses.py, app.py). Line 78 correctly says "Default is `qdrant`", contradicting line 32.
- **Impact**: Users following CLAUDE.md expect sqlite as default. Confusing for contributors.
- **Suggested Fix**: Change line 32 to "Dual backend: Qdrant server (default) + sqlite-vec (fallback)".

### M-3: Simplified psalm numbering loses verse-level accuracy
- **File**: `scripts/import_crossrefs.py:89-94`
- **Type**: Incomplete Implementation
- **Description**: `hebrew_to_lxx_psalm()` maps Hebrew Psalms 116 and 147 to single LXX numbers, but these each split into two LXX psalms. Hebrew 116:1-9 → LXX 114, Hebrew 116:10-19 → LXX 115. Without verse-level awareness, cross-references may point to the wrong LXX psalm.
- **Impact**: Some cross-references to Psalms 114-115 and 146-147 may be misassigned.
- **Suggested Fix**: Accept an optional `verse` parameter and use it to choose the correct LXX psalm for split cases.

### M-4: Unchecked attribute access on Qdrant sparse vector response
- **File**: `scripts/semantic_search.py:492-496`
- **Type**: Missing Validation
- **Description**: Code accesses `sv.indices` and `sv.values` on Qdrant sparse vector response without checking these attributes exist. If the response format changes or data is corrupted, this crashes with `AttributeError`.
- **Suggested Fix**: Add `hasattr()` checks before accessing sparse vector attributes.

### M-5: Missing QDRANT_URL environment variable documentation
- **File**: `scripts/semantic_search.py:39`, `scripts/embed_verses.py:50`
- **Type**: Documentation
- **Description**: `QDRANT_URL` is read from environment with `os.environ.get('QDRANT_URL', 'http://localhost:6333')` but not documented in README or CLAUDE.md.
- **Suggested Fix**: Add to README under Qdrant setup section.

---

## Low Findings

### L-1: Unused constant PROTESTANT_ONLY_OSIS
- **File**: `scripts/create_db.py:119`
- **Type**: Dead Code
- **Description**: `PROTESTANT_ONLY_OSIS = []` is defined but never referenced anywhere.
- **Suggested Fix**: Remove the line.

### L-2: Empty pass block in Streamlit layout
- **File**: `app.py:344`
- **Type**: Dead Code
- **Description**: `with col3: pass` serves no purpose.
- **Suggested Fix**: Remove the block.

### L-3: sys.path manipulation at module level
- **File**: `app.py:14`
- **Type**: Code Quality
- **Description**: `sys.path.insert(0, ...)` modifies the import path globally. Works but is fragile.
- **Suggested Fix**: Acceptable for a single-file app. No change needed unless the project grows.

### L-4: Stale default_bible_id cache
- **File**: `scripts/query_bible.py:14-34`
- **Type**: Code Quality
- **Description**: `_default_bid` is cached forever. If a new Bible is imported in the same process, the cache is stale.
- **Suggested Fix**: Acceptable for current use (scripts run once). No change needed.

### L-5: No type hints in codebase
- **File**: All Python files
- **Type**: Code Quality
- **Description**: Zero type annotations across ~4,650 lines. Not critical for a small project but hurts IDE support.
- **Suggested Fix**: Add return type hints to public API functions in `query_bible.py` and `semantic_search.py` as a start.

### L-6: print() used instead of logging module
- **File**: All scripts
- **Type**: Code Quality
- **Description**: All output uses `print()`. No log levels, no filtering capability.
- **Suggested Fix**: Acceptable for CLI scripts. Consider `logging` if the project grows.

### L-7: Incomplete docstrings on embed_verses.py helper functions
- **File**: `scripts/embed_verses.py` (multiple functions)
- **Type**: Documentation
- **Description**: ~18 helper functions in the embedding pipeline lack complete docstrings (e.g., `flush_qdrant()`, `make_verse_point()`, `create_qdrant_collections()`).
- **Suggested Fix**: Add docstrings to public-facing helpers. Internal helpers are fine without.

### L-8: README schema diagram is oversimplified
- **File**: `README.md:28-37`
- **Type**: Documentation
- **Description**: Schema diagram shows only the main tables but omits footnote, commentary, section_heading, and embedding tables (24 tables total).
- **Suggested Fix**: Add note: "Simplified — see db/schema.sql for full 24-table schema."

---

## Skipped / Out of Scope

- **qdrant_data/**: Binary vector storage, not source code
- **data/sword/**: Downloaded SWORD module ZIP, binary
- **Biblia Straubinger v28.pdf**: Source PDF, binary
- **bible.db**: Built locally by scripts, not reviewed as source
- **Dependency security**: FlagEmbedding, Qdrant, Streamlit package vulnerabilities not audited

---

## Recommended Fix Order

1. **Immediate** (Critical):
   - C-1: HTML-escape verse text before rendering with `unsafe_allow_html=True`

2. **This sprint** (High):
   - H-1: Add try/finally to all `get_conn()` usage in `query_bible.py` and `app.py`
   - H-2: Replace bare `except Exception:` with specific types in import scripts
   - H-3: Add try/except ValueError around int() conversions in `import_crossrefs.py`

3. **Next sprint** (Medium):
   - M-1: Add threading.Lock to global caches in `semantic_search.py`
   - M-2: Fix CLAUDE.md line 32 default backend claim
   - M-3: Enhance psalm mapping with verse-level awareness
   - M-4: Add hasattr checks for Qdrant sparse vector access
   - M-5: Document QDRANT_URL in README

4. **Backlog** (Low):
   - L-1, L-2: Remove dead code (2 lines)
   - L-5: Add type hints to public API
   - L-7, L-8: Documentation improvements

---

## Notes

- The codebase is well-organized with clean separation: scripts for pipeline operations, a query API layer, and a UI layer. No spaghetti code or tangled dependencies.
- The "SQL injection" finding in dynamic table names (`semantic_search.py:178`) was investigated and is a **false positive** — table names come from `_table_names()` which returns hardcoded strings, never user input.
- The sparse vector key int() conversion (`embed_verses.py:532`) was also downgraded — these keys come from BGE-M3 model output and are always numeric token IDs.
- The project has zero external dependencies for core functionality (pure stdlib). This is a strong security posture.
- All scripts are designed to be re-run safely (idempotent imports with --force flags).
