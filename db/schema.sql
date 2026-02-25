-- Sagrada Biblia Relational Database Schema
-- Supports multiple Bibles, cross-references, footnotes, and commentary

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ─── Core Tables ───

CREATE TABLE IF NOT EXISTS testament (
    id          INTEGER PRIMARY KEY,
    name_es     TEXT NOT NULL UNIQUE,
    name_en     TEXT NOT NULL UNIQUE,
    abbrev      TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS bible (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    full_name   TEXT,
    language    TEXT NOT NULL,          -- ISO 639-1
    canon       TEXT NOT NULL DEFAULT 'catholic',  -- catholic/protestant/orthodox
    parent_id   INTEGER REFERENCES bible(id),  -- NULL if original; points to parent for revisions or language translations
    method      TEXT NOT NULL DEFAULT 'human',        -- human/ai/hybrid
    year        INTEGER,
    license     TEXT,
    description TEXT
);

CREATE TABLE IF NOT EXISTS book (
    id              INTEGER PRIMARY KEY,  -- canonical order 1–73
    testament_id    INTEGER NOT NULL REFERENCES testament(id),
    name_es         TEXT NOT NULL UNIQUE,
    name_en         TEXT,
    abbrev_es       TEXT,
    abbrev_en       TEXT,
    slug            TEXT NOT NULL UNIQUE,
    osis_id         TEXT NOT NULL UNIQUE,
    total_chapters  INTEGER,
    category        TEXT,  -- pentateuch/history/wisdom/prophecy/gospel/epistle/apocalypse
    alt_name_es     TEXT
);

CREATE TABLE IF NOT EXISTS chapter (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    book_id     INTEGER NOT NULL REFERENCES book(id),
    number      INTEGER NOT NULL,
    UNIQUE(book_id, number)
);

CREATE TABLE IF NOT EXISTS verse (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bible_id        INTEGER NOT NULL REFERENCES bible(id),
    chapter_id      INTEGER NOT NULL REFERENCES chapter(id),
    book_id         INTEGER NOT NULL REFERENCES book(id),
    chapter_number  INTEGER NOT NULL,
    verse_number    INTEGER NOT NULL,
    text_clean      TEXT,      -- plain text, footnote refs stripped
    UNIQUE(bible_id, book_id, chapter_number, verse_number)
);

CREATE INDEX IF NOT EXISTS idx_verse_book_chapter ON verse(book_id, chapter_number);
CREATE INDEX IF NOT EXISTS idx_verse_bible ON verse(bible_id);

-- FTS5 virtual table for full-text search
CREATE VIRTUAL TABLE IF NOT EXISTS verse_fts USING fts5(
    text_clean,
    bible_id UNINDEXED,
    book_id UNINDEXED,
    chapter_number UNINDEXED,
    verse_number UNINDEXED,
    verse_id UNINDEXED,
    tokenize = 'unicode61 remove_diacritics 2'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS verse_ai AFTER INSERT ON verse BEGIN
    INSERT INTO verse_fts(text_clean, bible_id, book_id, chapter_number, verse_number, verse_id)
    VALUES (NEW.text_clean, NEW.bible_id, NEW.book_id, NEW.chapter_number, NEW.verse_number, NEW.id);
END;

CREATE TRIGGER IF NOT EXISTS verse_ad AFTER DELETE ON verse BEGIN
    DELETE FROM verse_fts WHERE verse_id = OLD.id;
END;

CREATE TRIGGER IF NOT EXISTS verse_au AFTER UPDATE ON verse BEGIN
    DELETE FROM verse_fts WHERE verse_id = OLD.id;
    INSERT INTO verse_fts(text_clean, bible_id, book_id, chapter_number, verse_number, verse_id)
    VALUES (NEW.text_clean, NEW.bible_id, NEW.book_id, NEW.chapter_number, NEW.verse_number, NEW.id);
END;

-- ─── Cross-Reference Tables ───

CREATE TABLE IF NOT EXISTS cross_reference (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_book_id  INTEGER NOT NULL REFERENCES book(id),
    source_chapter  INTEGER NOT NULL,
    source_verse    INTEGER NOT NULL,
    target_book_id  INTEGER NOT NULL REFERENCES book(id),
    target_chapter  INTEGER NOT NULL,
    target_verse    INTEGER NOT NULL,
    votes           INTEGER,
    source_dataset  TEXT NOT NULL DEFAULT 'openbible',
    relation_type   TEXT,  -- parallel/quotation/allusion/prophecy_fulfillment
    UNIQUE(source_book_id, source_chapter, source_verse,
           target_book_id, target_chapter, target_verse, source_dataset)
);

CREATE INDEX IF NOT EXISTS idx_xref_source ON cross_reference(source_book_id, source_chapter, source_verse);
CREATE INDEX IF NOT EXISTS idx_xref_target ON cross_reference(target_book_id, target_chapter, target_verse);

CREATE TABLE IF NOT EXISTS osis_book_map (
    osis_id     TEXT PRIMARY KEY,
    book_id     INTEGER REFERENCES book(id)  -- NULL if not in Catholic canon
);

CREATE TABLE IF NOT EXISTS psalm_number_map (
    lxx_number      TEXT PRIMARY KEY,     -- "9a", "9b", "22", "113a", etc.
    hebrew_number   INTEGER NOT NULL,
    lxx_display     TEXT                  -- "Salmo 9 a", "Salmo 22 (23)"
);

-- ─── Commentary & Footnote Tables ───

CREATE TABLE IF NOT EXISTS commentary_source (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    full_name   TEXT,
    description TEXT,
    language    TEXT,   -- ISO 639-1
    year        INTEGER,
    license     TEXT
);

CREATE TABLE IF NOT EXISTS footnote (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       INTEGER NOT NULL REFERENCES commentary_source(id),
    book_id         INTEGER NOT NULL REFERENCES book(id),
    chapter_number  INTEGER NOT NULL,
    verse_start     INTEGER NOT NULL,
    verse_end       INTEGER,          -- NULL if single verse
    text            TEXT NOT NULL,
    UNIQUE(source_id, book_id, chapter_number, verse_start)
);

CREATE INDEX IF NOT EXISTS idx_footnote_verse
    ON footnote(book_id, chapter_number, verse_start);

CREATE TABLE IF NOT EXISTS commentary (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       INTEGER NOT NULL REFERENCES commentary_source(id),
    bible_id        INTEGER REFERENCES bible(id),  -- NULL = Bible-independent
    book_id         INTEGER NOT NULL REFERENCES book(id),
    chapter_start   INTEGER NOT NULL,
    verse_start     INTEGER NOT NULL,
    chapter_end     INTEGER,   -- NULL = same as start
    verse_end       INTEGER,   -- NULL = single verse
    title           TEXT,
    text            TEXT,
    text_plain      TEXT,
    topic           TEXT
);

CREATE INDEX IF NOT EXISTS idx_commentary_verse ON commentary(book_id, chapter_start, verse_start);

-- ─── Metadata Tables ───

CREATE TABLE IF NOT EXISTS section_heading (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    book_id         INTEGER NOT NULL REFERENCES book(id),
    chapter_number  INTEGER NOT NULL,
    before_verse    INTEGER NOT NULL,
    heading_text    TEXT NOT NULL,
    heading_style   TEXT  -- title/caps/outline
);

CREATE INDEX IF NOT EXISTS idx_heading_chapter
    ON section_heading(book_id, chapter_number);

CREATE TABLE IF NOT EXISTS import_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    step            TEXT NOT NULL,
    status          TEXT NOT NULL,  -- started/completed/failed
    records         INTEGER,
    message         TEXT,
    started_at      TEXT,
    finished_at     TEXT
);
