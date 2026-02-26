#!/usr/bin/env python3
"""
Embed Bible verses using BGE-M3 with late chunking.

Instead of embedding each verse independently, feeds entire chapters to the
encoder so every token attends to the full chapter context, then extracts
per-verse embeddings from the hidden states.

Produces three embedding types per verse:
  - Dense: 1024-dim L2-normalized vector (stored in sqlite-vec virtual table)
  - Sparse: learned token weights via sparse_linear head (stored as JSON)
  - ColBERT: per-token 1024-dim embeddings (stored as float16 blob)

Plus chapter-level dense vectors (length-weighted mean of verse vectors).

Usage:
    python scripts/embed_verses.py [--bible-id N] [--device cuda|cpu] [--force]

Requires: pip install -r requirements-embeddings.txt
          Also: torch, transformers (installed by FlagEmbedding)
"""

import argparse
import json
import os
import sqlite3
import struct
import sys
import time

import numpy as np
import torch
from transformers import AutoModel, AutoTokenizer
from huggingface_hub import hf_hub_download

# ─── Constants ───

MODEL_NAME = 'BAAI/bge-m3'
MAX_TOKENS = 8192
OVERLAP_TOKENS = 200
BATCH_SIZE = 1000  # DB transaction batch size
DENSE_DIM = 1024

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'bible.db')


# ─── Model Loading ───

def load_model(device):
    """Load BGE-M3 base encoder + projection heads."""
    print(f"Loading {MODEL_NAME} on {device}...")
    t0 = time.time()

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModel.from_pretrained(MODEL_NAME)

    if device == 'cuda':
        model = model.half().to(device)
    else:
        model = model.to(device)
    model.eval()

    # Load projection heads from HuggingFace repo
    sparse_path = hf_hub_download(MODEL_NAME, 'sparse_linear.pt')
    colbert_path = hf_hub_download(MODEL_NAME, 'colbert_linear.pt')

    sparse_linear = torch.nn.Linear(in_features=DENSE_DIM, out_features=1)
    sparse_linear.load_state_dict(torch.load(sparse_path, map_location=device, weights_only=True))
    if device == 'cuda':
        sparse_linear = sparse_linear.half()
    sparse_linear = sparse_linear.to(device)
    sparse_linear.eval()

    colbert_linear = torch.nn.Linear(in_features=DENSE_DIM, out_features=DENSE_DIM)
    colbert_linear.load_state_dict(torch.load(colbert_path, map_location=device, weights_only=True))
    if device == 'cuda':
        colbert_linear = colbert_linear.half()
    colbert_linear = colbert_linear.to(device)
    colbert_linear.eval()

    print(f"  Model loaded in {time.time() - t0:.1f}s")
    return model, tokenizer, sparse_linear, colbert_linear


# ─── Late Chunking ───

def tokenize_verses(tokenizer, verses):
    """Pre-tokenize each verse separately and return token IDs + boundaries.

    Args:
        tokenizer: HuggingFace tokenizer
        verses: list of (verse_id, text) tuples

    Returns:
        all_token_ids: flat list of token IDs (no special tokens)
        boundaries: list of (start, end) index pairs into all_token_ids
    """
    all_token_ids = []
    boundaries = []

    for _, text in verses:
        tokens = tokenizer.encode(text, add_special_tokens=False)
        start = len(all_token_ids)
        all_token_ids.extend(tokens)
        boundaries.append((start, start + len(tokens)))

    return all_token_ids, boundaries


def split_chapter_tokens(all_token_ids, boundaries, max_tokens=MAX_TOKENS, overlap=OVERLAP_TOKENS):
    """Split chapter tokens into chunks that fit within max_tokens.

    Each chunk gets [CLS] + tokens + [SEP], so usable space is max_tokens - 2.

    Returns list of (chunk_token_ids, chunk_boundaries, verse_indices) tuples.
    chunk_boundaries are adjusted relative to the hidden state positions (after [CLS]).
    verse_indices maps each boundary to the original verse index.
    """
    usable = max_tokens - 2  # space for [CLS] and [SEP]

    if len(all_token_ids) <= usable:
        # Everything fits in one chunk
        # Boundaries shift by 1 for [CLS] token
        shifted = [(s + 1, e + 1) for s, e in boundaries]
        return [(all_token_ids, shifted, list(range(len(boundaries))))]

    # Need to split at verse boundaries
    chunks = []
    verse_idx = 0
    n_verses = len(boundaries)

    while verse_idx < n_verses:
        chunk_start_verse = verse_idx
        token_offset = boundaries[verse_idx][0]  # token index in all_token_ids

        # Find how many verses fit
        chunk_end_verse = verse_idx
        while chunk_end_verse < n_verses:
            verse_end_token = boundaries[chunk_end_verse][1]
            if verse_end_token - token_offset > usable:
                break
            chunk_end_verse += 1

        # Ensure at least one verse per chunk
        if chunk_end_verse == chunk_start_verse:
            chunk_end_verse = chunk_start_verse + 1

        # Extract tokens for this chunk
        t_start = boundaries[chunk_start_verse][0]
        t_end = boundaries[min(chunk_end_verse, n_verses) - 1][1]
        chunk_tokens = all_token_ids[t_start:t_end]

        # Build boundaries relative to hidden states (shift by 1 for [CLS])
        chunk_bounds = []
        verse_idxs = []
        for vi in range(chunk_start_verse, min(chunk_end_verse, n_verses)):
            s, e = boundaries[vi]
            chunk_bounds.append((s - t_start + 1, e - t_start + 1))
            verse_idxs.append(vi)

        chunks.append((chunk_tokens, chunk_bounds, verse_idxs))

        # Advance, adding overlap by backing up a few verses
        if chunk_end_verse >= n_verses:
            break

        # Find overlap start: go back from chunk_end_verse until we have ~overlap tokens
        overlap_verse = chunk_end_verse
        overlap_accumulated = 0
        while overlap_verse > chunk_start_verse and overlap_accumulated < overlap:
            overlap_verse -= 1
            vs, ve = boundaries[overlap_verse]
            overlap_accumulated += ve - vs

        verse_idx = overlap_verse if overlap_verse > chunk_start_verse else chunk_end_verse

    return chunks


def select_best_verse_embedding(verse_idx, chunks, chunk_results):
    """For a verse that appears in multiple chunks (overlap), pick the best one.

    Prefers the chunk where the verse is most central (furthest from edges).
    """
    best_chunk = None
    best_centrality = -1

    for ci, (_, chunk_bounds, verse_idxs) in enumerate(chunks):
        if verse_idx in verse_idxs:
            local_idx = verse_idxs.index(verse_idx)
            n = len(verse_idxs)
            # Centrality: distance from nearest edge, normalized
            centrality = min(local_idx, n - 1 - local_idx) / max(n - 1, 1)
            if centrality > best_centrality:
                best_centrality = centrality
                best_chunk = ci

    return best_chunk


@torch.no_grad()
def encode_chapter(model, tokenizer, sparse_linear, colbert_linear, verses, device):
    """Encode a chapter of verses using late chunking.

    Args:
        model: BGE-M3 base encoder
        tokenizer: tokenizer
        sparse_linear: sparse projection head
        colbert_linear: ColBERT projection head
        verses: list of (verse_id, text) tuples
        device: 'cuda' or 'cpu'

    Returns:
        verse_embeddings: list of dicts with keys:
            'verse_id', 'dense', 'sparse', 'colbert'
    """
    all_token_ids, boundaries = tokenize_verses(tokenizer, verses)
    chunks = split_chapter_tokens(all_token_ids, boundaries)

    # Process each chunk
    chunk_results = []
    for chunk_tokens, chunk_bounds, verse_idxs in chunks:
        # Build input: [CLS] + tokens + [SEP]
        cls_id = tokenizer.cls_token_id
        sep_id = tokenizer.sep_token_id
        input_ids = [cls_id] + chunk_tokens + [sep_id]

        input_tensor = torch.tensor([input_ids], dtype=torch.long, device=device)
        attention_mask = torch.ones_like(input_tensor)

        outputs = model(input_ids=input_tensor, attention_mask=attention_mask)
        hidden = outputs.last_hidden_state[0]  # (seq_len, 1024)

        # Apply projection heads
        sparse_out = torch.relu(sparse_linear(hidden)).squeeze(-1)  # (seq_len,)
        colbert_out = colbert_linear(hidden)  # (seq_len, 1024)
        colbert_out = torch.nn.functional.normalize(colbert_out, p=2, dim=-1)

        chunk_results.append((hidden, sparse_out, colbert_out, input_ids))

    # Extract per-verse embeddings
    results = []
    # Track which verse indices we've processed (for overlap dedup)
    processed = set()

    for vi, (verse_id, _) in enumerate(verses):
        if vi in processed:
            continue
        processed.add(vi)

        # Find best chunk for this verse
        ci = select_best_verse_embedding(vi, chunks, chunk_results) if len(chunks) > 1 else 0
        chunk_tokens, chunk_bounds, verse_idxs = chunks[ci]
        hidden, sparse_out, colbert_out, input_ids = chunk_results[ci]

        local_idx = verse_idxs.index(vi)
        start, end = chunk_bounds[local_idx]

        if start >= end:
            # Empty verse (shouldn't happen, but be safe)
            continue

        # Dense: mean-pool + L2-normalize
        verse_hidden = hidden[start:end]
        dense = verse_hidden.mean(dim=0)
        dense = torch.nn.functional.normalize(dense, p=2, dim=0)
        dense_np = dense.float().cpu().numpy()

        # Sparse: max weight per token_id within verse range
        verse_sparse = sparse_out[start:end]
        verse_token_ids = input_ids[start:end]
        sparse_weights = {}
        for ti, tid in enumerate(verse_token_ids):
            w = verse_sparse[ti].item()
            if w > 0:
                tid_str = str(tid)
                if tid_str not in sparse_weights or w > sparse_weights[tid_str]:
                    sparse_weights[tid_str] = round(w, 4)

        # ColBERT: per-token embeddings as float16
        verse_colbert = colbert_out[start:end]
        colbert_np = verse_colbert.half().cpu().numpy()

        results.append({
            'verse_id': verse_id,
            'dense': dense_np,
            'sparse': sparse_weights,
            'colbert': colbert_np,
            'num_tokens': end - start,
        })

    return results


# ─── Context-Free Encoding ───

@torch.no_grad()
def encode_batch_nocontext(model, tokenizer, sparse_linear, colbert_linear, verses, device, batch_size=64):
    """Encode verses independently (no chapter context).

    Args:
        verses: list of (verse_id, text) tuples
        batch_size: number of verses per forward pass

    Returns:
        list of dicts with 'verse_id', 'dense', 'sparse', 'colbert', 'num_tokens'
    """
    results = []

    for i in range(0, len(verses), batch_size):
        batch = verses[i:i+batch_size]
        texts = [text for _, text in batch]

        inputs = tokenizer(
            texts, padding=True, truncation=True,
            max_length=512, return_tensors='pt'
        ).to(device)

        outputs = model(
            input_ids=inputs['input_ids'],
            attention_mask=inputs['attention_mask'],
        )
        hidden = outputs.last_hidden_state  # (batch, seq_len, 1024)
        attention_mask = inputs['attention_mask']  # (batch, seq_len)

        # Apply projection heads to all hidden states at once
        sparse_out = torch.relu(sparse_linear(hidden)).squeeze(-1)  # (batch, seq_len)
        colbert_out = colbert_linear(hidden)  # (batch, seq_len, 1024)
        colbert_out = torch.nn.functional.normalize(colbert_out, p=2, dim=-1)

        for j in range(len(batch)):
            verse_id = batch[j][0]
            mask = attention_mask[j]  # (seq_len,)
            token_count = mask.sum().item()

            # Real tokens: skip CLS (pos 0) and SEP (last real token)
            if token_count <= 2:
                continue
            start = 1
            end = token_count - 1

            # Dense: mean-pool real tokens, L2-normalize
            verse_hidden = hidden[j, start:end]
            dense = verse_hidden.mean(dim=0)
            dense = torch.nn.functional.normalize(dense, p=2, dim=0)
            dense_np = dense.float().cpu().numpy()

            # Sparse: max weight per token_id
            verse_sparse = sparse_out[j, start:end]
            verse_token_ids = inputs['input_ids'][j, start:end]
            sparse_weights = {}
            for ti in range(end - start):
                w = verse_sparse[ti].item()
                if w > 0:
                    tid_str = str(verse_token_ids[ti].item())
                    if tid_str not in sparse_weights or w > sparse_weights[tid_str]:
                        sparse_weights[tid_str] = round(w, 4)

            # ColBERT: per-token embeddings as float16
            verse_colbert = colbert_out[j, start:end]
            colbert_np = verse_colbert.half().cpu().numpy()

            results.append({
                'verse_id': verse_id,
                'dense': dense_np,
                'sparse': sparse_weights,
                'colbert': colbert_np,
                'num_tokens': end - start,
            })

    return results


# ─── sqlite-vec helpers ───

def serialize_float32(vec):
    """Serialize a numpy float32 vector for sqlite-vec."""
    return struct.pack(f'{len(vec)}f', *vec.tolist())


def load_sqlite_vec(conn):
    """Load sqlite-vec extension."""
    import sqlite_vec
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    conn.enable_load_extension(False)


def create_vec_tables(conn):
    """Create sqlite-vec virtual tables if they don't exist."""
    conn.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS verse_vec USING vec0(
            verse_id INTEGER PRIMARY KEY,
            embedding float[{DENSE_DIM}] distance_metric=cosine
        )
    """)
    conn.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS chapter_vec USING vec0(
            chapter_id INTEGER PRIMARY KEY,
            embedding float[{DENSE_DIM}] distance_metric=cosine
        )
    """)
    conn.execute(f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS verse_vec_noctx USING vec0(
            verse_id INTEGER PRIMARY KEY,
            embedding float[{DENSE_DIM}] distance_metric=cosine
        )
    """)


def create_regular_tables(conn):
    """Create embedding storage tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS verse_sparse (
            verse_id    INTEGER PRIMARY KEY REFERENCES verse(id),
            weights     TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS verse_colbert (
            verse_id          INTEGER PRIMARY KEY REFERENCES verse(id),
            num_tokens        INTEGER NOT NULL,
            token_embeddings  BLOB NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS verse_sparse_noctx (
            verse_id    INTEGER PRIMARY KEY REFERENCES verse(id),
            weights     TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS verse_colbert_noctx (
            verse_id          INTEGER PRIMARY KEY REFERENCES verse(id),
            num_tokens        INTEGER NOT NULL,
            token_embeddings  BLOB NOT NULL
        )
    """)


# ─── Main Pipeline ───

def get_bible_id(conn, bible_id):
    """Resolve bible_id: use provided, or auto-detect (Bible with most verses)."""
    if bible_id is not None:
        row = conn.execute("SELECT id, name FROM bible WHERE id=?", (bible_id,)).fetchone()
        if not row:
            print(f"Error: Bible id {bible_id} not found.")
            sys.exit(1)
        return row
    row = conn.execute(
        "SELECT b.id, b.name FROM bible b "
        "JOIN verse v ON v.bible_id = b.id "
        "GROUP BY b.id ORDER BY COUNT(*) DESC LIMIT 1"
    ).fetchone()
    if not row:
        print("Error: No Bible with verses found.")
        sys.exit(1)
    return row


def get_existing_verse_ids(conn, verse_ids, table='verse_sparse'):
    """Check which verse_ids already have embeddings."""
    existing = set()
    for i in range(0, len(verse_ids), 500):
        batch = verse_ids[i:i+500]
        placeholders = ','.join('?' * len(batch))
        rows = conn.execute(
            f"SELECT verse_id FROM {table} WHERE verse_id IN ({placeholders})",
            batch
        ).fetchall()
        existing.update(r[0] for r in rows)
    return existing


def main():
    parser = argparse.ArgumentParser(description='Embed Bible verses using BGE-M3')
    parser.add_argument('--bible-id', type=int, default=None, help='Bible ID to embed')
    parser.add_argument('--device', choices=['cuda', 'cpu'], default=None, help='Force device')
    parser.add_argument('--force', action='store_true', help='Drop and recreate embeddings')
    args = parser.parse_args()

    # Device selection
    if args.device:
        device = args.device
    elif torch.cuda.is_available():
        device = 'cuda'
    else:
        device = 'cpu'
        print("WARNING: CUDA not available, falling back to CPU (fp32, much slower)")

    if device == 'cuda':
        gpu_name = torch.cuda.get_device_name(0)
        gpu_mem = torch.cuda.get_device_properties(0).total_memory / (1024**3)
        print(f"Using GPU: {gpu_name} ({gpu_mem:.0f} GB)")

    # Connect to DB
    db_path = os.path.abspath(DB_PATH)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")

    bible_id, bible_name = get_bible_id(conn, args.bible_id)
    print(f"Embedding Bible: {bible_name} (id={bible_id})")

    # Log start
    conn.execute(
        "INSERT INTO import_log (step, status, message, started_at) VALUES (?, ?, ?, datetime('now'))",
        ('embed_verses', 'started', f'BGE-M3 embeddings for bible_id={bible_id}')
    )
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    # Load sqlite-vec and create tables
    load_sqlite_vec(conn)
    create_vec_tables(conn)
    create_regular_tables(conn)

    # Load all verses grouped by chapter
    rows = conn.execute("""
        SELECT v.id, v.book_id, v.chapter_number, v.verse_number, v.text_clean,
               v.chapter_id
        FROM verse v
        WHERE v.bible_id = ?
        ORDER BY v.book_id, v.chapter_number, v.verse_number
    """, (bible_id,)).fetchall()

    if not rows:
        print("No verses found.")
        conn.close()
        return

    # Group by chapter
    chapters = {}
    for row in rows:
        vid, book_id, ch_num, v_num, text, ch_id = row
        key = (book_id, ch_num, ch_id)
        if key not in chapters:
            chapters[key] = []
        chapters[key].append((vid, text or ''))

    total_verses = len(rows)
    total_chapters = len(chapters)
    print(f"Found {total_verses} verses in {total_chapters} chapters")

    # Check existing embeddings
    all_verse_ids = [r[0] for r in rows]
    if args.force:
        print("Force mode: clearing existing embeddings...")
        # Delete context-aware
        for vid in all_verse_ids:
            conn.execute("DELETE FROM verse_vec WHERE verse_id=?", (vid,))
            conn.execute("DELETE FROM verse_vec_noctx WHERE verse_id=?", (vid,))
        conn.execute(
            "DELETE FROM verse_sparse WHERE verse_id IN "
            "(SELECT id FROM verse WHERE bible_id=?)", (bible_id,)
        )
        conn.execute(
            "DELETE FROM verse_colbert WHERE verse_id IN "
            "(SELECT id FROM verse WHERE bible_id=?)", (bible_id,)
        )
        # Delete context-free
        conn.execute(
            "DELETE FROM verse_sparse_noctx WHERE verse_id IN "
            "(SELECT id FROM verse WHERE bible_id=?)", (bible_id,)
        )
        conn.execute(
            "DELETE FROM verse_colbert_noctx WHERE verse_id IN "
            "(SELECT id FROM verse WHERE bible_id=?)", (bible_id,)
        )
        # Delete chapter vectors
        conn.execute(
            "DELETE FROM chapter_vec WHERE chapter_id IN "
            "(SELECT DISTINCT chapter_id FROM verse WHERE bible_id=?)", (bible_id,)
        )
        conn.commit()
        existing = set()
    else:
        existing = get_existing_verse_ids(conn, all_verse_ids)
        if existing:
            print(f"  {len(existing)} context-aware embeddings exist, will skip")

    # Load model
    model, tokenizer, sparse_linear, colbert_linear = load_model(device)

    # Process chapters
    t_start = time.time()
    embedded_count = 0
    chapter_vectors = []  # (chapter_id, dense_vec)

    # Batch accumulators for DB inserts
    vec_batch = []
    sparse_batch = []
    colbert_batch = []

    chapter_items = list(chapters.items())
    for ci, ((book_id, ch_num, ch_id), verses) in enumerate(chapter_items):
        # Filter out already-embedded verses
        if not args.force:
            verses_to_embed = [(vid, text) for vid, text in verses if vid not in existing]
            all_in_chapter = verses  # need all for chapter vector even if some exist
        else:
            verses_to_embed = verses
            all_in_chapter = verses

        if not verses_to_embed and not args.force:
            # Still need chapter vector if not yet computed
            # Skip entirely if all verses already done
            if ci % 200 == 0:
                print(f"  Chapter {ci+1}/{total_chapters} (skipped)")
            continue

        # Encode chapter (always encode full chapter for context)
        verse_results = encode_chapter(
            model, tokenizer, sparse_linear, colbert_linear,
            all_in_chapter, device
        )

        # Separate: verse embeddings + chapter vector
        verse_dense_vecs = []
        verse_lengths = []
        for vr in verse_results:
            verse_dense_vecs.append(vr['dense'])
            verse_lengths.append(vr['num_tokens'])

            # Only store if this verse needs embedding
            if vr['verse_id'] in existing:
                continue

            vec_batch.append((vr['verse_id'], serialize_float32(vr['dense'])))
            sparse_batch.append((vr['verse_id'], json.dumps(vr['sparse'])))
            colbert_batch.append((
                vr['verse_id'],
                vr['num_tokens'],
                vr['colbert'].tobytes()
            ))
            embedded_count += 1

        # Chapter dense = length-weighted mean of verse dense vectors
        if verse_dense_vecs:
            weights = np.array(verse_lengths, dtype=np.float32)
            weights /= weights.sum()
            chapter_dense = np.zeros(DENSE_DIM, dtype=np.float32)
            for w, v in zip(weights, verse_dense_vecs):
                chapter_dense += w * v
            # L2-normalize
            norm = np.linalg.norm(chapter_dense)
            if norm > 0:
                chapter_dense /= norm
            chapter_vectors.append((ch_id, chapter_dense))

        # Flush batches
        if len(vec_batch) >= BATCH_SIZE:
            _flush_batches(conn, vec_batch, sparse_batch, colbert_batch)
            vec_batch.clear()
            sparse_batch.clear()
            colbert_batch.clear()

        if (ci + 1) % 100 == 0:
            elapsed = time.time() - t_start
            rate = (ci + 1) / elapsed
            eta = (total_chapters - ci - 1) / rate
            print(f"  Chapter {ci+1}/{total_chapters} — "
                  f"{embedded_count} verses embedded — "
                  f"{rate:.1f} ch/s — ETA {eta:.0f}s")

    # Final flush
    if vec_batch:
        _flush_batches(conn, vec_batch, sparse_batch, colbert_batch)

    # Insert chapter vectors
    print(f"Inserting {len(chapter_vectors)} chapter vectors...")
    for ch_id, ch_vec in chapter_vectors:
        conn.execute(
            "INSERT OR REPLACE INTO chapter_vec (chapter_id, embedding) VALUES (?, ?)",
            (ch_id, serialize_float32(ch_vec))
        )
    conn.commit()

    ctx_elapsed = time.time() - t_start

    # Update log for context-aware pass
    conn.execute(
        "UPDATE import_log SET status=?, records=?, message=?, finished_at=datetime('now') WHERE id=?",
        ('completed', embedded_count,
         f'BGE-M3 context-aware: {embedded_count} verses, {len(chapter_vectors)} chapters in {ctx_elapsed:.1f}s',
         log_id)
    )
    conn.commit()

    if embedded_count > 0:
        print(f"\n  Context-aware pass: {embedded_count} verses, "
              f"{len(chapter_vectors)} chapters in {ctx_elapsed:.1f}s")
    else:
        print(f"\n  Context-aware pass: skipped (already embedded)")

    # ─── Context-Free Pass ───
    print(f"\n{'='*60}")
    print("Context-free pass (independent verse encoding)...")

    # Check existing noctx embeddings
    existing_noctx = get_existing_verse_ids(conn, all_verse_ids, table='verse_sparse_noctx')
    if existing_noctx and not args.force:
        print(f"  {len(existing_noctx)} context-free embeddings exist, will skip")

    verses_for_noctx = [(vid, text) for vid, text in
                        [(r[0], r[4] or '') for r in rows]
                        if vid not in existing_noctx]

    if not verses_for_noctx:
        print("  All verses already have context-free embeddings.")
        noctx_count = 0
    else:
        # Log start
        conn.execute(
            "INSERT INTO import_log (step, status, message, started_at) VALUES (?, ?, ?, datetime('now'))",
            ('embed_verses_noctx', 'started', f'BGE-M3 context-free for bible_id={bible_id}')
        )
        noctx_log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.commit()

        t_noctx = time.time()
        noctx_count = 0
        vec_batch = []
        sparse_batch = []
        colbert_batch = []

        # Process in batches of 64 verses
        encode_batch_size = 64
        for bi in range(0, len(verses_for_noctx), encode_batch_size):
            batch = verses_for_noctx[bi:bi+encode_batch_size]
            batch_results = encode_batch_nocontext(
                model, tokenizer, sparse_linear, colbert_linear,
                batch, device, batch_size=encode_batch_size
            )

            for vr in batch_results:
                vec_batch.append((vr['verse_id'], serialize_float32(vr['dense'])))
                sparse_batch.append((vr['verse_id'], json.dumps(vr['sparse'])))
                colbert_batch.append((
                    vr['verse_id'],
                    vr['num_tokens'],
                    vr['colbert'].tobytes()
                ))
                noctx_count += 1

            if len(vec_batch) >= BATCH_SIZE:
                _flush_batches_noctx(conn, vec_batch, sparse_batch, colbert_batch)
                vec_batch.clear()
                sparse_batch.clear()
                colbert_batch.clear()

            total_batches = (len(verses_for_noctx) + encode_batch_size - 1) // encode_batch_size
            current_batch = bi // encode_batch_size + 1
            if current_batch % 50 == 0:
                elapsed = time.time() - t_noctx
                rate = noctx_count / elapsed if elapsed > 0 else 0
                eta = (len(verses_for_noctx) - noctx_count) / rate if rate > 0 else 0
                print(f"  Batch {current_batch}/{total_batches} — "
                      f"{noctx_count} verses — "
                      f"{rate:.0f} v/s — ETA {eta:.0f}s")

        # Final flush
        if vec_batch:
            _flush_batches_noctx(conn, vec_batch, sparse_batch, colbert_batch)

        noctx_elapsed = time.time() - t_noctx

        conn.execute(
            "UPDATE import_log SET status=?, records=?, message=?, finished_at=datetime('now') WHERE id=?",
            ('completed', noctx_count,
             f'BGE-M3 context-free: {noctx_count} verses in {noctx_elapsed:.1f}s',
             noctx_log_id)
        )
        conn.commit()

        print(f"\n  Context-free pass: {noctx_count} verses in {noctx_elapsed:.1f}s")

    # Summary
    total_elapsed = time.time() - t_start
    db_size_mb = os.path.getsize(db_path) / (1024 * 1024)

    print(f"\n{'='*60}")
    print(f"Done!")
    print(f"  Context-aware:  {embedded_count} verses, {len(chapter_vectors)} chapters")
    print(f"  Context-free:   {len(verses_for_noctx)} verses")
    print(f"  Total time:     {total_elapsed:.1f}s")
    print(f"  DB size:        {db_size_mb:.1f} MB")

    conn.close()


def _flush_batches(conn, vec_batch, sparse_batch, colbert_batch):
    """Insert accumulated context-aware embeddings into the database."""
    for vid, emb in vec_batch:
        conn.execute(
            "INSERT OR REPLACE INTO verse_vec (verse_id, embedding) VALUES (?, ?)",
            (vid, emb)
        )
    conn.executemany(
        "INSERT OR REPLACE INTO verse_sparse (verse_id, weights) VALUES (?, ?)",
        sparse_batch
    )
    conn.executemany(
        "INSERT OR REPLACE INTO verse_colbert (verse_id, num_tokens, token_embeddings) VALUES (?, ?, ?)",
        colbert_batch
    )
    conn.commit()


def _flush_batches_noctx(conn, vec_batch, sparse_batch, colbert_batch):
    """Insert accumulated context-free embeddings into the database."""
    for vid, emb in vec_batch:
        conn.execute(
            "INSERT OR REPLACE INTO verse_vec_noctx (verse_id, embedding) VALUES (?, ?)",
            (vid, emb)
        )
    conn.executemany(
        "INSERT OR REPLACE INTO verse_sparse_noctx (verse_id, weights) VALUES (?, ?)",
        sparse_batch
    )
    conn.executemany(
        "INSERT OR REPLACE INTO verse_colbert_noctx (verse_id, num_tokens, token_embeddings) VALUES (?, ?, ?)",
        colbert_batch
    )
    conn.commit()


if __name__ == '__main__':
    main()
