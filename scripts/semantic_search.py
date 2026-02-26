#!/usr/bin/env python3
"""
Semantic search for the Bible database using BGE-M3 embeddings.

Supports hybrid search (dense + sparse + ColBERT), cross-reference discovery,
chapter-level search, hierarchical search, and quality evaluation.

Dense-only searches (find_similar, find_similar_chapters) work without loading
the model — they read embeddings directly from the database.

Model is loaded lazily only when encoding user query text.

Usage:
    python scripts/semantic_search.py   # runs CLI demo

Requires: pip install -r requirements-embeddings.txt
"""

import json
import os
import struct
import sys
import time

import numpy as np

# Import helpers from query_bible.py (same directory)
sys.path.insert(0, os.path.dirname(__file__))
from query_bible import resolve_book, get_conn, default_bible_id

# ─── Constants ───

DENSE_DIM = 1024
HYBRID_WEIGHTS = (0.4, 0.2, 0.4)  # dense, sparse, colbert


def _table_names(use_context):
    """Return (vec_table, sparse_table, colbert_table) based on context mode."""
    if use_context:
        return 'verse_vec', 'verse_sparse', 'verse_colbert'
    return 'verse_vec_noctx', 'verse_sparse_noctx', 'verse_colbert_noctx'


# ─── sqlite-vec helpers ───

def serialize_float32(vec):
    """Serialize a float32 vector for sqlite-vec."""
    if hasattr(vec, 'tolist'):
        vec = vec.tolist()
    return struct.pack(f'{len(vec)}f', *vec)


def load_sqlite_vec(conn):
    """Load sqlite-vec extension. Returns True if successful."""
    try:
        import sqlite_vec
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        return True
    except (ImportError, Exception) as e:
        print(f"Warning: sqlite-vec not available: {e}")
        return False


# ─── Lazy Model Loading ───

_model_cache = None


def _get_model():
    """Lazily load BGE-M3 for query encoding."""
    global _model_cache
    if _model_cache is None:
        from FlagEmbedding import BGEM3FlagModel
        import torch
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        use_fp16 = device == 'cuda'
        print(f"Loading BGE-M3 for query encoding ({device})...")
        _model_cache = BGEM3FlagModel('BAAI/bge-m3', use_fp16=use_fp16, devices=[device])
    return _model_cache


def encode_query(text):
    """Encode query text using BGE-M3. Returns (dense, sparse_weights, colbert_vecs)."""
    model = _get_model()
    output = model.encode(
        [text],
        return_dense=True,
        return_sparse=True,
        return_colbert_vecs=True,
    )
    dense = output['dense_vecs'][0]  # (1024,) float32
    sparse = output['lexical_weights'][0]  # dict {token_id_str: weight}
    colbert = output['colbert_vecs'][0]  # (n_tokens, 1024) float32

    # Normalize dense
    norm = np.linalg.norm(dense)
    if norm > 0:
        dense = dense / norm

    # Convert sparse keys to strings for consistency
    sparse_str = {str(k): float(v) for k, v in sparse.items()}

    return dense, sparse_str, colbert


# ─── Scoring Functions ───

def sparse_sim(q_weights, d_weights):
    """Compute sparse similarity between query and document weight dicts."""
    score = 0.0
    for t, qw in q_weights.items():
        if t in d_weights:
            score += qw * d_weights[t]
    return score


def colbert_maxsim(q_vecs, d_vecs):
    """Compute ColBERT MaxSim: sum of per-query-token max similarities."""
    # q_vecs: (q_tokens, 1024), d_vecs: (d_tokens, 1024)
    sims = q_vecs @ d_vecs.T  # (q_tokens, d_tokens)
    return float(np.sum(np.max(sims, axis=1)))


def normalize_scores(scores):
    """Min-max normalize a list of scores to [0, 1]."""
    if not scores:
        return scores
    mn = min(scores)
    mx = max(scores)
    rng = mx - mn
    if rng == 0:
        return [0.5] * len(scores)
    return [(s - mn) / rng for s in scores]


# ─── Core Search Functions ───

def _dense_knn_verse(conn, query_vec, top_k, bible_id=None, use_context=True):
    """Dense KNN search over verse vectors. Returns [(verse_id, distance), ...]."""
    vec_table = _table_names(use_context)[0]
    results = conn.execute(
        f"SELECT verse_id, distance FROM {vec_table} WHERE embedding MATCH ? AND k = ?",
        [serialize_float32(query_vec), top_k]
    ).fetchall()

    if bible_id is not None:
        verse_ids = [r[0] for r in results]
        if not verse_ids:
            return []
        placeholders = ','.join('?' * len(verse_ids))
        valid = set(r[0] for r in conn.execute(
            f"SELECT id FROM verse WHERE id IN ({placeholders}) AND bible_id=?",
            verse_ids + [bible_id]
        ).fetchall())
        results = [(vid, dist) for vid, dist in results if vid in valid]

    return results


def _dense_knn_chapter(conn, query_vec, top_k):
    """Dense KNN search over chapter_vec. Returns [(chapter_id, distance), ...]."""
    return conn.execute("""
        SELECT chapter_id, distance FROM chapter_vec
        WHERE embedding MATCH ? AND k = ?
    """, [serialize_float32(query_vec), top_k]).fetchall()


def _load_sparse(conn, verse_id, use_context=True):
    """Load sparse weights for a verse."""
    sparse_table = _table_names(use_context)[1]
    row = conn.execute(
        f"SELECT weights FROM {sparse_table} WHERE verse_id=?", (verse_id,)
    ).fetchone()
    if row:
        return json.loads(row[0])
    return {}


def _load_colbert(conn, verse_id, use_context=True):
    """Load ColBERT token embeddings for a verse as float32 numpy array."""
    colbert_table = _table_names(use_context)[2]
    row = conn.execute(
        f"SELECT num_tokens, token_embeddings FROM {colbert_table} WHERE verse_id=?",
        (verse_id,)
    ).fetchone()
    if row:
        num_tokens = row[0]
        blob = row[1]
        arr = np.frombuffer(blob, dtype=np.float16).reshape(num_tokens, DENSE_DIM)
        return arr.astype(np.float32)
    return None


def _get_verse_dense(conn, verse_id, use_context=True):
    """Read dense embedding for a verse."""
    vec_table = _table_names(use_context)[0]
    row = conn.execute(
        f"SELECT embedding FROM {vec_table} WHERE verse_id=?", (verse_id,)
    ).fetchone()
    if row:
        return np.frombuffer(row[0], dtype=np.float32)
    return None


def _verse_info(conn, verse_id):
    """Get verse reference info."""
    row = conn.execute("""
        SELECT b.abbrev_es, v.chapter_number, v.verse_number, v.text_clean,
               b.id, v.book_id
        FROM verse v JOIN book b ON v.book_id = b.id
        WHERE v.id = ?
    """, (verse_id,)).fetchone()
    if row:
        return {
            'verse_id': verse_id,
            'ref': f'{row[0]} {row[1]}:{row[2]}',
            'text': row[3],
            'book_id': row[4],
            'chapter': row[1],
            'verse': row[2],
        }
    return None


def _chapter_info(conn, chapter_id):
    """Get chapter reference info."""
    row = conn.execute("""
        SELECT b.abbrev_es, c.number, b.id
        FROM chapter c JOIN book b ON c.book_id = b.id
        WHERE c.id = ?
    """, (chapter_id,)).fetchone()
    if row:
        return {
            'chapter_id': chapter_id,
            'ref': f'{row[0]} {row[1]}',
            'book_id': row[2],
            'chapter': row[1],
        }
    return None


def _hybrid_rescore(conn, candidates, q_sparse, q_colbert, dense_weight=0.4, sparse_weight=0.2, colbert_weight=0.4, use_context=True):
    """Rescore dense candidates with sparse + ColBERT, return combined ranked results."""
    # candidates: [(verse_id, cosine_distance), ...]
    if not candidates:
        return []

    dense_scores = []
    sparse_scores = []
    colbert_scores = []
    verse_ids = []

    for vid, dist in candidates:
        verse_ids.append(vid)
        # Dense: convert distance to similarity
        dense_scores.append(1.0 - dist)

        # Sparse
        if q_sparse:
            d_sparse = _load_sparse(conn, vid, use_context)
            sparse_scores.append(sparse_sim(q_sparse, d_sparse))
        else:
            sparse_scores.append(0.0)

        # ColBERT
        if q_colbert is not None:
            d_colbert = _load_colbert(conn, vid, use_context)
            if d_colbert is not None:
                colbert_scores.append(colbert_maxsim(q_colbert, d_colbert))
            else:
                colbert_scores.append(0.0)
        else:
            colbert_scores.append(0.0)

    # Normalize each
    dense_norm = normalize_scores(dense_scores)
    sparse_norm = normalize_scores(sparse_scores)
    colbert_norm = normalize_scores(colbert_scores)

    # Combine
    combined = []
    for i, vid in enumerate(verse_ids):
        score = (dense_weight * dense_norm[i] +
                 sparse_weight * sparse_norm[i] +
                 colbert_weight * colbert_norm[i])
        combined.append((vid, score, dense_norm[i], sparse_norm[i], colbert_norm[i]))

    combined.sort(key=lambda x: x[1], reverse=True)
    return combined


# ─── Public API ───

def find_similar(book, chapter, verse, top_k=20, bible_id=None, exclude_same_chapter=False, use_context=True):
    """Find verses semantically similar to a given verse.

    Uses hybrid scoring: dense KNN → sparse + ColBERT rescore.
    Set use_context=False to use context-free embeddings (better for cross-ref discovery).
    Set exclude_same_chapter=True to filter out verses from the same chapter.
    """
    if bible_id is None:
        bible_id = default_bible_id()

    conn = get_conn()
    load_sqlite_vec(conn)

    book_id, _ = resolve_book(conn, book)
    if book_id is None:
        conn.close()
        return []

    # Get verse_id
    row = conn.execute(
        "SELECT id FROM verse WHERE book_id=? AND chapter_number=? AND verse_number=? AND bible_id=?",
        (book_id, chapter, verse, bible_id)
    ).fetchone()
    if not row:
        conn.close()
        return []
    verse_id = row[0]

    # Get dense embedding
    query_vec = _get_verse_dense(conn, verse_id, use_context)
    if query_vec is None:
        conn.close()
        return []

    # Dense KNN (fetch extra for bible_id filtering)
    candidates = _dense_knn_verse(conn, query_vec, top_k * 3, bible_id, use_context)

    # Load query sparse + colbert for rescoring
    q_sparse = _load_sparse(conn, verse_id, use_context)
    q_colbert = _load_colbert(conn, verse_id, use_context)

    # Hybrid rescore
    scored = _hybrid_rescore(conn, candidates, q_sparse, q_colbert, use_context=use_context)

    # Build results (exclude the query verse itself)
    results = []
    for vid, score, d, s, c in scored:
        if vid == verse_id:
            continue
        info = _verse_info(conn, vid)
        if info:
            if exclude_same_chapter and info['book_id'] == book_id and info['chapter'] == chapter:
                continue
            info['score'] = score
            info['scores'] = {'dense': d, 'sparse': s, 'colbert': c}
            results.append(info)
        if len(results) >= top_k:
            break

    conn.close()
    return results


def search_meaning(text, top_k=20, bible_id=None, use_context=True):
    """Search for verses by meaning using free text query.

    Encodes query with BGE-M3, then hybrid-scores candidates.
    Set use_context=False to search against context-free embeddings.
    """
    if bible_id is None:
        bible_id = default_bible_id()

    q_dense, q_sparse, q_colbert = encode_query(text)

    conn = get_conn()
    load_sqlite_vec(conn)

    candidates = _dense_knn_verse(conn, q_dense, top_k * 5, bible_id, use_context)
    scored = _hybrid_rescore(conn, candidates, q_sparse, q_colbert, use_context=use_context)

    results = []
    for vid, score, d, s, c in scored:
        info = _verse_info(conn, vid)
        if info:
            info['score'] = score
            info['scores'] = {'dense': d, 'sparse': s, 'colbert': c}
            results.append(info)
        if len(results) >= top_k:
            break

    conn.close()
    return results


def find_similar_chapters(book, chapter, top_k=10, bible_id=None):
    """Find chapters similar to a given chapter using dense vectors."""
    if bible_id is None:
        bible_id = default_bible_id()

    conn = get_conn()
    load_sqlite_vec(conn)

    book_id, _ = resolve_book(conn, book)
    if book_id is None:
        conn.close()
        return []

    # Get chapter_id
    row = conn.execute(
        "SELECT id FROM chapter WHERE book_id=? AND number=?",
        (book_id, chapter)
    ).fetchone()
    if not row:
        conn.close()
        return []
    chapter_id = row[0]

    # Get chapter embedding
    row = conn.execute(
        "SELECT embedding FROM chapter_vec WHERE chapter_id=?", (chapter_id,)
    ).fetchone()
    if not row:
        conn.close()
        return []
    query_vec = np.frombuffer(row[0], dtype=np.float32)

    # KNN
    candidates = _dense_knn_chapter(conn, query_vec, top_k + 1)

    results = []
    for ch_id, dist in candidates:
        if ch_id == chapter_id:
            continue
        info = _chapter_info(conn, ch_id)
        if info:
            info['similarity'] = 1.0 - dist
            results.append(info)
        if len(results) >= top_k:
            break

    conn.close()
    return results


def search_chapters(text, top_k=10):
    """Search for chapters by meaning using free text query."""
    q_dense, _, _ = encode_query(text)

    conn = get_conn()
    load_sqlite_vec(conn)

    candidates = _dense_knn_chapter(conn, q_dense, top_k)

    results = []
    for ch_id, dist in candidates:
        info = _chapter_info(conn, ch_id)
        if info:
            info['similarity'] = 1.0 - dist
            results.append(info)

    conn.close()
    return results


def hierarchical_search(text, top_chapters=5, top_verses=10, bible_id=None, use_context=True):
    """Two-stage search: find top chapters, then top verses within them."""
    if bible_id is None:
        bible_id = default_bible_id()

    q_dense, q_sparse, q_colbert = encode_query(text)

    conn = get_conn()
    load_sqlite_vec(conn)

    # Stage 1: chapter-level (always uses context-aware chapter_vec)
    ch_candidates = _dense_knn_chapter(conn, q_dense, top_chapters)

    results = []
    for ch_id, ch_dist in ch_candidates:
        ch_info = _chapter_info(conn, ch_id)
        if not ch_info:
            continue
        ch_info['similarity'] = 1.0 - ch_dist

        # Stage 2: get verses in this chapter
        verse_ids = conn.execute("""
            SELECT id FROM verse
            WHERE chapter_id=? AND bible_id=?
            ORDER BY verse_number
        """, (ch_id, bible_id)).fetchall()

        # Score each verse
        verse_scores = []
        for (vid,) in verse_ids:
            d_vec = _get_verse_dense(conn, vid, use_context)
            if d_vec is None:
                continue
            # Cosine similarity (both L2-normalized)
            sim = float(np.dot(q_dense, d_vec))
            verse_scores.append((vid, sim))

        verse_scores.sort(key=lambda x: x[1], reverse=True)

        # Top verses in this chapter
        ch_verses = []
        for vid, sim in verse_scores[:top_verses]:
            info = _verse_info(conn, vid)
            if info:
                info['score'] = sim
                ch_verses.append(info)

        ch_info['verses'] = ch_verses
        results.append(ch_info)

    conn.close()
    return results


def discover_crossrefs(book, chapter, verse, top_k=50, bible_id=None, exclude_same_chapter=False, use_context=True):
    """Find semantically similar verses that are NOT in existing cross-references.

    Returns novel connections that could be new cross-references.
    Set use_context=False for better verse-to-verse matching.
    Set exclude_same_chapter=True to filter out verses from the same chapter.
    """
    if bible_id is None:
        bible_id = default_bible_id()

    # Get all similar verses
    similar = find_similar(book, chapter, verse, top_k=top_k, bible_id=bible_id,
                           exclude_same_chapter=exclude_same_chapter,
                           use_context=use_context)
    if not similar:
        return []

    conn = get_conn()

    # Resolve source verse address
    book_id, _ = resolve_book(conn, book)
    if book_id is None:
        conn.close()
        return []

    # Get existing cross-refs (both directions)
    existing = set()
    rows = conn.execute("""
        SELECT target_book_id, target_chapter, target_verse
        FROM cross_reference
        WHERE source_book_id=? AND source_chapter=? AND source_verse=?
    """, (book_id, chapter, verse)).fetchall()
    for r in rows:
        existing.add((r[0], r[1], r[2]))

    # Also check reverse direction
    rows = conn.execute("""
        SELECT source_book_id, source_chapter, source_verse
        FROM cross_reference
        WHERE target_book_id=? AND target_chapter=? AND target_verse=?
    """, (book_id, chapter, verse)).fetchall()
    for r in rows:
        existing.add((r[0], r[1], r[2]))

    conn.close()

    # Filter out existing cross-refs
    novel = []
    for v in similar:
        addr = (v['book_id'], v['chapter'], v['verse'])
        v['is_novel'] = addr not in existing
        novel.append(v)

    return novel


def evaluate_quality(sample_size=500, bible_id=None, use_context=True):
    """Evaluate embedding quality against high-vote cross-references.

    Samples verses that have high-vote (>100) cross-references and measures
    how well semantic similarity recovers them.

    Returns dict with recall@10, recall@50, MRR.
    """
    if bible_id is None:
        bible_id = default_bible_id()

    conn = get_conn()
    load_sqlite_vec(conn)

    # Find verses with high-vote cross-refs
    rows = conn.execute("""
        SELECT DISTINCT cr.source_book_id, cr.source_chapter, cr.source_verse
        FROM cross_reference cr
        WHERE cr.votes > 100
        ORDER BY RANDOM()
        LIMIT ?
    """, (sample_size,)).fetchall()

    if not rows:
        print("No high-vote cross-references found for evaluation.")
        conn.close()
        return {}

    recall_at_10 = []
    recall_at_50 = []
    mrrs = []
    evaluated = 0

    for src_book_id, src_ch, src_v in rows:
        # Get source verse_id
        src_row = conn.execute(
            "SELECT id FROM verse WHERE book_id=? AND chapter_number=? AND verse_number=? AND bible_id=?",
            (src_book_id, src_ch, src_v, bible_id)
        ).fetchone()
        if not src_row:
            continue
        src_vid = src_row[0]

        # Get dense embedding
        q_vec = _get_verse_dense(conn, src_vid, use_context)
        if q_vec is None:
            continue

        # Get target cross-refs (high-vote)
        targets = conn.execute("""
            SELECT target_book_id, target_chapter, target_verse
            FROM cross_reference
            WHERE source_book_id=? AND source_chapter=? AND source_verse=?
                  AND votes > 100
        """, (src_book_id, src_ch, src_v)).fetchall()

        target_addrs = set()
        for t in targets:
            target_addrs.add((t[0], t[1], t[2]))

        if not target_addrs:
            continue

        # Dense KNN
        candidates = _dense_knn_verse(conn, q_vec, 55, bible_id, use_context)

        # Map candidates to addresses
        found_at_10 = 0
        found_at_50 = 0
        first_rank = None

        for rank, (vid, _) in enumerate(candidates):
            if vid == src_vid:
                continue
            info = conn.execute(
                "SELECT book_id, chapter_number, verse_number FROM verse WHERE id=?",
                (vid,)
            ).fetchone()
            if info:
                addr = (info[0], info[1], info[2])
                if addr in target_addrs:
                    if rank < 10:
                        found_at_10 += 1
                    if rank < 50:
                        found_at_50 += 1
                    if first_rank is None:
                        first_rank = rank + 1

        n_targets = len(target_addrs)
        recall_at_10.append(found_at_10 / n_targets)
        recall_at_50.append(found_at_50 / n_targets)
        mrrs.append(1.0 / first_rank if first_rank else 0.0)
        evaluated += 1

    conn.close()

    if evaluated == 0:
        return {}

    result = {
        'evaluated': evaluated,
        'recall@10': np.mean(recall_at_10),
        'recall@50': np.mean(recall_at_50),
        'MRR': np.mean(mrrs),
    }
    return result


# ─── CLI Demo ───

def _print_verse_results(results, label, show_novel=False):
    """Pretty-print verse search results."""
    print(f"\n{'─'*60}")
    print(f"  {label}")
    print(f"{'─'*60}")
    for i, v in enumerate(results):
        score_str = f" (score: {v['score']:.3f})" if 'score' in v else ""
        novel_str = ""
        if show_novel and 'is_novel' in v:
            novel_str = " [NEW]" if v['is_novel'] else " [existing xref]"
        print(f"  {i+1:2d}. {v['ref']}{score_str}{novel_str}")
        if v.get('text'):
            text = v['text'][:120] + ('...' if len(v.get('text', '')) > 120 else '')
            print(f"      {text}")


def _print_chapter_results(results, label):
    """Pretty-print chapter search results."""
    print(f"\n{'─'*60}")
    print(f"  {label}")
    print(f"{'─'*60}")
    for i, ch in enumerate(results):
        sim_str = f" (similarity: {ch['similarity']:.3f})" if 'similarity' in ch else ""
        print(f"  {i+1:2d}. {ch['ref']}{sim_str}")


if __name__ == '__main__':
    print("=" * 60)
    print("  Semantic Search Demo — BGE-M3 Hybrid")
    print("=" * 60)

    # Check embeddings exist
    conn = get_conn()
    has_vec = load_sqlite_vec(conn)
    if not has_vec:
        print("\nError: sqlite-vec extension not available.")
        print("Install: pip install sqlite-vec")
        sys.exit(1)

    # Check if verse_vec has data
    try:
        ctx_count = conn.execute("SELECT COUNT(*) FROM verse_vec").fetchone()[0]
    except Exception:
        ctx_count = 0
    try:
        noctx_count = conn.execute("SELECT COUNT(*) FROM verse_vec_noctx").fetchone()[0]
    except Exception:
        noctx_count = 0

    if ctx_count == 0 and noctx_count == 0:
        print("\nNo embeddings found. Run embed_verses.py first:")
        print("  python scripts/embed_verses.py")
        conn.close()
        sys.exit(1)

    print(f"\n  Context-aware:  {ctx_count} verse embeddings")
    print(f"  Context-free:   {noctx_count} verse embeddings\n")
    has_noctx = noctx_count > 0
    conn.close()

    # 1. Find similar verses (context-aware)
    print("\n1. find_similar('Gen', 1, 1, use_context=True)")
    results = find_similar('Gen', 1, 1, top_k=10, use_context=True)
    _print_verse_results(results, "Genesis 1:1 — context-aware (late-chunked)")

    # 2. Find similar verses (context-free) — compare
    if has_noctx:
        print("\n2. find_similar('Gen', 1, 1, use_context=False)")
        results = find_similar('Gen', 1, 1, top_k=10, use_context=False)
        _print_verse_results(results, "Genesis 1:1 — context-free (independent)")

    # 3. Semantic search
    print("\n3. search_meaning('el amor de Dios')")
    results = search_meaning('el amor de Dios', top_k=10)
    _print_verse_results(results, "Search: 'el amor de Dios'")

    # 4. Similar chapters
    print("\n4. find_similar_chapters('Gen', 1)")
    results = find_similar_chapters('Gen', 1, top_k=5)
    _print_chapter_results(results, "Chapters similar to Genesis 1")

    # 5. Discover cross-refs (context-free, exclude same chapter)
    if has_noctx:
        print("\n5. discover_crossrefs('Gen', 1, 1, use_context=False, exclude_same_chapter=True)")
        results = discover_crossrefs('Gen', 1, 1, top_k=20, use_context=False, exclude_same_chapter=True)
        _print_verse_results(results[:15], "Cross-ref discovery — context-free, cross-chapter", show_novel=True)
        novel_count = sum(1 for r in results if r.get('is_novel'))
        print(f"\n  Novel connections: {novel_count}/{len(results)}")

    # 6. Evaluate quality — compare both modes
    print("\n6. Evaluation comparison")
    for mode_name, use_ctx in [("context-aware", True), ("context-free", False)]:
        if not use_ctx and not has_noctx:
            continue
        t0 = time.time()
        metrics = evaluate_quality(sample_size=200, use_context=use_ctx)
        elapsed = time.time() - t0
        if metrics:
            print(f"\n{'─'*60}")
            print(f"  {mode_name} ({metrics['evaluated']} verses, {elapsed:.1f}s)")
            print(f"{'─'*60}")
            print(f"  Recall@10:  {metrics['recall@10']:.3f}")
            print(f"  Recall@50:  {metrics['recall@50']:.3f}")
            print(f"  MRR:        {metrics['MRR']:.3f}")

    print(f"\n{'='*60}")
    print("  Demo complete.")
    print(f"{'='*60}")
