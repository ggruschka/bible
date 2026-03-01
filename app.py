#!/usr/bin/env python3
"""
Streamlit Bible Explorer — interactive UI for the multi-Bible database.

Run with:  streamlit run app.py
"""

import html as html_mod
import sys
import os

import streamlit as st

# Add scripts/ to path so we can import the existing API
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts'))

from query_bible import (
    get_conn, default_bible_id, resolve_book,
    get_verse, get_chapter, search_text,
    get_footnotes, get_cross_refs, get_section_headings,
)

# Semantic search — optional (requires embeddings)
try:
    import semantic_search as ss
    _SEMANTIC_AVAILABLE = True
except Exception:
    _SEMANTIC_AVAILABLE = False

# ─── Page Config ───

st.set_page_config(
    page_title="Sagrada Biblia",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Category Labels (Spanish) ───

CATEGORY_LABELS = {
    'pentateuch': 'Pentateuco',
    'history': 'Historicos',
    'wisdom': 'Sapienciales',
    'prophecy': 'Profeticos',
    'gospel': 'Evangelios',
    'epistle': 'Epistolas',
    'apocalypse': 'Apocalipsis',
    'appendix': 'Apendice',
}

AT_CATEGORIES = ['pentateuch', 'history', 'wisdom', 'prophecy', 'appendix']
NT_CATEGORIES = ['gospel', 'history', 'epistle', 'apocalypse', 'appendix']

# ─── Cached Data ───

@st.cache_data(ttl=3600)
def load_books():
    """Load all books with testament and category info."""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT b.id, b.name_es, b.abbrev_es, b.category, b.total_chapters,
                   b.testament_id, t.name_es as testament
            FROM book b JOIN testament t ON b.testament_id = t.id
            ORDER BY b.id
        """).fetchall()
        return [
            {'id': r[0], 'name': r[1], 'abbrev': r[2], 'category': r[3],
             'chapters': r[4], 'testament_id': r[5], 'testament': r[6]}
            for r in rows
        ]
    finally:
        conn.close()


@st.cache_data(ttl=3600)
def get_verse_count(book_id, chapter):
    """Get max verse number in a chapter."""
    conn = get_conn()
    try:
        bid = default_bible_id()
        row = conn.execute(
            "SELECT MAX(verse_number) FROM verse WHERE book_id=? AND chapter_number=? AND bible_id=?",
            (book_id, chapter, bid)
        ).fetchone()
        return row[0] if row and row[0] else 0
    finally:
        conn.close()


def get_book_by_id(book_id, books=None):
    """Get book dict by id."""
    if books is None:
        books = load_books()
    for b in books:
        if b['id'] == book_id:
            return b
    return None


# ─── Session State Defaults ───

def init_state():
    defaults = {
        'book_id': 1,
        'chapter': 1,
        'verse': 0,
        'backend': 'qdrant',
        'top_k': 20,
        'annotations': False,
        'search_mode': 'texto',
        'verse_tab_book': 1,
        'verse_tab_chapter': 1,
        'verse_tab_verse': 1,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ─── Navigation ───

def navigate_to_chapter(book_id, chapter):
    """Navigate the reader to a specific chapter."""
    st.session_state['book_id'] = book_id
    st.session_state['chapter'] = chapter
    st.session_state['verse'] = 0


def navigate_to_verse_tab(book_id, chapter, verse):
    """Set up the verse explorer tab with a specific verse."""
    st.session_state['verse_tab_book'] = book_id
    st.session_state['verse_tab_chapter'] = chapter
    st.session_state['verse_tab_verse'] = verse


def parse_ref(ref_str):
    """Parse a reference like 'Gn 1:1' into (book_id, chapter, verse)."""
    parts = ref_str.rsplit(' ', 1)
    if len(parts) != 2:
        return None
    abbrev, ch_v = parts
    ch_v_parts = ch_v.split(':')
    if not ch_v_parts:
        return None
    try:
        chapter = int(ch_v_parts[0])
        verse = int(ch_v_parts[1]) if len(ch_v_parts) > 1 else 1
    except ValueError:
        return None
    conn = get_conn()
    try:
        book_id, _ = resolve_book(conn, abbrev)
    finally:
        conn.close()
    if book_id is None:
        return None
    return (book_id, chapter, verse)


# ─── Display Helpers ───

def verse_button(ref, idx, prefix=""):
    """Render a small button to navigate to a verse in the explorer tab."""
    key = f"{prefix}_goto_{idx}_{ref}"
    if st.button("->", key=key, help=f"Explorar {ref}"):
        parsed = parse_ref(ref)
        if parsed:
            navigate_to_verse_tab(*parsed)
            st.rerun()


def display_verse_result(v, idx, prefix="res"):
    """Display a single verse result with score and navigation button."""
    score_str = f"({v['score']:.3f})" if 'score' in v else ""
    col1, col2 = st.columns([0.92, 0.08])
    with col1:
        st.markdown(f"**{idx}. {v['ref']}** {score_str}")
        if v.get('text'):
            st.caption(v['text'][:300])
    with col2:
        verse_button(v['ref'], idx, prefix)


def display_crossref_result(xr, idx, prefix="xref"):
    """Display a cross-reference with vote count and navigation button."""
    col1, col2 = st.columns([0.92, 0.08])
    with col1:
        st.markdown(f"**{idx}. {xr['ref']}** ({xr['votes']} votos)")
    with col2:
        verse_button(xr['ref'], idx, prefix)


# ─── Sidebar ───

def render_sidebar():
    books = load_books()

    with st.sidebar:
        st.title("Sagrada Biblia")

        # Testament-level expanders
        for testament_id, testament_name in [(1, 'Antiguo Testamento'), (2, 'Nuevo Testamento')]:
            cat_order = AT_CATEGORIES if testament_id == 1 else NT_CATEGORIES
            testament_books = [b for b in books if b['testament_id'] == testament_id]

            with st.expander(testament_name, expanded=False):
                for cat in cat_order:
                    cat_books = [b for b in testament_books if b['category'] == cat]
                    if not cat_books:
                        continue
                    st.caption(CATEGORY_LABELS.get(cat, cat))
                    for book in cat_books:
                        with st.expander(book['name'], expanded=False):
                            # Chapter buttons in a grid
                            n_cols = 10
                            for row_start in range(1, book['chapters'] + 1, n_cols):
                                cols = st.columns(n_cols)
                                for i, col in enumerate(cols):
                                    ch = row_start + i
                                    if ch > book['chapters']:
                                        break
                                    with col:
                                        if st.button(
                                            str(ch),
                                            key=f"nav_{book['id']}_{ch}",
                                            use_container_width=True,
                                        ):
                                            navigate_to_chapter(book['id'], ch)
                                            st.rerun()

        st.divider()

        # Settings
        with st.expander("Configuracion", expanded=False):
            st.session_state['backend'] = st.selectbox(
                "Backend",
                ['qdrant', 'sqlite'],
                index=0 if st.session_state.get('backend', 'qdrant') == 'qdrant' else 1,
                key='_cfg_backend',
            )
            st.session_state['top_k'] = st.slider(
                "Resultados",
                min_value=5, max_value=100, value=st.session_state.get('top_k', 20),
                key='_cfg_topk',
            )
            st.session_state['annotations'] = st.checkbox(
                "Anotaciones",
                value=st.session_state.get('annotations', False),
                key='_cfg_annotations',
            )


# ─── Tab 1: Leer ───

def render_tab_leer():
    book_id = st.session_state['book_id']
    chapter = st.session_state['chapter']
    book = get_book_by_id(book_id)

    if book is None:
        st.error("Libro no encontrado.")
        return

    st.header(f"{book['name']} {chapter}")

    verses = get_chapter(book_id, chapter)
    if not verses:
        st.warning("No hay versiculos para este capitulo.")
        return

    # Preload annotation data if toggle is on
    annotations_on = st.session_state.get('annotations', False)
    xref_counts = {}
    footnote_counts = {}
    headings = {}
    if annotations_on:
        conn = get_conn()
        try:
            # Cross-ref counts per verse
            rows = conn.execute("""
                SELECT source_verse, COUNT(*) FROM cross_reference
                WHERE source_book_id=? AND source_chapter=?
                GROUP BY source_verse
            """, (book_id, chapter)).fetchall()
            xref_counts = {r[0]: r[1] for r in rows}
            # Footnote counts per verse
            rows = conn.execute("""
                SELECT verse_start, COUNT(*) FROM footnote
                WHERE book_id=? AND chapter_number=?
                GROUP BY verse_start
            """, (book_id, chapter)).fetchall()
            footnote_counts = {r[0]: r[1] for r in rows}
            # Section headings
            rows = conn.execute("""
                SELECT before_verse, heading_text FROM section_heading
                WHERE book_id=? AND chapter_number=?
                ORDER BY before_verse
            """, (book_id, chapter)).fetchall()
            headings = {}
            for r in rows:
                headings.setdefault(r[0], []).append(r[1])
        finally:
            conn.close()

    # Render chapter as continuous text
    text_parts = []
    for v in verses:
        vnum = v['verse']

        # Section headings before this verse
        if annotations_on and vnum in headings:
            for h in headings[vnum]:
                text_parts.append(f"\n\n**{html_mod.escape(h)}**\n\n")

        # Verse number as superscript (clickable via a unique key)
        sup = f"<sup>{vnum}</sup>"
        text = html_mod.escape(v['text'])

        # Annotation badges
        badges = ""
        if annotations_on:
            xc = xref_counts.get(vnum, 0)
            fc = footnote_counts.get(vnum, 0)
            if xc > 0:
                badges += f' <small>[{xc} refs]</small>'
            if fc > 0:
                badges += f' <small>[{fc} notas]</small>'

        text_parts.append(f"{sup} {text}{badges} ")

    # Display as one block of HTML
    chapter_html = "".join(text_parts)
    st.markdown(chapter_html, unsafe_allow_html=True)

    # Verse selector for clicking through to explorer
    st.divider()
    max_v = get_verse_count(book_id, chapter)
    col1, col2, col3 = st.columns([0.4, 0.3, 0.3])
    with col1:
        selected_v = st.selectbox(
            "Seleccionar versiculo",
            list(range(1, max_v + 1)),
            index=0,
            key='_leer_verse_select',
        )
    with col2:
        if st.button("Explorar versiculo", key='_leer_explore'):
            navigate_to_verse_tab(book_id, chapter, selected_v)
            st.rerun()
    # Prev/next chapter navigation
    st.divider()
    col_prev, col_mid, col_next = st.columns([0.3, 0.4, 0.3])
    with col_prev:
        if chapter > 1:
            if st.button("< Capitulo anterior", key='_leer_prev'):
                navigate_to_chapter(book_id, chapter - 1)
                st.rerun()
    with col_next:
        if chapter < book['chapters']:
            if st.button("Capitulo siguiente >", key='_leer_next'):
                navigate_to_chapter(book_id, chapter + 1)
                st.rerun()


# ─── Tab 2: Busqueda ───

def render_tab_busqueda():
    st.header("Busqueda")

    mode = st.radio(
        "Modo",
        ["Texto", "Semantica"],
        horizontal=True,
        index=0 if st.session_state.get('search_mode', 'texto') == 'texto' else 1,
        key='_search_mode_radio',
    )
    st.session_state['search_mode'] = mode.lower()

    if mode == "Texto":
        render_search_texto()
    else:
        render_search_semantica()


def render_search_texto():
    query = st.text_input(
        "Buscar palabras:",
        placeholder="bienaventurado, pastor, amor...",
        key='_fts_query',
    )
    st.caption("Soporta: AND (implicito), OR, NOT, \"frase exacta\", prefijo*")

    if not query:
        return

    top_k = st.session_state.get('top_k', 20)
    try:
        results = search_text(query, limit=top_k)
    except Exception as e:
        st.error(f"Error en busqueda: {e}")
        return

    if not results:
        st.info("No se encontraron resultados.")
        return

    st.subheader(f"{len(results)} resultados")

    # Group by book (parse the ref to get the book abbreviation)
    grouped = {}
    for r in results:
        book_abbrev = r['ref'].rsplit(' ', 1)[0]
        grouped.setdefault(book_abbrev, []).append(r)

    for book_abbrev, book_results in grouped.items():
        st.markdown(f"**{book_abbrev}** ({len(book_results)})")
        for i, r in enumerate(book_results):
            snippet = r['snippet'].replace('>>>', '**').replace('<<<', '**')
            col1, col2 = st.columns([0.92, 0.08])
            with col1:
                st.markdown(f"  {r['ref']}: {snippet}")
            with col2:
                verse_button(r['ref'], i, f"fts_{book_abbrev}")


def render_search_semantica():
    if not _SEMANTIC_AVAILABLE:
        st.warning("Busqueda semantica no disponible. Instala dependencias: `pip install -r requirements-embeddings.txt`")
        return

    query = st.text_input(
        "Buscar por significado:",
        placeholder="el amor incondicional de Dios...",
        key='_sem_query',
    )

    if not query:
        return

    if not st.button("Buscar", key='_sem_go'):
        return

    top_k = st.session_state.get('top_k', 20)
    backend = st.session_state.get('backend', 'qdrant')

    # Run both context-free and context-aware searches
    col_v2v, col_ctx = st.columns(2)

    with col_v2v:
        st.subheader("Verso a verso")
        with st.spinner("Buscando (context-free)..."):
            try:
                results_v2v = ss.search_meaning(
                    query, top_k=top_k, use_context=False, backend=backend
                )
            except Exception as e:
                st.error(f"Error: {e}")
                results_v2v = []

        for i, v in enumerate(results_v2v):
            display_verse_result(v, i + 1, "sem_v2v")

    with col_ctx:
        st.subheader("Contextual")
        with st.spinner("Buscando (context-aware)..."):
            try:
                results_ctx = ss.search_meaning(
                    query, top_k=top_k, use_context=True, backend=backend
                )
            except Exception as e:
                st.error(f"Error: {e}")
                results_ctx = []

        for i, v in enumerate(results_ctx):
            display_verse_result(v, i + 1, "sem_ctx")


# ─── Tab 3: Versiculo ───

def render_tab_versiculo():
    st.header("Explorador de versiculo")

    books = load_books()
    book_names = {b['id']: b['name'] for b in books}
    book_ids = [b['id'] for b in books]

    # Verse selector
    col1, col2, col3 = st.columns([0.4, 0.2, 0.2])

    with col1:
        current_book_id = st.session_state.get('verse_tab_book', 1)
        idx = book_ids.index(current_book_id) if current_book_id in book_ids else 0
        selected_book_id = st.selectbox(
            "Libro",
            book_ids,
            index=idx,
            format_func=lambda bid: book_names.get(bid, str(bid)),
            key='_vt_book',
        )
        st.session_state['verse_tab_book'] = selected_book_id

    book_info = get_book_by_id(selected_book_id, books)
    max_ch = book_info['chapters'] if book_info else 1

    with col2:
        current_ch = st.session_state.get('verse_tab_chapter', 1)
        if current_ch > max_ch:
            current_ch = 1
        selected_ch = st.selectbox(
            "Capitulo",
            list(range(1, max_ch + 1)),
            index=current_ch - 1,
            key='_vt_chapter',
        )
        st.session_state['verse_tab_chapter'] = selected_ch

    max_v = get_verse_count(selected_book_id, selected_ch)
    with col3:
        current_v = st.session_state.get('verse_tab_verse', 1)
        if current_v > max_v:
            current_v = 1
        if max_v > 0:
            selected_v = st.selectbox(
                "Versiculo",
                list(range(1, max_v + 1)),
                index=current_v - 1 if current_v <= max_v else 0,
                key='_vt_verse',
            )
        else:
            selected_v = 1
            st.selectbox("Versiculo", [1], key='_vt_verse_empty')
        st.session_state['verse_tab_verse'] = selected_v

    # Show the selected verse text
    verse_data = get_verse(selected_book_id, selected_ch, selected_v)
    if verse_data:
        st.markdown(f"> {verse_data['text']}")

        # "Read in context" button
        if st.button("Leer en contexto", key='_vt_read_ctx'):
            navigate_to_chapter(selected_book_id, selected_ch)
            st.rerun()
    else:
        st.info("Versiculo no encontrado.")
        return

    st.divider()

    # ── Section 1: Similar verso a verso ──
    if _SEMANTIC_AVAILABLE:
        with st.expander("Similar: verso a verso", expanded=True):
            _render_similar_section(
                selected_book_id, selected_ch, selected_v,
                use_context=False, exclude_same_chapter=True, prefix="v2v"
            )

        # ── Section 2: Similar contextual ──
        with st.expander("Similar: contextual", expanded=False):
            _render_similar_section(
                selected_book_id, selected_ch, selected_v,
                use_context=True, exclude_same_chapter=True, prefix="ctx"
            )
    else:
        st.info("Busqueda semantica no disponible. Instala: `pip install -r requirements-embeddings.txt`")

    # ── Section 3: Notas al pie ──
    with st.expander("Notas al pie", expanded=False):
        footnotes = get_footnotes(selected_book_id, selected_ch, selected_v)
        if footnotes:
            for fn in footnotes:
                st.markdown(f"- {fn['text']}")
        else:
            st.caption("No hay notas para este versiculo.")

    # ── Section 4: Referencias cruzadas ──
    with st.expander("Referencias cruzadas", expanded=True):
        top_k = st.session_state.get('top_k', 20)
        xrefs = get_cross_refs(selected_book_id, selected_ch, selected_v, limit=top_k)
        if xrefs:
            for i, xr in enumerate(xrefs):
                display_crossref_result(xr, i + 1, "vt_xref")
        else:
            st.caption("No hay referencias cruzadas para este versiculo.")

    # ── Section 5: Comentarios ──
    with st.expander("Comentarios", expanded=False):
        st.caption("No hay comentarios aun.")


def _render_similar_section(book_id, chapter, verse, use_context, exclude_same_chapter, prefix):
    """Render a similarity search section inside an expander."""
    top_k = st.session_state.get('top_k', 20)
    backend = st.session_state.get('backend', 'qdrant')

    # Use a cache key to avoid re-running on every rerun
    cache_key = f"_cache_{prefix}_{book_id}_{chapter}_{verse}_{top_k}_{backend}"
    if cache_key not in st.session_state:
        try:
            results = ss.find_similar(
                book_id, chapter, verse, top_k=top_k,
                exclude_same_chapter=exclude_same_chapter,
                use_context=use_context, backend=backend,
            )
            st.session_state[cache_key] = results
        except Exception as e:
            st.error(f"Error: {e}")
            return

    results = st.session_state[cache_key]
    if not results:
        st.caption("No se encontraron resultados similares.")
        return

    for i, v in enumerate(results):
        display_verse_result(v, i + 1, prefix)


# ─── Main ───

def main():
    init_state()
    render_sidebar()

    tab_leer, tab_busqueda, tab_versiculo = st.tabs(["Leer", "Busqueda", "Versiculo"])

    with tab_leer:
        render_tab_leer()

    with tab_busqueda:
        render_tab_busqueda()

    with tab_versiculo:
        render_tab_versiculo()


if __name__ == '__main__':
    main()
