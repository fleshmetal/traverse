"""Build a similarity graph from a records CSV.

Supports two node modes controlled by *node_by*:

* ``'artist'`` — each unique artist becomes a node; two artists are
  linked when they share genre/style tags.
* ``'album'``  — each unique album (title + artist) becomes a node;
  two albums are linked when they share genre/style tags.

Uses an inverted-index approach: for every tag, emit pairwise edges
among all nodes that carry that tag.  A ``Counter`` accumulates
shared-tag counts per pair, then edges are thresholded and capped.
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, Tuple

import pandas as pd

from traverse.graph.cooccurrence import CooccurrenceGraph
from traverse.graph.external_links import build_external_links
from traverse.processing.normalize import coerce_year, is_skip_artist, matches_required_tags, pretty_label, split_tags


def _detect_col(colmap: Dict[str, str], *candidates: str) -> Optional[str]:
    """Case-insensitive column lookup."""
    for c in candidates:
        if c in colmap:
            return colmap[c]
    return None


def build_genre_graph(
    records_csv: Path,
    *,
    node_by: Literal["artist", "album"] = "artist",
    max_nodes: int = 0,
    min_shared_tags: int = 2,
    max_edges: int = 0,
    max_tag_degree: int = 200,
    require_tags: Optional[Dict[str, List[str]]] = None,
    chunksize: int = 200_000,
    progress: bool = True,
) -> Tuple[CooccurrenceGraph, pd.DataFrame]:
    """Stream *records_csv* and build a similarity graph.

    Parameters
    ----------
    records_csv : Path
        Path to the records CSV (must have artist + genres/styles cols).
    node_by : ``'artist'`` | ``'album'``
        What each node represents.

        * ``'artist'`` — one node per unique artist name.  Tags from all
          of that artist's records are merged together.
        * ``'album'``  — one node per unique title+artist combination.
          Tags are taken per-album.
    max_nodes : int
        Maximum number of nodes.  Nodes are ranked by the total number
        of distinct tags they carry (most diverse first).  0 = unlimited.
    min_shared_tags : int
        Minimum number of shared genre/style tags required to create an
        edge (default 2).  Lower → denser; higher → sparser.
    max_edges : int
        Hard cap on edges, keeping the highest-weighted.  0 = unlimited.
        Tune this to keep Cosmograph responsive.
    max_tag_degree : int
        Tags shared by more than this many nodes are skipped to avoid
        enormous cliques from ubiquitous tags like "Rock" (default 200).
    chunksize : int
        CSV read chunk size.
    progress : bool
        Show tqdm progress bars.

    Returns
    -------
    (graph, records_df)
        *graph* is a ``CooccurrenceGraph`` (points + links).
        *records_df* has columns ``artist_name, genres, styles`` (artist
        mode) or ``track_name, artist_name, genres, styles, release_year``
        (album mode).
    """
    by_album = node_by == "album"
    label = "album" if by_album else "artist"

    # ------------------------------------------------------------------
    # Pass 1: Stream CSV, collect unique nodes and their tag sets
    # ------------------------------------------------------------------
    # node_key → metadata and tag sets
    node_genres: Dict[str, Set[str]] = {}
    node_styles: Dict[str, Set[str]] = {}
    node_all_tags: Dict[str, Set[str]] = {}
    # Album mode keeps extra per-node metadata
    node_meta: Dict[str, Dict[str, Any]] = {}  # only used in album mode
    total_rows = 0

    raw_reader = pd.read_csv(
        records_csv,
        chunksize=chunksize,
        dtype="string",
        keep_default_na=True,
        na_filter=True,
    )
    reader: Any = raw_reader

    if progress:
        try:
            from tqdm import tqdm

            reader = tqdm(raw_reader, desc="Reading records", unit="chunk")
        except ImportError:
            pass

    for chunk in reader:
        total_rows += len(chunk)
        colmap = {c.lower(): c for c in chunk.columns}

        gcol = _detect_col(colmap, "genres", "genre")
        scol = _detect_col(colmap, "styles", "style")
        acol = _detect_col(colmap, "artist", "artists")
        tcol = _detect_col(colmap, "title", "album", "record")
        ycol = _detect_col(colmap, "release_year", "year", "released")

        for idx in range(len(chunk)):
            row = chunk.iloc[idx]
            aval = str(row[acol]).strip() if acol and pd.notna(row[acol]) else ""
            if not aval or is_skip_artist(aval):
                continue

            gval = row[gcol] if gcol else ""
            sval = row[scol] if scol else ""

            genre_tags = split_tags(gval)
            style_tags = split_tags(sval)
            all_tags = genre_tags + style_tags
            if not all_tags:
                continue

            if by_album:
                tval = str(row[tcol]).strip() if tcol and pd.notna(row[tcol]) else ""
                if not tval:
                    continue
                key = f"{tval.lower()}::{aval.lower()}"
                # First occurrence defines the metadata
                if key not in node_all_tags:
                    yval = row[ycol] if ycol else None
                    node_meta[key] = {
                        "label": tval,
                        "artist": aval,
                        "release_year": coerce_year(yval),
                    }
            else:
                key = aval

            # Merge tags (accumulate across all records for this node)
            if key not in node_all_tags:
                node_all_tags[key] = set()
                node_genres[key] = set()
                node_styles[key] = set()

            node_all_tags[key].update(all_tags)
            node_genres[key].update(genre_tags)
            node_styles[key].update(style_tags)

    n_total = len(node_all_tags)
    print(
        f"Pass 1: {total_rows:,} rows scanned, "
        f"{n_total:,} unique {label}s with tags",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Filter by require_tags (after full tag accumulation)
    # ------------------------------------------------------------------
    if require_tags:
        before = len(node_all_tags)
        if by_album:
            keep_keys = [
                k for k in node_all_tags
                if matches_required_tags(
                    node_genres.get(k, set()),
                    node_styles.get(k, set()),
                    node_meta.get(k, {}).get("artist", ""),
                    require_tags,
                )
            ]
        else:
            keep_keys = [
                k for k in node_all_tags
                if matches_required_tags(
                    node_genres.get(k, set()),
                    node_styles.get(k, set()),
                    k,
                    require_tags,
                )
            ]
        keep = set(keep_keys)
        node_all_tags = {k: t for k, t in node_all_tags.items() if k in keep}
        node_genres = {k: g for k, g in node_genres.items() if k in keep}
        node_styles = {k: s for k, s in node_styles.items() if k in keep}
        if by_album:
            node_meta = {k: m for k, m in node_meta.items() if k in keep}
        n_total = len(node_all_tags)
        print(
            f"  require_tags filter: {before:,} → {n_total:,} {label}s "
            f"(require={require_tags})",
            file=sys.stderr,
        )

    # ------------------------------------------------------------------
    # Cap nodes by tag diversity (most genre-diverse first)
    # ------------------------------------------------------------------
    if max_nodes > 0 and n_total > max_nodes:
        ranked = sorted(
            node_all_tags.keys(),
            key=lambda k: len(node_all_tags[k]),
            reverse=True,
        )
        keep = set(ranked[:max_nodes])
        node_all_tags = {k: t for k, t in node_all_tags.items() if k in keep}
        node_genres = {k: g for k, g in node_genres.items() if k in keep}
        node_styles = {k: s for k, s in node_styles.items() if k in keep}
        if by_album:
            node_meta = {k: m for k, m in node_meta.items() if k in keep}
        print(
            f"  capped to {max_nodes:,} {label}s "
            f"(min tag count in kept set: "
            f"{min(len(t) for t in node_all_tags.values())})",
            file=sys.stderr,
        )

    # Integer mapping
    keys_sorted = sorted(node_all_tags.keys())
    key_to_int: Dict[str, int] = {k: i for i, k in enumerate(keys_sorted)}

    # ------------------------------------------------------------------
    # Pass 2: Build inverted index (tag → node ints), then emit edges
    # ------------------------------------------------------------------
    tag_to_nodes: Dict[str, List[int]] = defaultdict(list)
    for key, tags in node_all_tags.items():
        nid = key_to_int[key]
        for tag in tags:
            tag_to_nodes[tag].append(nid)

    edge_weights: Counter[Tuple[int, int]] = Counter()
    skipped_tags = 0

    tag_items: Any = tag_to_nodes.items()
    if progress:
        try:
            from tqdm import tqdm

            tag_items = tqdm(
                list(tag_items),
                total=len(tag_to_nodes),
                desc="Building edges",
                unit="tag",
            )
        except ImportError:
            pass

    for _tag, nids in tag_items:
        if len(nids) > max_tag_degree:
            skipped_tags += 1
            continue
        if len(nids) < 2:
            continue
        nids_sorted = sorted(nids)
        for i in range(len(nids_sorted)):
            for j in range(i + 1, len(nids_sorted)):
                edge_weights[(nids_sorted[i], nids_sorted[j])] += 1

    del tag_to_nodes

    print(
        f"Pass 2: {len(edge_weights):,} unique {label} pairs, "
        f"{skipped_tags} tags skipped (degree > {max_tag_degree})",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Pass 3: Threshold, cap, build output
    # ------------------------------------------------------------------
    if min_shared_tags > 1:
        edge_weights = Counter(
            {pair: w for pair, w in edge_weights.items() if w >= min_shared_tags}
        )

    edges_sorted = edge_weights.most_common(max_edges if max_edges > 0 else None)

    # Collect node set (only nodes with at least one edge)
    node_ints: Set[int] = set()
    for (a, b), _w in edges_sorted:
        node_ints.add(a)
        node_ints.add(b)

    print(
        f"Pass 3: {len(node_ints):,} {label} nodes, {len(edges_sorted):,} edges "
        f"(min_shared_tags={min_shared_tags}, max_edges={max_edges})",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Build points and links
    # ------------------------------------------------------------------
    points: List[Dict[str, Any]] = []
    for nid in sorted(node_ints):
        key = keys_sorted[nid]
        genres_str = " | ".join(
            pretty_label(g) for g in sorted(node_genres.get(key, set()))
        )
        styles_str = " | ".join(
            pretty_label(s) for s in sorted(node_styles.get(key, set()))
        )

        if by_album:
            meta = node_meta[key]
            pt: Dict[str, Any] = {
                "id": key,
                "label": meta["label"],
                "artist": meta["artist"],
            }
            if genres_str:
                pt["genres"] = genres_str
            if styles_str:
                pt["styles"] = styles_str
            if meta["release_year"] is not None:
                pt["release_year"] = meta["release_year"]
        else:
            pt = {"id": key, "label": key}
            if genres_str:
                pt["genres"] = genres_str
            if styles_str:
                pt["styles"] = styles_str

        pt["external_links"] = build_external_links(pt)
        points.append(pt)

    links: List[Dict[str, Any]] = [
        {
            "source": keys_sorted[a],
            "target": keys_sorted[b],
            "weight": w,
        }
        for (a, b), w in edges_sorted
    ]

    graph = CooccurrenceGraph(points=points, links=links)

    # ------------------------------------------------------------------
    # Records DataFrame for downstream compatibility
    # ------------------------------------------------------------------
    if by_album:
        records_rows = [
            {
                "track_name": node_meta[keys_sorted[nid]]["label"],
                "artist_name": node_meta[keys_sorted[nid]]["artist"],
                "genres": " | ".join(
                    pretty_label(g)
                    for g in sorted(node_genres.get(keys_sorted[nid], set()))
                ),
                "styles": " | ".join(
                    pretty_label(s)
                    for s in sorted(node_styles.get(keys_sorted[nid], set()))
                ),
            }
            for nid in sorted(node_ints)
        ]
    else:
        records_rows = [
            {
                "artist_name": keys_sorted[nid],
                "genres": " | ".join(
                    pretty_label(g)
                    for g in sorted(node_genres.get(keys_sorted[nid], set()))
                ),
                "styles": " | ".join(
                    pretty_label(s)
                    for s in sorted(node_styles.get(keys_sorted[nid], set()))
                ),
            }
            for nid in sorted(node_ints)
        ]

    records_df = pd.DataFrame(records_rows)
    return graph, records_df
