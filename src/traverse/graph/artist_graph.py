"""Build an artist-centered similarity graph from a records CSV.

Each unique artist becomes a node; two artists are linked when they share
genre/style tags.  Tags from all of an artist's records are merged
together, so prolific artists accumulate a rich tag profile.

Uses an inverted-index approach to avoid O(n^2) all-pairs comparison:
  1. Stream CSV -> collect unique artists + merged tag sets
  2. Cap by tag diversity (most diverse first) BEFORE edge building
  3. Build inverted index, emit pairwise edges via Counter
  4. Threshold, cap, and return a CooccurrenceGraph
"""

from __future__ import annotations

import random
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from traverse.graph.cooccurrence import CooccurrenceGraph
from traverse.graph.external_links import build_external_links
from traverse.processing.normalize import is_skip_artist, matches_required_tags, pretty_label, split_tags


def _detect_col(colmap: Dict[str, str], *candidates: str) -> Optional[str]:
    """Case-insensitive column lookup."""
    for c in candidates:
        if c in colmap:
            return colmap[c]
    return None


def build_artist_graph(
    records_csv: Path,
    *,
    tag_types: Optional[List[str]] = None,
    max_nodes: int = 0,
    min_shared_tags: int = 2,
    max_edges: int = 0,
    max_edges_per_node: int = 0,
    max_tag_degree: int = 200,
    sample_high_degree: bool = True,
    require_tags: Optional[Dict[str, List[str]]] = None,
    chunksize: int = 200_000,
    progress: bool = True,
) -> Tuple[CooccurrenceGraph, pd.DataFrame]:
    """Stream *records_csv* and build an artist-centered similarity graph.

    Returns ``(graph, records_df)`` where *graph* is a
    ``CooccurrenceGraph`` (points + links) and *records_df* has columns
    ``artist_name, genres, styles``.

    Parameters
    ----------
    records_csv : Path
        Path to the records CSV (must have artist + genres/styles cols).
    tag_types : list[str] | None
        Which tag columns to use for building edges.  Accepts any
        combination of ``'genres'`` and ``'styles'``.  Defaults to
        ``['styles']``.  All tag types are still stored in node metadata
        regardless of this setting.
    max_nodes : int
        Maximum number of unique artist nodes.  Artists are ranked by
        the total number of distinct genre+style tags they carry (most
        diverse first) and capped *before* edge building, which
        dramatically reduces memory and time.  0 = unlimited.
    min_shared_tags : int
        Minimum number of shared genre/style tags required to create an
        edge (default 2).  Lower -> denser; higher -> sparser.
    max_edges : int
        Hard cap on edges, keeping the highest-weighted.  0 = unlimited.
        Tune this to keep Cosmograph responsive.
    max_edges_per_node : int
        Maximum edges to keep per node (default 0 = unlimited).  After
        all edges are built and thresholded, each node keeps only its
        top-K strongest edges.  This maximizes node coverage while
        keeping the graph sparse — e.g. ``max_edges_per_node=2`` means
        every connected artist has at most 2 edges.  Applied before
        ``max_edges``.
    max_tag_degree : int
        Threshold for high-degree tags (default 200).  Behaviour depends
        on *sample_high_degree*.
    sample_high_degree : bool
        If True (default), tags exceeding *max_tag_degree* are randomly
        sampled down to *max_tag_degree* nodes so they still contribute
        edges.  If False, such tags are skipped entirely.
    require_tags : dict[str, list[str]] | None
        Optional tag filter.  Keys are ``"genres"``, ``"styles"``, or
        ``"artists"``; values are lists of acceptable values.  A node
        passes if it has at least one match per key (OR within a key,
        AND across keys).  Applied after full tag accumulation, before
        edge building.
    chunksize : int
        CSV read chunk size.
    progress : bool
        Show tqdm progress bars.
    """
    if tag_types is None:
        tag_types = ["styles"]
    use_genres = "genres" in tag_types
    use_styles = "styles" in tag_types

    # ------------------------------------------------------------------
    # Pass 1a: Stream CSV, collect unique artists + merged tag sets
    # ------------------------------------------------------------------
    artist_genres: Dict[str, Set[str]] = {}
    artist_styles: Dict[str, Set[str]] = {}
    artist_all_tags: Dict[str, Set[str]] = {}
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

        for idx in range(len(chunk)):
            row = chunk.iloc[idx]
            aval = str(row[acol]).strip() if acol and pd.notna(row[acol]) else ""
            if not aval or is_skip_artist(aval):
                continue

            gval = row[gcol] if gcol else ""
            sval = row[scol] if scol else ""

            genre_tags = split_tags(gval)
            style_tags = split_tags(sval)

            # Tags used for edge building (controlled by tag_types)
            edge_tags: List[str] = []
            if use_genres:
                edge_tags.extend(genre_tags)
            if use_styles:
                edge_tags.extend(style_tags)
            if not edge_tags:
                continue

            # Merge tags across all records for this artist
            if aval not in artist_all_tags:
                artist_all_tags[aval] = set()
                artist_genres[aval] = set()
                artist_styles[aval] = set()

            artist_all_tags[aval].update(edge_tags)
            artist_genres[aval].update(genre_tags)
            artist_styles[aval].update(style_tags)

    n_total = len(artist_all_tags)
    print(
        f"Pass 1a: {total_rows:,} rows scanned, "
        f"{n_total:,} unique artists with tags "
        f"(edge tag_types={tag_types})",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Pass 1a+: Filter by require_tags (after full tag accumulation)
    # ------------------------------------------------------------------
    if require_tags:
        before = len(artist_all_tags)
        keep_keys = [
            k for k in artist_all_tags
            if matches_required_tags(
                artist_genres.get(k, set()),
                artist_styles.get(k, set()),
                k,
                require_tags,
            )
        ]
        keep = set(keep_keys)
        artist_all_tags = {k: t for k, t in artist_all_tags.items() if k in keep}
        artist_genres = {k: g for k, g in artist_genres.items() if k in keep}
        artist_styles = {k: s for k, s in artist_styles.items() if k in keep}
        n_total = len(artist_all_tags)
        print(
            f"  require_tags filter: {before:,} → {n_total:,} artists "
            f"(require={require_tags})",
            file=sys.stderr,
        )

    # ------------------------------------------------------------------
    # Pass 1b: Cap artists by tag diversity BEFORE building inverted index
    # ------------------------------------------------------------------
    if max_nodes > 0 and n_total > max_nodes:
        ranked = sorted(
            artist_all_tags.keys(),
            key=lambda k: len(artist_all_tags[k]),
            reverse=True,
        )
        keep = set(ranked[:max_nodes])
        artist_all_tags = {k: t for k, t in artist_all_tags.items() if k in keep}
        artist_genres = {k: g for k, g in artist_genres.items() if k in keep}
        artist_styles = {k: s for k, s in artist_styles.items() if k in keep}
        print(
            f"Pass 1b: capped to {max_nodes:,} artists "
            f"(min tag count in kept set: "
            f"{min(len(t) for t in artist_all_tags.values())})",
            file=sys.stderr,
        )

    # Integer mapping
    keys_sorted = sorted(artist_all_tags.keys())
    key_to_int: Dict[str, int] = {k: i for i, k in enumerate(keys_sorted)}

    # ------------------------------------------------------------------
    # Pass 2: Build inverted index + edges
    # ------------------------------------------------------------------
    tag_to_nodes: Dict[str, List[int]] = defaultdict(list)
    for key, tags in artist_all_tags.items():
        nid = key_to_int[key]
        for tag in tags:
            tag_to_nodes[tag].append(nid)

    # Also store per-node tag list for nearest-neighbor mode
    node_tags: Dict[int, List[str]] = {}
    for key, tags in artist_all_tags.items():
        node_tags[key_to_int[key]] = list(tags)

    del artist_all_tags

    # Sample / skip high-degree tags
    sampled_tags = 0
    skipped_tags = 0
    for tag in list(tag_to_nodes.keys()):
        nids = tag_to_nodes[tag]
        if len(nids) > max_tag_degree:
            if sample_high_degree:
                tag_to_nodes[tag] = random.sample(nids, max_tag_degree)
                sampled_tags += 1
            else:
                del tag_to_nodes[tag]
                skipped_tags += 1

    n_nodes = len(keys_sorted)

    if max_edges_per_node > 0:
        # ----- Nearest-neighbor mode -----
        # For each node, find its top-K neighbors by overlap count.
        # This avoids materializing all O(n^2) pairs globally.
        edge_weights: Counter[Tuple[int, int]] = Counter()

        node_iter: Any = range(n_nodes)
        if progress:
            try:
                from tqdm import tqdm

                node_iter = tqdm(
                    node_iter, total=n_nodes,
                    desc="Finding neighbors", unit="node",
                )
            except ImportError:
                pass

        for nid in node_iter:
            # Count how many tags this node shares with each neighbor
            overlap: Counter[int] = Counter()
            for tag in node_tags[nid]:
                for other in tag_to_nodes.get(tag, ()):
                    if other != nid:
                        overlap[other] += 1

            # Keep top-K by overlap, respecting min_shared_tags
            for other, w in overlap.most_common(max_edges_per_node):
                if w < min_shared_tags:
                    break
                pair = (min(nid, other), max(nid, other))
                # Keep the max weight seen for this pair
                if w > edge_weights.get(pair, 0):
                    edge_weights[pair] = w

        del node_tags

        print(
            f"Pass 2 (nearest-neighbor): {len(edge_weights):,} unique edges, "
            f"{sampled_tags} tags sampled, "
            f"{skipped_tags} tags skipped (degree > {max_tag_degree})",
            file=sys.stderr,
        )
    else:
        # ----- Full pairwise mode -----
        del node_tags
        edge_weights = Counter()

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
            if len(nids) < 2:
                continue
            nids_sorted = sorted(nids)
            for i in range(len(nids_sorted)):
                for j in range(i + 1, len(nids_sorted)):
                    edge_weights[(nids_sorted[i], nids_sorted[j])] += 1

        print(
            f"Pass 2 (pairwise): {len(edge_weights):,} unique pairs, "
            f"{sampled_tags} tags sampled, "
            f"{skipped_tags} tags skipped (degree > {max_tag_degree})",
            file=sys.stderr,
        )

    del tag_to_nodes

    # ------------------------------------------------------------------
    # Pass 3: Threshold, cap, build output
    # ------------------------------------------------------------------
    if min_shared_tags > 1 and max_edges_per_node <= 0:
        # In nearest-neighbor mode, min_shared_tags is already applied
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
        f"Pass 3: {len(node_ints):,} artist nodes, {len(edges_sorted):,} edges "
        f"(min_shared_tags={min_shared_tags}, max_edges={max_edges}, "
        f"max_edges_per_node={max_edges_per_node})",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Build points and links
    # ------------------------------------------------------------------
    points: List[Dict[str, Any]] = []
    for nid in sorted(node_ints):
        key = keys_sorted[nid]
        genres_str = " | ".join(
            pretty_label(g) for g in sorted(artist_genres.get(key, set()))
        )
        styles_str = " | ".join(
            pretty_label(s) for s in sorted(artist_styles.get(key, set()))
        )
        pt: Dict[str, Any] = {"id": key, "label": key}
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

    # Records DataFrame for downstream compatibility
    records_rows = [
        {
            "artist_name": keys_sorted[nid],
            "genres": " | ".join(
                pretty_label(g)
                for g in sorted(artist_genres.get(keys_sorted[nid], set()))
            ),
            "styles": " | ".join(
                pretty_label(s)
                for s in sorted(artist_styles.get(keys_sorted[nid], set()))
            ),
        }
        for nid in sorted(node_ints)
    ]
    records_df = pd.DataFrame(records_rows)

    return graph, records_df
