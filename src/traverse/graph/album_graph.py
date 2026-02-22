"""Build an album-centered similarity graph from a records CSV.

Each record row becomes a node; two records are linked if they share
genre/style tags.  This is the *inverse* of the tag co-occurrence graph
(where nodes are tags and links represent shared albums).

Uses an inverted-index approach to avoid O(n^2) all-pairs comparison:
  1. Stream CSV -> build tag->record_ids mapping + node metadata
  2. For each tag, emit edges between all record pairs that share it
  3. Threshold, cap, and return a CooccurrenceGraph

Memory optimization: edge accumulation uses numpy int32 arrays instead
of a Python Counter.  Pairs are batched, periodically consolidated via
np.unique, and pruned — keeping memory bounded even for 10M+ records.
"""

from __future__ import annotations

import random
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
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


def _pairs_from_sorted(arr: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Given a sorted 1-D int32 array, return (rows, cols) for all
    upper-triangle pairs.  Uses numpy broadcasting — much faster than
    itertools.combinations for arrays up to ~2000 elements."""
    n = len(arr)
    if n < 2:
        return np.empty(0, dtype=np.int32), np.empty(0, dtype=np.int32)
    idx_i, idx_j = np.triu_indices(n, k=1)
    return arr[idx_i], arr[idx_j]


def _consolidate(
    all_rows: List[np.ndarray],
    all_cols: List[np.ndarray],
    min_weight: int,
    prune: bool,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Concatenate batched (row, col) arrays, sum duplicate pairs via
    np.unique, and optionally prune singletons.

    Returns (rows, cols, weights) — all 1-D arrays of equal length.
    """
    if not all_rows:
        return (
            np.empty(0, dtype=np.int32),
            np.empty(0, dtype=np.int32),
            np.empty(0, dtype=np.int32),
        )

    rows = np.concatenate(all_rows)
    cols = np.concatenate(all_cols)

    # Pack (row, col) into a single int64 for fast grouping
    packed = rows.astype(np.int64) * np.int64(2_200_000_000) + cols.astype(np.int64)
    unique_packed, counts = np.unique(packed, return_counts=True)

    if prune and min_weight >= 2:
        mask = counts >= 2
        unique_packed = unique_packed[mask]
        counts = counts[mask]

    out_rows = (unique_packed // np.int64(2_200_000_000)).astype(np.int32)
    out_cols = (unique_packed % np.int64(2_200_000_000)).astype(np.int32)
    return out_rows, out_cols, counts.astype(np.int32)


def build_album_graph(
    records_csv: Path,
    *,
    tag_types: Optional[List[str]] = None,
    min_weight: int = 2,
    max_nodes: int = 0,
    max_edges: int = 0,
    max_tag_degree: int = 200,
    sample_high_degree: bool = True,
    unweighted: bool = False,
    max_edge_weight: int = 0,
    require_tags: Optional[Dict[str, List[str]]] = None,
    chunksize: int = 200_000,
    progress: bool = True,
) -> Tuple[CooccurrenceGraph, pd.DataFrame]:
    """Stream *records_csv* and build an album-centered similarity graph.

    Returns ``(graph, records_df)`` matching the contract of
    :func:`build_genre_graph`.

    Parameters
    ----------
    records_csv : Path
        Path to the records CSV (must have title, artist, genres/styles cols).
    tag_types : list[str] | None
        Which tag columns to use for building edges.  Accepts any
        combination of ``'genres'``, ``'styles'``, and ``'artists'``.
        Defaults to ``['styles']`` — edges are based on shared style tags
        only.  All tag types are still stored in node metadata regardless
        of this setting.
    min_weight : int
        Minimum number of shared tags to create an edge (default 2).
        Ignored when *unweighted* is True (any shared tag = edge).
    max_nodes : int
        Maximum number of unique album nodes.  Albums are ranked by
        the total number of distinct edge-building tags they carry
        (most diverse first) and capped *before* edge building, which
        dramatically reduces memory and time.  0 = unlimited.
    max_edges : int
        Cap edge count; 0 = unlimited.
    max_tag_degree : int
        Threshold for high-degree tags (default 200).  Behaviour depends
        on *sample_high_degree*.
    sample_high_degree : bool
        If True (default), tags exceeding *max_tag_degree* are randomly
        sampled down to *max_tag_degree* records so they still contribute
        edges.  If False, such tags are skipped entirely.
    unweighted : bool
        If True, edges carry no weight — any shared tag produces an edge.
        Equivalent to ``min_weight=1`` with all weights set to 1.
        Much faster and uses less memory since duplicate pairs need not
        be summed.
    max_edge_weight : int
        Cap edge weights at this value; 0 = unlimited.  Ignored when
        *unweighted* is True.
    chunksize : int
        CSV read chunk size.
    progress : bool
        Show tqdm progress bars.
    """
    if tag_types is None:
        tag_types = ["styles"]
    use_genres = "genres" in tag_types
    use_styles = "styles" in tag_types
    use_artists = "artists" in tag_types
    if unweighted:
        min_weight = 1

    # ------------------------------------------------------------------
    # Pass 1a: Stream CSV, collect unique albums + their edge tag sets
    # ------------------------------------------------------------------
    node_meta: Dict[str, Dict[str, Any]] = {}
    node_edge_tags: Dict[str, Set[str]] = {}  # record_id → set of edge tags
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
        tcol = _detect_col(colmap, "title", "album", "record")
        acol = _detect_col(colmap, "artist", "artists")
        ycol = _detect_col(colmap, "release_year", "year", "released")

        for idx in range(len(chunk)):
            row = chunk.iloc[idx]
            tval = str(row[tcol]).strip() if tcol and pd.notna(row[tcol]) else ""
            aval = str(row[acol]).strip() if acol and pd.notna(row[acol]) else ""

            if not tval:
                continue
            if is_skip_artist(aval):
                continue

            record_id = f"{tval.lower()}::{aval.lower()}"
            if record_id in node_meta:
                continue

            gval = row[gcol] if gcol else ""
            sval = row[scol] if scol else ""
            yval = row[ycol] if ycol else None

            genre_tags = split_tags(gval)
            style_tags = split_tags(sval)
            artist_tags = split_tags(aval) if use_artists else []

            # Tags used for edge building (controlled by tag_types)
            edge_tags: List[str] = []
            if use_genres:
                edge_tags.extend(genre_tags)
            if use_styles:
                edge_tags.extend(style_tags)
            if use_artists:
                edge_tags.extend(artist_tags)
            if not edge_tags:
                continue

            node_edge_tags[record_id] = set(edge_tags)

            # Node metadata always includes all tag types
            node_meta[record_id] = {
                "id": record_id,
                "label": tval,
                "artist": aval,
                "genres": " | ".join(pretty_label(t) for t in genre_tags),
                "styles": " | ".join(pretty_label(t) for t in style_tags),
                "release_year": coerce_year(yval),
            }

    n_total = len(node_edge_tags)
    print(
        f"Pass 1a: {total_rows:,} rows scanned, "
        f"{n_total:,} unique albums with tags "
        f"(edge tag_types={tag_types})",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Pass 1a+: Filter by require_tags (after full tag accumulation)
    # ------------------------------------------------------------------
    if require_tags:
        before = len(node_edge_tags)
        keep_keys = [
            k for k, meta in node_meta.items()
            if matches_required_tags(
                [t.strip() for t in meta.get("genres", "").split("|") if t.strip()],
                [t.strip() for t in meta.get("styles", "").split("|") if t.strip()],
                meta.get("artist", ""),
                require_tags,
            )
        ]
        keep = set(keep_keys)
        node_edge_tags = {k: t for k, t in node_edge_tags.items() if k in keep}
        node_meta = {k: m for k, m in node_meta.items() if k in keep}
        n_total = len(node_edge_tags)
        print(
            f"  require_tags filter: {before:,} → {n_total:,} albums "
            f"(require={require_tags})",
            file=sys.stderr,
        )

    # ------------------------------------------------------------------
    # Pass 1b: Cap albums by tag diversity BEFORE building inverted index
    # ------------------------------------------------------------------
    if max_nodes > 0 and n_total > max_nodes:
        ranked = sorted(
            node_edge_tags.keys(),
            key=lambda k: len(node_edge_tags[k]),
            reverse=True,
        )
        keep = set(ranked[:max_nodes])
        node_edge_tags = {k: t for k, t in node_edge_tags.items() if k in keep}
        node_meta = {k: m for k, m in node_meta.items() if k in keep}
        print(
            f"Pass 1b: capped to {max_nodes:,} albums "
            f"(min tag count in kept set: "
            f"{min(len(t) for t in node_edge_tags.values())})",
            file=sys.stderr,
        )

    # ------------------------------------------------------------------
    # Pass 1c: Build inverted index from kept albums only
    # ------------------------------------------------------------------
    int_to_str: List[str] = sorted(node_edge_tags.keys())
    str_to_int: Dict[str, int] = {k: i for i, k in enumerate(int_to_str)}
    tag_to_ints: Dict[str, List[int]] = defaultdict(list)

    for record_id, tags in node_edge_tags.items():
        rid = str_to_int[record_id]
        for tag in tags:
            tag_to_ints[tag].append(rid)

    del node_edge_tags, str_to_int
    n_records = len(int_to_str)

    print(
        f"Pass 1c: {n_records:,} albums → {len(tag_to_ints):,} unique tags "
        f"in inverted index",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Pass 2: Derive edges from inverted index (numpy arrays)
    #
    # Pairs are accumulated as numpy int32 arrays in batches.  When the
    # batch buffer exceeds BATCH_CAP pairs, it is consolidated via
    # np.unique (which sums duplicate pairs) and pruned of singletons.
    # This keeps peak memory bounded regardless of graph scale.
    # ------------------------------------------------------------------
    BATCH_CAP = 30_000_000  # consolidate every ~30M raw pairs (~240 MB)
    batch_rows: List[np.ndarray] = []
    batch_cols: List[np.ndarray] = []
    batch_size = 0
    skipped_tags = 0
    sampled_tags = 0

    # Accumulated unique edges from prior consolidations
    acc_rows = np.empty(0, dtype=np.int32)
    acc_cols = np.empty(0, dtype=np.int32)
    acc_weights = np.empty(0, dtype=np.int32)
    consolidation_count = 0

    tag_items: Any = tag_to_ints.items()
    if progress:
        try:
            from tqdm import tqdm

            tag_items = tqdm(
                tag_items, total=len(tag_to_ints), desc="Building edges", unit="tag"
            )
        except ImportError:
            pass

    for _tag, rec_ints in tag_items:
        if len(rec_ints) > max_tag_degree:
            if sample_high_degree:
                rec_ints = random.sample(rec_ints, max_tag_degree)
                sampled_tags += 1
            else:
                skipped_tags += 1
                continue

        arr = np.array(sorted(rec_ints), dtype=np.int32)
        r, c = _pairs_from_sorted(arr)
        if len(r) == 0:
            continue
        batch_rows.append(r)
        batch_cols.append(c)
        batch_size += len(r)

        # Consolidate when batch is large enough
        if batch_size >= BATCH_CAP:
            # Include accumulated edges from previous consolidations
            if len(acc_rows) > 0:
                batch_rows.append(acc_rows)
                batch_cols.append(acc_cols)
                # Expand accumulated weights into repeated pairs so
                # np.unique re-sums them correctly
                rep_rows = np.repeat(acc_rows, acc_weights)
                rep_cols = np.repeat(acc_cols, acc_weights)
                # Replace last two appends with expanded versions
                batch_rows[-1] = rep_rows
                batch_cols[-1] = rep_cols

                # Actually, simpler approach: consolidate batch first,
                # then merge with accumulator
                batch_rows.pop()
                batch_cols.pop()

                b_r, b_c, b_w = _consolidate(
                    batch_rows, batch_cols, min_weight, prune=not unweighted
                )

                # Merge batch with accumulator
                if len(b_r) > 0:
                    m_rows = np.concatenate([acc_rows, b_r])
                    m_cols = np.concatenate([acc_cols, b_c])
                    m_weights = np.concatenate([acc_weights, b_w])

                    packed = m_rows.astype(np.int64) * np.int64(2_200_000_000) + m_cols.astype(np.int64)
                    order = np.argsort(packed)
                    packed = packed[order]
                    m_weights = m_weights[order]

                    # Sum weights for duplicate pairs
                    mask = np.empty(len(packed), dtype=np.bool_)
                    mask[0] = True
                    mask[1:] = packed[1:] != packed[:-1]

                    unique_packed = packed[mask]
                    summed_weights = np.zeros(mask.sum(), dtype=np.int32)
                    group_idx = np.cumsum(mask) - 1
                    np.add.at(summed_weights, group_idx, m_weights)

                    # Prune singletons
                    if not unweighted and min_weight >= 2:
                        keep = summed_weights >= 2
                        unique_packed = unique_packed[keep]
                        summed_weights = summed_weights[keep]

                    acc_rows = (unique_packed // np.int64(2_200_000_000)).astype(np.int32)
                    acc_cols = (unique_packed % np.int64(2_200_000_000)).astype(np.int32)
                    acc_weights = summed_weights
                else:
                    pass  # nothing from batch, keep accumulator
            else:
                acc_rows, acc_cols, acc_weights = _consolidate(
                    batch_rows, batch_cols, min_weight, prune=not unweighted
                )

            consolidation_count += 1
            print(
                f"  consolidated #{consolidation_count}: "
                f"{len(acc_rows):,} unique edges",
                file=sys.stderr,
            )
            batch_rows = []
            batch_cols = []
            batch_size = 0

    # Final consolidation of remaining batch
    if batch_size > 0 or len(acc_rows) > 0:
        if len(acc_rows) > 0 and batch_size > 0:
            b_r, b_c, b_w = _consolidate(
                batch_rows, batch_cols, min_weight, prune=not unweighted
            )
            if len(b_r) > 0:
                m_rows = np.concatenate([acc_rows, b_r])
                m_cols = np.concatenate([acc_cols, b_c])
                m_weights = np.concatenate([acc_weights, b_w])

                packed = m_rows.astype(np.int64) * np.int64(2_200_000_000) + m_cols.astype(np.int64)
                order = np.argsort(packed)
                packed = packed[order]
                m_weights = m_weights[order]

                mask = np.empty(len(packed), dtype=np.bool_)
                mask[0] = True
                mask[1:] = packed[1:] != packed[:-1]

                unique_packed = packed[mask]
                summed_weights = np.zeros(mask.sum(), dtype=np.int32)
                group_idx = np.cumsum(mask) - 1
                np.add.at(summed_weights, group_idx, m_weights)

                acc_rows = (unique_packed // np.int64(2_200_000_000)).astype(np.int32)
                acc_cols = (unique_packed % np.int64(2_200_000_000)).astype(np.int32)
                acc_weights = summed_weights
        elif batch_size > 0:
            acc_rows, acc_cols, acc_weights = _consolidate(
                batch_rows, batch_cols, min_weight, prune=not unweighted
            )

    del batch_rows, batch_cols, tag_to_ints

    print(
        f"Pass 2: {len(acc_rows):,} unique edges after consolidation, "
        f"{sampled_tags} tags sampled, "
        f"{skipped_tags} tags skipped (degree > {max_tag_degree})",
        file=sys.stderr,
    )

    # ------------------------------------------------------------------
    # Pass 3: Threshold, cap, build output
    # ------------------------------------------------------------------
    # Apply min_weight filter
    if min_weight > 1 and not unweighted:
        keep_mask = acc_weights >= min_weight
        acc_rows = acc_rows[keep_mask]
        acc_cols = acc_cols[keep_mask]
        acc_weights = acc_weights[keep_mask]

    # Apply max_edge_weight cap
    if max_edge_weight > 0 and not unweighted:
        np.clip(acc_weights, 0, max_edge_weight, out=acc_weights)

    # For unweighted mode, all weights are 1
    if unweighted:
        acc_weights = np.ones(len(acc_rows), dtype=np.int32)

    # Sort by weight descending for capping
    if len(acc_rows) > 0:
        sort_idx = np.argsort(-acc_weights)
        acc_rows = acc_rows[sort_idx]
        acc_cols = acc_cols[sort_idx]
        acc_weights = acc_weights[sort_idx]

    # Cap edges
    if max_edges > 0 and len(acc_rows) > max_edges:
        acc_rows = acc_rows[:max_edges]
        acc_cols = acc_cols[:max_edges]
        acc_weights = acc_weights[:max_edges]

    # Collect final node set
    node_int_ids: Set[int] = set()
    node_int_ids.update(acc_rows.tolist())
    node_int_ids.update(acc_cols.tolist())

    print(
        f"Pass 3: {len(node_int_ids):,} nodes, {len(acc_rows):,} edges "
        f"(min_weight={min_weight}, unweighted={unweighted})",
        file=sys.stderr,
    )

    # Build points
    points: List[Dict[str, Any]] = []
    for nid_int in sorted(node_int_ids):
        nid = int_to_str[nid_int]
        meta = node_meta.get(nid)
        if meta is None:
            continue
        pt: Dict[str, Any] = {"id": nid, "label": meta["label"]}
        if meta["artist"]:
            pt["artist"] = meta["artist"]
        if meta["genres"]:
            pt["genres"] = meta["genres"]
        if meta["styles"]:
            pt["styles"] = meta["styles"]
        if meta["release_year"] is not None:
            pt["release_year"] = meta["release_year"]
        pt["external_links"] = build_external_links(pt)
        points.append(pt)

    # Build links
    if unweighted:
        links: List[Dict[str, Any]] = [
            {"source": int_to_str[int(r)], "target": int_to_str[int(c)]}
            for r, c in zip(acc_rows, acc_cols)
        ]
    else:
        links = [
            {"source": int_to_str[int(r)], "target": int_to_str[int(c)], "weight": int(w)}
            for r, c, w in zip(acc_rows, acc_cols, acc_weights)
        ]

    graph = CooccurrenceGraph(points=points, links=links)

    # Records DataFrame for cache compatibility
    records_rows = [
        {
            "track_name": node_meta[int_to_str[nid_int]]["label"],
            "artist_name": node_meta[int_to_str[nid_int]]["artist"],
            "genres": node_meta[int_to_str[nid_int]]["genres"],
            "styles": node_meta[int_to_str[nid_int]]["styles"],
        }
        for nid_int in sorted(node_int_ids)
        if int_to_str[nid_int] in node_meta
    ]
    records_df = pd.DataFrame(records_rows)

    return graph, records_df
