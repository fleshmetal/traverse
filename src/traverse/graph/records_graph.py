"""Build a genre/style co-occurrence graph from a records CSV.

Streams the CSV in chunks, extracts genre/style tags per row, feeds them
into a :class:`CooccurrenceBuilder`, and returns the finished graph plus
a records DataFrame suitable for album lookup.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from traverse.graph.cooccurrence import CooccurrenceBuilder, CooccurrenceGraph
from traverse.processing.normalize import pretty_label, split_tags


def _detect_col(colmap: Dict[str, str], *candidates: str) -> Optional[str]:
    """Case-insensitive column lookup."""
    for c in candidates:
        if c in colmap:
            return colmap[c]
    return None


def build_records_graph(
    records_csv: Path,
    *,
    min_cooccurrence: int = 2,
    max_nodes: int = 0,
    max_edges: int = 0,
    chunksize: int = 200_000,
    progress: bool = True,
) -> Tuple[CooccurrenceGraph, pd.DataFrame]:
    """Stream *records_csv* and build a co-occurrence graph.

    Returns ``(graph, records_df)`` where *records_df* has columns
    ``track_name, artist_name, genres, styles`` for album lookup.
    """
    builder = CooccurrenceBuilder(
        min_cooccurrence=min_cooccurrence,
        max_nodes=max_nodes,
        max_edges=max_edges,
    )

    records_acc: List[Dict[str, str]] = []
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

        for idx in range(len(chunk)):
            row = chunk.iloc[idx]
            gval = row[gcol] if gcol else ""
            sval = row[scol] if scol else ""
            tval = row[tcol] if tcol else ""
            aval = row[acol] if acol else ""

            genre_tags = split_tags(gval)
            style_tags = split_tags(sval)
            tags = genre_tags + style_tags
            if not tags:
                continue

            tag_categories: Dict[str, str] = {}
            for t in set(genre_tags):
                tag_categories[t] = "genre"
            for t in set(style_tags):
                tag_categories[t] = "style"

            builder.add(tags, label_fn=pretty_label, tag_categories=tag_categories)

            records_acc.append(
                {
                    "track_name": str(tval) if pd.notna(tval) else "",
                    "artist_name": str(aval) if pd.notna(aval) else "",
                    "genres": " | ".join(pretty_label(t) for t in genre_tags),
                    "styles": " | ".join(pretty_label(t) for t in style_tags),
                }
            )

    import sys

    stats = builder.stats
    print(
        f"Scanned {total_rows:,} rows, {stats['rows_with_tags']:,} with tags, "
        f"{stats['unique_pairs']:,} unique edge keys",
        file=sys.stderr,
    )

    graph = builder.build()
    records_df = pd.DataFrame(records_acc)

    print(
        f"Graph: {len(graph['points'])} nodes, {len(graph['links'])} edges",
        file=sys.stderr,
    )
    return graph, records_df
