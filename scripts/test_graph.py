# scripts/test_graph.py
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from traverse.data.spotify_export import SpotifyExtendedExport
from traverse.processing.enrich_fast import FastGenreStyleEnricher
from traverse.processing.tables import BuildCanonicalTables
from traverse.graph.builder import GraphBuilder, GraphTables
from traverse.graph.adapters_webgl import WebGLJSONAdapter


def main(
    extended_dir: str,
    records_csv: str | None,
    chunksize: int,
    progress: bool,
    out_json: str,
    agg: str,
    min_weight: float,
) -> None:
    # 1) Load Spotify extended
    sx = SpotifyExtendedExport(extended_dir, progress=progress)
    t_ext = sx.load()
    print(
        f"[extended] plays={len(t_ext.get('plays', pd.DataFrame())):,} "
        f"tracks={len(t_ext.get('tracks', pd.DataFrame())):,} "
        f"artists={len(t_ext.get('artists', pd.DataFrame())):,}"
    )

    # 2) Optional fast genre/style enrichment from Records CSV
    tables = t_ext
    if records_csv:
        print("[enrich] FastGenreStyleEnricher (records CSV semi-join)…")
        enr = FastGenreStyleEnricher(
            records_csv=records_csv, progress=progress, chunksize=chunksize
        )
        tables = enr.run(tables)

    # 3) Canonical tables (plays_wide, tracks_wide)
    canon = BuildCanonicalTables()
    tables = canon.run(tables)
    pw = tables.get("plays_wide", pd.DataFrame())
    tw = tables.get("tracks_wide", pd.DataFrame())
    print(f"[canonical] plays_wide={len(pw):,} tracks_wide={len(tw):,}")

    # 4) Build graph
    gb = GraphBuilder(
        agg=("ms_played" if agg == "ms_played" else "play_count"), min_weight=min_weight
    )
    tables = gb.run(tables)

    # 5) Collect nodes/edges into a GraphTables object
    nodes = tables.get("graph_nodes", pd.DataFrame(columns=["id", "key", "label", "type"]))
    edges = tables.get("graph_edges", pd.DataFrame(columns=["src", "dst", "weight", "label"]))
    print(f"[graph] nodes={len(nodes):,} edges={len(edges):,}")

    graph: GraphTables = {"nodes": nodes, "edges": edges}

    # 6) Write WebGL/PyCosmograph JSON
    out_path = Path(out_json)
    WebGLJSONAdapter.write(graph, out_path, indent=None)
    print(f"[out] wrote {out_path} ({out_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Build WebGL-ready graph JSON from Spotify + Records.")
    ap.add_argument(
        "--extended-dir",
        required=True,
        help="Folder with Spotify ExtendedStreamingHistory JSON files",
    )
    ap.add_argument(
        "--records-csv", default=None, help="records.csv path for enrichment (optional)"
    )
    ap.add_argument(
        "--chunksize", type=int, default=200_000, help="CSV chunk size for records loader"
    )
    ap.add_argument("--progress", action="store_true", help="Show tqdm progress bars")
    ap.add_argument("--out-json", default=str(Path("_out") / "graph.json"), help="Output JSON path")
    ap.add_argument(
        "--agg",
        choices=["play_count", "ms_played"],
        default="play_count",
        help="Edge weight aggregation",
    )
    ap.add_argument(
        "--min-weight", type=float, default=1.0, help="Drop edges with weight < min-weight"
    )
    args = ap.parse_args()

    main(
        extended_dir=args.extended_dir,
        records_csv=args.records_csv,
        chunksize=args.chunksize,
        progress=args.progress,
        out_json=args.out_json,
        agg=args.agg,
        min_weight=args.min_weight,
    )
