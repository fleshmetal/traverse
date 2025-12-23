from __future__ import annotations
import argparse
from traverse.data.spotify_export import SpotifyExtendedExport
from traverse.processing.enrich_fast import FastGenreStyleEnricher
from traverse.processing.tables import BuildCanonicalTables
from traverse.graph.builder import GraphBuilder
from traverse.graph.adapters_networkx import to_networkx


def main(
    extended_dir: str, records_csv: str | None, chunksize: int, progress: bool, undirected: bool
):
    sx = SpotifyExtendedExport(extended_dir, progress=progress)
    t = sx.load()
    if records_csv:
        t = FastGenreStyleEnricher(
            records_csv=records_csv, chunksize=chunksize, progress=progress
        ).run(t)
    t = BuildCanonicalTables().run(t)
    t = GraphBuilder(agg="play_count", min_weight=2).run(t)

    nodes = t["graph_nodes"]
    edges = t["graph_edges"]
    G = to_networkx({"nodes": nodes, "edges": edges}, directed=not undirected)

    # Simple metrics
    print(f"nodes={G.number_of_nodes():,} edges={G.number_of_edges():,}")
    try:
        import networkx as nx

        deg = dict(G.degree())
        top = sorted(deg.items(), key=lambda kv: kv[1], reverse=True)[:10]
        print("top-degree:", top)
        bc = nx.betweenness_centrality(G, k=min(200, G.number_of_nodes()))
        topbc = sorted(bc.items(), key=lambda kv: kv[1], reverse=True)[:10]
        print("top-bc:", topbc)
    except Exception as e:
        print("centrality error:", e)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--extended-dir", required=True)
    ap.add_argument("--records-csv", default=None)
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--progress", action="store_true")
    ap.add_argument("--undirected", action="store_true")
    args = ap.parse_args()
    main(args.extended_dir, args.records_csv, args.chunksize, args.progress, args.undirected)
