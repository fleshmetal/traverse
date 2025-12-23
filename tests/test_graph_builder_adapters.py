# tests/test_graph_builder_adapters.py
import pandas as pd

from traverse.graph.builder import GraphBuilder
from traverse.graph.adapters_networkx import NetworkXAdapter


def test_graph_builder_nodes_edges_contract():
    # Minimal canonical tables
    plays_wide = pd.DataFrame(
        {
            "played_at": pd.to_datetime(["2021-01-01T00:00:00Z", "2021-01-01T00:01:00Z"], utc=True),
            "track_id": ["t1", "t1"],
            "ms_played": [1000, 2000],
            "genres": ["electronic | idm", "electronic | idm"],
            "track_name": ["Song A", "Song A"],
            "artist_name": ["Artist A", "Artist A"],
        }
    )
    tracks_wide = pd.DataFrame(
        {
            "track_id": ["t1"],
            "track_name": ["Song A"],
            "artist_name": ["Artist A"],
            "genres": ["electronic | idm"],
        }
    )

    gb = GraphBuilder(agg="play_count")
    g = gb.build(plays=plays_wide, tracks=tracks_wide)

    assert list(g.keys()) == ["nodes", "edges"]
    assert {"key", "id", "type"}.issubset(set(g["nodes"].columns))
    assert {"src", "dst", "weight"}.issubset(set(g["edges"].columns))
    # expect 1 track node, 2 genre nodes
    assert (g["nodes"]["type"] == "track").sum() == 1
    assert (g["nodes"]["type"] == "genre").sum() == 2
    # edges connect track to both genres
    assert len(g["edges"]) == 2

    G = NetworkXAdapter.to_networkx(g)
    stats = NetworkXAdapter.basic_stats(G)
    assert stats["nodes"] >= 3
    assert stats["edges"] >= 2
