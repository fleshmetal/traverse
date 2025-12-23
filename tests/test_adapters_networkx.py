import pandas as pd
from traverse.graph.adapters_networkx import to_networkx


def test_networkx_adapter():
    nodes = pd.DataFrame(
        [
            {"id": "trk:a", "key": "trk:a", "label": "Song A", "type": "track"},
            {"id": "art:1", "key": "art:1", "label": "Alpha", "type": "artist"},
        ]
    )
    edges = pd.DataFrame([{"src": "trk:a", "dst": "art:1", "weight": 2.0, "label": "plays"}])
    G = to_networkx({"nodes": nodes, "edges": edges}, directed=False)
    assert set(G.nodes()) == {"trk:a", "art:1"}
    assert G["trk:a"]["art:1"]["weight"] == 2.0
