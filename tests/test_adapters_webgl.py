import pandas as pd
from traverse.graph.adapters_webgl import WebGLJSONAdapter


def test_webgl_json_adapter_schema():
    nodes = pd.DataFrame([{"id": "trk:a", "key": "trk:a", "label": "Song A", "type": "track"}])
    edges = pd.DataFrame([{"src": "trk:a", "dst": "art:1", "weight": 3.0, "label": "plays"}])
    payload = WebGLJSONAdapter.to_json_dict({"nodes": nodes, "edges": edges})
    assert "nodes" in payload and "edges" in payload
    assert payload["nodes"][0]["id"] == "trk:a"
    assert payload["edges"][0]["source"] == "trk:a"
