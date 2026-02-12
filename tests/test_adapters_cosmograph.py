from __future__ import annotations

import json
from pathlib import Path

from traverse.graph.cooccurrence import CooccurrenceBuilder
from traverse.graph.adapters_cosmograph import CosmographAdapter, detect_cluster_field


def test_schema_has_points_and_links() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"], timestamp_ms=1000)
    graph = b.build()

    payload = CosmographAdapter.to_json_dict(graph)
    assert "points" in payload
    assert "links" in payload
    # Must NOT be the WebGL format
    assert "nodes" not in payload
    assert "edges" not in payload

    assert payload["points"][0]["id"] in ("pop", "rock")
    assert payload["links"][0]["source"] in ("pop", "rock")
    assert "weight" in payload["links"][0]


def test_dumps_is_valid_json() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    graph = b.build()
    s = CosmographAdapter.dumps(graph)
    d = json.loads(s)
    assert "points" in d
    assert "links" in d


def test_write_creates_file(tmp_path: Path) -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    graph = b.build()
    out = CosmographAdapter.write(graph, tmp_path / "test.json")
    assert out.exists()
    d = json.loads(out.read_text(encoding="utf-8"))
    assert "points" in d


def test_empty_graph() -> None:
    graph = {"points": [], "links": []}
    payload = CosmographAdapter.to_json_dict(graph)  # type: ignore[arg-type]
    assert payload == {"points": [], "links": []}


def test_timeline_fields_present() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["a", "b"], timestamp_ms=5000)
    graph = b.build()
    payload = CosmographAdapter.to_json_dict(graph)
    for pt in payload["points"]:
        assert "first_seen" in pt
    for lk in payload["links"]:
        assert "first_seen" in lk


def test_meta_included_in_output() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    graph = b.build()
    meta = {"clusterField": "category", "version": 2}
    payload = CosmographAdapter.to_json_dict(graph, meta=meta)
    assert payload["meta"] == meta
    assert "points" in payload
    assert "links" in payload


def test_no_meta_means_no_meta_key() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    graph = b.build()
    payload = CosmographAdapter.to_json_dict(graph)
    assert "meta" not in payload


def test_meta_in_dumps() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    graph = b.build()
    s = CosmographAdapter.dumps(graph, meta={"clusterField": "category"})
    d = json.loads(s)
    assert d["meta"]["clusterField"] == "category"


def test_meta_in_write(tmp_path: Path) -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    graph = b.build()
    out = CosmographAdapter.write(graph, tmp_path / "meta.json", meta={"clusterField": "category"})
    d = json.loads(out.read_text(encoding="utf-8"))
    assert d["meta"]["clusterField"] == "category"
    assert "points" in d


def test_detect_cluster_field_with_category() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"], tag_categories={"rock": "genre", "pop": "style"})
    graph = b.build()
    assert detect_cluster_field(graph) == "category"


def test_detect_cluster_field_without_category() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    graph = b.build()
    assert detect_cluster_field(graph) is None


def test_detect_cluster_field_with_community() -> None:
    graph = {
        "points": [
            {"id": "rock", "label": "Rock", "community": 0},
            {"id": "pop", "label": "Pop", "community": 1},
        ],
        "links": [{"source": "rock", "target": "pop", "weight": 5}],
    }
    assert detect_cluster_field(graph) == "community"


def test_detect_cluster_field_category_over_community() -> None:
    """``category`` takes priority over ``community`` when both are present."""
    graph = {
        "points": [
            {"id": "rock", "label": "Rock", "category": "genre", "community": 0},
            {"id": "pop", "label": "Pop", "category": "style", "community": 1},
        ],
        "links": [{"source": "rock", "target": "pop", "weight": 5}],
    }
    assert detect_cluster_field(graph) == "category"
