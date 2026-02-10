from __future__ import annotations

import json
from pathlib import Path

from traverse.graph.cooccurrence import CooccurrenceBuilder
from traverse.graph.adapters_cosmograph import CosmographAdapter


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
