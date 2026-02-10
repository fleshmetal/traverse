from __future__ import annotations

from traverse.graph.cooccurrence import CooccurrenceBuilder
from traverse.processing.normalize import pretty_label


def test_basic_cooccurrence() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop", "jazz"])
    b.add(["rock", "pop"])
    result = b.build()
    links = {(lk["source"], lk["target"]): lk["weight"] for lk in result["links"]}
    assert links[("pop", "rock")] == 2
    assert links[("jazz", "pop")] == 1
    assert links[("jazz", "rock")] == 1


def test_min_cooccurrence_threshold() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=2)
    b.add(["rock", "pop", "jazz"])
    b.add(["rock", "pop"])
    result = b.build()
    assert len(result["links"]) == 1
    assert result["links"][0]["source"] == "pop"
    assert result["links"][0]["target"] == "rock"


def test_max_nodes_caps() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1, max_nodes=2)
    b.add(["a", "b", "c"])
    b.add(["a", "b"])
    b.add(["a", "c"])
    result = b.build()
    ids = {p["id"] for p in result["points"]}
    assert "a" in ids
    assert len(ids) <= 2


def test_max_edges_caps() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1, max_edges=1)
    b.add(["rock", "pop", "jazz"])
    b.add(["rock", "pop"])
    result = b.build()
    assert len(result["links"]) <= 1


def test_first_seen_tracking() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"], timestamp_ms=2000)
    b.add(["rock", "pop"], timestamp_ms=1000)
    result = b.build()
    point_fs = {p["id"]: p.get("first_seen") for p in result["points"]}
    assert point_fs["rock"] == 1000
    assert point_fs["pop"] == 1000
    assert result["links"][0]["first_seen"] == 1000


def test_label_fn() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["idm", "edm"], label_fn=pretty_label)
    result = b.build()
    labels = {p["id"]: p["label"] for p in result["points"]}
    assert labels["idm"] == "IDM"
    assert labels["edm"] == "EDM"


def test_empty_build() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    result = b.build()
    assert result["points"] == []
    assert result["links"] == []


def test_single_tag_no_pairs() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock"])
    result = b.build()
    assert result["links"] == []
    assert result["points"] == []


def test_stats() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    b.add([])
    b.add(["jazz"])
    assert b.stats["rows_seen"] == 3
    assert b.stats["rows_with_tags"] == 2
    assert b.stats["rows_with_pairs"] == 1


def test_no_timestamp_means_no_first_seen() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    result = b.build()
    assert "first_seen" not in result["points"][0]
    assert "first_seen" not in result["links"][0]


def test_reset() -> None:
    b = CooccurrenceBuilder(min_cooccurrence=1)
    b.add(["rock", "pop"])
    b.reset()
    result = b.build()
    assert result["points"] == []
    assert result["links"] == []
    assert b.stats["rows_seen"] == 0
