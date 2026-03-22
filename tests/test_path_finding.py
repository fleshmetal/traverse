from __future__ import annotations

import pytest

from traverse.graph.cooccurrence import CooccurrenceGraph
from traverse.graph.path_finding import find_community_paths


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _two_cluster_graph() -> CooccurrenceGraph:
    """Two dense cliques (A-B-C and D-E-F) connected by one weak edge (C-D)."""
    points = [
        {"id": "a", "label": "A"},
        {"id": "b", "label": "B"},
        {"id": "c", "label": "C"},
        {"id": "d", "label": "D"},
        {"id": "e", "label": "E"},
        {"id": "f", "label": "F"},
    ]
    links = [
        # Clique 1
        {"source": "a", "target": "b", "weight": 10},
        {"source": "a", "target": "c", "weight": 10},
        {"source": "b", "target": "c", "weight": 10},
        # Clique 2
        {"source": "d", "target": "e", "weight": 10},
        {"source": "d", "target": "f", "weight": 10},
        {"source": "e", "target": "f", "weight": 10},
        # Weak bridge
        {"source": "c", "target": "d", "weight": 1},
    ]
    return CooccurrenceGraph(points=points, links=links)


def _disconnected_graph() -> CooccurrenceGraph:
    """Two completely disconnected cliques."""
    points = [
        {"id": "a", "label": "A"},
        {"id": "b", "label": "B"},
        {"id": "c", "label": "C"},
        {"id": "d", "label": "D"},
    ]
    links = [
        {"source": "a", "target": "b", "weight": 5},
        {"source": "c", "target": "d", "weight": 5},
    ]
    return CooccurrenceGraph(points=points, links=links)


def _chain_graph() -> CooccurrenceGraph:
    """Linear chain: A - B - C - D - E."""
    points = [{"id": n, "label": n.upper()} for n in "abcde"]
    links = [
        {"source": "a", "target": "b", "weight": 1},
        {"source": "b", "target": "c", "weight": 1},
        {"source": "c", "target": "d", "weight": 1},
        {"source": "d", "target": "e", "weight": 1},
    ]
    return CooccurrenceGraph(points=points, links=links)


@pytest.fixture
def two_cluster_graph() -> CooccurrenceGraph:
    return _two_cluster_graph()


@pytest.fixture
def disconnected_graph() -> CooccurrenceGraph:
    return _disconnected_graph()


@pytest.fixture
def chain_graph() -> CooccurrenceGraph:
    return _chain_graph()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFindCommunityPaths:
    """Tests for find_community_paths()."""

    def test_shortest_path_found(self, two_cluster_graph: CooccurrenceGraph) -> None:
        result = find_community_paths(
            two_cluster_graph,
            community_a_ids={"a", "b", "c"},
            community_b_ids={"d", "e", "f"},
        )
        assert result["pathCount"] >= 1
        shortest = next(p for p in result["paths"] if p["pathType"] == "shortest")
        assert shortest["length"] >= 1
        assert len(shortest["nodes"]) == shortest["length"] + 1
        assert shortest["nodes"][0] in {"a", "b", "c"}
        assert shortest["nodes"][-1] in {"d", "e", "f"}
        assert result["message"] is None

    def test_labels_populated(self, two_cluster_graph: CooccurrenceGraph) -> None:
        result = find_community_paths(
            two_cluster_graph,
            community_a_ids={"a", "b", "c"},
            community_b_ids={"d", "e", "f"},
        )
        for path in result["paths"]:
            assert len(path["labels"]) == len(path["nodes"])
            # Labels should be uppercase versions of the ids
            for node, label in zip(path["nodes"], path["labels"]):
                assert label == node.upper()

    def test_disconnected_returns_no_path(self, disconnected_graph: CooccurrenceGraph) -> None:
        result = find_community_paths(
            disconnected_graph,
            community_a_ids={"a", "b"},
            community_b_ids={"c", "d"},
        )
        assert result["pathCount"] == 0
        assert result["paths"] == []
        assert result["message"] is not None
        assert "No path" in result["message"]

    def test_restrict_to_communities_no_path(self, chain_graph: CooccurrenceGraph) -> None:
        """When restricted and communities don't share a direct edge, no path should be found."""
        result = find_community_paths(
            chain_graph,
            community_a_ids={"a"},
            community_b_ids={"e"},
            restrict_to_communities=True,
        )
        # a and e have no direct edge; intermediate nodes b,c,d are excluded
        assert result["pathCount"] == 0

    def test_restrict_to_communities_with_path(self, chain_graph: CooccurrenceGraph) -> None:
        """When restricted but communities include adjacent nodes, path should be found."""
        result = find_community_paths(
            chain_graph,
            community_a_ids={"a", "b"},
            community_b_ids={"c", "d"},
            restrict_to_communities=True,
        )
        assert result["pathCount"] >= 1
        shortest = next(p for p in result["paths"] if p["pathType"] == "shortest")
        # b-c is the bridge between the two sets
        assert "b" in shortest["nodes"]
        assert "c" in shortest["nodes"]

    def test_unrestricted_uses_intermediate_nodes(self, chain_graph: CooccurrenceGraph) -> None:
        result = find_community_paths(
            chain_graph,
            community_a_ids={"a"},
            community_b_ids={"e"},
            restrict_to_communities=False,
        )
        assert result["pathCount"] >= 1
        shortest = next(p for p in result["paths"] if p["pathType"] == "shortest")
        assert shortest["nodes"] == ["a", "b", "c", "d", "e"]
        assert shortest["length"] == 4

    def test_total_weight_calculated(self, two_cluster_graph: CooccurrenceGraph) -> None:
        result = find_community_paths(
            two_cluster_graph,
            community_a_ids={"a", "b", "c"},
            community_b_ids={"d", "e", "f"},
        )
        for path in result["paths"]:
            assert path["totalWeight"] > 0

    def test_diverse_longest_differs_from_shortest(self, two_cluster_graph: CooccurrenceGraph) -> None:
        result = find_community_paths(
            two_cluster_graph,
            community_a_ids={"a", "b", "c"},
            community_b_ids={"d", "e", "f"},
        )
        if result["pathCount"] == 2:
            shortest = next(p for p in result["paths"] if p["pathType"] == "shortest")
            longest = next(p for p in result["paths"] if p["pathType"] == "diverse_longest")
            assert longest["nodes"] != shortest["nodes"]
            assert longest["length"] >= shortest["length"]

    def test_max_diverse_attempts_capped(self, two_cluster_graph: CooccurrenceGraph) -> None:
        """max_diverse_attempts > 500 should be capped."""
        result = find_community_paths(
            two_cluster_graph,
            community_a_ids={"a", "b", "c"},
            community_b_ids={"d", "e", "f"},
            max_diverse_attempts=9999,
        )
        # Should not error; just caps internally
        assert result["pathCount"] >= 1

    def test_same_community_overlapping_ids(self, two_cluster_graph: CooccurrenceGraph) -> None:
        """If communities share nodes, should still work."""
        result = find_community_paths(
            two_cluster_graph,
            community_a_ids={"a", "b", "c"},
            community_b_ids={"c", "d", "e"},
        )
        assert result["pathCount"] >= 1
