from __future__ import annotations

import pytest

from traverse.graph.cooccurrence import CooccurrenceGraph
from traverse.graph.community import (
    CommunityAlgorithm,
    add_communities,
    apply_communities,
    cooccurrence_to_networkx,
    detect_communities,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _two_cluster_graph() -> CooccurrenceGraph:
    """Two dense cliques (A-B-C and D-E-F) connected by one weak edge (C-D).

    Community detection should consistently discover the two groups.
    """
    points = [
        {"id": "a", "label": "A"},
        {"id": "b", "label": "B"},
        {"id": "c", "label": "C", "first_seen": 1000},
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


@pytest.fixture
def two_cluster_graph() -> CooccurrenceGraph:
    return _two_cluster_graph()


# ---------------------------------------------------------------------------
# cooccurrence_to_networkx
# ---------------------------------------------------------------------------


class TestCooccurrenceToNetworkx:
    def test_basic_conversion(self, two_cluster_graph: CooccurrenceGraph) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assert G.number_of_nodes() == 6
        assert G.number_of_edges() == 7

    def test_empty_graph(self) -> None:
        G = cooccurrence_to_networkx(CooccurrenceGraph(points=[], links=[]))
        assert G.number_of_nodes() == 0
        assert G.number_of_edges() == 0

    def test_attribute_preservation(self, two_cluster_graph: CooccurrenceGraph) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assert G.nodes["a"]["label"] == "A"
        assert G.nodes["c"]["first_seen"] == 1000
        assert G.edges["a", "b"]["weight"] == 10
        assert G.edges["c", "d"]["weight"] == 1


# ---------------------------------------------------------------------------
# detect_communities
# ---------------------------------------------------------------------------


class TestDetectCommunities:
    def test_louvain_finds_two_clusters(self, two_cluster_graph: CooccurrenceGraph) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assignments = detect_communities(G, CommunityAlgorithm.LOUVAIN, seed=42)
        assert len(assignments) == 6
        # a, b, c in same cluster; d, e, f in same cluster
        assert assignments["a"] == assignments["b"] == assignments["c"]
        assert assignments["d"] == assignments["e"] == assignments["f"]
        assert assignments["a"] != assignments["d"]

    def test_greedy_modularity_finds_two_clusters(
        self, two_cluster_graph: CooccurrenceGraph
    ) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assignments = detect_communities(G, CommunityAlgorithm.GREEDY_MODULARITY)
        assert len(assignments) == 6
        assert assignments["a"] == assignments["b"] == assignments["c"]
        assert assignments["d"] == assignments["e"] == assignments["f"]
        assert assignments["a"] != assignments["d"]

    def test_label_propagation_finds_two_clusters(
        self, two_cluster_graph: CooccurrenceGraph
    ) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assignments = detect_communities(
            G, CommunityAlgorithm.LABEL_PROPAGATION, seed=42,
        )
        assert len(assignments) == 6
        assert assignments["a"] == assignments["b"] == assignments["c"]
        assert assignments["d"] == assignments["e"] == assignments["f"]
        assert assignments["a"] != assignments["d"]

    def test_all_nodes_assigned(self, two_cluster_graph: CooccurrenceGraph) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assignments = detect_communities(G, CommunityAlgorithm.LOUVAIN, seed=42)
        assert set(assignments.keys()) == {"a", "b", "c", "d", "e", "f"}

    def test_zero_indexed(self, two_cluster_graph: CooccurrenceGraph) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assignments = detect_communities(G, CommunityAlgorithm.LOUVAIN, seed=42)
        assert min(assignments.values()) == 0

    def test_louvain_resolution(self, two_cluster_graph: CooccurrenceGraph) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        low_res = detect_communities(
            G, CommunityAlgorithm.LOUVAIN, resolution=0.1, seed=42,
        )
        high_res = detect_communities(
            G, CommunityAlgorithm.LOUVAIN, resolution=10.0, seed=42,
        )
        # Higher resolution should produce >= as many communities
        assert max(high_res.values()) >= max(low_res.values())

    def test_greedy_modularity_best_n(self, two_cluster_graph: CooccurrenceGraph) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assignments = detect_communities(
            G, CommunityAlgorithm.GREEDY_MODULARITY, best_n=2,
        )
        n_communities = len(set(assignments.values()))
        assert n_communities == 2

    def test_kernighan_lin_finds_two_clusters(
        self, two_cluster_graph: CooccurrenceGraph
    ) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assignments = detect_communities(G, CommunityAlgorithm.KERNIGHAN_LIN, seed=42)
        assert len(assignments) == 6
        assert assignments["a"] == assignments["b"] == assignments["c"]
        assert assignments["d"] == assignments["e"] == assignments["f"]
        assert assignments["a"] != assignments["d"]

    def test_edge_betweenness_finds_two_clusters(
        self, two_cluster_graph: CooccurrenceGraph
    ) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        assignments = detect_communities(G, CommunityAlgorithm.EDGE_BETWEENNESS)
        assert len(assignments) == 6
        assert assignments["a"] == assignments["b"] == assignments["c"]
        assert assignments["d"] == assignments["e"] == assignments["f"]
        assert assignments["a"] != assignments["d"]

    def test_edge_betweenness_best_n(self, two_cluster_graph: CooccurrenceGraph) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        # best_n=2 means take 2nd split → up to 3 communities
        assignments = detect_communities(
            G, CommunityAlgorithm.EDGE_BETWEENNESS, best_n=2,
        )
        assert len(set(assignments.values())) >= 2

    def test_k_clique_finds_two_clusters(
        self, two_cluster_graph: CooccurrenceGraph
    ) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        # k=3: each triangle is a 3-clique, the two cliques are disjoint
        assignments = detect_communities(G, CommunityAlgorithm.K_CLIQUE, k=3)
        assert len(assignments) == 6
        assert assignments["a"] == assignments["b"] == assignments["c"]
        assert assignments["d"] == assignments["e"] == assignments["f"]
        assert assignments["a"] != assignments["d"]

    def test_k_clique_requires_k(self, two_cluster_graph: CooccurrenceGraph) -> None:
        G = cooccurrence_to_networkx(two_cluster_graph)
        with pytest.raises(ValueError, match="k is required"):
            detect_communities(G, CommunityAlgorithm.K_CLIQUE)


# ---------------------------------------------------------------------------
# apply_communities
# ---------------------------------------------------------------------------


class TestApplyCommunities:
    def test_adds_field(self, two_cluster_graph: CooccurrenceGraph) -> None:
        assignments = {"a": 0, "b": 0, "c": 0, "d": 1, "e": 1, "f": 1}
        result = apply_communities(two_cluster_graph, assignments)
        for pt in result["points"]:
            assert "community" in pt

    def test_custom_field_name(self, two_cluster_graph: CooccurrenceGraph) -> None:
        assignments = {"a": 0, "b": 0, "c": 0, "d": 1, "e": 1, "f": 1}
        result = apply_communities(two_cluster_graph, assignments, field="cluster")
        for pt in result["points"]:
            assert "cluster" in pt
            assert "community" not in pt

    def test_does_not_mutate_original(self, two_cluster_graph: CooccurrenceGraph) -> None:
        assignments = {"a": 0, "b": 0, "c": 0, "d": 1, "e": 1, "f": 1}
        apply_communities(two_cluster_graph, assignments)
        for pt in two_cluster_graph["points"]:
            assert "community" not in pt

    def test_preserves_existing_fields(self, two_cluster_graph: CooccurrenceGraph) -> None:
        assignments = {"a": 0, "b": 0, "c": 0, "d": 1, "e": 1, "f": 1}
        result = apply_communities(two_cluster_graph, assignments)
        c_point = next(pt for pt in result["points"] if pt["id"] == "c")
        assert c_point["first_seen"] == 1000
        assert c_point["label"] == "C"
        assert c_point["community"] == 0


# ---------------------------------------------------------------------------
# add_communities (convenience)
# ---------------------------------------------------------------------------


class TestAddCommunities:
    def test_end_to_end(self, two_cluster_graph: CooccurrenceGraph) -> None:
        result = add_communities(
            two_cluster_graph, CommunityAlgorithm.LOUVAIN, seed=42,
        )
        communities = {pt["id"]: pt["community"] for pt in result["points"]}
        assert communities["a"] == communities["b"] == communities["c"]
        assert communities["d"] == communities["e"] == communities["f"]
        assert communities["a"] != communities["d"]
