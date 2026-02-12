"""NetworkX community detection for co-occurrence graphs.

Converts a :class:`CooccurrenceGraph` to a NetworkX graph, runs community
detection, and writes the cluster assignments back onto the graph points.

Supported algorithms: Louvain, greedy modularity, label propagation,
Kernighan-Lin bisection, edge betweenness (Girvan-Newman), and k-clique.
"""
from __future__ import annotations

import copy
from enum import Enum
from itertools import islice
from typing import Any, Dict, List, Optional

import networkx as nx

from traverse.graph.cooccurrence import CooccurrenceGraph


class CommunityAlgorithm(str, Enum):
    """Supported community-detection algorithms."""

    LOUVAIN = "louvain"
    GREEDY_MODULARITY = "greedy_modularity"
    LABEL_PROPAGATION = "label_propagation"
    KERNIGHAN_LIN = "kernighan_lin"
    EDGE_BETWEENNESS = "edge_betweenness"
    K_CLIQUE = "k_clique"


def cooccurrence_to_networkx(graph: CooccurrenceGraph) -> nx.Graph:
    """Convert a :class:`CooccurrenceGraph` to a weighted undirected NetworkX graph.

    All node and edge attributes (label, first_seen, category, weight, etc.)
    are preserved.  Distinct from :func:`adapters_networkx.to_networkx` which
    operates on DataFrame-based ``GraphTables``.
    """
    G = nx.Graph()
    for pt in graph.get("points", []):
        attrs = {k: v for k, v in pt.items() if k != "id"}
        G.add_node(pt["id"], **attrs)
    for lk in graph.get("links", []):
        attrs = {k: v for k, v in lk.items() if k not in ("source", "target")}
        G.add_edge(lk["source"], lk["target"], **attrs)
    return G


def detect_communities(
    G: nx.Graph,
    algorithm: CommunityAlgorithm = CommunityAlgorithm.LOUVAIN,
    *,
    resolution: float = 1.0,
    seed: Optional[int] = None,
    best_n: Optional[int] = None,
    k: Optional[int] = None,
) -> Dict[str, int]:
    """Run community detection and return ``{node_id: cluster_id}``.

    Cluster IDs are 0-indexed and sorted by community size descending
    (largest community = 0).

    Parameters
    ----------
    G : nx.Graph
        Weighted undirected graph.
    algorithm : CommunityAlgorithm
        Which algorithm to use.
    resolution : float
        Resolution parameter for Louvain and greedy modularity (higher = more
        communities).  Ignored by other algorithms.
    seed : int | None
        Random seed for Louvain, label propagation, and Kernighan-Lin.
    best_n : int | None
        Target number of communities for greedy modularity (``best_n`` param).
        For edge betweenness, the number of splits from the Girvan-Newman
        dendrogram (``best_n=4`` → take the 4th split, yielding up to 5
        communities).  Ignored by other algorithms.
    k : int | None
        Clique size for k-clique percolation.  Required when *algorithm* is
        ``K_CLIQUE``.  Ignored by other algorithms.
    """
    if algorithm == CommunityAlgorithm.LOUVAIN:
        communities = nx.community.louvain_communities(
            G, weight="weight", resolution=resolution, seed=seed,
        )
    elif algorithm == CommunityAlgorithm.GREEDY_MODULARITY:
        kwargs: Dict[str, Any] = {"weight": "weight", "resolution": resolution}
        if best_n is not None:
            kwargs["best_n"] = best_n
            kwargs["cutoff"] = 1
        communities = nx.community.greedy_modularity_communities(G, **kwargs)
    elif algorithm == CommunityAlgorithm.LABEL_PROPAGATION:
        communities = nx.community.asyn_lpa_communities(
            G, weight="weight", seed=seed,
        )
    elif algorithm == CommunityAlgorithm.KERNIGHAN_LIN:
        # Recursive bisection: produces exactly 2 partitions.
        communities = nx.community.kernighan_lin_bisection(
            G, weight="weight", seed=seed,
        )
    elif algorithm == CommunityAlgorithm.EDGE_BETWEENNESS:
        # Girvan-Newman iteratively removes highest-betweenness edges.
        # Each step yields one more community; take the (best_n)th step.
        n_splits = best_n if best_n is not None else 1
        gn_iter = nx.community.girvan_newman(G)
        communities = next(islice(gn_iter, n_splits - 1, n_splits), next(nx.community.girvan_newman(G)))
    elif algorithm == CommunityAlgorithm.K_CLIQUE:
        if k is None:
            raise ValueError("k is required for K_CLIQUE algorithm")
        communities = list(nx.community.k_clique_communities(G, k))
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")

    # Sort by community size descending, assign 0-indexed IDs
    sorted_comms = sorted(communities, key=len, reverse=True)
    assignments: Dict[str, int] = {}
    for cluster_id, members in enumerate(sorted_comms):
        for node in members:
            assignments[node] = cluster_id
    return assignments


def apply_communities(
    graph: CooccurrenceGraph,
    assignments: Dict[str, int],
    *,
    field: str = "community",
) -> CooccurrenceGraph:
    """Return a new graph with community labels added to each point.

    Does *not* mutate the original graph.

    Parameters
    ----------
    graph : CooccurrenceGraph
        Original graph.
    assignments : dict
        ``{node_id: cluster_id}`` from :func:`detect_communities`.
    field : str
        Name of the field to add on each point (default ``"community"``).
    """
    new_points: List[Dict[str, Any]] = []
    for pt in graph["points"]:
        new_pt = dict(pt)
        node_id = new_pt["id"]
        if node_id in assignments:
            new_pt[field] = assignments[node_id]
        new_points.append(new_pt)
    return CooccurrenceGraph(points=new_points, links=copy.deepcopy(graph["links"]))


def add_communities(
    graph: CooccurrenceGraph,
    algorithm: CommunityAlgorithm = CommunityAlgorithm.LOUVAIN,
    *,
    field: str = "community",
    resolution: float = 1.0,
    seed: Optional[int] = None,
    best_n: Optional[int] = None,
    k: Optional[int] = None,
) -> CooccurrenceGraph:
    """One-call convenience: convert → detect → apply.

    Returns a new :class:`CooccurrenceGraph` with the ``field`` set on each
    point.  See :func:`detect_communities` for parameter docs.
    """
    G = cooccurrence_to_networkx(graph)
    assignments = detect_communities(
        G, algorithm, resolution=resolution, seed=seed, best_n=best_n, k=k,
    )
    return apply_communities(graph, assignments, field=field)
