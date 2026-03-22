"""Cross-community path finding for co-occurrence graphs.

Finds shortest and diverse-longest paths between two communities,
optionally restricting traversal to only nodes within those communities.
"""

from __future__ import annotations

import random
from typing import Any, Dict, List, Set, Tuple

import networkx as nx

from traverse.graph.cooccurrence import CooccurrenceGraph


def _cooccurrence_to_nx(graph: CooccurrenceGraph) -> nx.Graph[Any]:
    """Lightweight converter (same logic as edge_analysis._cooccurrence_to_nx)."""
    G: nx.Graph[Any] = nx.Graph()
    for pt in graph.get("points", []):
        attrs = {k: v for k, v in pt.items() if k != "id"}
        G.add_node(pt["id"], **attrs)
    for lk in graph.get("links", []):
        attrs = {k: v for k, v in lk.items() if k not in ("source", "target")}
        G.add_edge(lk["source"], lk["target"], **attrs)
    return G


def _label_map(graph: CooccurrenceGraph) -> Dict[str, str]:
    """Build node-id → label mapping."""
    return {p["id"]: p.get("label", p["id"]) for p in graph.get("points", [])}


def _path_weight(G: nx.Graph[Any], path: List[str]) -> float:
    """Sum edge weights along a path."""
    total = 0.0
    for i in range(len(path) - 1):
        data = G.get_edge_data(path[i], path[i + 1])
        total += data.get("weight", 1.0) if data else 1.0
    return round(total, 4)


def _shortest_path_via_virtuals(
    G: nx.Graph[Any],
    community_a_ids: Set[str],
    community_b_ids: Set[str],
) -> List[str] | None:
    """Find shortest path between two communities using virtual source/target nodes."""
    _SRC = "__virtual_src__"
    _TGT = "__virtual_tgt__"

    G.add_node(_SRC)
    G.add_node(_TGT)
    for nid in community_a_ids:
        if G.has_node(nid):
            G.add_edge(_SRC, nid, weight=0)
    for nid in community_b_ids:
        if G.has_node(nid):
            G.add_edge(nid, _TGT, weight=0)

    try:
        path = nx.shortest_path(G, _SRC, _TGT, weight="weight")
        # Strip virtual nodes
        return [n for n in path if n not in (_SRC, _TGT)]
    except nx.NetworkXNoPath:
        return None
    finally:
        G.remove_node(_SRC)
        G.remove_node(_TGT)


def _diverse_longest_path(
    G: nx.Graph[Any],
    community_a_ids: Set[str],
    community_b_ids: Set[str],
    max_attempts: int = 200,
) -> List[str] | None:
    """Sample random (source, target) pairs to find the most-hops path."""
    a_nodes = [n for n in community_a_ids if G.has_node(n)]
    b_nodes = [n for n in community_b_ids if G.has_node(n)]

    if not a_nodes or not b_nodes:
        return None

    # Generate pairs
    pairs: List[Tuple[str, str]]
    if len(a_nodes) * len(b_nodes) <= max_attempts:
        # Exhaustive for small communities
        pairs = [(a, b) for a in a_nodes for b in b_nodes]
    else:
        pairs = [(random.choice(a_nodes), random.choice(b_nodes)) for _ in range(max_attempts)]

    best_path: List[str] | None = None
    best_length = -1

    for src, tgt in pairs:
        if src == tgt:
            continue
        try:
            if not nx.has_path(G, src, tgt):
                continue
            path = nx.shortest_path(G, src, tgt, weight="weight")
            if len(path) - 1 > best_length:
                best_length = len(path) - 1
                best_path = path
        except nx.NetworkXNoPath:
            continue

    return best_path


def find_community_paths(
    graph: CooccurrenceGraph,
    community_a_ids: Set[str],
    community_b_ids: Set[str],
    restrict_to_communities: bool = False,
    max_diverse_attempts: int = 200,
) -> Dict[str, Any]:
    """Find shortest and diverse-longest paths between two communities.

    Parameters
    ----------
    graph : CooccurrenceGraph
        Full co-occurrence graph.
    community_a_ids : set[str]
        Node IDs in community A.
    community_b_ids : set[str]
        Node IDs in community B.
    restrict_to_communities : bool
        If True, only allow traversal through nodes in the two communities.
    max_diverse_attempts : int
        Maximum random (source, target) pairs to sample for diverse path.

    Returns
    -------
    dict
        ``{"paths": [...], "pathCount": int, "message": str | None}``
    """
    max_diverse_attempts = min(max_diverse_attempts, 500)

    labels = _label_map(graph)
    G = _cooccurrence_to_nx(graph)

    if restrict_to_communities:
        allowed = community_a_ids | community_b_ids
        G = G.subgraph(allowed).copy()

    paths: List[Dict[str, Any]] = []

    # Shortest path
    shortest = _shortest_path_via_virtuals(G, community_a_ids, community_b_ids)
    if shortest:
        paths.append(
            {
                "nodes": shortest,
                "labels": [labels.get(n, n) for n in shortest],
                "length": len(shortest) - 1,
                "totalWeight": _path_weight(G, shortest),
                "pathType": "shortest",
            }
        )

    # Diverse longest path
    longest = _diverse_longest_path(G, community_a_ids, community_b_ids, max_diverse_attempts)
    if longest and longest != shortest:
        paths.append(
            {
                "nodes": longest,
                "labels": [labels.get(n, n) for n in longest],
                "length": len(longest) - 1,
                "totalWeight": _path_weight(G, longest),
                "pathType": "diverse_longest",
            }
        )

    message = None
    if not paths:
        message = "No path exists between the selected communities"

    return {
        "paths": paths,
        "pathCount": len(paths),
        "message": message,
    }
