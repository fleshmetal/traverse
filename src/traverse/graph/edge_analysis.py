"""NetworkX edge analysis for co-occurrence graphs.

Provides edge-level scoring algorithms that identify structurally
important edges within a graph or community subgraph.

Supported algorithms:
- **Edge betweenness centrality** — fraction of shortest paths passing
  through each edge.
- **Current-flow betweenness** (random-walk betweenness) — based on
  electrical current flow / random walks.
- **Bridge detection** — edges whose removal disconnects components.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx

from traverse.graph.cooccurrence import CooccurrenceGraph


class EdgeAlgorithm(str, Enum):
    """Supported edge-analysis algorithms."""

    EDGE_BETWEENNESS = "edge_betweenness"
    CURRENT_FLOW_BETWEENNESS = "current_flow_betweenness"
    BRIDGES = "bridges"


def subgraph_from_nodes(
    graph: CooccurrenceGraph,
    node_ids: Set[str],
) -> CooccurrenceGraph:
    """Extract the induced subgraph for *node_ids*.

    Returns a new ``CooccurrenceGraph`` containing only the specified
    nodes and edges whose *both* endpoints are in the set.
    """
    points = [p for p in graph["points"] if p["id"] in node_ids]
    links = [lk for lk in graph["links"] if lk["source"] in node_ids and lk["target"] in node_ids]
    return CooccurrenceGraph(points=points, links=links)


def _cooccurrence_to_nx(graph: CooccurrenceGraph) -> nx.Graph[Any]:
    """Lightweight converter (same logic as community.cooccurrence_to_networkx)."""
    G: nx.Graph[Any] = nx.Graph()
    for pt in graph.get("points", []):
        attrs = {k: v for k, v in pt.items() if k != "id"}
        G.add_node(pt["id"], **attrs)
    for lk in graph.get("links", []):
        attrs = {k: v for k, v in lk.items() if k not in ("source", "target")}
        G.add_edge(lk["source"], lk["target"], **attrs)
    return G


def analyze_edges(
    G: nx.Graph[Any],
    algorithm: EdgeAlgorithm = EdgeAlgorithm.EDGE_BETWEENNESS,
    *,
    normalized: bool = True,
    top_k: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Score edges and return a sorted list (highest score first).

    Parameters
    ----------
    G : nx.Graph
        Undirected graph (may be a community subgraph).
    algorithm : EdgeAlgorithm
        Which algorithm to use.
    normalized : bool
        Whether to normalise betweenness scores (default True).
    top_k : int | None
        If given, only return the *top_k* highest-scoring edges.

    Returns
    -------
    list[dict]
        Each dict has ``source``, ``target``, ``score``, and
        ``algorithm``.  For bridges, ``score`` is always ``1.0``.
    """
    scores: Dict[Tuple[str, str], float]
    if algorithm == EdgeAlgorithm.EDGE_BETWEENNESS:
        scores = nx.edge_betweenness_centrality(
            G,
            weight="weight",
            normalized=normalized,
        )
    elif algorithm == EdgeAlgorithm.CURRENT_FLOW_BETWEENNESS:
        try:
            import scipy  # type: ignore[import-untyped]  # noqa: F401
        except ImportError:
            raise ImportError(
                "Current-flow betweenness requires scipy. Install it with: pip install scipy"
            )
        if not nx.is_connected(G):
            # Current-flow requires a connected graph.  Score each
            # component separately and merge results.
            scores = {}
            for comp_nodes in nx.connected_components(G):
                if len(comp_nodes) < 2:
                    continue
                sub = G.subgraph(comp_nodes)
                comp_scores = nx.edge_current_flow_betweenness_centrality(
                    sub,
                    weight="weight",
                    normalized=normalized,
                )
                scores.update(comp_scores)
        else:
            scores = nx.edge_current_flow_betweenness_centrality(
                G,
                weight="weight",
                normalized=normalized,
            )
    elif algorithm == EdgeAlgorithm.BRIDGES:
        bridge_set: set[Tuple[str, str]] = set(nx.bridges(G))
        scores = {e: 1.0 for e in G.edges()}
        for e in scores:
            rev = (e[1], e[0])
            if e in bridge_set or rev in bridge_set:
                scores[e] = 1.0
            else:
                scores[e] = 0.0
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")

    results = [
        {
            "source": str(u),
            "target": str(v),
            "score": round(score, 6),
            "algorithm": algorithm.value,
        }
        for (u, v), score in scores.items()
    ]
    results.sort(key=lambda r: r["score"], reverse=True)

    if top_k is not None:
        results = results[:top_k]

    return results


def analyze_community_edges(
    graph: CooccurrenceGraph,
    node_ids: Set[str],
    algorithm: EdgeAlgorithm = EdgeAlgorithm.EDGE_BETWEENNESS,
    *,
    normalized: bool = True,
    top_k: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Convenience: extract subgraph → analyse edges.

    Parameters
    ----------
    graph : CooccurrenceGraph
        Full graph.
    node_ids : set[str]
        Node IDs of the community to analyse.
    algorithm : EdgeAlgorithm
        Which algorithm to use.
    normalized : bool
        Normalise betweenness scores.
    top_k : int | None
        Return only the top-k edges.
    """
    sub = subgraph_from_nodes(graph, node_ids)
    G = _cooccurrence_to_nx(sub)
    if G.number_of_edges() == 0:
        return []
    return analyze_edges(G, algorithm, normalized=normalized, top_k=top_k)
