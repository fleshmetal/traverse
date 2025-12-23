# --- top of file: replace your import block with this ---
from __future__ import annotations

from typing import Any, Dict, TypedDict

import pandas as pd

try:
    import networkx as nx
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "networkx is required. Install via `uv add networkx` (or `pip install networkx`)."
    ) from e


class GraphTables(TypedDict):
    nodes: pd.DataFrame  # ['id','key','label','type']
    edges: pd.DataFrame  # ['src','dst','weight','label']


def _ensure_columns(df: pd.DataFrame, spec: Dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    for col, default in spec.items():
        if col not in out.columns:
            out[col] = default
    return out


def to_networkx(
    g: GraphTables,
    *,
    directed: bool = False,
    keep_attrs: bool = True,
    weight_col: str = "weight",
    label_col: str = "label",
) -> Any:
    nodes = _ensure_columns(g["nodes"], {"id": "", "key": "", "label": "", "type": ""})
    edges = _ensure_columns(g["edges"], {"src": "", "dst": "", weight_col: 1.0, label_col: ""})

    G: Any = nx.DiGraph() if directed else nx.Graph()

    # nodes
    if keep_attrs:
        for _, r in nodes.iterrows():
            G.add_node(
                str(r["id"]),
                label="" if pd.isna(r.get("label")) else str(r.get("label")),
                type="" if pd.isna(r.get("type")) else str(r.get("type")),
                key="" if pd.isna(r.get("key")) else str(r.get("key")),
            )
    else:
        for _, r in nodes.iterrows():
            G.add_node(str(r["id"]))

    # edges
    if keep_attrs:
        for _, r in edges.iterrows():
            src = str(r["src"])
            dst = str(r["dst"])
            wt = r.get(weight_col, 1.0)
            try:
                wtf = float(wt) if pd.notna(wt) else 1.0
            except Exception:
                wtf = 1.0
            G.add_edge(
                src,
                dst,
                weight=wtf,
                label="" if pd.isna(r.get(label_col)) else str(r.get(label_col)),
            )
    else:
        for _, r in edges.iterrows():
            G.add_edge(str(r["src"]), str(r["dst"]))

    return G


def to_graph(g: GraphTables, **kwargs: Any) -> Any:
    return to_networkx(g, directed=False, **kwargs)


def to_digraph(g: GraphTables, **kwargs: Any) -> Any:
    return to_networkx(g, directed=True, **kwargs)


class NetworkXAdapter:
    """OO wrapper so tests can `from ... import NetworkXAdapter` and call helpers."""

    def __init__(
        self,
        *,
        directed: bool = False,
        keep_attrs: bool = True,
        weight_col: str = "weight",
        label_col: str = "label",
    ) -> None:
        self.directed = directed
        self.keep_attrs = keep_attrs
        self.weight_col = weight_col
        self.label_col = label_col

    def from_tables(self, g: GraphTables) -> Any:
        return to_networkx(
            g,
            directed=self.directed,
            keep_attrs=self.keep_attrs,
            weight_col=self.weight_col,
            label_col=self.label_col,
        )

    @staticmethod
    def to_networkx(g: GraphTables, **kwargs: Any) -> Any:
        return to_networkx(g, **kwargs)

    @staticmethod
    def basic_stats(G: Any) -> dict[str, int]:
        """Return minimal counts used in tests."""
        return {"nodes": G.number_of_nodes(), "edges": G.number_of_edges()}
