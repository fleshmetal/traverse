from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, TypedDict, Union

import pandas as pd

from .builder import GraphTables


class WebGLGraphJSON(TypedDict):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]


@dataclass
class WebGLJSONAdapter:
    """
    Minimal WebGL/PyCosmograph-friendly JSON adapter.

    Input GraphTables schema (from GraphBuilder):
      nodes: DataFrame[id, key, label, type]
      edges: DataFrame[src, dst, weight, label]

    Output JSON schema:
    {
      "nodes": [{"id": str, "label": str, "type": str, "key": str}],
      "edges": [{"source": str, "target": str, "weight": float, "label": str}]
    }
    """

    @staticmethod
    def to_json_dict(g: GraphTables) -> WebGLGraphJSON:
        nodes_df: pd.DataFrame = g["nodes"]
        edges_df: pd.DataFrame = g["edges"]

        # Ensure expected columns exist on nodes (strings)
        node_defaults: list[tuple[str, str]] = [
            ("id", ""),
            ("label", ""),
            ("type", ""),
            ("key", ""),
        ]
        for col, dstr in node_defaults:
            if col not in nodes_df.columns:
                nodes_df[col] = dstr

        # Ensure expected columns exist on edges (src/dst/label=str, weight=float)
        edge_str_defaults: list[tuple[str, str]] = [
            ("src", ""),
            ("dst", ""),
            ("label", ""),
        ]
        for col, dstr in edge_str_defaults:
            if col not in edges_df.columns:
                edges_df[col] = dstr
        if "weight" not in edges_df.columns:
            edges_df["weight"] = 1.0  # float

        nodes_out: List[Dict[str, Any]] = [
            {
                "id": str(row["id"]),
                "label": "" if pd.isna(row.get("label")) else str(row.get("label")),
                "type": "" if pd.isna(row.get("type")) else str(row.get("type")),
                "key": "" if pd.isna(row.get("key")) else str(row.get("key")),
            }
            for _, row in nodes_df.iterrows()
        ]

        edges_out: List[Dict[str, Any]] = [
            {
                "source": str(row["src"]),
                "target": str(row["dst"]),
                "weight": float(row.get("weight", 1.0))
                if pd.notna(row.get("weight", 1.0))
                else 1.0,
                "label": "" if pd.isna(row.get("label")) else str(row.get("label")),
            }
            for _, row in edges_df.iterrows()
        ]

        return WebGLGraphJSON(nodes=nodes_out, edges=edges_out)

    @staticmethod
    def dumps(g: GraphTables, *, indent: Union[int, None] = None) -> str:
        payload = WebGLJSONAdapter.to_json_dict(g)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), indent=indent)

    @staticmethod
    def write(g: GraphTables, path: Union[str, Path], *, indent: Union[int, None] = None) -> Path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(WebGLJSONAdapter.dumps(g, indent=indent), encoding="utf-8")
        return p
