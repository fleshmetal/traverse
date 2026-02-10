"""Cosmograph-compatible JSON adapter.

Produces the ``{points: [...], links: [...]}`` format expected by
cosmograph-smoke/ and the Cosmograph web viewer.

Distinct from :class:`WebGLJSONAdapter` which produces ``{nodes, edges}``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Union

from traverse.graph.cooccurrence import CooccurrenceGraph


@dataclass
class CosmographAdapter:
    """Serialize a :class:`CooccurrenceGraph` to the JSON format expected by
    Cosmograph / cosmograph-smoke.

    Output schema::

        {
          "points": [{"id": str, "label": str, "first_seen"?: int}],
          "links":  [{"source": str, "target": str, "weight": int, "first_seen"?: int}]
        }
    """

    @staticmethod
    def to_json_dict(graph: CooccurrenceGraph) -> Dict[str, List[Dict[str, Any]]]:
        """Return the points/links dict (ready for ``json.dumps``)."""
        return {"points": graph["points"], "links": graph["links"]}

    @staticmethod
    def dumps(
        graph: CooccurrenceGraph,
        *,
        indent: Union[int, None] = 2,
    ) -> str:
        payload = CosmographAdapter.to_json_dict(graph)
        return json.dumps(payload, ensure_ascii=False, indent=indent)

    @staticmethod
    def write(
        graph: CooccurrenceGraph,
        path: Union[str, Path],
        *,
        indent: Union[int, None] = 2,
    ) -> Path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(CosmographAdapter.dumps(graph, indent=indent), encoding="utf-8")
        return p
