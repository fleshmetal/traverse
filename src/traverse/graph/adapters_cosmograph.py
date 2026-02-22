"""Cosmograph-compatible JSON adapter.

Produces the ``{points: [...], links: [...]}`` format expected by
the Cosmograph frontend (``traverse.cosmograph``) and the Cosmograph web viewer.

Distinct from :class:`WebGLJSONAdapter` which produces ``{nodes, edges}``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Union

from traverse.graph.cooccurrence import CooccurrenceGraph


def detect_cluster_field(graph: CooccurrenceGraph) -> Optional[str]:
    """Return the name of the first categorical field found on points, or None.

    Checks for ``"category"``, ``"community"``, and ``"cluster"`` in priority
    order.  ``"category"`` (from :class:`CooccurrenceBuilder`) takes precedence
    over ``"community"`` (from :mod:`community` detection).
    """
    candidates = ("category", "community", "cluster")
    for pt in graph.get("points", []):
        for name in candidates:
            if name in pt:
                return name
    return None


@dataclass
class CosmographAdapter:
    """Serialize a :class:`CooccurrenceGraph` to the JSON format expected by
    the Cosmograph frontend viewer.

    Output schema (without meta)::

        {
          "points": [{"id": str, "label": str, ...}],
          "links":  [{"source": str, "target": str, "weight": int, ...}]
        }

    With ``meta``::

        {
          "meta": { ... },
          "points": [...],
          "links":  [...]
        }
    """

    @staticmethod
    def to_json_dict(
        graph: CooccurrenceGraph,
        *,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Return the points/links dict (ready for ``json.dumps``).

        If *meta* is provided it is included as a top-level ``"meta"`` key.
        """
        d: Dict[str, Any] = {}
        if meta:
            d["meta"] = meta
        d["points"] = graph["points"]
        d["links"] = graph["links"]
        return d

    @staticmethod
    def dumps(
        graph: CooccurrenceGraph,
        *,
        indent: Union[int, None] = 2,
        meta: Optional[Dict[str, Any]] = None,
    ) -> str:
        payload = CosmographAdapter.to_json_dict(graph, meta=meta)
        return json.dumps(payload, ensure_ascii=False, indent=indent)

    @staticmethod
    def write(
        graph: CooccurrenceGraph,
        path: Union[str, Path],
        *,
        indent: Union[int, None] = 2,
        meta: Optional[Dict[str, Any]] = None,
        compact_threshold: int = 50_000,
    ) -> Path:
        """Write graph JSON to *path*.

        If the total number of points + links exceeds *compact_threshold*,
        ``indent`` is forced to ``None`` to produce compact JSON (saves
        30-50% file size on large graphs).
        """
        import sys

        n_items = len(graph["points"]) + len(graph["links"])
        if n_items > compact_threshold and indent is not None:
            print(
                f"Graph has {n_items:,} items — writing compact JSON "
                f"(override with indent=None)",
                file=sys.stderr,
            )
            indent = None

        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            CosmographAdapter.dumps(graph, indent=indent, meta=meta),
            encoding="utf-8",
        )

        size_mb = p.stat().st_size / 1_048_576
        print(f"Wrote {p} ({size_mb:.1f} MB)", file=sys.stderr)
        if size_mb > 200:
            print(
                f"WARNING: {size_mb:.0f} MB is very large for browser "
                f"visualization. Consider reducing MAX_NODES/MAX_EDGES "
                f"or increasing MIN_WEIGHT.",
                file=sys.stderr,
            )
        return p
