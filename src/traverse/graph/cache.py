"""Graph cache — save/load a CooccurrenceGraph + records DataFrame.

Avoids repeating an expensive CSV scan by persisting the results to disk.
Modeled after :class:`traverse.processing.tables.CanonicalTableCache`.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Tuple

import pandas as pd

from traverse.graph.cooccurrence import CooccurrenceGraph


@dataclass
class GraphCache:
    """Build-or-load cache for a co-occurrence graph and records DataFrame.

    Cache files:
    - ``graph.json`` — the graph (points + links)
    - ``canonical_plays.parquet`` — the records DataFrame
    """

    cache_dir: Path
    build_fn: Callable[[], Tuple[CooccurrenceGraph, pd.DataFrame]]
    force: bool = False

    def _graph_path(self) -> Path:
        return self.cache_dir / "graph.json"

    def _records_path(self) -> Path:
        return self.cache_dir / "canonical_plays.parquet"

    def _cache_exists(self) -> bool:
        return self._graph_path().exists() and self._records_path().exists()

    def load_or_build(self) -> Tuple[CooccurrenceGraph, pd.DataFrame]:
        """Return ``(graph, records_df)``, loading from cache if available."""
        if not self.force and self._cache_exists():
            print("Loading graph from cache…", file=sys.stderr)
            graph = self._load_graph()
            records_df = pd.read_parquet(self._records_path())
            print(
                f"  {len(graph['points'])} nodes, {len(graph['links'])} edges, "
                f"{len(records_df):,} records",
                file=sys.stderr,
            )
            return graph, records_df

        print("Building graph (this may take a while)…", file=sys.stderr)
        graph, records_df = self.build_fn()
        self._save(graph, records_df)
        return graph, records_df

    def _load_graph(self) -> CooccurrenceGraph:
        raw = json.loads(self._graph_path().read_text(encoding="utf-8"))
        return CooccurrenceGraph(
            points=raw.get("points", []),
            links=raw.get("links", []),
        )

    def _save(self, graph: CooccurrenceGraph, records_df: pd.DataFrame) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Save graph JSON
        payload = {"points": graph["points"], "links": graph["links"]}
        self._graph_path().write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"Cached graph → {self._graph_path()}", file=sys.stderr)

        # Save records parquet
        records_df.to_parquet(self._records_path(), index=False)
        print(f"Cached records → {self._records_path()} ({len(records_df):,} rows)", file=sys.stderr)
