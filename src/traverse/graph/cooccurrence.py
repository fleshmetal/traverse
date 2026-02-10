"""Co-occurrence graph builder for tag-based (genre/style) networks.

Consolidates the duplicated co-occurrence logic from scripts/export_cosmo_*.py
into a reusable core module.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import combinations
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypedDict,
)


class CooccurrenceGraph(TypedDict):
    """Result of a co-occurrence build."""

    points: List[Dict[str, Any]]
    links: List[Dict[str, Any]]


@dataclass
class CooccurrenceBuilder:
    """Accumulate tag co-occurrence counts with optional timestamps,
    then finalize into a thresholded, capped graph.

    Usage::

        builder = CooccurrenceBuilder(min_cooccurrence=2, max_nodes=500)
        for tags, ts_ms in rows:
            builder.add(tags, timestamp_ms=ts_ms, label_fn=pretty_label)
        result = builder.build()
    """

    min_cooccurrence: int = 2
    max_nodes: int = 0  # 0 = no cap
    max_edges: int = 0  # 0 = no cap

    # Internal accumulators
    _counts: Counter = field(init=False, repr=False, default_factory=Counter)
    _node_first_seen: Dict[str, int] = field(init=False, repr=False, default_factory=dict)
    _edge_first_seen: Dict[Tuple[str, str], int] = field(
        init=False, repr=False, default_factory=dict
    )
    _node_labels: Dict[str, str] = field(init=False, repr=False, default_factory=dict)
    _rows_seen: int = field(init=False, repr=False, default=0)
    _rows_with_tags: int = field(init=False, repr=False, default=0)
    _rows_with_pairs: int = field(init=False, repr=False, default=0)

    # ------------------------------------------------------------------
    def reset(self) -> None:
        """Clear all accumulated state."""
        self._counts = Counter()
        self._node_first_seen = {}
        self._edge_first_seen = {}
        self._node_labels = {}
        self._rows_seen = 0
        self._rows_with_tags = 0
        self._rows_with_pairs = 0

    # ------------------------------------------------------------------
    @staticmethod
    def _ordered_pair(a: str, b: str) -> Tuple[str, str]:
        return (a, b) if a <= b else (b, a)

    @staticmethod
    def _cooccurrence_pairs(tags: Sequence[str]) -> Iterable[Tuple[str, str]]:
        uniq = sorted(set(t for t in tags if t))
        if len(uniq) < 2:
            return []
        return combinations(uniq, 2)

    # ------------------------------------------------------------------
    def add(
        self,
        tags: Sequence[str],
        *,
        timestamp_ms: Optional[int] = None,
        label_fn: Optional[Callable[[str], str]] = None,
    ) -> None:
        """Add a single observation (e.g. one play row) with its tags.

        Args:
            tags: normalized tag strings (already lowered/cleaned).
            timestamp_ms: optional epoch-millisecond timestamp for timeline.
            label_fn: optional function to compute display label from tag id.
        """
        self._rows_seen += 1
        if not tags:
            return
        self._rows_with_tags += 1

        unique_tags = set(tags)

        for t in unique_tags:
            if t not in self._node_labels:
                self._node_labels[t] = label_fn(t) if label_fn else t
            if timestamp_ms is not None:
                prev = self._node_first_seen.get(t)
                if prev is None or timestamp_ms < prev:
                    self._node_first_seen[t] = timestamp_ms

        pairs = list(self._cooccurrence_pairs(list(unique_tags)))
        if pairs:
            self._rows_with_pairs += 1
        for a, b in pairs:
            key = self._ordered_pair(a, b)
            self._counts[key] += 1
            if timestamp_ms is not None:
                prev = self._edge_first_seen.get(key)
                if prev is None or timestamp_ms < prev:
                    self._edge_first_seen[key] = timestamp_ms

    # ------------------------------------------------------------------
    def build(
        self,
        *,
        label_fn: Optional[Callable[[str], str]] = None,
    ) -> CooccurrenceGraph:
        """Apply thresholds + caps and return ``{points, links}``."""
        # 1. Threshold
        edges: List[Tuple[str, str, int]] = [
            (a, b, w) for (a, b), w in self._counts.items() if w >= self.min_cooccurrence
        ]
        edges.sort(key=lambda x: x[2], reverse=True)

        # 2. Node strength (weighted degree)
        strength: Dict[str, int] = defaultdict(int)
        for a, b, w in edges:
            strength[a] += w
            strength[b] += w

        # 3. Cap nodes by strength
        if self.max_nodes and self.max_nodes > 0:
            keep: Set[str] = {
                n
                for n, _ in sorted(strength.items(), key=lambda kv: kv[1], reverse=True)[
                    : self.max_nodes
                ]
            }
            edges = [(a, b, w) for a, b, w in edges if a in keep and b in keep]

        # 4. Cap edges
        if self.max_edges and self.max_edges > 0 and len(edges) > self.max_edges:
            edges = edges[: self.max_edges]

        # 5. Collect final node set
        node_ids: Set[str] = set()
        for a, b, _ in edges:
            node_ids.add(a)
            node_ids.add(b)

        # 6. Build points
        points: List[Dict[str, Any]] = []
        for nid in sorted(node_ids):
            lbl = self._node_labels.get(nid)
            if lbl is None and label_fn is not None:
                lbl = label_fn(nid)
            pt: Dict[str, Any] = {"id": nid, "label": lbl or nid}
            if nid in self._node_first_seen:
                pt["first_seen"] = int(self._node_first_seen[nid])
            points.append(pt)

        # 7. Build links
        links: List[Dict[str, Any]] = []
        for a, b, w in edges:
            key = self._ordered_pair(a, b)
            lk: Dict[str, Any] = {"source": a, "target": b, "weight": int(w)}
            if key in self._edge_first_seen:
                lk["first_seen"] = int(self._edge_first_seen[key])
            links.append(lk)

        return CooccurrenceGraph(points=points, links=links)

    # ------------------------------------------------------------------
    @property
    def stats(self) -> Dict[str, int]:
        return {
            "rows_seen": self._rows_seen,
            "rows_with_tags": self._rows_with_tags,
            "rows_with_pairs": self._rows_with_pairs,
            "unique_pairs": len(self._counts),
            "unique_tags": len(self._node_labels),
        }
