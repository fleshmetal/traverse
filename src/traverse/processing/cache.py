"""Canonical table caching: build-or-load pattern for plays_wide / tracks_wide.

Consolidates the ``_build_or_load_canonicals`` logic duplicated across export
scripts into a reusable utility.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple, cast

import pandas as pd

from traverse.processing.base import Pipeline
from traverse.processing.tables import BuildCanonicalTables, TablesDict


def _status(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


@dataclass
class CanonicalTableCache:
    """Load or build+cache canonical (plays_wide, tracks_wide) tables.

    Args:
        cache_dir: directory where parquet/csv cache files live.
        build_fn: callable returning a raw TablesDict (plays, tracks, etc.).
                  Only called on cache miss or when *force* is True.
        enrich_fn: optional callable that enriches raw tables before
                   running :class:`BuildCanonicalTables`.
        force: always rebuild even when cache exists.
    """

    cache_dir: Path
    build_fn: Callable[[], Dict[str, pd.DataFrame]]
    enrich_fn: Optional[Callable[[Dict[str, pd.DataFrame]], Dict[str, pd.DataFrame]]] = None
    force: bool = False

    # -- cache paths -------------------------------------------------------

    def _plays_parquet(self) -> Path:
        return self.cache_dir / "canonical_plays.parquet"

    def _tracks_parquet(self) -> Path:
        return self.cache_dir / "canonical_tracks.parquet"

    def _plays_csv(self) -> Path:
        return self.cache_dir / "canonical_plays.csv"

    def _tracks_csv(self) -> Path:
        return self.cache_dir / "canonical_tracks.csv"

    # -- public API --------------------------------------------------------

    def load_or_build(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Return ``(plays_wide, tracks_wide)``, loading from cache or
        building fresh."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if not self.force:
            cached = self._try_load_cache()
            if cached is not None:
                return cached

        # Build from scratch
        _status("Building canonical tables from source data...")
        tables = self.build_fn()

        if self.enrich_fn is not None:
            _status("Enriching tables...")
            tables = self.enrich_fn(tables)

        pipe = Pipeline([BuildCanonicalTables()])
        out = pipe.run(cast(TablesDict, tables))
        plays_wide = cast(pd.DataFrame, out.get("plays_wide", pd.DataFrame()))
        tracks_wide = cast(pd.DataFrame, out.get("tracks_wide", pd.DataFrame()))

        if plays_wide.empty:
            raise RuntimeError("Canonical plays_wide empty after build; check inputs.")

        self._persist(plays_wide, tracks_wide)
        return plays_wide, tracks_wide

    # -- internals ---------------------------------------------------------

    def _try_load_cache(self) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        if self._plays_parquet().exists() and self._tracks_parquet().exists():
            _status(f"Using cached canonical tables in {self.cache_dir} (parquet)")
            return (
                pd.read_parquet(self._plays_parquet()),
                pd.read_parquet(self._tracks_parquet()),
            )
        if self._plays_csv().exists() and self._tracks_csv().exists():
            _status(f"Using cached canonical tables in {self.cache_dir} (csv)")
            return (
                pd.read_csv(self._plays_csv()),
                pd.read_csv(self._tracks_csv()),
            )
        return None

    def _persist(self, plays_wide: pd.DataFrame, tracks_wide: pd.DataFrame) -> None:
        try:
            plays_wide.to_parquet(self._plays_parquet(), index=False)
            tracks_wide.to_parquet(self._tracks_parquet(), index=False)
            _status(f"Cached canonical tables (parquet) in {self.cache_dir}")
        except Exception as e:
            _status(f"Failed to cache parquet: {e}")
            try:
                plays_wide.to_csv(self._plays_csv(), index=False)
                tracks_wide.to_csv(self._tracks_csv(), index=False)
                _status(f"Cached canonical tables (csv) in {self.cache_dir}")
            except Exception as e2:
                _status(f"Failed to cache csv: {e2}")
