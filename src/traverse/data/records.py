from __future__ import annotations
import pandas as pd
from traverse.core.types import TablesDict
from traverse.data.base import DataSource
from pathlib import Path


class RecordsData(DataSource):
    def __init__(self, root: str | Path | None = None, *, auto_latest: bool = True):
        self.root = None if root is None else Path(root)
        self.auto_latest = auto_latest

    def load(self) -> TablesDict:
        # TODO (Week 3): read tracks/artists/genres CSVs or auto-resolve latest snapshot
        return {
            "plays": pd.DataFrame([]),  # may remain empty for Records alone
            "tracks": pd.DataFrame([]),
            "artists": pd.DataFrame([]),
            "genres": pd.DataFrame([]),
        }
