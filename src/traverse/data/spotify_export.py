from __future__ import annotations
import pandas as pd
from traverse.core.types import TablesDict
from traverse.data.base import DataSource
from pathlib import Path


class SpotifyExtendedExport(DataSource):
    def __init__(self, export_dir: str | Path):
        self.export_dir = Path(export_dir)

    def load(self) -> TablesDict:
        return {
            "plays": pd.DataFrame([]),
            "tracks": pd.DataFrame([]),
            "artists": pd.DataFrame([]),
            "genres": pd.DataFrame([]),
        }
