from __future__ import annotations
from abc import ABC, abstractmethod
import pandas as pd
from typing import Any

class GraphBuilder(ABC):
    @abstractmethod
    def build(self, tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Return {'nodes': pd.DataFrame, 'edges': pd.DataFrame}"""
        raise NotImplementedError

class GraphAdapter(ABC):
    @abstractmethod
    def adapt(self, graph: dict[str, pd.DataFrame]) -> Any:
        raise NotImplementedError