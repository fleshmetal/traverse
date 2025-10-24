from __future__ import annotations
from abc import ABC, abstractmethod
import pandas as pd 

class DataSource(ABC):
    """Abstract base class for data sources, yeilding normalized tables (plays, tracks, artists, genres)."""

    @abstractmethod
    def load(self) -> dict[str, pd.DataFrame]:
        raise NotImplementedError
        
