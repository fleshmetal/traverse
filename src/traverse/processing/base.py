from __future__ import annotations
from abc import ABC, abstractmethod
import pandas as pd

class Processor(ABC):
    @abstractmethod
    def run(self, tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        raise NotImplementedError

class Pipeline:
    def __init__(self, processors: list[Processor]):
        self.processors = processors

    def run(self, tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        for p in self.processors:
            tables = p.run(tables)
        return tables