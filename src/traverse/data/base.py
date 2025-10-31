from __future__ import annotations
from abc import ABC, abstractmethod
from traverse.core.types import TablesDict


class DataSource(ABC):
    """Reads raw inputs and yields canonical tables."""

    @abstractmethod
    def load(self) -> TablesDict:
        raise NotImplementedError
