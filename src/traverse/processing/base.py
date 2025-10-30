from __future__ import annotations
from abc import ABC, abstractmethod
from typing import cast, List
from traverse.core.types import TablesDict


class Processor(ABC):
    """Pure table -> table transform; must not mutate inputs in-place."""

    @abstractmethod
    def run(self, tables: TablesDict) -> TablesDict:
        raise NotImplementedError


class Pipeline:
    """Compose processors sequentially."""

    def __init__(self, processors: List[Processor]):
        self.processors = processors

    def run(self, tables: TablesDict) -> TablesDict:
        out: TablesDict = cast(TablesDict, dict(tables))
        for p in self.processors:
            out = p.run(out)
        return out
