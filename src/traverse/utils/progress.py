from __future__ import annotations

import importlib
from typing import Any, Iterable, Optional, TypeVar, cast

_T = TypeVar("_T")


class Progress:
    """
    Tiny adapter around tqdm (if available). If tqdm isn't installed or enabled=False,
    iteration proceeds without a progress bar.

    Usage:
        prog = Progress(enabled=True)
        for x in prog.iter(items, desc="Loading"):
            ...
    """

    def __init__(self, enabled: bool = False) -> None:
        self.enabled = enabled
        self._tqdm: Optional[Any] = None  # tqdm callable, if available
        if enabled:
            try:
                mod = importlib.import_module("tqdm")
                self._tqdm = getattr(mod, "tqdm", None)
                if self._tqdm is None:
                    self.enabled = False
            except Exception:
                self._tqdm = None
                self.enabled = False

    def iter(
        self,
        iterable: Iterable[_T],
        *,
        desc: Optional[str] = None,
        total: Optional[int] = None,
    ) -> Iterable[_T]:
        if self.enabled and self._tqdm is not None:
            # tqdm(...) returns an iterator; cast to Iterable[_T] for typing.
            return cast(Iterable[_T], self._tqdm(iterable, desc=desc, total=total))
        return iterable
