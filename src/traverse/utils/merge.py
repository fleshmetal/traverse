# src/traverse/utils/merge.py
from __future__ import annotations


import pandas as pd


def merge_tables(*sources: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Merge multiple table dicts by UNIONing dataframes with the same key.
    - Aligns columns (union) before concat
    - Drops duplicate rows
    - Returns a plain dict[str, DataFrame] on purpose (caller can cast/narrow)
    """
    out: dict[str, pd.DataFrame] = {}
    keys: set[str] = set()
    for s in sources:
        keys.update(k for k, v in s.items() if isinstance(v, pd.DataFrame))

    for k in sorted(keys):
        parts = [s[k] for s in sources if isinstance(s.get(k), pd.DataFrame)]
        if len(parts) == 1:
            out[k] = parts[0].copy()
        else:
            cols = sorted(set().union(*(df.columns.tolist() for df in parts)))
            aligned = [df.reindex(columns=cols) for df in parts]
            out[k] = pd.concat(aligned, ignore_index=True).drop_duplicates()

    return out
