from __future__ import annotations
from typing import Literal, TypedDict
import pandas as pd

TableKey = Literal["plays", "tracks", "artists", "genres"]


class GraphDFs(TypedDict):
    nodes: pd.DataFrame
    edges: pd.DataFrame


class TablesDict(TypedDict, total=False):
    plays: pd.DataFrame
    tracks: pd.DataFrame
    artists: pd.DataFrame
    genres: pd.DataFrame
