from __future__ import annotations

from typing import TypedDict

import pandas as pd

from traverse.core.types import TablesDict
from traverse.processing.base import Processor


class GraphTables(TypedDict):
    nodes: pd.DataFrame
    edges: pd.DataFrame


def _split_pipe_series(s: pd.Series, name: str) -> pd.DataFrame:
    """Explode a ' | '-delimited string Series into a one-column DataFrame with name `name`."""
    df = pd.DataFrame({name: s})
    df = df.dropna(subset=[name])
    if df.empty:
        return pd.DataFrame(columns=[name])
    parts = df[name].astype("string").str.split("|").explode().str.strip().dropna().to_frame(name)
    parts = parts[parts[name] != ""]
    return parts.reset_index(drop=True)


def _ensure_col(df: pd.DataFrame, col: str, default: object, dtype: str) -> None:
    """Create a column with a typed Series if it doesn't exist."""
    if col not in df.columns:
        df[col] = pd.Series([default] * len(df), index=df.index, dtype=dtype)


class GraphBuilder(Processor):
    """
    Build a bipartite graph.

    Mode A (track→genre): if usable `genres` exist in tracks (or plays), create edges track→genre.
    Mode B (track→artist): otherwise fall back to edges track→artist.

    Weight:
      - agg="play_count": count plays per (src,dst)
      - agg="ms_played":  sum ms_played per (src,dst)

    `min_weight` filters edges with weight >= threshold.
    """

    def __init__(self, agg: str = "play_count", min_weight: int | float = 1) -> None:
        self.agg = agg
        self.min_weight = min_weight

    def build(self, **tables: pd.DataFrame) -> GraphTables:
        plays = tables.get("plays", pd.DataFrame())
        tracks = tables.get("tracks", pd.DataFrame())
        artists = tables.get("artists", pd.DataFrame())

        # Ensure minimal schema with proper dtypes
        _ensure_col(plays, "track_id", "", "string")
        _ensure_col(plays, "artist_id", "", "string")
        _ensure_col(plays, "ms_played", 0, "Int64")
        _ensure_col(tracks, "track_id", "", "string")
        _ensure_col(tracks, "track_name", "", "string")
        _ensure_col(tracks, "genres", "", "string")
        _ensure_col(artists, "artist_id", "", "string")
        _ensure_col(artists, "artist_name", "", "string")

        # Track nodes
        track_nodes = (
            (
                tracks[["track_id", "track_name"]].drop_duplicates()
                if not tracks.empty
                else plays[["track_id"]].drop_duplicates().assign(track_name="")
            )
            .rename(columns={"track_id": "id", "track_name": "label"})
            .assign(type="track")
        )
        track_nodes["key"] = track_nodes["id"]

        # Determine if we can use genres
        track_genres = pd.DataFrame(columns=["track_id", "genres"]).astype(
            {"track_id": "string", "genres": "string"}
        )
        if tracks["genres"].astype("string").str.len().fillna(0).gt(0).any():
            track_genres = tracks[["track_id", "genres"]].drop_duplicates()
        elif (
            "genres" in plays.columns
            and plays["genres"].astype("string").str.len().fillna(0).gt(0).any()
        ):
            tmp = plays.loc[
                plays["genres"].astype("string").str.len().fillna(0).gt(0), ["track_id", "genres"]
            ].drop_duplicates("track_id")
            track_genres = tmp

        has_genres = (
            not track_genres.empty
            and track_genres["genres"].astype("string").str.len().fillna(0).gt(0).any()
        )

        if has_genres:
            # explode (track_id, genre)
            tg = track_genres.merge(
                track_nodes.rename(columns={"id": "track_id"})[["track_id"]],
                on="track_id",
                how="inner",
            )
            tg = tg.dropna(subset=["genres"])
            tg["genres"] = tg["genres"].astype("string")
            tg = tg[tg["genres"].str.len().gt(0)]
            tg_exp = (
                tg.assign(_parts=tg["genres"].str.split("|"))
                .explode("_parts")
                .assign(genre=lambda d: d["_parts"].fillna("").astype("string").str.strip())
            )
            tg_exp = tg_exp[tg_exp["genre"] != ""]
            tg_exp = tg_exp[["track_id", "genre"]].drop_duplicates()

            # weights per track
            if self.agg == "ms_played":
                w = plays.groupby("track_id", dropna=False)["ms_played"].sum()
            else:
                w = plays.groupby("track_id", dropna=False).size()
            w = w.rename("weight")

            tg_w = tg_exp.merge(w, left_on="track_id", right_index=True, how="left").fillna(
                {"weight": 0}
            )
            tg_w = tg_w[tg_w["weight"] >= self.min_weight]

            genre_nodes = (
                tg_w[["genre"]]
                .drop_duplicates()
                .rename(columns={"genre": "id"})
                .assign(label=lambda d: d["id"], type="genre")
            )
            genre_nodes["key"] = genre_nodes["id"]

            edges = tg_w.rename(columns={"track_id": "src", "genre": "dst"})[
                ["src", "dst", "weight"]
            ]
            edges["label"] = ""

            nodes = pd.concat([track_nodes, genre_nodes], ignore_index=True)

        else:
            # track → artist
            pa = plays[["track_id", "artist_id", "ms_played"]].copy()
            if self.agg == "ms_played":
                e = (
                    pa.groupby(["track_id", "artist_id"], dropna=False)["ms_played"]
                    .sum()
                    .reset_index()
                )
                e = e.rename(columns={"ms_played": "weight"})
            else:
                e = (
                    pa.groupby(["track_id", "artist_id"], dropna=False)
                    .size()
                    .reset_index(name="weight")
                )

            e = e[e["weight"] >= self.min_weight]

            artist_nodes = (
                artists[["artist_id", "artist_name"]]
                .drop_duplicates()
                .rename(columns={"artist_id": "id", "artist_name": "label"})
                .assign(type="artist")
            )
            artist_nodes["key"] = artist_nodes["id"]

            edges = e.rename(columns={"track_id": "src", "artist_id": "dst"})[
                ["src", "dst", "weight"]
            ]
            edges["label"] = ""

            nodes = pd.concat([track_nodes, artist_nodes], ignore_index=True)

        nodes = nodes[["id", "key", "label", "type"]].reset_index(drop=True)
        edges = edges[["src", "dst", "weight", "label"]].reset_index(drop=True)
        return {"nodes": nodes, "edges": edges}

    def run(self, tables: TablesDict) -> TablesDict:
        g = self.build(
            plays=tables.get("plays", pd.DataFrame()),
            tracks=tables.get("tracks", pd.DataFrame()),
            artists=tables.get("artists", pd.DataFrame()),
        )
        out = dict(tables)  # shallow copy; TablesDict is Structural in usage
        out["graph_nodes"] = g["nodes"]
        out["graph_edges"] = g["edges"]
        return out  # type: ignore[return-value]
