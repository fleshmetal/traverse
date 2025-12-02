# src/traverse/processing/tables.py
from __future__ import annotations

from typing import Any, cast

import pandas as pd

from traverse.core.types import TablesDict
from traverse.processing.base import Processor
from traverse.processing.normalize import split_genres_styles, safe_str, coerce_year


def _get_df(tables: TablesDict, key: str) -> pd.DataFrame:
    """Safely fetch a DataFrame from TablesDict; return empty DF if not present/typed."""
    val: Any = tables.get(key)
    return val if isinstance(val, pd.DataFrame) else pd.DataFrame()


class BuildCanonicalTables(Processor):
    """
    Produce canonical, analysis-ready tables:
      - plays_wide: one row per play (joined genre/style strings when available)
      - tracks_wide: one row per track (joined genre/style strings)

    Expected inputs (best-effort, all optional):
      plays[played_at, track_id, ms_played, source, artist_name, track_name, ...]
      tracks[track_id, track_name, artist_id?, artist_name?, release_year?]
      artists[artist_id, artist_name]
      genres[track_id, genre]
      styles[track_id, style]
    """

    def __init__(self, join_delim: str = " | "):
        self.join_delim = join_delim

    def run(self, tables: TablesDict) -> TablesDict:
        # --- Fetch as proper DataFrames for mypy and robustness
        plays = _get_df(tables, "plays").copy()
        tracks = _get_df(tables, "tracks").copy()
        artists = _get_df(tables, "artists").copy()
        genres = _get_df(tables, "genres").copy()
        styles = _get_df(tables, "styles").copy()

        # ---- Dedup & basic coercions
        if not plays.empty:
            plays = plays.drop_duplicates()
            # Ensure essential columns exist
            for c in ("track_id", "played_at"):
                if c not in plays.columns:
                    plays[c] = pd.NA
            plays["track_id"] = plays["track_id"].astype("string")
            plays["played_at"] = pd.to_datetime(plays["played_at"], errors="coerce", utc=True)

        if not tracks.empty:
            tracks = tracks.drop_duplicates()
            for c in ("track_id", "track_name"):
                if c not in tracks.columns:
                    tracks[c] = pd.NA
            tracks["track_id"] = tracks["track_id"].astype("string")
            tracks["track_name"] = tracks["track_name"].astype("string")
            # Some exports have artist_name in tracks (without artist_id); keep it if present
            if "artist_name" in tracks.columns:
                tracks["artist_name"] = tracks["artist_name"].astype("string")
            if "release_year" in tracks.columns:
                tracks["release_year"] = tracks["release_year"].apply(coerce_year)

        if not artists.empty:
            artists = artists.drop_duplicates()
            for c in ("artist_id", "artist_name"):
                if c not in artists.columns:
                    artists[c] = pd.NA
            artists["artist_id"] = artists["artist_id"].astype("string")
            artists["artist_name"] = artists["artist_name"].astype("string")

        # ---- Aggregate genres/styles per track_id to lists
        g_agg = pd.DataFrame({"track_id": [], "genres": []})
        if not genres.empty and {"track_id", "genre"}.issubset(genres.columns):
            tmp = (
                genres[["track_id", "genre"]]
                .dropna(subset=["track_id"])
                .astype({"track_id": "string"})
            )
            tmp["genre"] = tmp["genre"].astype("string").apply(split_genres_styles)
            tmp = tmp.explode("genre", ignore_index=True).dropna(subset=["genre"])
            g_agg = (
                tmp.groupby("track_id")["genre"]
                .agg(lambda s: sorted(set(map(safe_str, s))))
                .reset_index()
                .rename(columns={"genre": "genres"})
            )

        s_agg = pd.DataFrame({"track_id": [], "styles": []})
        if not styles.empty and {"track_id", "style"}.issubset(styles.columns):
            tmp = (
                styles[["track_id", "style"]]
                .dropna(subset=["track_id"])
                .astype({"track_id": "string"})
            )
            tmp["style"] = tmp["style"].astype("string").apply(split_genres_styles)
            tmp = tmp.explode("style", ignore_index=True).dropna(subset=["style"])
            s_agg = (
                tmp.groupby("track_id")["style"]
                .agg(lambda s: sorted(set(map(safe_str, s))))
                .reset_index()
                .rename(columns={"style": "styles"})
            )

        # ---- tracks_wide: tracks + artists + tags
        tracks_wide = tracks.copy()

        # If we have artist_id on tracks, enrich with artists table
        if not artists.empty and "artist_id" in tracks_wide.columns:
            tracks_wide = tracks_wide.merge(artists, on="artist_id", how="left")

        # Attach tag aggregates when available; otherwise ensure columns exist
        if not g_agg.empty:
            tracks_wide = tracks_wide.merge(g_agg, on="track_id", how="left")
        else:
            tracks_wide["genres"] = [[] for _ in range(len(tracks_wide))]

        if not s_agg.empty:
            tracks_wide = tracks_wide.merge(s_agg, on="track_id", how="left")
        else:
            tracks_wide["styles"] = [[] for _ in range(len(tracks_wide))]

        # Join tag lists into single string columns
        for col in ("genres", "styles"):
            if col in tracks_wide.columns:
                tracks_wide[col] = (
                    tracks_wide[col]
                    .apply(
                        lambda v: self.join_delim.join(v) if isinstance(v, list) else safe_str(v)
                    )
                    .astype("string")
                )

        # ---- plays_wide = plays + selected track columns (only those that exist)
        # Prefer taking names from tracks_wide if present; otherwise plays already carries what it has.
        candidate_cols = [
            "track_id",
            "track_name",
            "artist_id",
            "artist_name",
            "release_year",
            "genres",
            "styles",
        ]
        sel_cols = [c for c in candidate_cols if c in tracks_wide.columns]
        if sel_cols:
            plays_wide = plays.merge(tracks_wide[sel_cols], on="track_id", how="left")
        else:
            plays_wide = plays.copy()

        # Stable column order, but only include columns that exist
        preferred_order = [
            "played_at",
            "track_id",
            "ms_played",
            "source",
            "artist_name",
            "track_name",
            "release_year",
            "genres",
            "styles",
        ]
        final_order = [c for c in preferred_order if c in plays_wide.columns] + [
            c for c in plays_wide.columns if c not in preferred_order
        ]
        plays_wide = plays_wide[final_order]

        # ---- Build return mapping as dict[str, DataFrame]
        out_dict: dict[str, pd.DataFrame] = {}
        for k, v in tables.items():
            if isinstance(v, pd.DataFrame):
                out_dict[k] = v
        out_dict["plays_wide"] = plays_wide
        out_dict["tracks_wide"] = tracks_wide
        return cast(TablesDict, out_dict)
