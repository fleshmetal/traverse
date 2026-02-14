# src/traverse/processing/tables.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, TypedDict, cast

import pandas as pd
from pandas import DataFrame

from traverse.processing.base import Processor
from traverse.processing.normalize import safe_str

# Canonical tables contract used across the pipeline
class TablesDict(TypedDict, total=False):
    # Inputs (may exist in various shapes)
    plays: DataFrame
    tracks: DataFrame
    artists: DataFrame
    plays_wide: DataFrame
    tracks_wide: DataFrame
    artists_wide: DataFrame

    # Optional extra pieces from earlier steps
    genres: DataFrame
    styles: DataFrame


def _ensure_columns(df: DataFrame, cols: List[str]) -> DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.Series([pd.NA] * len(out))
    return out


def _coerce_tracks_to_canonical(tracks_in: DataFrame) -> DataFrame:
    """
    Normalize any track-like table to the canonical columns:
      ['track_id','track_name','artist_name','genres','styles']
    Accepts possible source columns like ['id','name','artist'].
    """
    df = tracks_in.copy()

    # ID
    if "track_id" not in df.columns:
        if "id" in df.columns:
            df = df.rename(columns={"id": "track_id"})
        elif "track" in df.columns:
            df = df.rename(columns={"track": "track_id"})
        else:
            # create empty track_id if truly missing
            df["track_id"] = pd.NA

    # Track name
    if "track_name" not in df.columns:
        if "name" in df.columns:
            df = df.rename(columns={"name": "track_name"})
        elif "title" in df.columns:
            df = df.rename(columns={"title": "track_name"})
        else:
            df["track_name"] = pd.NA

    # Artist display name (not ID)
    if "artist_name" not in df.columns:
        if "artist" in df.columns:
            df = df.rename(columns={"artist": "artist_name"})
        elif "artists" in df.columns:
            # if list-like, keep as string for display
            df["artist_name"] = df["artists"].astype("string")
        else:
            df["artist_name"] = pd.NA

    # Genres / Styles: ensure string-delimited ( ' | ' ) columns exist
    df = _ensure_columns(df, ["genres", "styles"])
    for col in ("genres", "styles"):
        # Accept list-like or string; normalize to ' | ' delimited strings
        s = df[col]
        if pd.api.types.is_list_like(s.iloc[0]) if len(df) else False:
            df[col] = s.apply(lambda x: " | ".join(map(safe_str, cast(List[str], x))) if isinstance(x, list) else safe_str(x))
        else:
            # already string-like; just coerce to string and clean doubles
            df[col] = df[col].astype("string").fillna("")

    # Minimal select & dedupe
    keep = ["track_id", "track_name", "artist_name", "genres", "styles"]
    df = _ensure_columns(df, keep)[keep].drop_duplicates(subset=["track_id"], keep="first").reset_index(drop=True)
    return df


def _coerce_plays_to_canonical(plays_in: DataFrame) -> DataFrame:
    """
    Normalize play rows to ensure at least:
      ['played_at','track_id','ms_played']
    """
    df = plays_in.copy()

    # track_id
    if "track_id" not in df.columns:
        if "id" in df.columns:
            df = df.rename(columns={"id": "track_id"})
        elif "track" in df.columns:
            df = df.rename(columns={"track": "track_id"})
        else:
            df["track_id"] = pd.NA

    # played_at
    if "played_at" not in df.columns:
        if "ts" in df.columns:
            df = df.rename(columns={"ts": "played_at"})
        else:
            df["played_at"] = pd.NaT
    df["played_at"] = pd.to_datetime(df["played_at"], errors="coerce", utc=True)

    # ms_played
    if "ms_played" not in df.columns:
        if "ms" in df.columns:
            df = df.rename(columns={"ms": "ms_played"})
        else:
            df["ms_played"] = pd.NA

    keep = ["played_at", "track_id", "ms_played"]
    df = _ensure_columns(df, keep)[keep].copy()
    return df


def _fold_genre_style_tables(tracks: DataFrame, tables: Dict[str, DataFrame]) -> DataFrame:
    """Merge separate genres/styles tables onto tracks as pipe-delimited columns.

    FastGenreStyleEnricher outputs genres and styles as separate DataFrames
    (track_id, genre) and (track_id, style). This folds them into the tracks
    DataFrame as ' | '-delimited string columns so _coerce_tracks_to_canonical
    can pick them up.
    """
    df = tracks.copy()

    genres_df = tables.get("genres", pd.DataFrame())
    if not genres_df.empty and "track_id" in genres_df.columns and "genre" in genres_df.columns:
        g_agg = (
            genres_df.groupby("track_id")["genre"]
            .agg(lambda x: " | ".join(sorted(set(x.dropna().astype(str)))))
            .reset_index()
            .rename(columns={"genre": "genres"})
        )
        if "genres" in df.columns:
            df = df.drop(columns=["genres"])
        df = df.merge(g_agg, on="track_id", how="left")

    styles_df = tables.get("styles", pd.DataFrame())
    if not styles_df.empty and "track_id" in styles_df.columns and "style" in styles_df.columns:
        s_agg = (
            styles_df.groupby("track_id")["style"]
            .agg(lambda x: " | ".join(sorted(set(x.dropna().astype(str)))))
            .reset_index()
            .rename(columns={"style": "styles"})
        )
        if "styles" in df.columns:
            df = df.drop(columns=["styles"])
        df = df.merge(s_agg, on="track_id", how="left")

    return df


@dataclass
class BuildCanonicalTables(Processor):
    """
    Week 4 processor: produce canonical wide tables with a stable schema
    that downstream code depends on.

    Output keys:
      - plays_wide:  per-play rows with joined track_name / artist_name / genres / styles
      - tracks_wide: per-track rows with the same fields
      - artists_wide: optional, pass-through if available (normalized name only)
    """

    def run(self, tables: Dict[str, DataFrame]) -> TablesDict:  # type: ignore[override]
        # Accept multiple possible keys from earlier steps
        plays_in: DataFrame = cast(DataFrame, tables.get("plays_wide", tables.get("plays", pd.DataFrame())))
        tracks_in: DataFrame = cast(DataFrame, tables.get("tracks_wide", tables.get("tracks", pd.DataFrame())))
        artists_in: DataFrame = cast(DataFrame, tables.get("artists_wide", tables.get("artists", pd.DataFrame())))

        # Fold separate genres/styles tables (from FastGenreStyleEnricher) onto tracks
        tracks_in = _fold_genre_style_tables(tracks_in, tables)

        plays = _coerce_plays_to_canonical(plays_in)
        tracks_wide = _coerce_tracks_to_canonical(tracks_in)

        # Left join to decorate plays with display fields
        sel_cols = ["track_id", "track_name", "artist_name", "genres", "styles"]
        plays_wide = plays.merge(tracks_wide[sel_cols], on="track_id", how="left")

        # Clean up string columns (no NA, consistent delimiter)
        for col in ("track_id", "track_name", "artist_name", "genres", "styles"):
            if col in plays_wide.columns:
                plays_wide[col] = plays_wide[col].astype("string").fillna("")

        # Artists wide (optional) — normalize a minimal display table if present
        artists_wide = pd.DataFrame(columns=["artist_id", "artist_name"])
        if len(artists_in):
            a = artists_in.copy()
            if "artist_id" not in a.columns:
                if "id" in a.columns:
                    a = a.rename(columns={"id": "artist_id"})
                else:
                    a["artist_id"] = pd.NA
            if "artist_name" not in a.columns:
                if "name" in a.columns:
                    a = a.rename(columns={"name": "artist_name"})
                else:
                    a["artist_name"] = pd.NA
            artists_wide = a[["artist_id", "artist_name"]].drop_duplicates("artist_id").reset_index(drop=True)
            artists_wide["artist_id"] = artists_wide["artist_id"].astype("string").fillna("")
            artists_wide["artist_name"] = artists_wide["artist_name"].astype("string").fillna("")

        out: TablesDict = {
            "plays_wide": plays_wide,
            "tracks_wide": tracks_wide,
        }
        if len(artists_wide):
            out["artists_wide"] = artists_wide
        return out
