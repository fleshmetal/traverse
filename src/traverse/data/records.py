from __future__ import annotations

import hashlib
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from traverse.core.types import TablesDict
from traverse.data.base import DataSource


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _split_pipe(s: Optional[str]) -> List[str]:
    """
    Split 'a|b|c' → ['a','b','c'], trimming whitespace and dropping empties.
    """
    if s is None:
        return []
    parts = [p.strip() for p in str(s).split("|")]
    return [p for p in parts if p]


def _stable_track_id(
    title: str, primary_artist: str, year: Optional[pd.Series] | Optional[int] | Optional[str]
) -> str:
    """
    Produce a stable track_id from (primary artist, title, release_year).
    Format: h:<sha1(normalized_artist::normalized_title::year)>
    """
    y = "" if year is None else str(year)
    base = f"{_norm(primary_artist)}::{_norm(title)}::{y}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()
    return f"h:{digest}"


class RecordsData(DataSource):
    """
    Load canonical tables from a SINGLE CSV with columns:
      - title, release_year, artists, genres, styles, region
    Multi-valued columns are '|' delimited.

    Returns:
      plays  : empty DataFrame
      tracks : (track_id, track_name, album_id, album_name, artist_id, isrc, release_year)
      artists: (artist_id, artist_name)
      genres : (track_id, genre)
      styles : (track_id, style)
    """

    def __init__(self, path: str | Path):
        p = Path(path)
        if p.is_dir():
            # If a directory is passed, look for a sensible default filename.
            # Adjust the default here if your file has a different name.
            candidate = p / "records.csv"
            if not candidate.exists():
                raise FileNotFoundError(
                    f"Expected a single CSV at {candidate}. "
                    "Pass the file path directly if it has a different name."
                )
            self.path = candidate
        else:
            if p.suffix.lower() != ".csv":
                raise ValueError(f"Expected a .csv file, got: {p}")
            self.path = p

    def load(self) -> TablesDict:
        # Read CSV as string columns, then coerce year to nullable Int64
        df = pd.read_csv(self.path, dtype="string").fillna(pd.NA)

        # Normalize expected columns; raise early if required ones missing
        required = {"title", "release_year", "artists"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Records CSV missing required columns: {sorted(missing)}")

        # Optional columns: genres, styles, region
        if "genres" not in df.columns:
            df["genres"] = pd.NA
        if "styles" not in df.columns:
            df["styles"] = pd.NA
        if "region" not in df.columns:
            df["region"] = pd.NA

        # Coerce release_year to nullable integer
        df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce").astype("Int64")

        # Parse list-like columns
        df["artists_list"] = df["artists"].apply(_split_pipe)
        df["genres_list"] = df["genres"].apply(_split_pipe)
        df["styles_list"] = df["styles"].apply(_split_pipe)

        # Primary artist = first in list (if any)
        df["primary_artist"] = df["artists_list"].apply(lambda xs: xs[0] if xs else "")

        # Build stable track_id
        df["track_id"] = df.apply(
            lambda r: _stable_track_id(
                title=str(r.get("title", "")),
                primary_artist=str(r.get("primary_artist", "")),
                year=r.get("release_year"),
            ),
            axis=1,
        ).astype("string")

        # Canonical TRACKS
        tracks = (
            pd.DataFrame(
                {
                    "track_id": df["track_id"].astype("string"),
                    "track_name": df["title"].astype("string"),
                    "album_id": pd.Series([None] * len(df), dtype="string"),
                    "album_name": pd.Series([None] * len(df), dtype="string"),
                    "artist_id": df["primary_artist"]
                    .apply(lambda a: f"art::{a}" if a else "art::")
                    .astype("string"),
                    "isrc": pd.Series([None] * len(df), dtype="string"),
                    "release_year": df["release_year"].astype("Int64"),
                }
            )
            .drop_duplicates(subset=["track_id"])
            .reset_index(drop=True)
        )

        # Canonical ARTISTS (every unique artist token across rows)
        all_artists = sorted({a for xs in df["artists_list"].tolist() for a in xs})
        artists = pd.DataFrame(
            {
                "artist_id": pd.Series([f"art::{a}" for a in all_artists], dtype="string"),
                "artist_name": pd.Series(all_artists, dtype="string"),
            }
        )

        # Canonical GENRES (explode)
        genres_rows: List[Tuple[str, str]] = []
        for tid, glist in zip(df["track_id"].tolist(), df["genres_list"].tolist()):
            for g in glist:
                if g:
                    genres_rows.append((tid, g))
        genres = pd.DataFrame(
            genres_rows, columns=["track_id", "genre"], dtype="string"
        ).drop_duplicates()

        # Canonical STYLES (explode)
        styles_rows: List[Tuple[str, str]] = []
        for tid, slist in zip(df["track_id"].tolist(), df["styles_list"].tolist()):
            for s in slist:
                if s:
                    styles_rows.append((tid, s))
        styles = pd.DataFrame(
            styles_rows, columns=["track_id", "style"], dtype="string"
        ).drop_duplicates()

        # No plays in this source
        plays = pd.DataFrame([])

        out: TablesDict = {
            "plays": plays,
            "tracks": tracks,
            "artists": artists,
            "genres": genres,
        }
        # only include styles if non-empty (keeps baseline schema stable)
        if not styles.empty:
            out["styles"] = styles  # type: ignore[typeddict-unknown-key]
        return out
