from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, cast

import pandas as pd

from traverse.core.types import TablesDict
from traverse.data.base import DataSource
from traverse.utils.progress import Progress


def _split_pipe(s: Optional[str]) -> List[str]:
    if s is None:
        return []
    parts = [p.strip() for p in str(s).split("|")]
    return [p for p in parts if p]


class RecordsData(DataSource):
    """
    Ingest a SINGLE CSV with columns like:
      title, release_year, artists, [genres], [styles], [region]
    Multi-valued columns are '|' delimited.

    Supports chunked reading with a tqdm progress bar.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        progress: bool = False,
        chunksize: Optional[int] = None,  # e.g., 100_000 for large files
        engine: str = "auto",             # "auto" | "pyarrow" | "c" | "python"
    ) -> None:
        p = Path(path)
        if p.is_dir():
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

        self._progress = Progress(enabled=progress)
        self._chunksize = chunksize
        self._engine = engine

    # ---------- engine + header ----------

    def _pick_engine(self) -> str:
        """
        Choose CSV engine:
        - If chunking is requested, force 'c' (pyarrow can't chunk).
        - Else, prefer 'pyarrow' when available (fast), otherwise 'c'.
        - Respect explicit engine choice if set to 'c' or 'python'.
        """
        if self._chunksize:
            return "c"
        if self._engine in {"c", "python"}:
            return self._engine
        if self._engine == "pyarrow":
            return "pyarrow" if importlib.util.find_spec("pyarrow") else "c"
        # auto
        return "pyarrow" if importlib.util.find_spec("pyarrow") else "c"

    def _read_header(self) -> List[str]:
        # Lightweight header read (C engine is fine here)
        hdr = pd.read_csv(self.path, nrows=0, engine="c")
        return list(hdr.columns)

    # ---------- chunk processor ----------

    def _process_chunk(
        self,
        df: pd.DataFrame,
        tracks_rows: List[Dict[str, object]],
        artists_set: Set[str],
        genres_rows: List[Tuple[str, str]],
        styles_rows: List[Tuple[str, str]],
    ) -> None:
        # Ensure expected columns exist
        for col in ("title", "release_year", "artists"):
            if col not in df.columns:
                df[col] = pd.NA
        if "genres" not in df.columns:
            df["genres"] = pd.NA
        if "styles" not in df.columns:
            df["styles"] = pd.NA

        # Coerce dtypes
        df["title"] = df["title"].astype("string")
        df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce").astype("Int64")
        df["artists"] = df["artists"].astype("string")
        df["genres"] = df["genres"].astype("string")
        df["styles"] = df["styles"].astype("string")

        # Parse list-like columns
        df["artists_list"] = df["artists"].apply(_split_pipe)
        df["genres_list"] = df["genres"].apply(_split_pipe)
        df["styles_list"] = df["styles"].apply(_split_pipe)

        # Primary artist (vectorized-ish)
        pa = df["artists_list"].apply(lambda xs: xs[0] if xs else "").astype("string")
        df["primary_artist"] = pa

        # Stable track_id (vectorized base + sha1 map)
        base = (
            pa.str.strip().str.lower()
            + "::"
            + df["title"].fillna("").str.strip().str.lower()
            + "::"
            + df["release_year"].astype("Int64").astype("string").fillna("")
        )

        df["track_id"] = base.map(lambda s: "h:" + hashlib.sha1(s.encode("utf-8")).hexdigest()).astype("string")

        # Tracks rows
        tracks_rows.extend(
            dict(
                track_id=tid,
                track_name=title,
                album_id=pd.NA,
                album_name=pd.NA,
                artist_id=f"art::{pa_val}" if pa_val else "art::",
                isrc=pd.NA,
                release_year=ry,
            )
            for tid, title, pa_val, ry in zip(
                df["track_id"].tolist(),
                df["title"].tolist(),
                df["primary_artist"].tolist(),
                df["release_year"].tolist(),
            )
        )

        # Artists set (flatten)
        for xs in df["artists_list"].tolist():
            for a in xs:
                if a:
                    artists_set.add(f"art::{a}")

        # GENRES (explode vectorized)
        if "genres_list" in df.columns:
            g = (
                df[["track_id", "genres_list"]]
                .explode("genres_list", ignore_index=True)
                .rename(columns={"genres_list": "genre"})
            )
            if not g.empty:
                g = g[g["genre"].notna() & (g["genre"].astype(str) != "")]
                if not g.empty:
                    genres_rows.extend(list(map(tuple, g[["track_id", "genre"]].astype("string").to_numpy())))

        # STYLES (explode vectorized)
        if "styles_list" in df.columns:
            s = (
                df[["track_id", "styles_list"]]
                .explode("styles_list", ignore_index=True)
                .rename(columns={"styles_list": "style"})
            )
            if not s.empty:
                s = s[s["style"].notna() & (s["style"].astype(str) != "")]
                if not s.empty:
                    styles_rows.extend(list(map(tuple, s[["track_id", "style"]].astype("string").to_numpy())))

    # ---------- public API ----------

    def load(self) -> TablesDict:
        engine = self._pick_engine()
        header_cols = self._read_header()
        needed = ["title", "release_year", "artists", "genres", "styles"]
        usecols = [c for c in needed if c in header_cols]

        # Accumulators across chunks
        tracks_rows: List[Dict[str, object]] = []
        artists_set: Set[str] = set()
        genres_rows: List[Tuple[str, str]] = []
        styles_rows: List[Tuple[str, str]] = []

        print(f"[RecordsData] Reading CSV with engine={engine}, chunksize={self._chunksize or 0}")

        if self._chunksize:
            # Chunked read → must use engine 'c'
            reader = pd.read_csv(
                self.path,
                dtype="string",
                on_bad_lines="skip",
                engine="c",
                chunksize=self._chunksize,
                usecols=usecols,
            )
            for chunk in self._progress.iter(reader, desc="Reading Records CSV (chunks)"):
                self._process_chunk(chunk, tracks_rows, artists_set, genres_rows, styles_rows)
        else:
            # Single-pass read
            if engine == "pyarrow":
                df = pd.read_csv(self.path, dtype="string", engine="pyarrow", usecols=usecols)
            else:
                df = pd.read_csv(self.path, dtype="string", on_bad_lines="skip", engine="c", usecols=usecols)
            self._process_chunk(df, tracks_rows, artists_set, genres_rows, styles_rows)

        # Build canonical tables
        tracks = pd.DataFrame(tracks_rows).astype(
            {
                "track_id": "string",
                "track_name": "string",
                "album_id": "string",
                "album_name": "string",
                "artist_id": "string",
                "isrc": "string",
                "release_year": "Int64",
            }
        ).drop_duplicates(subset=["track_id"]).reset_index(drop=True)

        artists = pd.DataFrame({"artist_id": pd.Series(sorted(artists_set), dtype="string")})
        if not artists.empty:
            artists["artist_name"] = artists["artist_id"].str.replace(r"^art::", "", regex=True)

        genres = pd.DataFrame(genres_rows, columns=["track_id", "genre"], dtype="string").drop_duplicates()
        styles = pd.DataFrame(styles_rows, columns=["track_id", "style"], dtype="string").drop_duplicates()

        plays = pd.DataFrame([])

        out_dict: Dict[str, pd.DataFrame] = {
            "plays": plays,
            "tracks": tracks,
            "artists": artists,
            "genres": genres,
        }
        if not styles.empty:
            out_dict["styles"] = styles

        return cast(TablesDict, out_dict)
