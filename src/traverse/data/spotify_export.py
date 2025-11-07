# src/traverse/data/spotify_export.py
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, cast

import pandas as pd

from traverse.core.types import TablesDict
from traverse.data.base import DataSource
from traverse.utils.progress import Progress

_HISTORY_GLOB = "Streaming_History_Audio_*.json"


def _read_json_array(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        data: Any = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path} does not contain a JSON array.")
    return cast(List[Dict[str, Any]], data)


def _first_nonempty(*vals: Any) -> Optional[Any]:
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


def _to_utc(ts: Optional[str]) -> Optional[pd.Timestamp]:
    if ts is None:
        return None
    # NOTE: pd.to_datetime(..., utc=True) yields tz-aware Timestamp or NaT
    out = pd.to_datetime(ts, utc=True, errors="coerce")
    # Represent NaT to type checker as None (runtime stays NaT in the Series conversion)
    if isinstance(out, pd.Timestamp):
        return out
    return None


def _stable_track_id(
    spotify_track_uri: Optional[str], artist: Optional[str], track: Optional[str]
) -> str:
    """
    Prefer real Spotify track URI → 'trk:<id>'.
    If missing, synthesize a stable id from artist::track → 'h:<sha1>'.
    """
    if (
        spotify_track_uri
        and isinstance(spotify_track_uri, str)
        and "spotify:track:" in spotify_track_uri
    ):
        return "trk:" + spotify_track_uri.split("spotify:track:", 1)[1]
    base = f"{(artist or '').strip().lower()}::{(track or '').strip().lower()}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()
    return f"h:{digest}"


class SpotifyExtendedExport(DataSource):
    """
    Parse Spotify Extended Streaming History dumped from your privacy portal.
    Handles 'Streaming_History_Audio_*.json' files (recursively).
    Produces canonical tables; 'genres' is empty (to be enriched later).
    """

    def __init__(
        self, export_dir: str | Path, *, recursive: bool = True, progress: bool = True
    ) -> None:
        self.export_dir = Path(export_dir)
        self.recursive = bool(recursive)
        self._progress = Progress(enabled=progress)

    def _iter_files(self) -> Iterable[Path]:
        return (
            self.export_dir.rglob(_HISTORY_GLOB)
            if self.recursive
            else self.export_dir.glob(_HISTORY_GLOB)
        )

    def _coerce_record(self, r: Dict[str, Any]) -> Dict[str, Any]:
        # Names across vintages
        artist = _first_nonempty(
            r.get("master_metadata_album_artist_name"),
            r.get("master_metadata_track_artist_name"),
            r.get("artistName"),
            r.get("artist_name"),
        )
        track = _first_nonempty(
            r.get("master_metadata_track_name"),
            r.get("trackName"),
            r.get("track_name"),
        )
        album = _first_nonempty(
            r.get("master_metadata_album_album_name"),
            r.get("albumName"),
            r.get("album_name"),
        )
        ts = _first_nonempty(r.get("ts"), r.get("endTime"))
        ms_played_val = _first_nonempty(r.get("ms_played"), r.get("msPlayed"))

        spotify_track_uri = _first_nonempty(
            r.get("spotify_track_uri"),
            r.get("spotify_track_uri_decrypted"),
            r.get("trackUri"),
        )

        # extras (kept on plays)
        platform = _first_nonempty(r.get("platform"), r.get("platform_string"))
        country = _first_nonempty(r.get("conn_country"), r.get("country"))
        reason_start = r.get("reason_start")
        reason_end = r.get("reason_end")
        shuffle = r.get("shuffle")
        skipped = r.get("skipped")
        ua = r.get("user_agent_decrypted")

        # Normalize
        played_at = _to_utc(ts)
        try:
            ms_played = int(ms_played_val) if ms_played_val is not None else 0
        except Exception:
            ms_played = 0

        track_id = _stable_track_id(spotify_track_uri, artist, track)
        artist_id = f"art::{(artist or '').strip()}" if artist else "art::"

        rec: Dict[str, Any] = {
            "played_at": played_at,
            "track_id": track_id,
            "ms_played": ms_played,
            # for building tracks/artists + enrichment
            "_track_name": track,
            "_album_name": album,
            "_artist_name": artist,
            "_artist_id": artist_id,
            # extras surfaced on plays
            "_platform": platform,
            "_country": country,
            "_reason_start": reason_start,
            "_reason_end": reason_end,
            "_shuffle": shuffle,
            "_skipped": skipped,
            "_ua": ua,
        }
        return rec

    def load(self) -> TablesDict:
        rows: List[Dict[str, Any]] = []
        file_count = 0

        files = list(self._iter_files())
        for p in self._progress.iter(files, desc="Scanning Spotify history", total=len(files)):
            file_count += 1
            data = _read_json_array(p)
            for raw in self._progress.iter(data, desc=f"Reading {p.name}", total=len(data)):
                rec = self._coerce_record(raw)
                rows.append(rec)

        # Canonical schemas (used even when empty)
        PLAY_COLS = [
            "played_at",
            "track_id",
            "ms_played",
            "source",
            "user_id",
            "session_id",
            "artist_name",
            "track_name",
            "platform",
            "country",
            "reason_start",
            "reason_end",
            "shuffle",
            "skipped",
            "user_agent",
        ]
        TRACK_COLS = [
            "track_id",
            "track_name",
            "album_id",
            "album_name",
            "artist_id",
            "isrc",
            "release_year",
        ]
        ARTIST_COLS = ["artist_id", "artist_name"]
        GENRE_COLS = ["track_id", "genre"]

        if not rows:
            plays = pd.DataFrame(columns=PLAY_COLS)
            tracks = pd.DataFrame(columns=TRACK_COLS)
            artists = pd.DataFrame(columns=ARTIST_COLS)
            genres = pd.DataFrame(columns=GENRE_COLS)
            plays.attrs["source_files_count"] = file_count  # 0 if none matched
            return {"plays": plays, "tracks": tracks, "artists": artists, "genres": genres}

        df = pd.DataFrame.from_records(rows)

        # Canonical plays
        plays = (
            pd.DataFrame(
                {
                    "played_at": pd.to_datetime(df["played_at"], utc=True, errors="coerce"),
                    "track_id": df["track_id"].astype("string"),
                    "ms_played": pd.to_numeric(df["ms_played"], errors="coerce")
                    .fillna(0)
                    .astype("int64"),
                    "source": "spotify-extended",
                    "user_id": pd.Series([None] * len(df), dtype="string"),
                    "session_id": pd.Series([None] * len(df), dtype="string"),
                    "artist_name": df["_artist_name"].astype("string"),
                    "track_name": df["_track_name"].astype("string"),
                    "platform": df["_platform"].astype("string"),
                    "country": df["_country"].astype("string"),
                    "reason_start": df["_reason_start"].astype("string"),
                    "reason_end": df["_reason_end"].astype("string"),
                    "shuffle": df["_shuffle"].astype("boolean"),
                    "skipped": df["_skipped"].astype("boolean"),
                    "user_agent": df["_ua"].astype("string"),
                }
            )
            .sort_values("played_at")
            .reset_index(drop=True)
        )
        plays.attrs["source_files_count"] = file_count

        # Canonical tracks (dedup by track_id)
        tracks = (
            pd.DataFrame(
                {
                    "track_id": df["track_id"].astype("string"),
                    "track_name": df["_track_name"].astype("string"),
                    "album_id": pd.Series([None] * len(df), dtype="string"),
                    "album_name": df["_album_name"].astype("string"),
                    "artist_id": df["_artist_id"].astype("string"),
                    "isrc": pd.Series([None] * len(df), dtype="string"),
                    "release_year": pd.Series([None] * len(df), dtype="Int64"),
                }
            )
            .drop_duplicates(subset=["track_id"])
            .reset_index(drop=True)
        )

        # Canonical artists (dedup by artist_id)
        artists = (
            pd.DataFrame(
                {
                    "artist_id": df["_artist_id"].astype("string"),
                    "artist_name": df["_artist_name"].astype("string"),
                }
            )
            .dropna(subset=["artist_id"])
            .drop_duplicates(subset=["artist_id"])
            .reset_index(drop=True)
        )

        # No genres in Extended files — keep schema but empty
        genres = pd.DataFrame(columns=GENRE_COLS)

        return {"plays": plays, "tracks": tracks, "artists": artists, "genres": genres}
