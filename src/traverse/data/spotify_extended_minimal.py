"""Minimal Spotify Extended Streaming History loader.

Lighter than :class:`SpotifyExtendedExport`: produces only
(plays, tracks, artists) with name-key fallback IDs, and supports
``.json.gz`` files.

Consolidates the ``_load_spotify_extended_minimal()`` function duplicated
across several export scripts.
"""
from __future__ import annotations

import gzip
import io
import json
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def load_spotify_extended_minimal(
    extended_dir: Path,
    *,
    progress: bool = True,
) -> Dict[str, pd.DataFrame]:
    """Load Spotify Extended Streaming History JSON (and .json.gz) files.

    Returns a dict with keys ``plays``, ``tracks``, ``artists``.

    Columns in ``plays``:
        played_at, track_id, ms_played, track_name, artist_name
    """
    patterns = [
        str(extended_dir / "Streaming_History_Audio*.json"),
        str(extended_dir / "Streaming_History_Audio*.json.gz"),
    ]
    files: List[str] = []
    for pat in patterns:
        files.extend(sorted(glob(pat)))

    if not files:
        raise FileNotFoundError(f"No ExtendedStreamingHistory files in: {extended_dir}")

    rows: List[Dict[str, Any]] = []

    it: Any = files
    if progress:
        try:
            from tqdm import tqdm

            it = tqdm(files, desc="Reading Extended JSON", unit="file")
        except Exception:
            pass

    for fp in it:
        data: List[Dict[str, Any]]
        if fp.endswith(".gz"):
            with gzip.open(fp, "rb") as f:
                data = json.load(io.TextIOWrapper(f, encoding="utf-8"))
        else:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)

        for r in data:
            played_at = r.get("ts")
            ms_played = r.get("ms_played")
            track_name = r.get("master_metadata_track_name") or r.get("track_name")
            artist_name = (
                r.get("master_metadata_album_artist_name") or r.get("artist_name")
            )
            track_uri = r.get("spotify_track_uri") or r.get("track_uri")

            track_id: Optional[str] = None
            if isinstance(track_uri, str) and track_uri.startswith("spotify:track:"):
                track_id = "trk:" + track_uri.split(":")[-1]
            if not track_id:
                if track_name and artist_name:
                    track_id = (
                        f"nk:{str(artist_name).strip().lower()}"
                        f"||{str(track_name).strip().lower()}"
                    )

            if played_at is None or ms_played is None or track_id is None:
                continue

            rows.append(
                {
                    "played_at": played_at,
                    "track_id": track_id,
                    "ms_played": int(ms_played) if str(ms_played).isdigit() else None,
                    "track_name": track_name,
                    "artist_name": artist_name,
                }
            )

    plays = pd.DataFrame(rows)
    if not plays.empty:
        plays["played_at"] = pd.to_datetime(plays["played_at"], utc=True, errors="coerce")
        plays = plays.dropna(subset=["played_at", "track_id"]).reset_index(drop=True)

    tracks = (
        plays[["track_id", "track_name", "artist_name"]].drop_duplicates().reset_index(drop=True)
        if not plays.empty
        else pd.DataFrame(columns=["track_id", "track_name", "artist_name"])
    )

    return {"plays": plays, "tracks": tracks, "artists": pd.DataFrame()}
