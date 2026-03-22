"""Compute user listening overlap against a co-occurrence graph.

Given a Spotify Extended Streaming History and a ``CooccurrenceGraph``,
identify which graph nodes the user has actually listened to and
aggregate play statistics per matched node.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from traverse.graph.cooccurrence import CooccurrenceGraph

_HISTORY_GLOB = "Streaming_History_Audio_*.json"


def _first_nonempty(*vals: Any) -> Optional[Any]:
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return None


def _coerce_record(r: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise a single raw Spotify history record into a flat dict.

    Mirrors ``SpotifyExtendedExport._coerce_record()`` but avoids pulling
    in pandas for lightweight use.
    """
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

    try:
        ms_played = int(ms_played_val) if ms_played_val is not None else 0
    except Exception:
        ms_played = 0

    # Parse timestamp to epoch milliseconds
    ts_epoch_ms: Optional[int] = None
    if ts:
        try:
            from datetime import datetime, timezone

            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts_epoch_ms = int(dt.timestamp() * 1000)
        except Exception:
            pass

    return {
        "artist": str(artist).strip() if artist else None,
        "track": str(track).strip() if track else None,
        "album": str(album).strip() if album else None,
        "ms_played": ms_played,
        "ts_epoch_ms": ts_epoch_ms,
    }


def _load_records_from_dir(history_dir: Path) -> List[Dict[str, Any]]:
    """Load and coerce all Spotify history records from a directory."""
    records: List[Dict[str, Any]] = []
    for path in sorted(history_dir.glob(_HISTORY_GLOB)):
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for raw in data:
                records.append(_coerce_record(raw))
    return records


def _detect_graph_type(points: List[Dict[str, Any]]) -> str:
    """Detect whether the graph is artist-centric or album-centric.

    Album graph points have an ``artist`` field; artist graph points do not.
    """
    for pt in points[:20]:
        if "artist" in pt and pt["artist"]:
            return "album"
    return "artist"


def _build_artist_lookup(
    records: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Group records by lowercased artist name."""
    lookup: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        artist = rec.get("artist")
        if artist:
            lookup[artist.lower()].append(rec)
    return dict(lookup)


def _build_album_lookup(
    records: List[Dict[str, Any]],
) -> Tuple[Dict[Tuple[str, str], List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]:
    """Group records by (artist, album) and also by artist alone for fallback."""
    album_lookup: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    artist_lookup: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        artist = rec.get("artist")
        album = rec.get("album")
        if artist:
            artist_key = artist.lower()
            artist_lookup[artist_key].append(rec)
            if album:
                album_lookup[(artist_key, album.lower())].append(rec)
    return dict(album_lookup), dict(artist_lookup)


def _aggregate_match(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate play statistics from a list of matched records."""
    play_count = len(records)
    total_ms = sum(r.get("ms_played", 0) for r in records)

    timestamps = [r["ts_epoch_ms"] for r in records if r.get("ts_epoch_ms")]
    first_listen = min(timestamps) if timestamps else None
    last_listen = max(timestamps) if timestamps else None

    # Top tracks by play count
    track_counts: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        track_name = rec.get("track") or "Unknown"
        if track_name not in track_counts:
            track_counts[track_name] = {"trackName": track_name, "playCount": 0, "totalMs": 0}
        track_counts[track_name]["playCount"] += 1
        track_counts[track_name]["totalMs"] += rec.get("ms_played", 0)

    top_tracks = sorted(track_counts.values(), key=lambda x: x["playCount"], reverse=True)[:5]

    result: Dict[str, Any] = {
        "playCount": play_count,
        "totalMs": total_ms,
        "topTracks": top_tracks,
    }
    if first_listen is not None:
        result["firstListenEpochMs"] = first_listen
    if last_listen is not None:
        result["lastListenEpochMs"] = last_listen
    return result


def compute_user_overlap(
    graph: CooccurrenceGraph,
    history_dir: Optional[Path] = None,
    history_records: Optional[List[Dict[str, Any]]] = None,
    min_ms_played: int = 30_000,
) -> Dict[str, Any]:
    """Compute which graph nodes a user has listened to.

    Parameters
    ----------
    graph
        A ``CooccurrenceGraph`` with ``points`` and ``links``.
    history_dir
        Path to a Spotify Extended Streaming History directory.
    history_records
        Pre-parsed raw Spotify JSON records (alternative to ``history_dir``).
    min_ms_played
        Minimum ms_played threshold to count a play (default 30s).

    Returns
    -------
    dict
        ``matches`` list, ``totalMatched``, ``totalNodes``.
    """
    # Load records
    if history_dir is not None:
        records = _load_records_from_dir(history_dir)
    elif history_records is not None:
        records = [_coerce_record(r) for r in history_records]
    else:
        return {"matches": [], "totalMatched": 0, "totalNodes": len(graph["points"])}

    # Filter by minimum play duration
    records = [r for r in records if r.get("ms_played", 0) >= min_ms_played]

    if not records:
        return {"matches": [], "totalMatched": 0, "totalNodes": len(graph["points"])}

    points = graph["points"]
    graph_type = _detect_graph_type(points)

    matches: List[Dict[str, Any]] = []

    if graph_type == "artist":
        artist_lookup = _build_artist_lookup(records)
        for pt in points:
            node_id = pt.get("id", "")
            matched_records = artist_lookup.get(node_id.lower())
            if matched_records:
                match = _aggregate_match(matched_records)
                match["nodeId"] = node_id
                matches.append(match)
    else:
        # Album graph
        album_lookup, artist_fallback = _build_album_lookup(records)
        for pt in points:
            node_id = pt.get("id", "")
            artist = pt.get("artist", "")
            label = pt.get("label", "")

            # Try exact (artist, album) match first
            matched_records = album_lookup.get(
                (artist.lower() if artist else "", label.lower() if label else "")
            )
            # Fallback to artist-only match
            if not matched_records and artist:
                matched_records = artist_fallback.get(artist.lower())

            if matched_records:
                match = _aggregate_match(matched_records)
                match["nodeId"] = node_id
                matches.append(match)

    # Sort by play count descending
    matches.sort(key=lambda m: m["playCount"], reverse=True)

    return {
        "matches": matches,
        "totalMatched": len(matches),
        "totalNodes": len(points),
    }
