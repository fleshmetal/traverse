"""Tests for traverse.graph.user_overlap."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pytest

from traverse.graph.cooccurrence import CooccurrenceGraph
from traverse.graph.user_overlap import compute_user_overlap


def _artist_graph(names: List[str]) -> CooccurrenceGraph:
    """Build a minimal artist-type graph (no 'artist' field on points)."""
    points = [{"id": n, "label": n, "genres": "rock"} for n in names]
    links = (
        [{"source": names[0], "target": names[1], "weight": 1}]
        if len(names) >= 2
        else []
    )
    return CooccurrenceGraph(points=points, links=links)


def _album_graph(entries: List[Dict[str, str]]) -> CooccurrenceGraph:
    """Build a minimal album-type graph (has 'artist' field on points)."""
    points = [
        {
            "id": f"{e['label']}::{e['artist']}".lower(),
            "label": e["label"],
            "artist": e["artist"],
            "genres": "electronic",
        }
        for e in entries
    ]
    links = (
        [{"source": points[0]["id"], "target": points[1]["id"], "weight": 1}]
        if len(points) >= 2
        else []
    )
    return CooccurrenceGraph(points=points, links=links)


def _history_records(
    entries: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build raw Spotify-style history records."""
    records = []
    for e in entries:
        records.append(
            {
                "master_metadata_album_artist_name": e.get("artist"),
                "master_metadata_track_name": e.get("track", "Track A"),
                "master_metadata_album_album_name": e.get("album"),
                "ms_played": e.get("ms_played", 60_000),
                "ts": e.get("ts", "2024-06-15T12:00:00Z"),
            }
        )
    return records


class TestComputeUserOverlap:
    """Tests for compute_user_overlap()."""

    def test_no_history(self) -> None:
        graph = _artist_graph(["Artist A", "Artist B"])
        result = compute_user_overlap(graph)
        assert result["totalMatched"] == 0
        assert result["totalNodes"] == 2
        assert result["matches"] == []

    def test_artist_graph_match(self) -> None:
        graph = _artist_graph(["Artist A", "Artist B", "Artist C"])
        records = _history_records(
            [
                {"artist": "Artist A", "ms_played": 60_000},
                {"artist": "Artist A", "ms_played": 120_000},
                {"artist": "artist b", "ms_played": 45_000},  # case-insensitive
            ]
        )
        result = compute_user_overlap(graph, history_records=records)
        assert result["totalMatched"] == 2
        assert result["totalNodes"] == 3

        # Should be sorted by play count (Artist A first with 2 plays)
        match_map = {m["nodeId"]: m for m in result["matches"]}
        assert "Artist A" in match_map
        assert "Artist B" in match_map
        assert "Artist C" not in match_map
        assert match_map["Artist A"]["playCount"] == 2
        assert match_map["Artist A"]["totalMs"] == 180_000

    def test_artist_graph_min_ms_filter(self) -> None:
        graph = _artist_graph(["Artist A"])
        records = _history_records(
            [
                {"artist": "Artist A", "ms_played": 5_000},  # too short
                {"artist": "Artist A", "ms_played": 60_000},
            ]
        )
        result = compute_user_overlap(graph, history_records=records)
        assert result["totalMatched"] == 1
        assert result["matches"][0]["playCount"] == 1

    def test_album_graph_exact_match(self) -> None:
        graph = _album_graph(
            [
                {"label": "Album X", "artist": "Artist A"},
                {"label": "Album Y", "artist": "Artist B"},
            ]
        )
        records = _history_records(
            [
                {"artist": "Artist A", "album": "Album X", "ms_played": 60_000},
            ]
        )
        result = compute_user_overlap(graph, history_records=records)
        assert result["totalMatched"] == 1
        assert result["matches"][0]["nodeId"] == "album x::artist a"

    def test_album_graph_artist_fallback(self) -> None:
        """When album doesn't match, falls back to artist-only matching."""
        graph = _album_graph(
            [
                {"label": "Album X", "artist": "Artist A"},
                {"label": "Album Y", "artist": "Artist B"},
            ]
        )
        records = _history_records(
            [
                {
                    "artist": "Artist A",
                    "album": "Different Album",
                    "ms_played": 60_000,
                },
            ]
        )
        result = compute_user_overlap(graph, history_records=records)
        assert result["totalMatched"] == 1
        assert result["matches"][0]["nodeId"] == "album x::artist a"

    def test_top_tracks(self) -> None:
        graph = _artist_graph(["Artist A"])
        records = _history_records(
            [
                {"artist": "Artist A", "track": "Song 1", "ms_played": 60_000},
                {"artist": "Artist A", "track": "Song 1", "ms_played": 60_000},
                {"artist": "Artist A", "track": "Song 2", "ms_played": 60_000},
            ]
        )
        result = compute_user_overlap(graph, history_records=records)
        match = result["matches"][0]
        assert len(match["topTracks"]) == 2
        assert match["topTracks"][0]["trackName"] == "Song 1"
        assert match["topTracks"][0]["playCount"] == 2

    def test_timestamps(self) -> None:
        graph = _artist_graph(["Artist A"])
        records = _history_records(
            [
                {"artist": "Artist A", "ts": "2023-01-01T00:00:00Z", "ms_played": 60_000},
                {"artist": "Artist A", "ts": "2024-06-15T12:00:00Z", "ms_played": 60_000},
            ]
        )
        result = compute_user_overlap(graph, history_records=records)
        match = result["matches"][0]
        assert "firstListenEpochMs" in match
        assert "lastListenEpochMs" in match
        assert match["firstListenEpochMs"] < match["lastListenEpochMs"]

    def test_history_dir(self, tmp_path: Path) -> None:
        graph = _artist_graph(["Artist A", "Artist B"])
        raw_records = [
            {
                "master_metadata_album_artist_name": "Artist A",
                "master_metadata_track_name": "Track 1",
                "ms_played": 60_000,
                "ts": "2024-01-01T00:00:00Z",
            },
        ]
        hist_file = tmp_path / "Streaming_History_Audio_0.json"
        hist_file.write_text(json.dumps(raw_records), encoding="utf-8")

        result = compute_user_overlap(graph, history_dir=tmp_path)
        assert result["totalMatched"] == 1
        assert result["matches"][0]["nodeId"] == "Artist A"

    def test_empty_records(self) -> None:
        graph = _artist_graph(["Artist A"])
        result = compute_user_overlap(graph, history_records=[])
        assert result["totalMatched"] == 0

    def test_no_match(self) -> None:
        graph = _artist_graph(["Artist A", "Artist B"])
        records = _history_records(
            [{"artist": "Unknown Artist", "ms_played": 60_000}]
        )
        result = compute_user_overlap(graph, history_records=records)
        assert result["totalMatched"] == 0
        assert result["totalNodes"] == 2
