from __future__ import annotations

import pandas as pd

from traverse.processing.cache import CanonicalTableCache


def _mock_build_fn() -> dict[str, pd.DataFrame]:
    plays = pd.DataFrame(
        {
            "played_at": pd.to_datetime(["2021-01-01T00:00:00Z"], utc=True),
            "track_id": ["t1"],
            "ms_played": [1000],
            "track_name": ["Song A"],
            "artist_name": ["Artist A"],
        }
    )
    tracks = pd.DataFrame(
        {
            "track_id": ["t1"],
            "track_name": ["Song A"],
            "artist_name": ["Artist A"],
            "genres": ["rock|pop"],
            "styles": [""],
        }
    )
    return {"plays": plays, "tracks": tracks, "artists": pd.DataFrame()}


def test_build_and_cache(tmp_path: object) -> None:
    cache = CanonicalTableCache(cache_dir=tmp_path, build_fn=_mock_build_fn)  # type: ignore[arg-type]
    pw, tw = cache.load_or_build()
    assert not pw.empty
    assert not tw.empty
    assert (tmp_path / "canonical_plays.parquet").exists()  # type: ignore[operator]
    assert (tmp_path / "canonical_tracks.parquet").exists()  # type: ignore[operator]


def test_load_from_cache(tmp_path: object) -> None:
    cache = CanonicalTableCache(cache_dir=tmp_path, build_fn=_mock_build_fn)  # type: ignore[arg-type]
    cache.load_or_build()

    call_count = 0

    def counting_build() -> dict[str, pd.DataFrame]:
        nonlocal call_count
        call_count += 1
        return _mock_build_fn()

    cache2 = CanonicalTableCache(cache_dir=tmp_path, build_fn=counting_build)  # type: ignore[arg-type]
    cache2.load_or_build()
    assert call_count == 0  # cache hit, no rebuild


def test_force_rebuild(tmp_path: object) -> None:
    cache = CanonicalTableCache(cache_dir=tmp_path, build_fn=_mock_build_fn)  # type: ignore[arg-type]
    cache.load_or_build()

    call_count = 0

    def counting_build() -> dict[str, pd.DataFrame]:
        nonlocal call_count
        call_count += 1
        return _mock_build_fn()

    cache2 = CanonicalTableCache(cache_dir=tmp_path, build_fn=counting_build, force=True)  # type: ignore[arg-type]
    cache2.load_or_build()
    assert call_count == 1


def test_with_enrich_fn(tmp_path: object) -> None:
    enriched = False

    def enrich(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        nonlocal enriched
        enriched = True
        return tables

    cache = CanonicalTableCache(
        cache_dir=tmp_path,  # type: ignore[arg-type]
        build_fn=_mock_build_fn,
        enrich_fn=enrich,
    )
    pw, tw = cache.load_or_build()
    assert enriched
    assert not pw.empty
