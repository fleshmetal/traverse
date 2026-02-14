from __future__ import annotations
import sys
from pathlib import Path
from typing import Iterable, List, Tuple
from collections import Counter
import pandas as pd

# use Traverse splitter
from traverse.processing.normalize import split_genres_styles

BAD_STR = {"", "na", "nan", "none", "null", "[]", "<na>", "n/a"}

def _status(msg: str) -> None:
    print(msg, file=sys.stderr)

def _clean_tag_columns(df: pd.DataFrame, cols: Iterable[str]) -> None:
    for c in cols:
        if c in df.columns:
            s = df[c].astype("string")
            s = s.where(~s.str.strip().str.lower().isin(BAD_STR), pd.NA)
            df[c] = s

def _safe_split(val: object) -> List[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    s = str(val).strip()
    if s.lower() in BAD_STR:
        return []
    tokens = split_genres_styles(s) or []
    return [t for t in tokens if t and (t.strip().lower() not in BAD_STR)]

def _co_pairs(tags: Iterable[str]) -> Iterable[Tuple[str, str]]:
    uniq = sorted(set(t for t in tags if t))
    if len(uniq) < 2:
        return []
    import itertools
    return itertools.combinations(uniq, 2)

def main() -> None:
    import argparse
    ap = argparse.ArgumentParser("Diagnose canonical tags & co-occurrence")
    ap.add_argument("--cache-dir", required=True, help="Path containing canonical_plays.parquet and canonical_tracks.parquet")
    ap.add_argument("--sample", type=int, default=100000, help="Sample N rows for quick co-occurrence trial")
    args = ap.parse_args()

    cache = Path(args.cache_dir)
    p_parq = cache / "canonical_plays.parquet"
    t_parq = cache / "canonical_tracks.parquet"

    if not p_parq.exists() or not t_parq.exists():
        _status(f"ERROR: canonical parquet not found in {cache}")
        sys.exit(2)

    plays = pd.read_parquet(p_parq)
    tracks = pd.read_parquet(t_parq)

    _status(f"plays columns: {list(plays.columns)}")
    _status(f"tracks columns: {list(tracks.columns)}")
    _status(f"plays rows: {len(plays):,} | tracks rows: {len(tracks):,}")

    # Normalize & clean placeholders
    for df in (plays, tracks):
        for c in df.columns:
            if c.lower() in {"genres","styles"}:
                df.rename(columns={c: c.lower()}, inplace=True)
    _clean_tag_columns(plays, ["genres","styles"])
    _clean_tag_columns(tracks, ["genres","styles"])

    # Where do tags live?
    plays_g_nonnull = int(plays.get("genres", pd.Series(dtype="string")).notna().sum())
    plays_s_nonnull = int(plays.get("styles", pd.Series(dtype="string")).notna().sum())
    tracks_g_nonnull = int(tracks.get("genres", pd.Series(dtype="string")).notna().sum())
    tracks_s_nonnull = int(tracks.get("styles", pd.Series(dtype="string")).notna().sum())

    _status(f"plays[genres] non-null={plays_g_nonnull:,} | plays[styles] non-null={plays_s_nonnull:,}")
    _status(f"tracks[genres] non-null={tracks_g_nonnull:,} | tracks[styles] non-null={tracks_s_nonnull:,}")

    # If plays have no tags, try joining from tracks by track_id
    joined = plays.copy()
    if (plays_g_nonnull == 0 and plays_s_nonnull == 0) and "track_id" in plays.columns and "track_id" in tracks.columns:
        keep = [c for c in ("track_id","genres","styles") if c in tracks.columns]
        joined = joined.merge(tracks[keep], on="track_id", how="left", suffixes=("", "_trk"))
        _clean_tag_columns(joined, ["genres","styles"])
        _status("Performed track_id left-join to bring tags onto plays.")
        j_g_nonnull = int(joined.get("genres", pd.Series(dtype="string")).notna().sum())
        j_s_nonnull = int(joined.get("styles", pd.Series(dtype="string")).notna().sum())
        _status(f"joined[genres] non-null={j_g_nonnull:,} | joined[styles] non-null={j_s_nonnull:,}")
    else:
        _status("Using tags directly on plays.")

    # Quick co-occurrence probe on a sample
    probe = joined[["played_at","genres","styles"]].head(args.sample)
    counts: Counter[Tuple[str,str]] = Counter()
    rows_with_any = rows_with_pairs = 0
    for g, s in probe[["genres","styles"]].itertuples(index=False, name=None):
        tags: List[str] = []
        tags.extend(_safe_split(g))
        tags.extend(_safe_split(s))
        uniq = sorted(set(tags))
        if uniq:
            rows_with_any += 1
            for a,b in _co_pairs(uniq):
                counts[(a,b)] += 1
                rows_with_pairs += 1

    _status(f"DIAG: rows_with_any_tags(sample)={rows_with_any:,} | pairs_found(sample)={len(counts):,}")
    if counts:
        top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:25]
        _status("Top pairs (sample):")
        for (a,b),w in top:
            _status(f"  - {a} ~~ {b}: {w}")
    else:
        _status("No pairs in sample — root cause is likely empty tags on both plays & tracks or unexpected tag format.")

if __name__ == "__main__":
    main()
