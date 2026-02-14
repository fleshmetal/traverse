# scripts/export_cosmo_genres_timeline.py
from __future__ import annotations
import argparse
import json
import sys
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional

import pandas as pd

# --- Project imports (Week-3/4 tooling) ---------------------------------------
_Enricher = None
try:
    from traverse.processing.enrich_fast import FastGenreStyleEnricher as _Enricher  # type: ignore
except Exception:
    try:
        from traverse.processing.enrich import GenreStyleEnricher as _Enricher  # type: ignore
    except Exception:
        _Enricher = None

from traverse.processing.tables import BuildCanonicalTables  # noqa: E402
from traverse.processing.base import Pipeline  # noqa: E402
from traverse.processing.normalize import split_genres_styles  # noqa: E402

# --- Helpers ------------------------------------------------------------------

def _status(msg: str) -> None:
    print(msg, file=sys.stderr)

def _load_spotify_extended_minimal(extended_dir: Path, progress: bool = True) -> Dict[str, pd.DataFrame]:
    from glob import glob
    import gzip
    import io
    import json as _json

    patterns = [
        str(extended_dir / "Streaming_History_Audio*.json"),
        str(extended_dir / "Streaming_History_Audio*.json.gz"),
    ]
    files: List[str] = []
    for pat in patterns:
        files.extend(sorted(glob(pat)))
    if not files:
        raise FileNotFoundError(f"No ExtendedStreamingHistory files in: {extended_dir}")

    rows: List[Dict[str, object]] = []

    if progress:
        try:
            from tqdm import tqdm
            it = tqdm(files, desc="Reading Extended JSON", unit="file")
        except Exception:
            it = files
    else:
        it = files

    for fp in it:
        if fp.endswith(".gz"):
            with gzip.open(fp, "rb") as f:
                data = _json.load(io.TextIOWrapper(f, encoding="utf-8"))
        else:
            with open(fp, "r", encoding="utf-8") as f:
                data = _json.load(f)

        for r in data:
            played_at = r.get("ts")
            ms_played = r.get("ms_played")
            track_name = r.get("master_metadata_track_name") or r.get("track_name")
            artist_name = r.get("master_metadata_album_artist_name") or r.get("artist_name")
            track_uri = r.get("spotify_track_uri") or r.get("track_uri")

            track_id: Optional[str] = None
            if isinstance(track_uri, str) and track_uri.startswith("spotify:track:"):
                track_id = "trk:" + track_uri.split(":")[-1]
            if not track_id:
                if track_name and artist_name:
                    track_id = f"nk:{str(artist_name).strip().lower()}||{str(track_name).strip().lower()}"
                else:
                    track_id = None

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
        plays[["track_id", "track_name", "artist_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    artists = (
        tracks[["artist_name"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
        .rename(columns={"artist_name": "name"})
    )
    artists["artist_id"] = artists["name"].apply(lambda s: f"art:{str(s).strip().lower()}")
    artists = artists[["artist_id", "name"]]

    return {"plays": plays, "tracks": tracks, "artists": artists}

def _ensure_canonical(
    extended_dir: Path,
    records_csv: Path | None,
    out_dir: Path,
    chunksize: int,
    progress: bool,
    force: bool,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    out_dir.mkdir(parents=True, exist_ok=True)
    p_parquet = out_dir / "canonical_plays.parquet"
    t_parquet = out_dir / "canonical_tracks.parquet"

    if not force and p_parquet.exists() and t_parquet.exists():
        _status(f"✔ Using cached canonical tables in {out_dir}")
        return pd.read_parquet(p_parquet), pd.read_parquet(t_parquet)

    _status("⏳ Building canonical tables from Extended Streaming History...")
    t0 = _load_spotify_extended_minimal(extended_dir, progress=progress)

    if _Enricher and records_csv:
        _status("⏳ Enriching with Records (genres/styles)...")
        enr = _Enricher(records_csv=str(records_csv), records_chunksize=chunksize)
        t_enriched = enr.run(t0)
    else:
        if not _Enricher:
            _status("⚠ Enricher not available; proceeding without Records enrichment.")
        elif not records_csv:
            _status("⚠ No --records-csv provided; proceeding without enrichment.")
        t_enriched = t0

    pipe = Pipeline([BuildCanonicalTables()])
    tout = pipe.run(t_enriched)
    plays_wide = tout.get("plays_wide", pd.DataFrame())
    tracks_wide = tout.get("tracks_wide", pd.DataFrame())

    if plays_wide.empty:
        raise RuntimeError("Canonical plays_wide empty; check input/enrichment.")

    try:
        plays_wide.to_parquet(p_parquet, index=False)
        tracks_wide.to_parquet(t_parquet, index=False)
        _status(f"✔ Cached canonical tables in {out_dir}")
    except Exception as e:
        _status(f"⚠ Failed to cache canonical tables: {e}")

    return plays_wide, tracks_wide

def _cooccurrence_pairs(tags: Iterable[str]) -> Iterable[Tuple[str, str]]:
    uniq = sorted(set(t for t in tags if t))
    if len(uniq) < 2:
        return []
    return combinations(uniq, 2)

def _pretty_label(t: str) -> str:
    title = t.title()
    return title.replace("Idm", "IDM").replace("Edm", "EDM").replace("Dnb", "DnB")

# --- Main ---------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Spotify Extended → (Enrich) → Canonical → Genre co-occurrence with timeline → Cosmograph JSON."
    )
    ap.add_argument("--extended-dir", required=True)
    ap.add_argument("--records-csv")
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--min-cooccurrence", type=int, default=2)
    ap.add_argument("--max-edges", type=int, default=40_000, help="0 = no cap")
    ap.add_argument("--max-nodes", type=int, default=5_000, help="0 = no cap")
    ap.add_argument("--cache-dir", default="_out")
    ap.add_argument("--out-json", default="src/traverse/cosmograph/app/dist/cosmo_genres_timeline.json")
    ap.add_argument("--progress", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    extended_dir = Path(args.extended_dir)
    records_csv = Path(args.records_csv) if args.records_csv else None
    cache_dir = Path(args.cache_dir)
    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    plays_wide, tracks_wide = _ensure_canonical(
        extended_dir=extended_dir,
        records_csv=records_csv,
        out_dir=cache_dir,
        chunksize=args.chunksize,
        progress=args.progress,
        force=args.force,
    )

    # Ensure tags available on plays_wide; join from tracks_wide if needed.
    if "genres" not in plays_wide.columns and "genres" in tracks_wide.columns and "track_id" in plays_wide.columns:
        tag_cols = [c for c in ("track_id", "genres", "styles") if c in tracks_wide.columns]
        if len(tag_cols) >= 2:
            plays_wide = plays_wide.merge(tracks_wide[tag_cols], on="track_id", how="left")

    gcol = "genres" if "genres" in plays_wide.columns else None
    scol = "styles" if "styles" in plays_wide.columns else None
    if gcol is None and scol is None:
        raise RuntimeError("No 'genres'/'styles' columns found after canonical build.")

    # Co-occurrence and time extraction
    counts: Counter[Tuple[str, str]] = Counter()
    first_link_time: Dict[Tuple[str, str], int] = {}
    first_tag_time: Dict[str, int] = {}
    first_label: Dict[str, str] = {}

    # Iterate each play row; collect tags; update first_seen for tags and tag-pairs.
    it_cols = [c for c in ["played_at", gcol, scol] if c is not None]
    it = plays_wide[it_cols].itertuples(index=False, name=None)
    rows_seen = 0
    for row in it:
        rows_seen += 1
        # row = (played_at, genres, styles) with optional styles None
        played_at = row[0]
        ts_ms: Optional[int] = None
        if pd.notna(played_at):
            try:
                ts_ms = int(pd.Timestamp(played_at).value // 1_000_000)  # ms since epoch
            except Exception:
                ts_ms = None

        gval = row[1] if len(row) > 1 else None
        sval = row[2] if len(row) > 2 else None

        tags: List[str] = []
        tags.extend(split_genres_styles(gval))
        tags.extend(split_genres_styles(sval))
        if not tags:
            continue

        # first label
        for t in set(tags):
            if t not in first_label:
                first_label[t] = _pretty_label(t)
            # first_seen per tag
            if ts_ms is not None:
                prev = first_tag_time.get(t)
                if prev is None or ts_ms < prev:
                    first_tag_time[t] = ts_ms

        # pairs + first_seen per link
        for a, b in _cooccurrence_pairs(tags):
            counts[(a, b)] += 1
            if ts_ms is not None:
                k = (a, b)
                prev = first_link_time.get(k)
                if prev is None or ts_ms < prev:
                    first_link_time[k] = ts_ms

    if not counts:
        _status("⚠ No tag co-occurrences found from plays. Nothing to export.")
        out_json.write_text(json.dumps({"points": [], "links": []}, indent=2))
        print(f"✔ Wrote {out_json}  (nodes=0, edges=0)")
        return

    # Threshold + rank + caps
    edges = [(a, b, w) for (a, b), w in counts.items() if w >= args.min_cooccurrence]
    edges.sort(key=lambda x: x[2], reverse=True)

    strength = defaultdict(int)
    for a, b, w in edges:
        strength[a] += w
        strength[b] += w

    if args.max_nodes and args.max_nodes > 0:
        top = {n for n, _ in sorted(strength.items(), key=lambda kv: kv[1], reverse=True)[: args.max_nodes]}
        edges = [(a, b, w) for a, b, w in edges if a in top and b in top]

    if args.max_edges and args.max_edges > 0 and len(edges) > args.max_edges:
        edges = edges[: args.max_edges]

    node_ids = set()
    for a, b, _ in edges:
        node_ids.add(a)
        node_ids.add(b)

    # Build points with first_seen (from earliest play where the tag appears)
    points: List[Dict[str, object]] = []
    for nid in sorted(node_ids):
        points.append(
            {
                "id": nid,
                "label": first_label.get(nid, nid),
                "first_seen": int(first_tag_time[nid]) if nid in first_tag_time else None,
            }
        )

    links: List[Dict[str, object]] = []
    for a, b, w in edges:
        links.append(
            {
                "source": a,
                "target": b,
                "weight": int(w),
                "first_seen": int(first_link_time.get((a, b), 0)) if (a, b) in first_link_time else None,
            }
        )

    print(
        f"plays_rows={rows_seen:,} | unique_tags={len(first_label):,} | "
        f"edges_out={len(links):,} | nodes_out={len(points):,}"
    )

    out = {"points": points, "links": links}
    out_json.write_text(json.dumps(out, indent=2))
    print(f"✔ Wrote {out_json}  (nodes={len(points)}, edges={len(links)})")

if __name__ == "__main__":
    main()
