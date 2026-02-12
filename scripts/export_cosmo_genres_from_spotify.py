# scripts/export_cosmo_genres_spotify_timeline.py
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
# Try fast enrich first; fallback to the tested enrich if fast isn't available.
_Enricher = None
try:
    from traverse.processing.enrich_fast import FastGenreStyleEnricher as _Enricher  # type: ignore
except Exception:
    try:
        from traverse.processing.enrich import GenreStyleEnricher as _Enricher  # type: ignore
    except Exception:
        _Enricher = None  # we'll warn and proceed without enrich

from traverse.processing.tables import BuildCanonicalTables
from traverse.processing.base import Pipeline
from traverse.processing.normalize import split_genres_styles  # Week-4 splitter

# --- Helpers ------------------------------------------------------------------

def _status(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)

def _load_spotify_extended_minimal(extended_dir: Path, progress: bool = True) -> Dict[str, pd.DataFrame]:
    """
    Minimal, robust loader for Spotify Extended Streaming History directory.
    Produces tables: plays, tracks, artists (deduped from plays).

    Columns in plays: played_at, track_id, ms_played, track_name, artist_name
    """
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
        data: List[Dict[str, object]]
        if fp.endswith(".gz"):
            with gzip.open(fp, "rb") as f:
                data = _json.load(io.TextIOWrapper(f, encoding="utf-8"))
        else:
            with open(fp, "r", encoding="utf-8") as f:
                data = _json.load(f)

        for r in data:
            # Newer Spotify Extended schema (2023+) fields:
            # "ts","ms_played","master_metadata_track_name","master_metadata_album_artist_name","spotify_track_uri"
            played_at = r.get("ts")
            ms_played = r.get("ms_played")
            track_name = r.get("master_metadata_track_name") or r.get("track_name")
            artist_name = r.get("master_metadata_album_artist_name") or r.get("artist_name")
            track_uri = r.get("spotify_track_uri") or r.get("track_uri")
            # normalize track_id from uri if present: "spotify:track:<id>"
            track_id = None
            if isinstance(track_uri, str) and track_uri.startswith("spotify:track:"):
                track_id = "trk:" + track_uri.split(":")[-1]
            # Fallback: build a name-key if no ID
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
    records_csv: Optional[Path],
    out_dir: Path,
    chunksize: int,
    progress: bool,
    force: bool,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return (plays_wide, tracks_wide) canonical tables.
    - If cached in out_dir and not force, load them.
    - Else build from Extended + (optional) Enrich + BuildCanonicalTables, then cache.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    p_parquet = out_dir / "canonical_plays.parquet"
    t_parquet = out_dir / "canonical_tracks.parquet"
    p_csv = out_dir / "canonical_plays.csv"
    t_csv = out_dir / "canonical_tracks.csv"

    if not force:
        if p_parquet.exists() and t_parquet.exists():
            _status(f"✔ Using cached canonical tables in {out_dir} (parquet)")
            return pd.read_parquet(p_parquet), pd.read_parquet(t_parquet)
        if p_csv.exists() and t_csv.exists():
            _status(f"✔ Using cached canonical tables in {out_dir} (csv)")
            return pd.read_csv(p_csv), pd.read_csv(t_csv)

    _status("⏳ Building canonical tables from Extended Streaming History...")
    t0 = _load_spotify_extended_minimal(extended_dir, progress=progress)

    # Optionally enrich with Records to obtain genres/styles
    if _Enricher and records_csv:
        _status("⏳ Enriching with Records (genres/styles)...")
        enr = _Enricher(records_csv=str(records_csv), records_chunksize=chunksize)
        t_enriched = enr.run(t0)  # expects keys: plays, tracks, artists, genres, styles
    else:
        if not _Enricher:
            _status("⚠ Enricher not available; proceeding without Records enrichment.")
        elif not records_csv:
            _status("⚠ No --records-csv provided; proceeding without enrichment.")
        t_enriched = t0

    # Build canonical tables
    pipe = Pipeline([BuildCanonicalTables()])
    tout = pipe.run(t_enriched)
    plays_wide = tout.get("plays_wide", pd.DataFrame())
    tracks_wide = tout.get("tracks_wide", pd.DataFrame())

    if plays_wide.empty:
        raise RuntimeError("Canonical plays_wide empty; check input/enrichment.")

    # Cache
    try:
        plays_wide.to_parquet(p_parquet, index=False)
        tracks_wide.to_parquet(t_parquet, index=False)
        _status(f"✔ Cached canonical tables (parquet) in {out_dir}")
    except Exception as e:
        _status(f"⚠ Failed to cache parquet: {e}")
        try:
            plays_wide.to_csv(p_csv, index=False)
            tracks_wide.to_csv(t_csv, index=False)
            _status(f"✔ Cached canonical tables (csv) in {out_dir}")
        except Exception as e2:
            _status(f"⚠ Failed to cache csv: {e2}")

    return plays_wide, tracks_wide

def _cooccurrence_pairs(tags: Iterable[str]) -> Iterable[Tuple[str, str]]:
    uniq = sorted(set(t for t in tags if t))
    if len(uniq) < 2:
        return []
    return combinations(uniq, 2)

def _pretty_label(t: str) -> str:
    title = t.title()
    return title.replace("Idm", "IDM").replace("Edm", "EDM").replace("Dnb", "DnB")

def _extract_tags_from_row(genres_val: object, styles_val: object) -> List[str]:
    tags: List[str] = []
    tags.extend(split_genres_styles(genres_val))
    tags.extend(split_genres_styles(styles_val))
    return tags

def _to_epoch_ms(ts_val: object) -> Optional[int]:
    if ts_val is None:
        return None
    try:
        ts = pd.to_datetime(ts_val, utc=True)
        if pd.isna(ts):
            return None
        return int(ts.value // 1_000_000)  # ns -> ms
    except Exception:
        return None

# --- Main ---------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Spotify Extended → (Enrich) → Canonical → Genre/style co-occurrence with timeline → Cosmograph JSON."
    )
    ap.add_argument("--extended-dir", required=True, help="Directory of ExtendedStreamingHistory JSON files")
    ap.add_argument("--records-csv", help="records.csv (for enrichment); optional but recommended")
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--min-cooccurrence", type=int, default=2)
    ap.add_argument("--max-edges", type=int, default=40_000, help="0 = no cap")
    ap.add_argument("--max-nodes", type=int, default=5_000, help="0 = no cap")
    ap.add_argument("--out-json", default="src/traverse/cosmograph/app/dist/cosmo_genres_spotify.json")
    ap.add_argument("--cache-dir", default="_out", help="Where canonical_* are cached/loaded")
    ap.add_argument("--progress", action="store_true")
    ap.add_argument("--force", action="store_true", help="Rebuild canonicals even if cache exists")
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

    # Ensure we have tags on plays
    gcol = "genres" if "genres" in plays_wide.columns else None
    scol = "styles" if "styles" in plays_wide.columns else None

    if gcol is None and "genres" in tracks_wide.columns:
        _status("ℹ No 'genres' on plays_wide; merging from tracks_wide by track_id.")
        tag_cols = [c for c in ("track_id", "genres", "styles") if c in tracks_wide.columns]
        if "track_id" in plays_wide.columns and "track_id" in tracks_wide.columns and len(tag_cols) >= 2:
            plays_wide = plays_wide.merge(tracks_wide[tag_cols], on="track_id", how="left")
            gcol = "genres" if "genres" in plays_wide.columns else None
            scol = "styles" if "styles" in plays_wide.columns else None

    if gcol is None and scol is None:
        raise RuntimeError("No 'genres'/'styles' columns found after canonical build. Enrichment likely missing.")

    if "played_at" not in plays_wide.columns:
        raise RuntimeError("'played_at' not present in canonical plays_wide.")

    # Build co-occurrence over actual listening + collect first_seen timestamps.
    counts: Counter[Tuple[str, str]] = Counter()
    first_label: Dict[str, str] = {}

    # first occurrence for points and links (ms since epoch)
    point_first_seen: Dict[str, int] = {}
    link_first_seen: Dict[Tuple[str, str], int] = {}

    # Iterate rows once; derive tags and played_at epoch
    n_rows = 0
    cols = ["played_at"] + [c for c in [gcol, scol] if c is not None]
    for row in plays_wide[cols].itertuples(index=False, name=None):
        n_rows += 1
        played_at = row[0]
        gval = row[1] if gcol else None
        sval = row[2] if (gcol and scol and len(row) > 2) else (row[1] if (not gcol and scol) else None)

        ts_ms = _to_epoch_ms(played_at)
        tags = _extract_tags_from_row(gval, sval)
        if not tags:
            continue

        # labels + point first_seen
        for t in set(tags):
            if t not in first_label:
                first_label[t] = _pretty_label(t)
            if ts_ms is not None:
                older = point_first_seen.get(t)
                if older is None or ts_ms < older:
                    point_first_seen[t] = ts_ms

        # pair counts + link first_seen
        for a, b in _cooccurrence_pairs(tags):
            counts[(a, b)] += 1
            if ts_ms is not None:
                k = (a, b) if a <= b else (b, a)
                older = link_first_seen.get(k)
                if older is None or ts_ms < older:
                    link_first_seen[k] = ts_ms

        if n_rows % 100_000 == 0:
            _status(f"[export] processed rows: {n_rows:,}")

    if not counts:
        _status("⚠ No tag co-occurrences found from plays. Nothing to export.")
        out_json.write_text(json.dumps({"points": [], "links": []}, indent=2))
        print(f"✔ Wrote {out_json}  (nodes=0, edges=0)")
        return

    # Threshold, cap, and finalize
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

    # Compose output with labels + first_seen (ms)
    points = []
    for nid in sorted(node_ids):
        pt = {
            "id": nid,
            "label": first_label.get(nid, nid),
        }
        if nid in point_first_seen:
            pt["first_seen"] = int(point_first_seen[nid])
        points.append(pt)

    links = []
    for a, b, w in edges:
        k = (a, b) if a <= b else (b, a)
        lk = {
            "source": a,
            "target": b,
            "weight": int(w),
        }
        if k in link_first_seen:
            lk["first_seen"] = int(link_first_seen[k])
        links.append(lk)

    print(
        f"plays_rows={n_rows:,} | unique_tags={len(first_label):,} | "
        f"edges_out={len(links):,} | nodes_out={len(points):,}"
    )

    out = {"points": points, "links": links}
    out_json.write_text(json.dumps(out, indent=2))
    print(f"✔ Wrote {out_json}  (nodes={len(points)}, edges={len(links)})")

if __name__ == "__main__":
    main()
