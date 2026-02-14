from __future__ import annotations
import argparse
import json
import sys
import re
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional, Set

import pandas as pd

# -------------------- Debug helpers --------------------
def _status(msg: str) -> None:
    print(msg, file=sys.stderr)

DBG_DIR = Path("_debug")
def _ensure_dbg_dir() -> None:
    DBG_DIR.mkdir(parents=True, exist_ok=True)

def _writetxt(rel: str, text: str) -> None:
    p = DBG_DIR / rel
    try:
        p.write_text(text, encoding="utf-8")
        _status(f"[DBG] wrote {p.resolve()}")
    except Exception as e:
        _status(f"[DBG] failed to write {p}: {e}")

def _writecsv(rel: str, df: pd.DataFrame, n: int = 1000) -> None:
    p = DBG_DIR / rel
    try:
        df.head(n).to_csv(p, index=False)
        _status(f"[DBG] wrote {p.resolve()}")
    except Exception as e:
        _status(f"[DBG] failed to write {p}: {e}")

# -------------------- Tag parsing ----------------------
SEP_RE = re.compile(r"[|,;/]+")

def norm_tag(t: object) -> str:
    s = re.sub(r"\s+", " ", str(t or "")).strip().lower()
    s = s.strip(" '\"-–—·•")
    return s

def split_tags(val: object) -> List[str]:
    if val is None:
        return []
    try:
        if pd.isna(val):
            return []
    except Exception:
        pass
    s = str(val).strip()
    if not s:
        return []
    if s.lower() in {"nan", "none", "null"}:
        return []
    if s == "[]":
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            return [norm_tag(x) for x in arr if str(x).strip()]
        except Exception:
            pass
    return [norm_tag(x) for x in SEP_RE.split(s) if str(x).strip()]

def pretty_label(t: str) -> str:
    out = str(t).title()
    return out.replace("Idm", "IDM").replace("Edm", "EDM").replace("Dnb", "DnB")

def pair(a: str, b: str) -> Tuple[str, str]:
    return (a, b) if a <= b else (b, a)

def cooccurrence_pairs(tags: Iterable[str]) -> Iterable[Tuple[str, str]]:
    uniq = sorted(set(t for t in tags if t))
    if len(uniq) < 2:
        return []
    return combinations(uniq, 2)

# -------------------- Traverse tooling (optional) --------------------
_Enricher = None
try:
    from traverse.processing.enrich_fast import FastGenreStyleEnricher as _Enricher  # type: ignore
except Exception:
    try:
        from traverse.processing.enrich import GenreStyleEnricher as _Enricher  # type: ignore
    except Exception:
        _Enricher = None

try:
    from traverse.processing.tables import BuildCanonicalTables
    from traverse.processing.base import Pipeline
except Exception:
    BuildCanonicalTables = None
    Pipeline = None

# -------------------- Spotify Extended loader ------------------------
def _load_spotify_extended_minimal(extended_dir: Path, progress: bool = True) -> Dict[str, pd.DataFrame]:
    from glob import glob
    import gzip
    import io
    import json as _json

    pats = [
        str(extended_dir / "Streaming_History_Audio*.json"),
        str(extended_dir / "Streaming_History_Audio*.json.gz"),
    ]
    files: List[str] = []
    for p in pats:
        files.extend(sorted(glob(p)))
    if not files:
        raise FileNotFoundError(f"No ExtendedStreamingHistory files in: {extended_dir}")

    rows: List[Dict[str, object]] = []
    it = files
    if progress:
        try:
            from tqdm import tqdm
            it = tqdm(files, desc="Reading Extended JSON", unit="file")
        except Exception:
            pass

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

            track_id = None
            if isinstance(track_uri, str) and track_uri.startswith("spotify:track:"):
                track_id = "trk:" + track_uri.split(":")[-1]
            if not track_id:
                if track_name and artist_name:
                    track_id = f"nk:{str(artist_name).strip().lower()}||{str(track_name).strip().lower()}"

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
    return {"plays": plays, "tracks": tracks, "artists": pd.DataFrame()}  # artists unused here

# -------------------- Canonical build (with enrichment) ----------------
def _build_or_load_canonicals(
    cache_dir: Path,
    extended_dir: Optional[Path],
    records_csv: Optional[Path],
    chunksize: int,
    progress: bool,
    force: bool,
    debug: bool,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    p_parquet = cache_dir / "canonical_plays.parquet"
    t_parquet = cache_dir / "canonical_tracks.parquet"

    if not force and p_parquet.exists() and t_parquet.exists():
        _status(f"✔ Using cached canonicals in {cache_dir}")
        return pd.read_parquet(p_parquet), pd.read_parquet(t_parquet)

    if extended_dir is None or BuildCanonicalTables is None or Pipeline is None:
        raise RuntimeError("To rebuild canonicals, provide --extended-dir and ensure traverse processors are importable.")

    _status("⏳ Building canonical tables from Extended Streaming History…")
    t0 = _load_spotify_extended_minimal(extended_dir, progress=progress)

    # Enrich (preferred)
    if _Enricher and records_csv:
        _status("⏳ Enriching with Records (genres/styles) via traverse processors…")
        enr = _Enricher(records_csv=str(records_csv))
        try:
            t_enriched = enr.run(t0)
        except Exception as e:
            _status(f"⚠ Enricher failed: {e}; continuing with base tables.")
            t_enriched = t0
    else:
        if not _Enricher:
            _status("⚠ Enricher not available.")
        if not records_csv:
            _status("⚠ No --records-csv provided.")
        t_enriched = t0

    # Canonicals
    pipe = Pipeline([BuildCanonicalTables()])
    tout = pipe.run(t_enriched)
    plays_wide = tout.get("plays_wide", pd.DataFrame())
    tracks_wide = tout.get("tracks_wide", pd.DataFrame())
    if plays_wide.empty:
        raise RuntimeError("Canonical plays_wide empty after build; check inputs.")

    # Persist
    try:
        plays_wide.to_parquet(p_parquet, index=False)
        tracks_wide.to_parquet(t_parquet, index=False)
        _status(f"✔ Cached canonical tables in {cache_dir}")
    except Exception as e:
        _status(f"⚠ Failed to cache canonicals: {e}")

    return plays_wide, tracks_wide

# -------------------- Records fallback (name-key join) -----------------
def _records_namekey_enrichment(
    plays_wide: pd.DataFrame,
    tracks_wide: pd.DataFrame,
    records_csv: Path,
    chunksize: int,
    debug: bool,
) -> Tuple[pd.DataFrame, int, int]:
    """If canonicals have empty tag columns, map in genres/styles from records.csv by normalized name-key."""
    # Build name-keys on tracks_wide
    def _nk(artist: object, track: object) -> Optional[str]:
        if not artist or not track:
            return None
        a = str(artist).strip().lower()
        t = str(track).strip().lower()
        if not a or not t:
            return None
        return f"{a}||{t}"

    tracks = tracks_wide.copy()
    if "artist_name" not in tracks.columns or "track_name" not in tracks.columns:
        return plays_wide, 0, 0
    tracks["__name_key__"] = tracks.apply(lambda r: _nk(r.get("artist_name"), r.get("track_name")), axis=1)

    # Scan records.csv to collect (namekey -> genres/styles)
    needed: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

    reader = pd.read_csv(records_csv, chunksize=chunksize, dtype="string", keep_default_na=False)
    for i, chunk in enumerate(reader, start=1):
        # Be permissive about column names
        cols = {c.lower(): c for c in chunk.columns}
        artist_c = cols.get("artist") or cols.get("artist_name") or cols.get("artists")
        track_c  = cols.get("title")  or cols.get("track_name")  or cols.get("track")
        g_c      = cols.get("genres") or cols.get("genre")
        s_c      = cols.get("styles") or cols.get("style")
        if not artist_c or not track_c:
            continue

        # Build name keys and collect tags
        sub = chunk[[artist_c, track_c] + ([g_c] if g_c else []) + ([s_c] if s_c else [])].copy()
        sub["__name_key__"] = (sub[artist_c].astype(str).str.strip().str.lower() + "||" +
                               sub[track_c].astype(str).str.strip().str.lower())

        if g_c:
            sub["__genres__"] = sub[g_c]
        else:
            sub["__genres__"] = ""
        if s_c:
            sub["__styles__"] = sub[s_c]
        else:
            sub["__styles__"] = ""

        # Keep first-seen mapping per key
        for nk, g, s in sub[["__name_key__", "__genres__", "__styles__"]].itertuples(index=False, name=None):
            if nk and nk not in needed:
                needed[nk] = (g, s)

        if i % 25 == 0:
            matched_nk_now = len(needed)
            _status(f"[records scan] chunks processed={i}, unique name-keys collected so far: nk={matched_nk_now:,}")

    # Merge name-key tags to tracks
    map_df = pd.DataFrame(
        [{"__name_key__": nk, "genres_from_records": g, "styles_from_records": s}
         for nk, (g, s) in needed.items()]
    )
    if debug:
        _writecsv("records_map_nk_sample.csv", map_df)

    tracks = tracks.merge(map_df, on="__name_key__", how="left")
    # Now bring to plays_wide via track_id
    cols = ["track_id", "genres_from_records", "styles_from_records"]
    plays = plays_wide.merge(tracks[cols], on="track_id", how="left")

    # Only fill where current canonicals are blank/empty
    for col, src in (("genres", "genres_from_records"), ("styles", "styles_from_records")):
        if col not in plays.columns:
            plays[col] = pd.NA
        plays[col] = plays[col].where(~(plays[col].isna() | (plays[col] == "")), plays[src])
        if src in plays.columns:
            plays = plays.drop(columns=[src])

    nn_g = plays["genres"].fillna("").astype(str) != ""
    nn_s = plays["styles"].fillna("").astype(str) != ""
    return plays, int(nn_g.sum()), int(nn_s.sum())

# -------------------- Co-occurrence + timeline export -----------------
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Spotify → Canonicals(+enrich) → Genre co-occurrence with dense timeline → Cosmograph JSON"
    )
    ap.add_argument("--cache-dir", required=True, help="Dir with canonical_plays.parquet / canonical_tracks.parquet (or where to write them)")
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--extended-dir", help="ExtendedStreamingHistory dir (enable when rebuilding with --force)")
    ap.add_argument("--records-csv", help="records.csv for enrichment and fallback")
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--progress", action="store_true")
    ap.add_argument("--force", action="store_true", help="Rebuild canonicals even if cache exists")
    ap.add_argument("--cooccur-on", choices=["plays", "tracks"], default="plays")
    ap.add_argument("--min-cooccurrence", type=int, default=2)
    ap.add_argument("--max-edges", type=int, default=0, help="0 = no cap")
    ap.add_argument("--max-nodes", type=int, default=0, help="0 = no cap")
    ap.add_argument("--emit-occurrences", action="store_true")
    ap.add_argument("--occurrence-dedup", choices=["none", "hour", "day"], default="none")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    _ensure_dbg_dir()

    cache_dir = Path(args.cache_dir)
    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    # 1) Load or build canonicals
    plays_wide, tracks_wide = _build_or_load_canonicals(
        cache_dir=cache_dir,
        extended_dir=Path(args.extended_dir) if args.extended_dir else None,
        records_csv=Path(args.records_csv) if args.records_csv else None,
        chunksize=args.chunksize,
        progress=args.progress,
        force=args.force,
        debug=args.debug,
    )

    # 2) Quick raw stats before parsing
    def raw_stats(df: pd.DataFrame, name: str) -> None:
        for col in ("genres", "styles"):
            if col in df.columns:
                s = df[col].astype("string")
                nonnull = s.notna().sum()
                uniq = s.dropna().nunique()
                literal_nan = (s.str.lower() == "nan").sum()
                empty_str = (s == "").sum()
                bracket_empty = (s == "[]").sum()
                _status(
                    f"[raw:{name}] {col}: non-null={nonnull:,} unique={uniq:,} | literal 'nan'={literal_nan:,} | empty=''={empty_str:,} | '[]'={bracket_empty:,}"
                )

    _status(f"plays_wide cols: {list(plays_wide.columns)}")
    _status(f"tracks_wide cols: {list(tracks_wide.columns)}")
    raw_stats(plays_wide, "plays")
    raw_stats(tracks_wide, "tracks")

    # 3) If tags on plays are unusable, try to pull from tracks; if still empty and records.csv is provided, do fallback
    def has_usable_tags(df: pd.DataFrame) -> bool:
        if "genres" not in df.columns and "styles" not in df.columns:
            return False
        sample = df[["genres", "styles"]].head(20000).fillna("")
        for g, s in sample.itertuples(index=False, name=None):
            if split_tags(g) or split_tags(s):
                return True
        return False

    if not has_usable_tags(plays_wide):
        _status("ℹ No usable tags on plays; attempting join from tracks via track_id…")
        before_g = plays_wide["genres"].notna().sum() if "genres" in plays_wide.columns else 0
        before_s = plays_wide["styles"].notna().sum() if "styles" in plays_wide.columns else 0

        cols_avail = [c for c in ["track_id", "genres", "styles"] if c in tracks_wide.columns]
        if set(["track_id"]).issubset(cols_avail):
            for c in ["genres", "styles"]:
                if c in cols_avail:
                    j = tracks_wide[["track_id", c]].rename(columns={c: f"{c}__src"})
                    plays_wide = plays_wide.merge(j, on="track_id", how="left")
                    if c in plays_wide.columns:
                        plays_wide[c] = plays_wide[c].where(~(plays_wide[c].isna() | (plays_wide[c] == "")), plays_wide[f"{c}__src"])
                        plays_wide = plays_wide.drop(columns=[f"{c}__src"])
                    else:
                        plays_wide[c] = plays_wide[f"{c}__src"]
                        plays_wide = plays_wide.drop(columns=[f"{c}__src"])

        after_g = plays_wide["genres"].notna().sum() if "genres" in plays_wide.columns else 0
        after_s = plays_wide["styles"].notna().sum() if "styles" in plays_wide.columns else 0
        _status(f"[join-from-tracks] genres ({before_g:,})->({after_g:,}), styles ({before_s:,})->({after_s:,})")

    if not has_usable_tags(plays_wide) and args.records_csv:
        _status("ℹ Tags still unusable; applying records.csv fallback by name-key…")
        plays_wide, ng, ns = _records_namekey_enrichment(
            plays_wide=plays_wide,
            tracks_wide=tracks_wide,
            records_csv=Path(args.records_csv),
            chunksize=args.chunksize,
            debug=args.debug,
        )
        _status(f"[records-fallback] filled plays_wide: genres non-empty rows={ng:,}, styles non-empty rows={ns:,}")

    # Optional sample dump
    if args.debug:
        _writecsv("plays_tags_sample.csv", plays_wide[["played_at", "track_id", "artist_name", "track_name", "genres", "styles"]])

    # 4) Build the iterator dataframe for co-occurrence
    if args.cooccur_on == "plays":
        iter_df = plays_wide[["played_at", "genres", "styles"]].copy()
        iter_df = iter_df.sort_values("played_at", kind="stable").reset_index(drop=True)
    else:
        def _merge_tags(s: pd.Series) -> str:
            bag: Set[str] = set()
            for v in s.dropna().astype(str):
                bag.update(split_tags(v))
            return "|".join(sorted(bag))
        tg = plays_wide.groupby("track_id").agg(
            earliest=("played_at", "min"),
            genres=("genres", _merge_tags),
            styles=("styles", _merge_tags),
        ).reset_index()
        iter_df = tg.rename(columns={"earliest": "played_at"})[["played_at", "genres", "styles"]]
        iter_df = iter_df.sort_values("played_at", kind="stable").reset_index(drop=True)

    # 5) Dense timeline + counts
    round_div = 1
    if args.occurrence_dedup == "hour":
        round_div = 3_600_000
    elif args.occurrence_dedup == "day":
        round_div = 86_400_000
    def _round_ts(ts_ms: int) -> int:
        return ts_ms if round_div == 1 else (ts_ms // round_div) * round_div

    counts: Counter[Tuple[str, str]] = Counter()
    first_seen_edge: Dict[Tuple[str, str], int] = {}
    occurrences: Dict[Tuple[str, str], List[int]] = {} if args.emit_occurrences else {}
    first_seen_node: Dict[str, int] = {}

    n_rows = 0
    rows_any = 0
    rows_ge2 = 0

    for played_at, gval, sval in iter_df.itertuples(index=False, name=None):
        n_rows += 1
        tags = split_tags(gval) + split_tags(sval)
        if not tags:
            continue
        rows_any += 1
        ts_ms = None
        if pd.notna(played_at):
            try:
                ts_ms = int(pd.Timestamp(played_at).value // 10**6)
            except Exception:
                ts_ms = None
        if ts_ms is not None:
            rts = _round_ts(ts_ms)
            for t in set(tags):
                if t not in first_seen_node or rts < first_seen_node[t]:
                    first_seen_node[t] = rts

        prs = list(cooccurrence_pairs(tags))
        if prs:
            rows_ge2 += 1
        for a, b in prs:
            k = pair(a, b)
            counts[k] += 1
            if ts_ms is not None:
                rts = _round_ts(ts_ms)
                if k not in first_seen_edge or rts < first_seen_edge[k]:
                    first_seen_edge[k] = rts
                if args.emit_occurrences:
                    lst = occurrences.setdefault(k, [])
                    if not lst or lst[-1] != rts:
                        lst.append(rts)

    _status(
        f"rows scanned={n_rows:,} | rows with any tags={rows_any:,} | "
        f"rows with ≥2 tags={rows_ge2:,} | unique edge keys={len(counts):,}"
    )

    if args.debug:
        parsed_preview = []
        for i, (played_at, gval, sval) in enumerate(iter_df.head(200).itertuples(index=False, name=None)):
            gg = split_tags(gval)
            ss = split_tags(sval)
            parsed_preview.append({
                "played_at": str(played_at),
                "genres_raw": gval, "styles_raw": sval,
                "genres_tokens": gg, "styles_tokens": ss,
                "all_tokens": sorted(set(gg+ss)),
            })
        _writetxt("parsed_preview.json", json.dumps(parsed_preview, indent=2, default=str))

    if not counts:
        _status("⚠ No tag co-occurrences found. Nothing to export.")
        out_json.write_text(json.dumps({"points": [], "links": []}, indent=2))
        print(f"✔ Wrote {out_json}  (nodes=0, edges=0)")
        return

    # 6) Threshold & caps
    edges = [(a, b, w) for (a, b), w in counts.items() if w >= args.min_cooccurrence]
    edges.sort(key=lambda x: x[2], reverse=True)

    strength = defaultdict(int)
    for a, b, w in edges:
        strength[a] += w
        strength[b] += w

    if args.max_nodes and args.max_nodes > 0:
        keep = {n for n, _ in sorted(strength.items(), key=lambda kv: kv[1], reverse=True)[: args.max_nodes]}
        edges = [(a, b, w) for a, b, w in edges if a in keep and b in keep]

    if args.max_edges and args.max_edges > 0 and len(edges) > args.max_edges:
        edges = edges[: args.max_edges]

    # 7) Finalize output
    node_ids: Set[str] = set()
    for a, b, _ in edges:
        node_ids.add(a)
        node_ids.add(b)

    points = []
    for nid in sorted(node_ids):
        p = {"id": nid, "label": pretty_label(nid)}
        if nid in first_seen_node:
            p["first_seen"] = int(first_seen_node[nid])
        points.append(p)

    links = []
    for a, b, w in edges:
        k = pair(a, b)
        item = {"source": a, "target": b, "weight": int(w)}
        if k in first_seen_edge:
            item["first_seen"] = int(first_seen_edge[k])
        if args.emit_occurrences:
            occ = occurrences.get(k, [])
            if occ:
                item["occurrences"] = [int(x) for x in occ]
        links.append(item)

    if args.debug:
        _writetxt("export_samples.json", json.dumps({"points": points[:5], "links": links[:5]}, indent=2))
        _writetxt("edges_top50.txt", "\n".join([f"{a}\t{b}\t{w}" for a,b,w in edges[:50]]))

    _status(f"unique_tags={len(points)} | nodes_out={len(points)} | edges_out={len(links)}")
    out_json.write_text(json.dumps({"points": points, "links": links}, indent=2))
    print(f"✔ Wrote {out_json}  (nodes={len(points)}, edges={len(links)})")


if __name__ == "__main__":
    main()
