from __future__ import annotations
import argparse
import datetime
import json
import re
import sys
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

# Try Traverse splitter; otherwise fall back
_splitter = None
try:
    from traverse.processing.normalize import split_genres_styles as _splitter  # type: ignore
except Exception:
    _splitter = None

# ----------------- helpers -----------------

SEP_RE = re.compile(r"[|,;/]+")


def _fallback_split(val: object) -> List[str]:
    if val is None:
        return []
    s = str(val).strip()
    if not s or s.lower() in {"na", "nan", "none"}:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            return [str(t).strip() for t in arr if str(t).strip()]
        except Exception:
            pass
    return [p.strip() for p in SEP_RE.split(s) if p.strip()]


TRIM = re.compile(r"^[\s'\"`~!@#$%^*_=+<>?.,:;\\/\-\|&]+|[\s'\"`~!@#$%^*_=+<>?.,:;\\/\-\|&]+$")


def clean_tag(tag: str) -> Optional[str]:
    t = TRIM.sub("", str(tag)).lower()
    t = re.sub(r"\s+", " ", t).strip()
    if not t or not re.search(r"[a-z]", t):
        return None
    return t


def split_tags(val: object) -> List[str]:
    try:
        raw = (_splitter(val) or []) if _splitter else _fallback_split(val)
    except Exception:
        raw = _fallback_split(val)
    out = []
    for r in raw:
        ct = clean_tag(r)
        if ct:
            out.append(ct)
    return out


def pretty_label(tag: str) -> str:
    return tag.title().replace("Idm", "IDM").replace("Edm", "EDM").replace("Dnb", "DnB")


def cooccurrence_pairs(tags: Iterable[str]) -> Iterable[Tuple[str, str]]:
    uniq = sorted(set(t for t in tags if t))
    if len(uniq) < 2:
        return []
    return combinations(uniq, 2)


YR4 = re.compile(r"(?:^|[^0-9])(\d{4})(?:[^0-9]|$)")


def parse_year_cell(v: object) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    try:
        y = int(float(s))
        if 0 < y < 10000:
            return y
    except Exception:
        pass
    m = YR4.search(s)
    if m:
        y = int(m.group(1))
        if 0 < y < 10000:
            return y
    return None


def detect_column(colmap: Dict[str, str], *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in colmap:
            return colmap[c]
    return None


def status(msg: str) -> None:
    print(msg, file=sys.stderr)


# ----------------- main -----------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export dense genre/style co-occurrence graph from records.csv with year-based timeline (clamped)."
    )
    ap.add_argument("--records-csv", required=True)
    ap.add_argument("--year-col", default="release_year")
    ap.add_argument("--year-min", type=int, default=1860, help="min year to accept for timeline")
    ap.add_argument("--year-max", type=int, default=2025, help="max year to accept for timeline")
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--min-cooccurrence", type=int, default=2)
    ap.add_argument("--max-edges", type=int, default=40_000, help="0 = no cap")
    ap.add_argument("--max-nodes", type=int, default=5_000, help="0 = no cap")
    ap.add_argument(
        "--out-json", default="src/traverse/cosmograph/app/dist/cosmo_genres_records_timeline.json"
    )
    ap.add_argument("--progress", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    rec_path = Path(args.records_csv)
    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    dbg_dir = Path("_debug")
    if args.debug:
        dbg_dir.mkdir(parents=True, exist_ok=True)

    YEAR_MIN, YEAR_MAX = int(args.year_min), int(args.year_max)

    def clamp_year(y: Optional[int]) -> Optional[int]:
        if y is None:
            return None
        if y < YEAR_MIN or y > YEAR_MAX:
            return None
        return y

    def year_to_ts(y: Optional[int]) -> Optional[int]:
        """Convert year to Unix epoch milliseconds (Jan 1 of that year UTC)."""
        if y is None:
            return None
        return int(datetime.datetime(y, 1, 1, tzinfo=datetime.timezone.utc).timestamp() * 1000)

    counts: Counter[Tuple[str, str]] = Counter()
    edge_first_year: Dict[Tuple[str, str], int] = {}
    point_first_year: Dict[str, int] = {}
    first_label: Dict[str, str] = {}

    total_rows = 0
    tagged_rows = 0
    rows_ge2 = 0
    tags_global: set = set()
    raw_year_min, raw_year_max = None, None
    clamped_kept = 0
    clamped_dropped = 0

    def _chunks():
        reader = pd.read_csv(
            rec_path, chunksize=args.chunksize, dtype="string", keep_default_na=True, na_filter=True
        )
        if args.progress:
            try:
                from tqdm import tqdm

                return tqdm(reader, desc="Reading records (chunks)", unit="chunk")
            except Exception:
                return reader
        return reader

    for chunk in _chunks():
        total_rows += len(chunk)
        colmap = {c.lower(): c for c in chunk.columns}

        gcol = detect_column(colmap, "genres", "genre")
        scol = detect_column(colmap, "styles", "style")
        if not gcol and not scol:
            raise KeyError(f"Missing genres/styles columns. Available: {list(chunk.columns)}")

        ykey = args.year_col.strip().lower()
        if ykey not in colmap:
            ykey = detect_column(
                colmap,
                "release_year",
                "year",
                "releaseyear",
                "released_year",
                "release year",
                "released",
            )
        if not ykey:
            raise KeyError(
                f"Year column not found. Tried '{args.year_col}' and common variants. Available: {list(chunk.columns)}"
            )

        gs = chunk[gcol] if gcol else pd.Series([], dtype="string")
        ss = chunk[scol] if scol else pd.Series([], dtype="string")
        yrs = chunk[colmap[ykey]]

        years: List[Optional[int]] = []
        for v in yrs.tolist():
            y_raw = parse_year_cell(v)
            if y_raw is not None:
                raw_year_min = y_raw if raw_year_min is None else min(raw_year_min, y_raw)
                raw_year_max = y_raw if raw_year_max is None else max(raw_year_max, y_raw)
            y = clamp_year(y_raw)
            if y is None:
                clamped_dropped += 1
            else:
                clamped_kept += 1
            years.append(y)

        for gval, sval, y in zip(gs, ss, years):
            tags = split_tags(gval) + split_tags(sval)
            if not tags:
                continue
            tagged_rows += 1
            if len(set(tags)) >= 2:
                rows_ge2 += 1

            # earliest year per tag
            for t in set(tags):
                if t not in first_label:
                    first_label[t] = pretty_label(t)
                tags_global.add(t)
                if y is not None:
                    cur = point_first_year.get(t)
                    if cur is None or y < cur:
                        point_first_year[t] = y

            # co-occurrence per row + earliest year per edge
            for a, b in cooccurrence_pairs(tags):
                key = (a, b)
                counts[key] += 1
                if y is not None:
                    cur = edge_first_year.get(key)
                    if cur is None or y < cur:
                        edge_first_year[key] = y

    status(
        f"[DBG] scanned_rows={total_rows:,} | tagged_rows={tagged_rows:,} | rows_ge2={rows_ge2:,} | "
        f"unique_tags={len(tags_global):,} | raw_year_range={raw_year_min}..{raw_year_max} | "
        f"accepted_years={clamped_kept:,} | dropped_years(out-of-range)={clamped_dropped:,} "
        f"| clamp_window=[{YEAR_MIN},{YEAR_MAX}]"
    )

    if not counts:
        status("⚠ No tag co-occurrences found. Nothing to export.")
        out_path.write_text(json.dumps({"points": [], "links": []}, indent=2))
        print(f"✔ Wrote {out_path}  (nodes=0, edges=0)")
        return

    # threshold + caps
    edges = [(a, b, w) for (a, b), w in counts.items() if w >= args.min_cooccurrence]
    edges.sort(key=lambda x: x[2], reverse=True)

    strength = defaultdict(int)
    for a, b, w in edges:
        strength[a] += w
        strength[b] += w

    if args.max_nodes and args.max_nodes > 0:
        keep = {
            n
            for n, _ in sorted(strength.items(), key=lambda kv: kv[1], reverse=True)[
                : args.max_nodes
            ]
        }
        edges = [(a, b, w) for a, b, w in edges if a in keep and b in keep]

    if args.max_edges and args.max_edges > 0 and len(edges) > args.max_edges:
        edges = edges[: args.max_edges]

    node_ids = set()
    for a, b, _ in edges:
        node_ids.add(a)
        node_ids.add(b)

    # points
    points = []
    pts_with_time = 0
    for nid in sorted(node_ids):
        obj = {"id": nid, "label": first_label.get(nid, nid)}
        fy = point_first_year.get(nid)
        if fy is not None:
            obj["first_seen"] = int(fy)
            obj["first_seen_ts"] = year_to_ts(fy)
            pts_with_time += 1
        points.append(obj)

    # links (strict validation)
    links = []
    lks_with_time = 0
    skipped = {
        "empty_source": 0,
        "empty_target": 0,
        "non_str_source": 0,
        "non_str_target": 0,
        "not_in_points": 0,
    }
    pset = {p["id"] for p in points}

    def _ok(s):
        return isinstance(s, str) and len(s) > 0

    for a, b, w in edges:
        if not _ok(a):
            skipped["non_str_source" if not isinstance(a, str) else "empty_source"] += 1
            continue
        if not _ok(b):
            skipped["non_str_target" if not isinstance(b, str) else "empty_target"] += 1
            continue
        if (a not in pset) or (b not in pset):
            skipped["not_in_points"] += 1
            continue
        lk = {"source": a, "target": b, "weight": int(w)}
        fy = edge_first_year.get((a, b))
        if fy is not None:
            lk["first_seen"] = int(fy)
            lk["first_seen_ts"] = year_to_ts(fy)
            lks_with_time += 1
        links.append(lk)

    if args.debug:
        (dbg_dir / "records_links_skip_summary.json").write_text(json.dumps(skipped, indent=2))

    status(
        f"[DBG] points_out={len(points):,} (with time={pts_with_time:,}) | "
        f"links_out={len(links):,} (with time={lks_with_time:,}) | time_base_year={YEAR_MIN} "
        f"| skipped_links={skipped}"
    )

    out = {"points": points, "links": links}
    out_path.write_text(json.dumps(out, indent=2))
    print(f"✔ Wrote {out_path}  (nodes={len(points)}, edges={len(links)})")


if __name__ == "__main__":
    main()
