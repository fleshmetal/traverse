# scripts/export_cosmo_genres.py
from __future__ import annotations
import argparse
import json
import re
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Iterable, List, Tuple, Dict

import pandas as pd

SEP_RE = re.compile(r"[|,;/]+")

def split_multi(val: object) -> List[str]:
    """Return normalized tags list from a genres/styles field."""
    if val is None:
        return []
    s = str(val).strip()
    if not s or s.lower() in {"na", "nan", "none"}:
        return []
    # Try JSON-style list first: ["electronic","dubstep"]
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            return [norm(t) for t in arr if str(t).strip()]
        except Exception:
            pass
    # Fallback: split on common delimiters
    parts = [norm(x) for x in SEP_RE.split(s)]
    return [p for p in parts if p]

def norm(t: str) -> str:
    """Normalize a tag for identity; keep readable, merge small variations."""
    # collapse whitespace, lower, strip punctuation at ends
    out = re.sub(r"\s+", " ", str(t)).strip().lower()
    out = out.strip(" '\"-–—·•")
    return out

def cooccurrence_pairs(tags: Iterable[str]) -> Iterable[Tuple[str, str]]:
    uniq = sorted(set(tags))
    if len(uniq) < 2:
        return []
    return combinations(uniq, 2)  # unordered (a,b) with a<b

def main():
    ap = argparse.ArgumentParser(description="Export genre/style co-occurrence graph for Cosmograph.")
    ap.add_argument("--records-csv", required=True)
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--min-cooccurrence", type=int, default=2)
    ap.add_argument("--max-edges", type=int, default=40_000, help="0 = no cap")
    ap.add_argument("--max-nodes", type=int, default=5_000, help="0 = no cap")
    ap.add_argument("--out-json", default="cosmo_genres.json")
    args = ap.parse_args()

    counts: Counter[Tuple[str, str]] = Counter()
    tag_first_label: Dict[str, str] = {}
    total_rows = 0
    used_cols = None

    # Stream records
    for chunk in pd.read_csv(
        args.records_csv,
        chunksize=args.chunksize,
        dtype="string",
        keep_default_na=True,
        na_filter=True,
    ):
        total_rows += len(chunk)
        # lowercase columns for case-insensitive access
        colmap = {c.lower(): c for c in chunk.columns}
        if used_cols is None:
            # try to find genres & styles columns
            gcol = colmap.get("genres") or colmap.get("genre")
            scol = colmap.get("styles") or colmap.get("style")
            if not gcol and not scol:
                raise KeyError(
                    f"Could not find 'genres'/'styles' columns in CSV. Available: {list(chunk.columns)}"
                )
            used_cols = (gcol, scol)

        gcol, scol = used_cols
        g_series = chunk[gcol] if gcol else pd.Series([], dtype="string")
        s_series = chunk[scol] if scol else pd.Series([], dtype="string")

        for gval, sval in zip(g_series, s_series):
            tags = []
            tags.extend(split_multi(gval))
            tags.extend(split_multi(sval))
            if not tags:
                continue

            # Remember the first-seen pretty label for each normalized id
            for t in set(tags):
                if t not in tag_first_label:
                    # simple pretty: title-case unless contains known lowercase tokens
                    pretty = re.sub(r"\s+", " ", t).strip()
                    pretty = pretty.title().replace("Idm", "IDM").replace("Edm", "EDM")
                    tag_first_label[t] = pretty

            for a, b in cooccurrence_pairs(tags):
                counts[(a, b)] += 1

    if not counts:
        print("No co-occurrences found. Are 'genres'/'styles' empty?")
        Path(args.out_json).write_text(json.dumps({"points": [], "links": []}, indent=2))
        return

    # Filter by threshold
    edges = [(a, b, w) for (a, b), w in counts.items() if w >= args.min_cooccurrence]
    edges.sort(key=lambda x: x[2], reverse=True)

    # Node strength (weighted degree)
    strength = defaultdict(int)
    for a, b, w in edges:
        strength[a] += w
        strength[b] += w

    # Node capping by strength
    if args.max_nodes and args.max_nodes > 0:
        top_nodes = {n for n, _ in sorted(strength.items(), key=lambda kv: kv[1], reverse=True)[: args.max_nodes]}
        edges = [(a, b, w) for a, b, w in edges if a in top_nodes and b in top_nodes]

    # Edge capping
    if args.max_edges and args.max_edges > 0 and len(edges) > args.max_edges:
        edges = edges[: args.max_edges]

    # Final node set
    node_ids = set()
    for a, b, _ in edges:
        node_ids.add(a)
        node_ids.add(b)

    nodes = [{"id": nid, "label": tag_first_label.get(nid, nid)} for nid in sorted(node_ids)]
    links = [{"source": a, "target": b, "weight": int(w)} for a, b, w in edges]

    # Stats
    print(
        f"rows scanned={total_rows:,} | unique tags={len(tag_first_label):,} | "
        f"edges(before)={len(counts):,} | edges(after)={len(edges):,} | nodes(after)={len(nodes):,}"
    )

    out = {"points": nodes, "links": links}
    Path(args.out_json).write_text(json.dumps(out, indent=2))
    print(f"✔ Wrote {args.out_json}  (nodes={len(nodes)}, edges={len(links)})")

if __name__ == "__main__":
    main()
