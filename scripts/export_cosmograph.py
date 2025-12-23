#!/usr/bin/env python
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Iterable, List, Dict

import pandas as pd
from tqdm.auto import tqdm

# ----------------------------
# Helpers
# ----------------------------
def _iter_extended_json(paths: Iterable[Path]) -> Iterable[dict]:
    for p in paths:
        try:
            data = json.loads(Path(p).read_text(encoding="utf-8"))
            # file can be list[dict] or object with "payload"
            if isinstance(data, list):
                for row in data:
                    yield row
            elif isinstance(data, dict) and "payload" in data and isinstance(data["payload"], list):
                for row in data["payload"]:
                    yield row
        except Exception:
            # skip unreadable files
            continue

def _normalize_row(r: dict) -> dict | None:
    # Spotify Extended fields (defensive)
    ts = r.get("ts") or r.get("time")
    ms = r.get("ms_played") or r.get("msPlayed")
    track_name = r.get("master_metadata_track_name") or r.get("track_name") or r.get("trackName")
    artist_name = r.get("master_metadata_album_artist_name") or r.get("artist_name") or r.get("artistName")
    uri = r.get("spotify_track_uri") or r.get("spotifyTrackUri")
    # must have time + either uri or names
    if ts is None or (uri is None and (track_name is None or artist_name is None)):
        return None
    # derive a compact track id: trk:<id> if URI present, else name tuple
    if isinstance(uri, str) and ":" in uri:
        tid = "trk:" + uri.split(":")[-1]
    else:
        tid = f"trk:{(track_name or '').strip().lower()}|{(artist_name or '').strip().lower()}"
    return {
        "played_at": ts,
        "track_id": tid,
        "ms_played": int(ms) if ms is not None else 0,
        "track_name": (track_name or "").strip(),
        "artist_name": (artist_name or "").strip(),
    }

def load_plays_from_extended(extended_dir: Path) -> pd.DataFrame:
    files = sorted(extended_dir.rglob("*.json"))
    rows: List[dict] = []
    for row in tqdm(_iter_extended_json(files), total=len(files), desc="Reading Extended JSON", unit="file"):
        nr = _normalize_row(row)
        if nr:
            rows.append(nr)
    if not rows:
        return pd.DataFrame(columns=["played_at", "track_id", "ms_played", "track_name", "artist_name"])
    df = pd.DataFrame(rows)
    df["played_at"] = pd.to_datetime(df["played_at"], errors="coerce", utc=True)
    df = df.dropna(subset=["track_id"]).reset_index(drop=True)
    return df

def build_minimal_graph(plays: pd.DataFrame, agg: str = "play_count", min_weight: int = 1) -> Dict[str, pd.DataFrame]:
    """
    Build a simple bipartite graph between tracks and artists.
    Nodes: track(id=track_id), artist(id='art:'+normalized artist)
    Edges: (track -> artist) with weight = count or sum(ms_played)
    """
    if plays.empty:
        return {"nodes": pd.DataFrame(columns=["id", "label", "type"]),
                "edges": pd.DataFrame(columns=["source", "target", "value"])}

    # artist id
    art_id = plays["artist_name"].fillna("").str.strip().str.lower().replace("", pd.NA)
    plays = plays.assign(artist_id=("art:" + art_id.fillna("unknown")))

    # edge weights
    if agg == "ms_played":
        edges = (plays.groupby(["track_id", "artist_id"], dropna=False)["ms_played"]
                 .sum().reset_index(name="value"))
    else:
        edges = (plays.groupby(["track_id", "artist_id"], dropna=False)
                 .size().reset_index(name="value"))
    edges = edges[edges["value"] >= min_weight].copy()

    # nodes
    track_nodes = (plays.groupby(["track_id", "track_name"], dropna=False)
                   .size().reset_index()[["track_id", "track_name"]]
                   .rename(columns={"track_id": "id", "track_name": "label"}))
    track_nodes["type"] = "track"

    artist_nodes = (plays.groupby(["artist_id", "artist_name"], dropna=False)
                    .size().reset_index()[["artist_id", "artist_name"]]
                    .rename(columns={"artist_id": "id", "artist_name": "label"}))
    artist_nodes["type"] = "artist"

    nodes = pd.concat([track_nodes, artist_nodes], ignore_index=True).drop_duplicates(subset=["id"])
    # Cosmograph edge columns MUST be source/target (docs). :contentReference[oaicite:4]{index=4}
    edges = edges.rename(columns={"track_id": "source", "artist_id": "target"})[["source", "target", "value"]]
    return {"nodes": nodes, "edges": edges}

def write_cosmograph_csv(nodes: pd.DataFrame, edges: pd.DataFrame, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    nodes_path = out_dir / "nodes.csv"
    edges_path = out_dir / "edges.csv"
    # Keep minimal required columns (id,label,type) for metadata (docs). :contentReference[oaicite:5]{index=5}
    nodes = nodes[["id", "label", "type"]].copy()
    nodes.to_csv(nodes_path, index=False, quoting=csv.QUOTE_MINIMAL)
    edges.to_csv(edges_path, index=False, quoting=csv.QUOTE_MINIMAL)
    return nodes_path, edges_path

def write_cors_server(out_dir: Path) -> Path:
    code = r'''from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import os, sys
os.chdir(os.path.dirname(__file__))  # serve files from THIS folder
class CORS(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Expose-Headers", "Content-Length, Content-Range, Accept-Ranges")
        super().end_headers()
if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    httpd = ThreadingHTTPServer(("127.0.0.1", port), CORS)
    print(f"Serving {os.getcwd()} at http://127.0.0.1:{port}")
    httpd.serve_forever()
'''
    p = out_dir / "serve_cors.py"
    p.write_text(code, encoding="utf-8")
    return p

# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Export CSVs for Cosmograph web app (no bundler required).")
    ap.add_argument("--extended-dir", required=True, help="Path to ExtendedStreamingHistory dir")
    ap.add_argument("--out-dir", required=True, help="Output folder for CSVs and helper server")
    ap.add_argument("--min-weight", type=int, default=1, help="Min edge weight to keep")
    args = ap.parse_args()

    extended_dir = Path(args.extended_dir).expanduser()
    out_dir = Path(args.out_dir).expanduser()

    plays = load_plays_from_extended(extended_dir)
    print(f"Loaded plays: rows={len(plays):,} [played_at, track_id, ms_played, track_name, artist_name]")

    g = build_minimal_graph(plays, agg="play_count", min_weight=args.min_weight)
    nodes, edges = g["nodes"], g["edges"]
    print(f"Graph: nodes={len(nodes):,} edges={len(edges):,}")

    n_path, e_path = write_cosmograph_csv(nodes, edges, out_dir)
    s_path = write_cors_server(out_dir)

    # Compose a ready-to-open URL using Cosmograph QueryString API (docs). 
    share_url = (
        "https://run.cosmograph.app/?"
        f"data=http://127.0.0.1:8000/{e_path.name}"
        f"&meta=http://127.0.0.1:8000/{n_path.name}"
        f"&source=source&target=target&nodeLabel=label"
    )

    print(f"\n✔ Wrote {e_path}")
    print(f"✔ Wrote {n_path}")
    print(f"✔ Wrote {s_path}")
    print("\nNext steps:")
    print(f"  1) Start a CORS static server in the output folder:\n"
          f"       cd {out_dir}\n"
          f"       python serve_cors.py 8000")
    print("  2) Open this URL in your browser (labels are enabled):")
    print(f"       {share_url}\n")

if __name__ == "__main__":
    main()
