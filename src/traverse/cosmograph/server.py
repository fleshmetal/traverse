"""CORS-enabled static file server for the Cosmograph frontend.

Serves the pre-built ``dist/`` directory from the embedded React app.
Includes ``POST /api/cluster`` and ``POST /api/genre-tracks`` endpoints.
"""
from __future__ import annotations

import json
import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd

from traverse.graph.community import (
    CommunityAlgorithm,
    cooccurrence_to_networkx,
    detect_communities,
)
from traverse.graph.edge_analysis import (
    EdgeAlgorithm,
    analyze_community_edges,
)

# ── Module-level cache for canonical plays data ─────────────────────
_canonical_plays: Optional[pd.DataFrame] = None
_canonical_plays_loaded = False


def _load_canonical_plays() -> Optional[pd.DataFrame]:
    """Lazy-load canonical_plays from ``_out/`` (parquet preferred, CSV fallback)."""
    global _canonical_plays, _canonical_plays_loaded
    if _canonical_plays_loaded:
        return _canonical_plays

    out_dir = Path("_out")
    parquet = out_dir / "canonical_plays.parquet"
    csv = out_dir / "canonical_plays.csv"

    if parquet.is_file():
        _canonical_plays = pd.read_parquet(parquet)
    elif csv.is_file():
        _canonical_plays = pd.read_csv(csv)
    else:
        _canonical_plays = None

    _canonical_plays_loaded = True
    return _canonical_plays

# Hardcoded MIME map — Windows registry often maps .js to text/plain,
# which makes browsers reject ES module scripts.
_MIME_OVERRIDES: Dict[str, str] = {
    ".js": "text/javascript",
    ".mjs": "text/javascript",
    ".css": "text/css",
    ".json": "application/json",
    ".html": "text/html",
    ".svg": "image/svg+xml",
    ".wasm": "application/wasm",
}


def _default_dist_dir() -> Path:
    """Return the path to the built frontend dist/ directory."""
    return Path(__file__).resolve().parent / "app" / "dist"


class _CORSHandler(SimpleHTTPRequestHandler):
    """Static file handler with CORS headers and correct MIME types."""

    def guess_type(self, path: str) -> str:  # type: ignore[override]
        ext = Path(path).suffix.lower()
        return _MIME_OVERRIDES.get(ext, super().guess_type(path))

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header(
            "Access-Control-Expose-Headers",
            "Content-Length, Content-Range, Accept-Ranges",
        )
        # Prevent browsers from caching stale MIME types
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    # ── CORS preflight ──────────────────────────────────────────────
    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    # ── POST routing ────────────────────────────────────────────────
    def do_POST(self) -> None:
        if self.path == "/api/cluster":
            self._handle_cluster()
        elif self.path == "/api/edge-analysis":
            self._handle_edge_analysis()
        elif self.path == "/api/genre-tracks":
            self._handle_genre_tracks()
        else:
            self.send_error(404, "Not Found")

    # ── POST /api/cluster ────────────────────────────────────────────
    def _handle_cluster(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", 0))
            body: Dict[str, Any] = json.loads(self.rfile.read(length))
        except Exception:
            self._json_error(400, "Invalid JSON body")
            return

        data_file = body.get("dataFile", "")
        algo_name = body.get("algorithm", "")
        params = body.get("params", {})

        # Resolve the data file relative to the serve directory with
        # path-traversal protection.
        serve_dir = Path(self.directory)  # type: ignore[attr-defined]
        try:
            resolved = (serve_dir / data_file).resolve()
            if not str(resolved).startswith(str(serve_dir.resolve())):
                raise ValueError("path traversal")
            if not resolved.is_file():
                raise FileNotFoundError(data_file)
        except Exception as exc:
            self._json_error(400, f"Bad dataFile: {exc}")
            return

        try:
            graph_json = json.loads(resolved.read_text(encoding="utf-8"))
            graph = {
                "points": graph_json.get("points", []),
                "links": graph_json.get("links", []),
            }
        except Exception as exc:
            self._json_error(500, f"Failed to read data file: {exc}")
            return

        # Resolve algorithm enum
        try:
            algorithm = CommunityAlgorithm(algo_name)
        except ValueError:
            valid = [a.value for a in CommunityAlgorithm]
            self._json_error(400, f"Unknown algorithm '{algo_name}'. Valid: {valid}")
            return

        # Build kwargs for detect_communities
        kwargs: Dict[str, Any] = {}
        if "resolution" in params:
            kwargs["resolution"] = float(params["resolution"])
        if "seed" in params and params["seed"] is not None:
            kwargs["seed"] = int(params["seed"])
        if "best_n" in params and params["best_n"] is not None:
            kwargs["best_n"] = int(params["best_n"])
        if "k" in params and params["k"] is not None:
            kwargs["k"] = int(params["k"])

        try:
            G = cooccurrence_to_networkx(graph)
            assignments = detect_communities(G, algorithm, **kwargs)
        except Exception as exc:
            self._json_error(500, f"Clustering failed: {exc}")
            return

        num_communities = len(set(assignments.values())) if assignments else 0
        self._json_response(200, {
            "assignments": assignments,
            "numCommunities": num_communities,
        })

    # ── POST /api/edge-analysis ──────────────────────────────────────
    def _handle_edge_analysis(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", 0))
            body: Dict[str, Any] = json.loads(self.rfile.read(length))
        except Exception:
            self._json_error(400, "Invalid JSON body")
            return

        data_file = body.get("dataFile", "")
        algo_name = body.get("algorithm", "")
        node_ids = body.get("nodeIds", [])
        top_k = body.get("topK", None)

        if not node_ids:
            self._json_error(400, "Missing or empty 'nodeIds'")
            return

        # Resolve data file
        serve_dir = Path(self.directory)  # type: ignore[attr-defined]
        try:
            resolved = (serve_dir / data_file).resolve()
            if not str(resolved).startswith(str(serve_dir.resolve())):
                raise ValueError("path traversal")
            if not resolved.is_file():
                raise FileNotFoundError(data_file)
        except Exception as exc:
            self._json_error(400, f"Bad dataFile: {exc}")
            return

        try:
            graph_json = json.loads(resolved.read_text(encoding="utf-8"))
            graph = {
                "points": graph_json.get("points", []),
                "links": graph_json.get("links", []),
            }
        except Exception as exc:
            self._json_error(500, f"Failed to read data file: {exc}")
            return

        # Resolve algorithm enum
        try:
            algorithm = EdgeAlgorithm(algo_name)
        except ValueError:
            valid = [a.value for a in EdgeAlgorithm]
            self._json_error(400, f"Unknown algorithm '{algo_name}'. Valid: {valid}")
            return

        # Run edge analysis on the community subgraph
        try:
            results = analyze_community_edges(
                graph,
                set(node_ids),
                algorithm,
                top_k=int(top_k) if top_k is not None else None,
            )
        except Exception as exc:
            self._json_error(500, f"Edge analysis failed: {exc}")
            return

        self._json_response(200, {
            "algorithm": algo_name,
            "edgeCount": len(results),
            "edges": results,
        })

    # ── POST /api/genre-tracks ───────────────────────────────────────
    def _handle_genre_tracks(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", 0))
            body: Dict[str, Any] = json.loads(self.rfile.read(length))
        except Exception:
            self._json_error(400, "Invalid JSON body")
            return

        genre = body.get("genre", "").strip()
        if not genre:
            self._json_error(400, "Missing 'genre' field")
            return

        df = _load_canonical_plays()
        if df is None:
            self._json_error(
                404,
                "No canonical plays data found in _out/. "
                "Run a canonical table export first.",
            )
            return

        target = "|" + genre.lower() + "|"

        mask = pd.Series(False, index=df.index)
        for col in ("genres", "styles"):
            if col not in df.columns:
                continue
            padded = (
                "|"
                + df[col].fillna("").astype(str).str.lower().str.replace(" | ", "|", regex=False)
                + "|"
            )
            mask = mask | padded.str.contains(target, regex=False)

        matched = df[mask]
        if matched.empty:
            self._json_response(200, {
                "genre": genre,
                "totalPlays": 0,
                "tracks": [],
            })
            return

        # Group by track, count plays, sum ms_played
        group_cols = []
        for c in ("track_id", "track_name", "artist_name"):
            if c in matched.columns:
                group_cols.append(c)
        if not group_cols:
            group_cols = ["track_name"]

        grouped = matched.groupby(group_cols, dropna=False).agg(**{
            "playCount": pd.NamedAgg(column=group_cols[0], aggfunc="count"),
            **({"totalMs": pd.NamedAgg(column="ms_played", aggfunc="sum")}
               if "ms_played" in matched.columns else {}),
        }).reset_index()

        grouped = grouped.sort_values("playCount", ascending=False).head(200)

        tracks = []
        for _, row in grouped.iterrows():
            t: Dict[str, Any] = {
                "trackName": str(row.get("track_name", "")),
                "artistName": str(row.get("artist_name", "")),
                "playCount": int(row["playCount"]),
            }
            if "totalMs" in row.index:
                t["totalMs"] = int(row["totalMs"])
            tracks.append(t)

        self._json_response(200, {
            "genre": genre,
            "totalPlays": int(grouped["playCount"].sum()),
            "tracks": tracks,
        })

    # ── helpers ──────────────────────────────────────────────────────
    def _json_response(self, code: int, data: Any) -> None:
        payload = json.dumps(data).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _json_error(self, code: int, message: str) -> None:
        self._json_response(code, {"error": message})


def serve(
    *,
    port: int = 8080,
    directory: Union[str, Path, None] = None,
    host: str = "127.0.0.1",
) -> None:
    """Start a blocking CORS HTTP server.

    Parameters
    ----------
    port : int
        TCP port to bind (default 8080).
    directory : str | Path | None
        Directory to serve.  Defaults to the built frontend ``dist/``.
    host : str
        Address to bind (default ``127.0.0.1``).
    """
    serve_dir = Path(directory) if directory else _default_dist_dir()
    if not serve_dir.is_dir():
        print(
            f"Error: directory does not exist: {serve_dir}\n"
            "Hint: run 'npm run build' inside src/traverse/cosmograph/app/ first.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    handler = partial(_CORSHandler, directory=str(serve_dir))
    httpd = ThreadingHTTPServer((host, port), handler)
    print(f"Serving {serve_dir} at http://{host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        httpd.shutdown()
