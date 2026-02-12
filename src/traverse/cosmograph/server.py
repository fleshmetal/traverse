"""CORS-enabled static file server for the Cosmograph frontend.

Serves the pre-built ``dist/`` directory from the embedded React app.
"""
from __future__ import annotations

import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

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
