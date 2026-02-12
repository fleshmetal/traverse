# traverse

A Python library for ingesting music records, processing them into canonical tables, and producing interactive co-occurrence graphs via NetworkX, PyCosmograph, and WebGL.

## Quickstart

```bash
# Linux / macOS
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"

# Windows
python -m venv .venv && .venv\Scripts\activate
pip install -e ".[dev]"

pytest -q
```

## Project Structure

```
src/traverse/              Core Python library
src/traverse/cosmograph/   Cosmograph frontend integration (server + React app)
scripts/                   CLI export and test scripts
tests/                     pytest suite
```

## Usage

### End-to-End: Spotify Data to Visualization

Build a genre co-occurrence graph from your Spotify Extended Streaming History, optionally enriched with genres/styles from a records CSV, then serve the result in the browser.

```python
from pathlib import Path
import pandas as pd

from traverse.data.spotify_extended_minimal import load_spotify_extended_minimal
from traverse.processing.enrich_fast import FastGenreStyleEnricher
from traverse.processing.cache import CanonicalTableCache
from traverse.processing.normalize import split_tags, pretty_label
from traverse.graph.cooccurrence import CooccurrenceBuilder
from traverse.graph.adapters_cosmograph import CosmographAdapter
from traverse.cosmograph.server import serve

# ── 1. Load + enrich + cache canonical tables ───────────────────────
cache = CanonicalTableCache(
    cache_dir=Path("_out"),
    build_fn=lambda: load_spotify_extended_minimal(
        Path("path/to/ExtendedStreamingHistory")
    ),
    enrich_fn=lambda t: FastGenreStyleEnricher(
        records_csv="path/to/records.csv"
    ).run(t),
    force=False,  # flip to True to rebuild from scratch
)
plays_wide, tracks_wide = cache.load_or_build()

# ── 2. Build co-occurrence graph with timeline ──────────────────────
builder = CooccurrenceBuilder(min_cooccurrence=2, max_nodes=500)
for played_at, genres, styles in plays_wide[
    ["played_at", "genres", "styles"]
].itertuples(index=False):
    tags = split_tags(genres) + split_tags(styles)
    ts_ms = (
        int(pd.Timestamp(played_at).value // 1_000_000)
        if pd.notna(played_at)
        else None
    )
    builder.add(tags, timestamp_ms=ts_ms, label_fn=pretty_label)

graph = builder.build()

# ── 3. Write JSON into the frontend dist/ and serve ─────────────────
out = Path("src/traverse/cosmograph/app/dist/cosmo_genres_spotify_timeline.json")
CosmographAdapter.write(graph, out)
print(f"Wrote {out} ({len(graph['points'])} nodes, {len(graph['links'])} edges)")

# Starts a blocking HTTP server — open the URL in your browser
serve(port=8080)  # http://127.0.0.1:8080/?data=/cosmo_genres_spotify_timeline.json
```

On subsequent runs the cache loads instantly (no `--extended-dir` needed) — just set `force=False`.

### CLI Scripts

The same workflows are available as standalone scripts:

```bash
# Spotify Extended + Records enrichment → genre co-occurrence with listening timeline
python scripts/export_cosmo_genres_spotify_timeline.py \
  --extended-dir path/to/ExtendedStreamingHistory \
  --records-csv path/to/records.csv \
  --cache-dir _out \
  --out-json src/traverse/cosmograph/app/dist/cosmo_genres_spotify_timeline.json \
  --min-cooccurrence 2 --progress --force

# Records-only → genre co-occurrence with release-year timeline
python scripts/export_cosmo_genres_records_timeline.py \
  --records-csv path/to/records.csv \
  --out-json src/traverse/cosmograph/app/dist/cosmo_genres_records_timeline.json \
  --min-cooccurrence 2 --year-min 1860 --year-max 2025 --progress
```

### Serving the Frontend

```bash
# Serve the built frontend (default dist/ directory)
tm cosmo serve

# Custom port
tm cosmo serve --port 3000

# Serve a different directory containing your JSON files
tm cosmo serve --directory ./my_output/
```

Then open the data URL in your browser, e.g.:
- `http://127.0.0.1:8080/?data=/cosmo_genres_spotify_timeline.json`
- `http://127.0.0.1:8080/?data=/cosmo_genres_records_timeline.json`

Use the timeline scrubber at the bottom to filter by time. Toggle labels with the checkbox in the header.

> **First-time setup:** build the frontend before serving:
> ```bash
> cd src/traverse/cosmograph/app && npm install && npm run build
> ```

## Features

- Ingest Spotify Extended Streaming History and records CSV
- Genre/style enrichment via fast streaming semi-join
- Canonical table caching (parquet/CSV) to avoid redundant rebuilds
- Tag co-occurrence graph building with timeline support
- Cosmograph JSON export for interactive WebGL visualization
- Built-in static server with CORS (`tm cosmo serve`)
- NetworkX and WebGL graph adapters
- Composable processor pipeline architecture
