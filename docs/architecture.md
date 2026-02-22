# Traverse Architecture

Full system diagram and component details for the Traverse music graph
pipeline: from raw data ingestion through processing, graph construction,
community detection, and browser-based visualization with Cosmograph.

---

## System Diagram

```
                           DATA SOURCES
                           ============

  records.csv                          Spotify Extended Streaming History
  (title, artist,                      (Streaming_History_Audio_*.json)
   genres, styles,                     (played_at, ms_played, track URI,
   release_year)                        artist, track name, platform, ...)
       |                                          |
       v                                          v
  RecordsData                          SpotifyExtendedExport
  src/traverse/data/records.py         src/traverse/data/spotify_export.py
       |                                          |
       +------------------+---> TablesDict <------+
                           |
                           v
                   PROCESSING PIPELINE
                   ===================

  +----------------------------------------------------------+
  |  Pipeline( [FastGenreStyleEnricher, BuildCanonicalTables] )  |
  |  src/traverse/processing/base.py                          |
  |                                                            |
  |  Step 1: FastGenreStyleEnricher                            |
  |  +-------------------------------------------------+      |
  |  | Stream records.csv in chunks                     |      |
  |  | Semi-join by title_key, verify artist match      |      |
  |  | Union new genres/styles onto TablesDict          |      |
  |  | src/traverse/processing/enrich_fast.py           |      |
  |  +-------------------------------------------------+      |
  |           |                                                |
  |           v                                                |
  |  Step 2: BuildCanonicalTables                              |
  |  +-------------------------------------------------+      |
  |  | Fold genres/styles tables onto tracks            |      |
  |  | Normalize columns to canonical schema            |      |
  |  | Left-join tracks onto plays -> plays_wide        |      |
  |  | Output: {plays_wide, tracks_wide, artists_wide}  |      |
  |  | src/traverse/processing/tables.py                |      |
  |  +-------------------------------------------------+      |
  +----------------------------------------------------------+
                           |
                           v
               CanonicalTableCache (parquet)
               src/traverse/processing/cache.py
               _out/canonical_plays.parquet
               _out/canonical_tracks.parquet
                           |
                           v
                    GRAPH BUILDING
                    ==============

  +-- Tag Co-occurrence Graph -----+   +-- Album-Centered Graph ---------+
  |                                |   |                                  |
  |  build_records_graph()         |   |  build_album_graph()             |
  |  records_graph.py              |   |  album_graph.py                  |
  |                                |   |                                  |
  |  Stream CSV -> for each row:   |   |  Pass 1: Stream CSV, build      |
  |    split genre/style tags      |   |    tag -> record_id inverted     |
  |    feed to CooccurrenceBuilder |   |    index + node metadata         |
  |                                |   |                                  |
  |  CooccurrenceBuilder:          |   |  Pass 2: For each tag, emit     |
  |    accumulate (tag_a, tag_b)   |   |    edges between all record      |
  |    pair counts                 |   |    pairs sharing it (int IDs,    |
  |    threshold + cap             |   |    Counter with memory cap)      |
  |    -> CooccurrenceGraph        |   |                                  |
  |       {points, links}          |   |  Pass 3: Threshold, cap by      |
  |                                |   |    weighted degree, build        |
  |  Nodes = tags (genres/styles)  |   |    -> CooccurrenceGraph          |
  |  Edges = shared albums         |   |       {points, links}            |
  |                                |   |                                  |
  +--------------------------------+   |  Nodes = albums (with metadata)  |
                                       |  Edges = shared tag count        |
                                       +----------------------------------+
                           |
                           v
                GraphCache (JSON + parquet)
                src/traverse/graph/cache.py
                graph.json + canonical_plays.parquet
                           |
                           v
              COMMUNITY DETECTION (optional)
              ==============================

  +----------------------------------------------------------+
  |  add_communities(graph, algorithm, seed=42)               |
  |  src/traverse/graph/community.py                          |
  |                                                            |
  |  1. cooccurrence_to_networkx(graph) -> nx.Graph            |
  |  2. detect_communities(G, algorithm) -> {id: cluster}      |
  |     Algorithms: Louvain, Greedy Modularity,                |
  |       Label Propagation, Kernighan-Lin,                    |
  |       Edge Betweenness, K-Clique                           |
  |  3. apply_communities(graph, assignments)                  |
  |     -> new CooccurrenceGraph with "community" on points    |
  +----------------------------------------------------------+
                           |
                           v
              EDGE ANALYSIS (optional)
              ========================

  +----------------------------------------------------------+
  |  analyze_community_edges(graph, node_ids, algorithm)      |
  |  src/traverse/graph/edge_analysis.py                      |
  |                                                            |
  |  Algorithms: Edge Betweenness, Current Flow               |
  |    Betweenness, Bridge Detection                           |
  |  Returns: [{source, target, score, algorithm}]             |
  +----------------------------------------------------------+
                           |
                           v
                    SERIALIZATION
                    =============

  +----------------------------------------------------------+
  |  CosmographAdapter.write(graph, path, meta={...})         |
  |  src/traverse/graph/adapters_cosmograph.py                |
  |                                                            |
  |  Output JSON:                                              |
  |  {                                                         |
  |    "meta": {"clusterField": "community", "title": "..."},  |
  |    "points": [{"id", "label", "community", ...}],          |
  |    "links":  [{"source", "target", "weight", ...}]         |
  |  }                                                         |
  |                                                            |
  |  Auto-compact JSON for >50K items                          |
  |  Warns if output >200 MB                                   |
  +----------------------------------------------------------+
                           |
                           v
                    FRONTEND SERVING
                    ================

  +----------------------------------------------------------+
  |  tm cosmo serve  (or server.serve(port=8080))             |
  |  src/traverse/cosmograph/server.py                        |
  |                                                            |
  |  ThreadingHTTPServer with CORS                             |
  |  Serves: dist/ (React app) + graph JSON files             |
  |                                                            |
  |  API Endpoints:                                            |
  |    POST /api/cluster        -> re-run community detection  |
  |    POST /api/edge-analysis  -> edge scoring on subgraph    |
  |    POST /api/genre-tracks   -> track lookup from parquet   |
  |    GET  /api/corrections    -> list pending corrections    |
  |    POST /api/corrections    -> submit tag correction       |
  |    POST /api/corrections/approve[-all] -> apply fixes      |
  +----------------------------------------------------------+
                           |
                           v
                    BROWSER (Cosmograph)
                    ====================

  +----------------------------------------------------------+
  |  src/traverse/cosmograph/app/src/                         |
  |                                                            |
  |  DataLoader.ts                                             |
  |    fetch JSON -> detect clusters -> normalize time fields   |
  |    -> prepareCosmographData() -> LoadedInputs              |
  |                                                            |
  |  App.tsx                                                   |
  |    <Cosmograph> WebGL graph renderer                       |
  |    <CosmographTimeline> time brush (if first_seen_ts)      |
  |                                                            |
  |    Tool Panels (all draggable):                            |
  |    +-- Clustering ----+  +-- Communities --+               |
  |    | Algorithm select  |  | Cluster list    |               |
  |    | Params            |  | Rename/save     |               |
  |    | POST /api/cluster |  | Focus/restore   |               |
  |    +------------------+  +-----------------+               |
  |                                                            |
  |    +-- Edge Analysis --+  +-- Detail -------+              |
  |    | Algorithm select   |  | Node: top tracks|              |
  |    | Run on community   |  | Edge: both sides|              |
  |    | Ranked results     |  | Right-click edit|              |
  |    +-------------------+  +----------------+               |
  |                                                            |
  |    +-- Corrections ----+                                   |
  |    | Pending queue      |                                   |
  |    | Approve/deny       |                                   |
  |    +-------------------+                                   |
  |                                                            |
  |  Color encoding:                                           |
  |    Nodes: categorical cluster palette (6 colors)           |
  |    Links: plasma colormap by weight (rank-normalized)      |
  +----------------------------------------------------------+
```

---

## Component Details

### 1. Data Sources

#### RecordsData (`src/traverse/data/records.py`)

Reads a records CSV with columns: `title`, `artist`/`artists`, `genres`, `styles`, `release_year`.

- Generates stable `track_id = "h:<sha1(artist::title::year)>"`.
- Genre/style columns are `|`-delimited; exploded into separate `(track_id, genre)` and `(track_id, style)` rows.
- Returns `TablesDict` with `plays` (empty), `tracks`, `artists`, `genres`, `styles`.
- Supports chunked reads for large files.

#### SpotifyExtendedExport (`src/traverse/data/spotify_export.py`)

Reads Spotify Extended Streaming History JSON files (`Streaming_History_Audio_*.json`).

- Handles multiple schema vintages (2023+ `master_metadata_*` vs. older `trackName`/`artistName`).
- `track_id`: `"trk:<spotify_track_id>"` if URI present, else `"h:<sha1(artist::track)>"`.
- Returns `TablesDict` with full `plays` table (15 columns), `tracks`, `artists`, empty `genres`.

#### load_spotify_extended_minimal (`src/traverse/data/spotify_extended_minimal.py`)

Lightweight standalone loader (no class). Returns `{plays, tracks, artists}` with minimal columns.

---

### 2. Processing Pipeline

#### TablesDict (`src/traverse/core/types.py`)

The universal data contract. A TypedDict with optional keys:

| Key | Contents |
|---|---|
| `plays` | Raw play events: `played_at, track_id, ms_played` |
| `tracks` | Track metadata: `track_id, track_name, artist_name, genres, styles` |
| `artists` | Artist metadata: `artist_id, artist_name` |
| `genres` | Separate genre rows: `track_id, genre` |
| `styles` | Separate style rows: `track_id, style` |
| `plays_wide` | Plays joined with track metadata |
| `tracks_wide` | Tracks with genres/styles as `" \| "`-delimited strings |

#### Processor / Pipeline (`src/traverse/processing/base.py`)

```
Processor.run(tables: TablesDict) -> TablesDict
Pipeline([proc1, proc2, ...]).run(tables) -> tables
```

Processors are pure transforms. Pipeline chains them sequentially.

#### FastGenreStyleEnricher (`src/traverse/processing/enrich_fast.py`)

Enriches Spotify data with genres/styles from records.csv without loading it all into memory.

**Algorithm:**
1. Collect `title_key -> set(name_keys)` from current plays/tracks.
2. Stream records.csv in chunks; semi-join by title_key; verify artist match.
3. Accumulate `genres_by_nk` and `styles_by_nk` dicts.
4. Union with existing genres/styles tables.

Text normalization: NFKD ASCII-fold, casefold, strip parenthetical, strip suffix patterns (`- Remastered`, `- Live`).

#### BuildCanonicalTables (`src/traverse/processing/tables.py`)

Normalizes mixed-schema inputs into stable wide tables.

1. `_fold_genre_style_tables()`: merge separate genre/style DataFrames onto tracks as `" | "`-delimited columns.
2. `_coerce_tracks_to_canonical()`: normalize column names to `[track_id, track_name, artist_name, genres, styles]`.
3. `_coerce_plays_to_canonical()`: normalize to `[played_at, track_id, ms_played]`.
4. Left-join tracks onto plays -> `plays_wide`.

#### CanonicalTableCache (`src/traverse/processing/cache.py`)

Build-or-load cache for canonical tables. Persists to `_out/canonical_plays.parquet` and `_out/canonical_tracks.parquet`. Use `force=True` to rebuild.

#### Normalize utilities (`src/traverse/processing/normalize.py`)

| Function | Purpose |
|---|---|
| `split_tags(val)` | JSON arrays, sentinels, multi-delimiter split |
| `split_genres_styles(s)` | `\|`/`,`/`;` split with dedup and lowering |
| `pretty_label(tag)` | Title-case with special overrides (IDM, EDM, DnB) |
| `coerce_year(x)` | Parse year from various formats -> `int` or `None` |
| `safe_str(x)` | None-safe `str()` + strip |

---

### 3. Graph Building

#### CooccurrenceBuilder / CooccurrenceGraph (`src/traverse/graph/cooccurrence.py`)

The core accumulator for tag co-occurrence networks.

```python
CooccurrenceGraph = {"points": [...], "links": [...]}
```

`CooccurrenceBuilder` accumulates `(tag_a, tag_b)` pair counts from row observations:
1. `add(tags, timestamp_ms=..., label_fn=..., tag_categories=...)` per row.
2. `build()` applies threshold (`min_cooccurrence`), caps nodes by weighted degree, caps edges, emits `{points, links}`.

Points: `{id, label, [first_seen], [category]}`.
Links: `{source, target, weight, [first_seen]}`.

#### build_records_graph (`src/traverse/graph/records_graph.py`)

Streams records.csv -> feeds tags into CooccurrenceBuilder -> returns `(CooccurrenceGraph, records_df)`.

**Nodes** = genre/style tags. **Edges** = co-appearance on the same record.

#### build_album_graph (`src/traverse/graph/album_graph.py`)

The inverse: **nodes** = albums, **edges** = shared genre/style tags. Uses an inverted-index approach:

| Pass | Operation | Memory strategy |
|---|---|---|
| 1 | Stream CSV, build `tag -> [record_int_id]` inverted index + node metadata | String IDs mapped to ints |
| 2 | For each tag, emit `(int, int)` pairs via `combinations()` | Counter pruned at 5M entries |
| 3 | Threshold by `min_weight`, cap nodes/edges, build output | `del` intermediates eagerly |

`max_tag_degree`: tags with more records than this are randomly sampled down (or skipped if `sample_high_degree=False`).

#### GraphCache (`src/traverse/graph/cache.py`)

Build-or-load cache for any graph builder. Stores `graph.json` + `canonical_plays.parquet`.

---

### 4. Community Detection (`src/traverse/graph/community.py`)

Converts CooccurrenceGraph to NetworkX, runs detection, writes cluster IDs back onto points.

**Algorithms:**

| Algorithm | Parameters |
|---|---|
| Louvain | `resolution`, `seed` |
| Greedy Modularity | `resolution`, `best_n` |
| Label Propagation | `seed` |
| Kernighan-Lin | `seed` |
| Edge Betweenness | `best_n` |
| K-Clique | `k` |

`add_communities(graph, algorithm, seed=42)` is the one-call convenience: convert -> detect -> apply. Returns a new graph (no mutation) with `"community"` field on each point. Cluster IDs are 0-indexed, sorted by size descending.

---

### 5. Edge Analysis (`src/traverse/graph/edge_analysis.py`)

Scores edges within a community subgraph.

| Algorithm | What it measures |
|---|---|
| Edge Betweenness | Fraction of shortest paths passing through an edge |
| Current Flow Betweenness | Random-walk variant (requires scipy) |
| Bridge Detection | Whether removing the edge disconnects the graph |

`analyze_community_edges(graph, node_ids, algorithm, top_k=50)` -> `[{source, target, score, algorithm}]`.

---

### 6. Serialization (`src/traverse/graph/adapters_cosmograph.py`)

`CosmographAdapter.write(graph, path, meta={...})` produces the JSON consumed by the frontend:

```json
{
  "meta": {"clusterField": "community", "title": "Fuzz Archives"},
  "points": [{"id": "...", "label": "...", "community": 0, ...}],
  "links":  [{"source": "...", "target": "...", "weight": 3, ...}]
}
```

- Auto-switches to compact JSON (no indentation) when graph exceeds 50K items.
- Prints file size and warns if >200 MB.

Other adapters:
- `NetworkXAdapter` / `to_networkx()` — DataFrame-based `GraphTables` to NetworkX.
- `WebGLJSONAdapter` — `{nodes, edges}` format for generic WebGL viewers.

---

### 7. Frontend Serving

#### Server (`src/traverse/cosmograph/server.py`)

`ThreadingHTTPServer` with CORS headers. Serves the built React `dist/` directory and graph JSON files.

**API endpoints:**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/cluster` | Run community detection on a graph JSON file |
| `POST` | `/api/edge-analysis` | Run edge scoring on a community subgraph |
| `POST` | `/api/genre-tracks` | Look up tracks for a genre from canonical parquet |
| `GET` | `/api/corrections` | List pending genre/style corrections |
| `POST` | `/api/corrections` | Submit a tag correction |
| `POST` | `/api/corrections/approve` | Approve and apply a correction |
| `POST` | `/api/corrections/approve-all` | Approve all pending corrections |
| `POST` | `/api/corrections/deny` | Reject a pending correction |

Corrections modify `genre_style_overrides.csv` and patch canonical parquet files in place.

CLI: `tm cosmo serve [--port 8080]`

#### DataLoader (`src/traverse/cosmograph/app/src/DataLoader.ts`)

Client-side JSON loader. On mount:
1. Fetch `?data=<url>` (default: `/cosmo_genres_timeline.json`).
2. Detect cluster field: URL param > `meta.clusterField` > auto-detect `"category"`.
3. Normalize time fields (`first_seen`, `first_seen_ts`, year strings) to epoch ms.
4. `prepareCosmographData()` for Cosmograph's internal columnar store.
5. Build lookup maps and cluster groups.

#### App (`src/traverse/cosmograph/app/src/App.tsx`)

Main React component. Renders a full-screen `<Cosmograph>` with:

- **Header bar**: Title from `meta.title` (default "Fuzz Archives"), status, label/cluster toggles, "Powered by Traverse and Visualized with Cosmograph".
- **Timeline**: `<CosmographTimeline>` with sticky brush when `first_seen_ts` exists.
- **Left toolbar** with 5 draggable tool panels:
  - **Clustering**: Select algorithm + params, POST `/api/cluster`, re-render.
  - **Communities**: Cluster list with rename, save, focus, right-click context menu.
  - **Edge Analysis**: Select algorithm, run on selected community, ranked results.
  - **Detail**: Click node -> top tracks from `/api/genre-tracks`; click edge -> both sides.
  - **Corrections**: Review queue with approve/deny.

**Color encoding:**
- Nodes: 6-color categorical palette by cluster, with dimming for unselected.
- Links: Plasma colormap (rank-normalized weight -> deep purple -> pink -> orange).

---

## Package Structure

```
src/traverse/
  __init__.py
  cli/
    main.py                      tm cosmo serve
  config/
    settings.py                  Pydantic settings (.env)
  core/
    types.py                     TablesDict, GraphDFs
    errors.py                    MissingColumnError, etc.
  data/
    base.py                      DataSource ABC
    records.py                   RecordsData (records CSV)
    spotify_export.py            SpotifyExtendedExport
    spotify_extended_minimal.py  load_spotify_extended_minimal()
  processing/
    base.py                      Processor ABC, Pipeline
    normalize.py                 split_tags, pretty_label, coerce_year
    tables.py                    BuildCanonicalTables
    enrich.py                    GenreStyleEnricher
    enrich_fast.py               FastGenreStyleEnricher
    cache.py                     CanonicalTableCache
  graph/
    base.py                      GraphBuilder ABC, GraphAdapter ABC
    builder.py                   GraphBuilder (bipartite)
    cooccurrence.py              CooccurrenceBuilder, CooccurrenceGraph
    records_graph.py             build_records_graph()
    album_graph.py               build_album_graph()
    cache.py                     GraphCache
    community.py                 CommunityAlgorithm, add_communities()
    edge_analysis.py             EdgeAlgorithm, analyze_community_edges()
    adapters_cosmograph.py       CosmographAdapter
    adapters_networkx.py         NetworkXAdapter, to_networkx()
    adapters_webgl.py            WebGLJSONAdapter
  cosmograph/
    server.py                    HTTP server + API endpoints
    app/
      src/
        main.tsx                 React entry point
        App.tsx                  Main Cosmograph UI
        DataLoader.ts            JSON loader + data prep
        labels.css               Label styling
  utils/
    progress.py                  tqdm adapter
    merge.py                     merge_tables()

scripts/
  export_cosmo_genres.py                    Records -> co-occurrence JSON
  export_cosmo_genres_records_timeline.py   Records -> co-occurrence + release year timeline
  export_cosmo_genres_timeline.py           Spotify -> enriched -> co-occurrence + play date timeline
  export_cosmo_genres_spotify_timeline.py   Spotify -> timeline variant
  export_cosmo_genres_from_spotify.py       Spotify -> enriched -> co-occurrence
  export_cosmograph.py                      Legacy bipartite CSV export
  diag_canonical_tables.py                  Diagnostic tool

notebooks/
  fuzzarchives_albumcentered.ipynb          Album-centered graph pipeline
  end_to_end_records_community.ipynb        Records co-occurrence + community
  end_to_end_records.ipynb                  Records co-occurrence
  end_to_end_spotify.ipynb                  Spotify pipeline
  end_to_end_spotify_cluster.ipynb          Spotify + clustering
  end_to_end_community.ipynb                Community detection
```

---

## Cache Layers

| Cache | Location | Contents | Rebuild |
|---|---|---|---|
| CanonicalTableCache | `_out/` | `canonical_plays.parquet`, `canonical_tracks.parquet` | `force=True` |
| GraphCache | `_out/` or `_out_album/` | `graph.json`, `canonical_plays.parquet` | `force=True` |

Both caches use parquet as the primary format with CSV fallback. Album graph uses a separate `_out_album/` directory to avoid colliding with the tag graph cache.

---

## Data Flow: End-to-End Example

**Fuzz Archives (album-centered, from records.csv):**

```
records.csv (14.6M rows, 10.6M unique albums)
    |
    v  build_album_graph()
Pass 1: Stream CSV, dedup by title::artist, build inverted index
    |   710 unique tags, 10.6M records with tags
    v
Pass 2: For each tag, sample to max_tag_degree, emit (int, int) pairs
    |   Counter pruned at 5M entries to bound memory
    v
Pass 3: Filter min_weight >= 3, cap to 5K nodes / 100K edges
    |
    v  GraphCache.load_or_build()
graph.json + canonical_plays.parquet cached to _out_album/
    |
    v  add_communities(graph, LOUVAIN, seed=42)
CooccurrenceGraph with "community" field on each point
    |
    v  CosmographAdapter.write(graph, path, meta={"title": "Fuzz Archives"})
cosmo_albums_community.json (compact if >50K items)
    |
    v  serve(port=8080)
Browser: http://127.0.0.1:8080/?data=/cosmo_albums_community.json
```

**Tag co-occurrence with Spotify timeline:**

```
Spotify Extended JSON files
    |
    v  SpotifyExtendedExport.load()
TablesDict {plays, tracks, artists, genres(empty)}
    |
    v  FastGenreStyleEnricher.run(tables)  [streams records.csv]
TablesDict {plays, tracks, artists, genres, styles}
    |
    v  BuildCanonicalTables.run(tables)
{plays_wide, tracks_wide}  [cached to _out/ as parquet]
    |
    v  iterate plays_wide rows
CooccurrenceBuilder.add(tags, timestamp_ms=played_at_ms)
    |
    v  builder.build()
CooccurrenceGraph with first_seen timestamps on points + links
    |
    v  add_communities() + CosmographAdapter.write()
cosmo_genres_timeline.json -> serve -> browser with timeline brush
```
