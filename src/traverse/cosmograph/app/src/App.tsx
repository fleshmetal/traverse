import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import './labels.css';
import {
  Cosmograph,
  CosmographProvider,
  CosmographTimeline,
  CosmographButtonPolygonalSelection,
  CosmographButtonRectangularSelection,
  prepareCosmographData,
  type CosmographConfig,
  type Cosmograph as CosmographInstance
} from '@cosmograph/react';
import { loadAndPrepare, filterToCommunity, computeLinkColors, applyClusterAssignments, GRADIENT_PRESETS, type LoadedInputs, type ClusterGroup } from './DataLoader';

interface SavedCommunity {
  clusterValue: string;
  name: string;
  nodeCount: number;
  savedAt: number;
}

/** Generate a random HSL color with good saturation and visibility. */
function randomVisibleColor(): string {
  const h = Math.floor(Math.random() * 360);
  const s = 65 + Math.floor(Math.random() * 25);
  const l = 50 + Math.floor(Math.random() * 20);
  return `hsl(${h}, ${s}%, ${l}%)`;
}

const UNKNOWN_COLOR = 'rgba(205, 207, 213, 0.9)';
const DIM_COLOR = 'rgba(138, 138, 138, 0.2)';

/** Plasma colormap: t=0 → dark purple (homogeneous), t=1 → bright yellow (diverse). */
function plasmaColor(t: number): string {
  // Plasma LUT – 9 stops sampled from matplotlib's plasma colormap
  const stops: [number, number, number][] = [
    [13, 8, 135],    // 0.000
    [75, 3, 161],    // 0.125
    [126, 3, 168],   // 0.250
    [168, 34, 150],  // 0.375
    [203, 70, 121],  // 0.500
    [229, 107, 93],  // 0.625
    [248, 148, 65],  // 0.750
    [253, 195, 40],  // 0.875
    [240, 249, 33],  // 1.000
  ];
  const tc = Math.max(0, Math.min(1, t));
  const idx = tc * (stops.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.min(lo + 1, stops.length - 1);
  const f = idx - lo;
  const r = Math.round(stops[lo][0] + (stops[hi][0] - stops[lo][0]) * f);
  const g = Math.round(stops[lo][1] + (stops[hi][1] - stops[lo][1]) * f);
  const b = Math.round(stops[lo][2] + (stops[hi][2] - stops[lo][2]) * f);
  return `rgb(${r},${g},${b})`;
}

const LOADING_MESSAGES = [
  'Digging through the crates...',
  'Scanning the stacks...',
  'Flipping through records...',
  'Checking the back catalog...',
  'Dusting off the vinyl...',
  'Reading the liner notes...',
  'Queuing up the tracks...',
];

// ── Algorithm metadata for dynamic form generation ──────────────────
type AlgoParam = 'resolution' | 'seed' | 'best_n' | 'k';
interface AlgoMeta {
  value: string;
  label: string;
  params: AlgoParam[];
}

const ALGORITHMS: AlgoMeta[] = [
  { value: 'louvain',            label: 'Louvain',              params: ['resolution', 'seed'] },
  { value: 'greedy_modularity',  label: 'Greedy Modularity',    params: ['resolution', 'best_n'] },
  { value: 'label_propagation',  label: 'Label Propagation',    params: ['seed'] },
  { value: 'kernighan_lin',      label: 'Kernighan-Lin',        params: ['seed'] },
  { value: 'edge_betweenness',   label: 'Edge Betweenness',     params: ['best_n'] },
  { value: 'k_clique',           label: 'K-Clique',             params: ['k'] },
];

const PARAM_LABELS: Record<AlgoParam, string> = {
  resolution: 'Resolution',
  seed: 'Seed',
  best_n: 'Best N',
  k: 'K (clique size)',
};

// ── Edge analysis algorithms ─────────────────────────────────────────
const EDGE_ALGORITHMS = [
  { value: 'edge_betweenness',          label: 'Edge Betweenness' },
  { value: 'current_flow_betweenness',  label: 'Random Walk (Current Flow)' },
  { value: 'bridges',                   label: 'Bridge Detection' },
];

interface EdgeResult {
  source: string;
  target: string;
  score: number;
  algorithm: string;
}

interface SavedEdge {
  source: string;
  target: string;
  sourceLabel: string;
  targetLabel: string;
  score: number;
  algorithm: string;
  savedAt: number;
}

interface PathResult {
  nodes: string[];
  labels: string[];
  length: number;
  totalWeight: number;
  pathType: 'shortest' | 'diverse_longest';
}

// ── Panel resize hook (vertical + horizontal) ─────────────────────────────
function usePanelResize(minH = 80, _maxH?: number, minW = 160, maxW = 700) {
  const [size, setSize] = useState<{ w: number | null; h: number | null }>({ w: null, h: null });
  const vRef = useRef<{ sy: number; sh: number; panelTop: number } | null>(null);
  const hRef = useRef<{ sx: number; sw: number } | null>(null);

  const onVStart = useCallback((e: React.MouseEvent) => {
    if (e.button !== 0) return;
    const panel = (e.currentTarget as HTMLElement).closest('.tool-panel') as HTMLElement;
    if (!panel) return;
    const rect = panel.getBoundingClientRect();
    // Capture start values so the mousemove closure doesn't depend on the ref
    const startY = e.clientY;
    const startH = rect.height;
    const panelTop = rect.top;
    vRef.current = { sy: startY, sh: startH, panelTop };
    const onMove = (ev: MouseEvent) => {
      if (!vRef.current) return;
      const maxH = _maxH ?? (window.innerHeight - panelTop - 8);
      const newH = Math.min(maxH, Math.max(minH, startH + ev.clientY - startY));
      setSize(s => ({ ...s, h: newH }));
    };
    const onUp = () => { vRef.current = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    e.preventDefault();
  }, [minH, _maxH]);

  const onHStart = useCallback((e: React.MouseEvent) => {
    if (e.button !== 0) return;
    const panel = (e.currentTarget as HTMLElement).closest('.tool-panel') as HTMLElement;
    if (!panel) return;
    const startX = e.clientX;
    const startW = panel.getBoundingClientRect().width;
    hRef.current = { sx: startX, sw: startW };
    const onMove = (ev: MouseEvent) => {
      if (!hRef.current) return;
      const newW = Math.min(maxW, Math.max(minW, startW + ev.clientX - startX));
      setSize(s => ({ ...s, w: newW }));
    };
    const onUp = () => { hRef.current = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp); };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    e.preventDefault();
  }, [minW, maxW]);

  return { size, setSize, onVStart, onHStart };
}

// ── Draggable panel hook ─────────────────────────────────────────────
function clampPos(x: number, y: number): { x: number; y: number } {
  return {
    x: Math.max(0, Math.min(window.innerWidth - 60, x)),
    y: Math.max(0, Math.min(window.innerHeight - 40, y)),
  };
}

function useDrag() {
  const [pos, setPos] = useState<{ x: number; y: number } | null>(null);
  const dragRef = useRef<{ offsetX: number; offsetY: number } | null>(null);

  // Snap back into viewport on window resize
  useEffect(() => {
    const onResize = () => {
      setPos(prev => prev ? clampPos(prev.x, prev.y) : null);
    };
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);

  const onDragStart = useCallback((e: React.MouseEvent) => {
    if (e.button !== 0) return;
    const tag = (e.target as HTMLElement).tagName;
    if (tag === 'INPUT' || tag === 'SELECT' || tag === 'BUTTON') return;
    const panel = (e.currentTarget as HTMLElement).closest('.draggable-panel') as HTMLElement;
    if (!panel) return;
    const rect = panel.getBoundingClientRect();
    dragRef.current = {
      offsetX: e.clientX - rect.left,
      offsetY: e.clientY - rect.top,
    };
    const onMove = (ev: MouseEvent) => {
      if (!dragRef.current) return;
      const { offsetX, offsetY } = dragRef.current;
      setPos(clampPos(ev.clientX - offsetX, ev.clientY - offsetY));
    };
    const onUp = () => {
      dragRef.current = null;
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    e.preventDefault();
  }, []);

  const dragStyle: CSSProperties | undefined = pos
    ? { position: 'fixed', top: pos.y, left: pos.x, right: 'auto', bottom: 'auto', maxHeight: `calc(100vh - ${pos.y}px - 8px)` }
    : undefined;

  return { dragStyle, onDragStart, setPos };
}

/** Build a Discogs search URL for a track+artist. */
function discogsTrackUrl(trackName: string, artistName: string): string {
  const q = encodeURIComponent(`${trackName} ${artistName}`.trim());
  return `https://www.discogs.com/search/?q=${q}&type=release`;
}

export default function App() {
  // Graph selector state
  const [availableGraphs, setAvailableGraphs] = useState<{filename: string; label: string; sizeMB: number}[]>([]);
  const [selectedGraph, setSelectedGraph] = useState<string>(() => {
    const p = new URLSearchParams(window.location.search).get('data');
    return p ? p.replace(/^\//, '') : '';  // empty = will be set from manifest
  });
  const [graphLoading, setGraphLoading] = useState(false);
  const [autoCluster, setAutoCluster] = useState(true);

  // dataUrl derived from selectedGraph — drives localStorage keys + API calls
  const dataUrl = useMemo(
    () => selectedGraph ? '/' + selectedGraph : '/cosmo_genres_timeline.json',
    [selectedGraph]
  );

  const [loaded, setLoaded] = useState<LoadedInputs | null>(null);
  const [cfg, setCfg] = useState<CosmographConfig | null>(null);
  const [labelsOn, setLabelsOn] = useState(true);
  const [clusterOn, setClusterOn] = useState(false);
  const [colorCommunities, setColorCommunities] = useState(false);
  const [homogeneityMode, setHomogeneityMode] = useState(false);
  const [recolorKick, setRecolorKick] = useState(0);
  const [status, setStatus] = useState('Loading…');

  // Mobile: render one community at a time to avoid WebGL crash
  const isMobile = useMemo(() => window.innerWidth <= 768, []);
  const fullLoadedRef = useRef<LoadedInputs | null>(null);

  const cosmoRef = useRef<CosmographInstance | null>(null);
  const [selectedCluster, setSelectedCluster] = useState<string | null>(null);

  // Toolbar state
  type ToolId = 'clustering' | 'communities' | 'edges' | 'paths' | 'detail' | 'corrections' | 'customize' | 'search' | 'selection' | 'overlap' | 'genre-search';
  const [openTools, setOpenTools] = useState<Set<ToolId>>(new Set());

  const toggleTool = useCallback((id: ToolId) => {
    setOpenTools(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }, []);

  // Search panel state
  const [searchQuery, setSearchQuery] = useState('');

  // Context menu + community focus state
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; clusterValue: string } | null>(null);
  const [fullData, setFullData] = useState<{ points: any[]; links: any[] } | null>(null);
  const [focusedCluster, setFocusedCluster] = useState<string | null>(null);

  // Genre detail panel state
  const [selectedGenre, setSelectedGenre] = useState<string | null>(null);
  const [selectedNodeLinks, setSelectedNodeLinks] = useState<{ platform: string; url: string; label: string }[] | null>(null);
  const [genreTracks, setGenreTracks] = useState<any[]>([]);
  const [genreTotalPlays, setGenreTotalPlays] = useState(0);
  const [loadingTracks, setLoadingTracks] = useState(false);

  // Album detail panel state
  const [selectedAlbum, setSelectedAlbum] = useState<{
    id: string; label: string; artist: string;
    genres: string; styles: string; releaseYear?: number;
    externalLinks?: { platform: string; url: string; label: string }[];
  } | null>(null);
  const [albumTracks, setAlbumTracks] = useState<any[]>([]);
  const [albumTotalPlays, setAlbumTotalPlays] = useState(0);
  const [loadingAlbumTracks, setLoadingAlbumTracks] = useState(false);

  // Community rename + saved communities state
  const [communityNames, setCommunityNames] = useState<Map<string, string>>(new Map());
  const [savedCommunities, setSavedCommunities] = useState<SavedCommunity[]>([]);
  const [renamingCluster, setRenamingCluster] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState('');

  // Edge analysis state
  const [edgeAlgo, setEdgeAlgo] = useState(EDGE_ALGORITHMS[0].value);
  const [edgeResults, setEdgeResults] = useState<EdgeResult[]>([]);
  const [edgeLoading, setEdgeLoading] = useState(false);
  const [edgeError, setEdgeError] = useState<string | null>(null);

  // Edge selection + saved edges
  const [selectedEdge, setSelectedEdge] = useState<EdgeResult | null>(null);
  const [savedEdges, setSavedEdges] = useState<SavedEdge[]>([]);
  const [edgeDetailA, setEdgeDetailA] = useState<{ label: string; tracks: any[]; totalPlays: number } | null>(null);
  const [edgeDetailB, setEdgeDetailB] = useState<{ label: string; tracks: any[]; totalPlays: number } | null>(null);
  const [loadingEdgeDetail, setLoadingEdgeDetail] = useState(false);

  // Path finding state
  const [pathCommunityA, setPathCommunityA] = useState<string | null>(null);
  const [pathCommunityB, setPathCommunityB] = useState<string | null>(null);
  const [pathRestrict, setPathRestrict] = useState(false);
  // Node-to-node path mode
  const [pathMode, setPathMode] = useState<'community' | 'node'>('node');
  const [pathNodeA, setPathNodeA] = useState('');
  const [pathNodeB, setPathNodeB] = useState('');
  const [pathResults, setPathResults] = useState<PathResult[]>([]);
  const [pathLoading, setPathLoading] = useState(false);
  const [pathError, setPathError] = useState<string | null>(null);
  const [selectedPath, setSelectedPath] = useState<PathResult | null>(null);

  // Track context menu state
  const [trackContextMenu, setTrackContextMenu] = useState<{
    x: number; y: number;
    track: { trackId: string; trackName: string; artistName: string; genres: string; styles: string };
  } | null>(null);

  // Correction form state
  const [correctionForm, setCorrectionForm] = useState<{
    trackId: string; trackName: string; artistName: string;
    currentGenres: string; currentStyles: string;
    newGenres: string; newStyles: string;
  } | null>(null);

  const [pendingCorrections, setPendingCorrections] = useState<any[]>([]);

  // Edge customization state
  const [edgeGradient, setEdgeGradient] = useState('plasma');
  const [edgeOpacity, setEdgeOpacity] = useState(0.8);
  const [panelOpacity, setPanelOpacity] = useState(0.65);
  // Advanced tool visibility (hidden by default, toggled in settings)
  const [showClusteringTool, setShowClusteringTool] = useState(false);
  const [showEdgeTool, setShowEdgeTool] = useState(false);
  const [edgeWidthMin, setEdgeWidthMin] = useState(0.3);
  const [edgeWidthMax, setEdgeWidthMax] = useState(2.0);

  // Community color overrides
  const [communityColors, setCommunityColors] = useState<Map<string, string>>(new Map());
  const colorInputRef = useRef<HTMLInputElement>(null);
  const [colorPickerTarget, setColorPickerTarget] = useState<string | null>(null);

  // Node right-click context menu
  const [nodeContextMenu, setNodeContextMenu] = useState<{
    x: number; y: number; pointIndex: number; label: string;
  } | null>(null);

  // Freehand lasso selection
  const [lassoMode, setLassoMode] = useState(false);
  const lassoCanvasRef = useRef<HTMLCanvasElement>(null);
  const lassoActiveRef = useRef(false);
  const lassoPointsRef = useRef<[number, number][]>([]);

  // User listening overlap state
  interface OverlapMatch {
    nodeId: string; playCount: number; totalMs: number;
    firstListenEpochMs?: number; lastListenEpochMs?: number;
    topTracks: { trackName: string; playCount: number; totalMs: number }[];
  }
  const [overlapMatches, setOverlapMatches] = useState<Map<string, OverlapMatch> | null>(null);
  const [overlapHighlight, setOverlapHighlight] = useState(false);
  const [overlapOpacity, setOverlapOpacity] = useState(0.225);
  const [overlapHeatmap, setOverlapHeatmap] = useState(false);
  const [overlapLoading, setOverlapLoading] = useState(false);
  const [overlapError, setOverlapError] = useState<string | null>(null);
  const [overlapSummary, setOverlapSummary] = useState<{ matched: number; total: number } | null>(null);
  const [overlapInputMode, setOverlapInputMode] = useState<'path' | 'upload'>('path');
  const [overlapPath, setOverlapPath] = useState('');

  // Genre search state
  const [genreSearchQuery, setGenreSearchQuery] = useState('');
  const [genreSearchHighlight, setGenreSearchHighlight] = useState(false);
  const [genreSearchHeatmap, setGenreSearchHeatmap] = useState(false);

  // Build genre index: genre string → Set of point ids that have it
  const genreIndex = useMemo(() => {
    const idx = new Map<string, Set<string>>();
    if (!loaded) return idx;
    for (const pt of loaded.raw.points) {
      const id = String(pt.id ?? '');
      for (const field of ['genres', 'styles']) {
        const raw = pt[field];
        if (!raw || typeof raw !== 'string') continue;
        for (const tag of raw.split(/\s*\|\s*/)) {
          const t = tag.trim().toLowerCase();
          if (!t) continue;
          let s = idx.get(t);
          if (!s) { s = new Set(); idx.set(t, s); }
          s.add(id);
        }
      }
    }
    return idx;
  }, [loaded]);

  // All unique genre/style tags for autocomplete
  const allTags = useMemo(() =>
    [...genreIndex.keys()].sort(),
  [genreIndex]);

  // Genre search matches: set of point ids matching the query
  const genreSearchMatches = useMemo(() => {
    const q = genreSearchQuery.trim().toLowerCase();
    if (!q) return null;
    // Support multiple comma-separated terms (OR logic)
    const terms = q.split(',').map(t => t.trim()).filter(Boolean);
    const matched = new Set<string>();
    for (const term of terms) {
      // Exact match first, then substring
      const exact = genreIndex.get(term);
      if (exact) {
        for (const id of exact) matched.add(id);
      } else {
        for (const [tag, ids] of genreIndex) {
          if (tag.includes(term)) {
            for (const id of ids) matched.add(id);
          }
        }
      }
    }
    return matched.size > 0 ? matched : null;
  }, [genreSearchQuery, genreIndex]);

  // Genre search heatmap: count how many matching tags each node has
  const genreSearchCounts = useMemo(() => {
    const q = genreSearchQuery.trim().toLowerCase();
    if (!q || !genreSearchMatches) return null;
    const terms = q.split(',').map(t => t.trim()).filter(Boolean);
    const counts = new Map<string, number>();
    for (const term of terms) {
      const exact = genreIndex.get(term);
      if (exact) {
        for (const id of exact) counts.set(id, (counts.get(id) ?? 0) + 1);
      } else {
        for (const [tag, ids] of genreIndex) {
          if (tag.includes(term)) {
            for (const id of ids) counts.set(id, (counts.get(id) ?? 0) + 1);
          }
        }
      }
    }
    return counts;
  }, [genreSearchQuery, genreSearchMatches, genreIndex]);

  // Genre search sorted results: sort by % of matching tags across all records
  const genreSearchSorted = useMemo(() => {
    if (!loaded || !genreSearchMatches || genreSearchMatches.size === 0) return [];
    const q = genreSearchQuery.trim().toLowerCase();
    if (!q) return [];
    const terms = q.split(',').map(t => t.trim()).filter(Boolean);

    // For each matching point, compute match % from tag_counts
    const scored: { point: any; pct: number; matchCount: number; totalCount: number }[] = [];
    for (const pt of loaded.raw.points) {
      const id = String(pt.id ?? '');
      if (!genreSearchMatches.has(id)) continue;

      const tc: Record<string, number> | undefined = pt.tag_counts;
      if (tc) {
        // Sum counts of matching tags vs total tag occurrences
        let totalCount = 0;
        let matchCount = 0;
        for (const [tag, count] of Object.entries(tc)) {
          totalCount += count;
          const tagLower = tag.toLowerCase();
          for (const term of terms) {
            if (tagLower === term || tagLower.includes(term)) {
              matchCount += count;
              break;
            }
          }
        }
        const pct = totalCount > 0 ? matchCount / totalCount : 0;
        scored.push({ point: pt, pct, matchCount, totalCount });
      } else {
        // Fallback for album nodes or missing tag_counts: count unique matching tags / total tags
        let total = 0;
        let matched = 0;
        for (const field of ['genres', 'styles']) {
          const raw = pt[field];
          if (!raw || typeof raw !== 'string') continue;
          for (const tag of raw.split(/\s*\|\s*/)) {
            const t = tag.trim().toLowerCase();
            if (!t) continue;
            total++;
            for (const term of terms) {
              if (t === term || t.includes(term)) { matched++; break; }
            }
          }
        }
        const pct = total > 0 ? matched / total : 0;
        scored.push({ point: pt, pct, matchCount: matched, totalCount: total });
      }
    }

    scored.sort((a, b) => b.pct - a.pct || b.matchCount - a.matchCount);
    return scored;
  }, [loaded, genreSearchMatches, genreSearchQuery]);

  // Stable random colors for communities (survives re-renders, cleared on re-cluster)
  const generatedColorsRef = useRef(new Map<string, string>());

  const [selectedAlgo, setSelectedAlgo] = useState<string>(ALGORITHMS[0].value);
  const [algoParams, setAlgoParams] = useState<Record<string, string>>({});
  const [clustering, setClustering] = useState(false);
  const [clusterError, setClusterError] = useState<string | null>(null);

  const activeAlgo = useMemo(() => ALGORITHMS.find(a => a.value === selectedAlgo)!, [selectedAlgo]);

  // Genre-tracks response cache (persists across renders, never stale for this session)
  const trackCache = useRef<Map<string, { tracks: any[]; totalPlays: number }>>(new Map());

  // Cycling loading message
  const [loadingMessage, setLoadingMessage] = useState(LOADING_MESSAGES[0]);
  useEffect(() => {
    if (!loadingTracks && !loadingEdgeDetail && !loadingAlbumTracks) return;
    let idx = 0;
    setLoadingMessage(LOADING_MESSAGES[0]);
    const id = setInterval(() => {
      idx = (idx + 1) % LOADING_MESSAGES.length;
      setLoadingMessage(LOADING_MESSAGES[idx]);
    }, 1500);
    return () => clearInterval(id);
  }, [loadingTracks, loadingEdgeDetail, loadingAlbumTracks]);

  // (Overlap edge visibility is handled by selectPoints + linkGreyoutOpacity)

  // Drag handles for movable panels
  const clusterDrag = useDrag();
  const communityDrag = useDrag();
  const nodeListDrag = useDrag();
  const genreDrag = useDrag();
  const edgeDrag = useDrag();
  const correctionsDrag = useDrag();
  const correctionFormDrag = useDrag();
  const customizeDrag = useDrag();
  const searchDrag = useDrag();
  const selectionDrag = useDrag();
  const pathsDrag = useDrag();
  const overlapDrag = useDrag();
  const genreSearchDrag = useDrag();

  // Panel resize handles (vertical + horizontal)
  const clusterResize = usePanelResize();
  const communityResize = usePanelResize();
  const nodeListResize = usePanelResize();
  const edgeResize = usePanelResize();
  const genreResize = usePanelResize();
  const correctionsResize = usePanelResize();
  const customizeResize = usePanelResize();
  const searchResize = usePanelResize();
  const pathsResize = usePanelResize();
  const overlapResize = usePanelResize();
  const genreSearchResize = usePanelResize();

  // Build color map: cluster value → color (custom overrides > stable random)
  const clusterColorMap = useMemo(() => {
    if (!loaded?.hasCluster) return null;
    const map = new Map<string, string>();
    const generated = generatedColorsRef.current;
    [...loaded.clusterGroups.entries()].forEach(([key]) => {
      const override = communityColors.get(key);
      if (override) {
        map.set(key, override);
      } else {
        let color = generated.get(key);
        if (!color) {
          color = randomVisibleColor();
          generated.set(key, color);
        }
        map.set(key, color);
      }
    });
    return map;
  }, [loaded, communityColors]);

  // Aggregate genres/styles for selected community (album nodes) or just count (genre nodes)
  const communityAggregation = useMemo(() => {
    if (!loaded || selectedCluster == null) return null;
    const group = loaded.clusterGroups.get(selectedCluster);
    if (!group) return null;
    const pts = group.indices.map(i => loaded.raw.points[i]).filter(Boolean);
    const genreCount = new Map<string, number>();
    const styleCount = new Map<string, number>();
    for (const p of pts) {
      ((typeof p.genres === 'string' && p.genres) ? p.genres.split(/[|\s]*\|\s*|,\s*/).map((g: string) => g.trim()).filter(Boolean) : [])
        .forEach((g: string) => genreCount.set(g, (genreCount.get(g) ?? 0) + 1));
      ((typeof p.styles === 'string' && p.styles) ? p.styles.split(/[|\s]*\|\s*|,\s*/).map((s: string) => s.trim()).filter(Boolean) : [])
        .forEach((s: string) => styleCount.set(s, (styleCount.get(s) ?? 0) + 1));
    }
    return {
      total: pts.length,
      genres: [...genreCount.entries()].sort((a, b) => b[1] - a[1]).slice(0, 30),
      styles: [...styleCount.entries()].sort((a, b) => b[1] - a[1]).slice(0, 30),
    };
  }, [loaded, selectedCluster]);

  // Compute style homogeneity (normalized Shannon entropy) per community
  const homogeneityMap = useMemo(() => {
    if (!loaded?.hasCluster) return null;
    const map = new Map<string, number>();
    for (const [key, group] of loaded.clusterGroups.entries()) {
      const styleCounts = new Map<string, number>();
      let totalTags = 0;
      for (const idx of group.indices) {
        const p = loaded.raw.points[idx];
        if (!p) continue;
        const raw = typeof p.styles === 'string' ? p.styles : '';
        if (!raw) continue;
        const tags = raw.split(/[|\s]*\|\s*|,\s*/).map((s: string) => s.trim()).filter(Boolean);
        for (const t of tags) {
          styleCounts.set(t, (styleCounts.get(t) ?? 0) + 1);
          totalTags++;
        }
      }
      const n = styleCounts.size;
      if (n <= 1) { map.set(key, 0); continue; }
      let h = 0;
      for (const count of styleCounts.values()) {
        const p = count / totalTags;
        h -= p * Math.log(p);
      }
      map.set(key, h / Math.log(n)); // normalized entropy [0..1]
    }
    return map;
  }, [loaded]);

  // Search: filter points by query against label, artist, genres, styles
  const searchResults = useMemo(() => {
    if (!searchQuery.trim() || !loaded) return [];
    const q = searchQuery.trim().toLowerCase();
    const matches: any[] = [];
    for (const p of loaded.raw.points) {
      if (matches.length >= 50) break;
      const haystack = [p.label, p.artist, p.genres, p.styles]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
      if (haystack.includes(q)) matches.push(p);
    }
    return matches;
  }, [searchQuery, loaded]);

  // Helper: build base CosmographConfig from LoadedInputs
  const buildBaseCfg = useCallback((inputs: LoadedInputs): CosmographConfig => ({
    ...(inputs.prepared?.cosmographConfig ?? {}),
    points: inputs.prepared?.points,
    links:  inputs.prepared?.links,
    showLabels: true,
    showDynamicLabels: true,
    showTopLabels: true,
    showTopLabelsLimit: 200,
    pointLabelClassName: 'genre-label',
    clusterLabelClassName: 'cluster-label',
    pointLabelColor: '#ffffff',
    clusterLabelColor: '#ffffff',
    simulationFriction: 0.7,
    simulationDecay: 3000,
    enableSimulationDuringZoom: true,
    ...(isMobile ? {
      curvedLinks: false,
    } : {
      curvedLinks: true,
      curvedLinkSegments: 19,
      curvedLinkWeight: 0.8,
      curvedLinkControlPointDistance: 0.5,
    }),
    linkColorBy: '_color',
    linkWidthBy: 'weight',
    linkWidthRange: [edgeWidthMin, edgeWidthMax],
    ...(inputs.hasCluster ? {
      simulationCluster: 0.1,
      showClusterLabels: false,
      scaleClusterLabels: true,
    } : {}),
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [isMobile]);

  // Fetch available graphs on mount
  useEffect(() => {
    fetch('/api/graphs')
      .then(r => r.json())
      .then(data => {
        if (Array.isArray(data?.graphs) && data.graphs.length > 0) {
          setAvailableGraphs(data.graphs);
          // Default to first graph alphabetically if no URL param was given
          setSelectedGraph(prev => prev || data.graphs[0].filename);
        }
      })
      .catch(() => {});
  }, []);

  // Load a new graph (called from top toolbar)
  const handleLoadGraph = useCallback(async () => {
    if (!selectedGraph || graphLoading) return;
    setGraphLoading(true);
    setStatus('Loading graph…');
    try {
      const url = '/' + selectedGraph;
      const inputs = await loadAndPrepare(url);
      fullLoadedRef.current = inputs;

      let renderInputs = inputs;

      // Auto-cluster via /api/cluster
      if (autoCluster) {
        setStatus('Running community detection…');
        try {
          const clusterRes = await fetch('/api/cluster', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              dataFile: selectedGraph,
              algorithm: 'louvain',
              params: { resolution: 1.0, seed: 42 },
            }),
          });
          const clusterData = await clusterRes.json();
          if (clusterData.assignments && Object.keys(clusterData.assignments).length > 0) {
            renderInputs = applyClusterAssignments(inputs, clusterData.assignments);
            fullLoadedRef.current = renderInputs;
          }
        } catch (e) {
          console.warn('Auto-clustering failed:', e);
        }
      }

      // On mobile with clusters, filter to largest community
      let autoClusterKey: string | null = null;
      if (isMobile && renderInputs.hasCluster && renderInputs.clusterGroups.size > 0) {
        setStatus('Filtering to largest community…');
        let maxKey = '';
        let maxCount = 0;
        for (const [key, group] of renderInputs.clusterGroups) {
          if (group.count > maxCount) { maxCount = group.count; maxKey = key; }
        }
        autoClusterKey = maxKey;
        renderInputs = await filterToCommunity(renderInputs, maxKey);
      }

      // Reset all visualization state
      setSelectedGenre(null);
      setSelectedAlbum(null);
      setSelectedEdge(null);
      setEdgeDetailA(null);
      setEdgeDetailB(null);
      setCommunityNames(new Map());
      setCommunityColors(new Map());
      setSavedCommunities([]);
      setSavedEdges([]);
      setOverlapMatches(null);
      setOverlapSummary(null);
      setGenreSearchQuery('');
      setGenreSearchHighlight(false);
      setGenreSearchHeatmap(false);
      setOpenTools(new Set());
      setClusterOn(false);
      setHomogeneityMode(false);
      setSelectedPath(null);
      setPathResults([]);
      setEdgeResults([]);
      setFocusedCluster(null);
      setFullData(null);
      setSearchQuery('');

      // Apply
      const base = buildBaseCfg(renderInputs);
      setLoaded(renderInputs);
      setCfg(base);
      if (autoClusterKey) setSelectedCluster(autoClusterKey);
      if (renderInputs.hasCluster) {
        setColorCommunities(true);
        setTimeout(() => setRecolorKick(k => k + 1), 80);
      } else {
        setColorCommunities(false);
      }
      setStatus('Ready');

      // Restore persisted community names
      try {
        const namesJson = localStorage.getItem(`traverse:communityNames:${url}`);
        if (namesJson) setCommunityNames(new Map(JSON.parse(namesJson)));
        const savedJson = localStorage.getItem(`traverse:savedCommunities:${url}`);
        if (savedJson) setSavedCommunities(JSON.parse(savedJson));
        const edgesJson = localStorage.getItem(`traverse:savedEdges:${url}`);
        if (edgesJson) setSavedEdges(JSON.parse(edgesJson));
      } catch { /* ignore corrupt localStorage */ }
    } catch (e: any) {
      console.error(e);
      setStatus(`Error: ${e?.message ?? e}`);
    } finally {
      setGraphLoading(false);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedGraph, autoCluster, graphLoading, isMobile]);

  // Initial graph load — waits for selectedGraph to be set (from URL param or manifest)
  const initialLoadDone = useRef(false);
  useEffect(() => {
    if (!selectedGraph || initialLoadDone.current) return;
    initialLoadDone.current = true;
    let alive = true;
    (async () => {
      try {
        setStatus('Loading JSON…');
        const url = '/' + selectedGraph;
        const inputs = await loadAndPrepare(url);
        if (!alive) return;

        // Always store the full dataset for community switching
        fullLoadedRef.current = inputs;

        // On mobile with clusters, auto-filter to the largest community
        let renderInputs = inputs;
        let autoClusterKey: string | null = null;
        if (isMobile && inputs.hasCluster && inputs.clusterGroups.size > 0) {
          setStatus('Filtering to largest community…');
          let maxKey = '';
          let maxCount = 0;
          for (const [key, group] of inputs.clusterGroups) {
            if (group.count > maxCount) { maxCount = group.count; maxKey = key; }
          }
          autoClusterKey = maxKey;
          renderInputs = await filterToCommunity(inputs, maxKey);
          if (!alive) return;
        }

        const base = buildBaseCfg(renderInputs);

        setLoaded(renderInputs);
        setCfg(base);
        if (autoClusterKey) setSelectedCluster(autoClusterKey);
        if (renderInputs.hasCluster) {
          setColorCommunities(true);
          setTimeout(() => setRecolorKick(k => k + 1), 80);
        }
        setStatus('Ready');

        // Restore persisted community names and saved communities
        try {
          const namesJson = localStorage.getItem(`traverse:communityNames:${url}`);
          if (namesJson) setCommunityNames(new Map(JSON.parse(namesJson)));
          const savedJson = localStorage.getItem(`traverse:savedCommunities:${url}`);
          if (savedJson) setSavedCommunities(JSON.parse(savedJson));
          const edgesJson = localStorage.getItem(`traverse:savedEdges:${url}`);
          if (edgesJson) setSavedEdges(JSON.parse(edgesJson));
        } catch { /* ignore corrupt localStorage */ }
        console.log('App: time present? points=', inputs.hasPointTime, 'links=', inputs.hasLinkTime);
      } catch (e: any) {
        console.error(e);
        setStatus(`Error: ${e?.message ?? e}`);
      }
    })();
    return () => { alive = false; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedGraph]);

  // Mobile: when user switches community, re-filter data to that community only
  useEffect(() => {
    if (!isMobile || !fullLoadedRef.current || selectedCluster == null) return;
    const full = fullLoadedRef.current;
    if (!full.hasCluster || !full.clusterGroups.has(selectedCluster)) return;

    let alive = true;
    setStatus('Switching community…');
    filterToCommunity(full, selectedCluster).then(filtered => {
      if (!alive) return;
      const base = buildBaseCfg(filtered);
      setLoaded(filtered);
      setCfg(base);
      setStatus('Ready');
    });
    return () => { alive = false; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isMobile, selectedCluster]);

  useEffect(() => {
    if (!cfg) return;
    setCfg(prev => prev ? { ...prev, showLabels: labelsOn, showDynamicLabels: labelsOn, showTopLabels: labelsOn } : prev);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [labelsOn]);

  useEffect(() => {
    if (!cfg || !loaded?.hasCluster) return;
    setCfg(prev => {
      if (!prev) return prev;
      if (clusterOn) {
        return {
          ...prev,
          simulationCluster: 0.8,
          showClusterLabels: true,
          pointColorBy: loaded.clusterField ?? undefined,
          pointColorStrategy: undefined,
        };
      }
      return {
        ...prev,
        simulationCluster: 0.1,
        showClusterLabels: false,
      };
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [clusterOn]);

  // Re-bake _pointColor into data when community colors change (custom overrides)
  useEffect(() => {
    if (!loaded?.hasCluster || !clusterColorMap || !clusterOn) return;
    const clusterField = loaded.clusterField;
    if (!clusterField) return;
    const points = loaded.raw.points;
    const links = loaded.raw.links;

    const coloredPoints = points.map(p => {
      const key = p[clusterField] != null ? String(p[clusterField]) : undefined;
      return { ...p, _pointColor: key ? (clusterColorMap.get(key) ?? UNKNOWN_COLOR) : UNKNOWN_COLOR };
    });

    const pointIncludeCols = ['label', clusterField, '_pointColor'];
    const dataConfig: any = {
      points: {
        pointIdBy: 'id',
        pointLabelBy: 'label',
        pointIncludeColumns: pointIncludeCols,
        ...(loaded.hasPointTime ? { pointTimeBy: 'first_seen_ts' } : {}),
        pointClusterBy: clusterField,
        pointColorBy: '_pointColor',
        pointColorStrategy: 'direct',
      },
      links: {
        linkSourceBy: 'source',
        linkTargetsBy: ['target'],
        linkIncludeColumns: [
          ...(links.some((l: any) => typeof l.weight === 'number') ? ['weight'] : []),
          '_color',
          ...(loaded.hasLinkTime ? ['first_seen_ts'] : []),
        ],
        ...(loaded.hasLinkTime ? { linkTimeBy: 'first_seen_ts' } : {}),
        ...(links.some((l: any) => typeof l.weight === 'number') ? {
          linkColorBy: '_color',
          linkColorStrategy: 'direct',
          linkWidthBy: 'weight',
        } : {}),
      },
      labels: { enabled: true, maxLabelCount: 10000 },
      timeline: (loaded.hasPointTime || loaded.hasLinkTime) ? { enabled: true } : undefined,
    };

    prepareCosmographData(dataConfig, coloredPoints, links).then(prepared => {
      setCfg(prev => prev ? {
        ...prev,
        points: prepared?.points,
        links: prepared?.links,
      } : prev);
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [clusterColorMap]);

  // Persist community names to localStorage
  useEffect(() => {
    if (!loaded) return;
    localStorage.setItem(`traverse:communityNames:${dataUrl}`, JSON.stringify([...communityNames.entries()]));
  }, [communityNames, dataUrl, loaded]);

  // Persist saved communities to localStorage
  useEffect(() => {
    if (!loaded) return;
    localStorage.setItem(`traverse:savedCommunities:${dataUrl}`, JSON.stringify(savedCommunities));
  }, [savedCommunities, dataUrl, loaded]);

  // Persist saved edges to localStorage
  useEffect(() => {
    if (!loaded) return;
    localStorage.setItem(`traverse:savedEdges:${dataUrl}`, JSON.stringify(savedEdges));
  }, [savedEdges, dataUrl, loaded]);

  // Sync edge width range into config when customization changes
  useEffect(() => {
    if (!cfg) return;
    setCfg(prev => prev ? { ...prev, linkWidthRange: [edgeWidthMin, edgeWidthMax] } : prev);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [edgeWidthMin, edgeWidthMax]);

  // Recompute link colors when gradient or opacity changes
  useEffect(() => {
    if (!loaded) return;
    const links = loaded.raw.links;
    const points = loaded.raw.points;
    computeLinkColors(links, edgeGradient, edgeOpacity);

    const clusterField = loaded.clusterField;
    const hasCluster = loaded.hasCluster;
    const pointIncludeCols = ['label'];
    if (hasCluster && clusterField) pointIncludeCols.push(clusterField);
    if (hasCluster) pointIncludeCols.push('_pointColor');

    // Re-bake _pointColor if clusters exist
    const prepPoints = (hasCluster && clusterField)
      ? points.map(p => {
          const key = p[clusterField] != null ? String(p[clusterField]) : undefined;
          return { ...p, _pointColor: key ? (generatedColorsRef.current.get(key) ?? UNKNOWN_COLOR) : UNKNOWN_COLOR };
        })
      : points;

    const dataConfig: any = {
      points: {
        pointIdBy: 'id',
        pointLabelBy: 'label',
        pointIncludeColumns: pointIncludeCols,
        ...(loaded.hasPointTime ? { pointTimeBy: 'first_seen_ts' } : {}),
        ...(hasCluster && clusterField ? {
          pointClusterBy: clusterField,
          pointColorBy: '_pointColor',
          pointColorStrategy: 'direct',
        } : {}),
      },
      links: {
        linkSourceBy: 'source',
        linkTargetsBy: ['target'],
        linkIncludeColumns: [
          ...(links.some((l: any) => typeof l.weight === 'number') ? ['weight'] : []),
          '_color',
          ...(loaded.hasLinkTime ? ['first_seen_ts'] : []),
        ],
        ...(loaded.hasLinkTime ? { linkTimeBy: 'first_seen_ts' } : {}),
        ...(links.some((l: any) => typeof l.weight === 'number') ? {
          linkColorBy: '_color',
          linkColorStrategy: 'direct',
          linkWidthBy: 'weight',
        } : {}),
      },
      labels: { enabled: true, maxLabelCount: 10000 },
      timeline: (loaded.hasPointTime || loaded.hasLinkTime) ? { enabled: true } : undefined,
    };

    prepareCosmographData(dataConfig, prepPoints, links).then(prepared => {
      setCfg(prev => prev ? {
        ...prev,
        points: prepared?.points,
        links: prepared?.links,
        linkColorBy: '_color',
        linkWidthBy: 'weight',
        linkWidthRange: [edgeWidthMin, edgeWidthMax],
      } : prev);
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [edgeGradient, edgeOpacity]);

  const hasTimeline = !!loaded && (loaded.hasPointTime || loaded.hasLinkTime);

  // Point IDs for the selected cluster (used for showLabelsFor)
  const selectedPointIds = useMemo(() => {
    if (!loaded || selectedCluster == null) return undefined;
    const group = loaded.clusterGroups.get(selectedCluster);
    if (!group) return undefined;
    return group.indices
      .map(i => loaded.raw.points[i]?.id)
      .filter((id: any): id is string => id != null)
      .map(String);
  }, [loaded, selectedCluster]);

  // Node labels for the selected community (for the node list panel)
  const selectedNodes = useMemo(() => {
    if (!loaded || selectedCluster == null) return null;
    const group = loaded.clusterGroups.get(selectedCluster);
    if (!group) return null;
    return group.indices
      .map(i => {
        const p = loaded.raw.points[i];
        return p ? { id: String(p.id), label: String(p.label ?? p.id) } : null;
      })
      .filter((n): n is { id: string; label: string } => n != null)
      .sort((a, b) => a.label.localeCompare(b.label));
  }, [loaded, selectedCluster]);

  const handleGenreClick = useCallback(async (name: string) => {
    console.log('[genre-detail] handleGenreClick called:', name);
    setSelectedGenre(name);
    setSelectedAlbum(null); // clear album selection
    setSelectedEdge(null); // clear edge selection
    setEdgeDetailA(null);
    setEdgeDetailB(null);

    // Cache hit → instant
    const cached = trackCache.current.get(name);
    if (cached) {
      setGenreTracks(cached.tracks);
      setGenreTotalPlays(cached.totalPlays);
      return;
    }

    setGenreTracks([]);
    setGenreTotalPlays(0);
    setLoadingTracks(true);
    try {
      const resp = await fetch('/api/genre-tracks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ genre: name }),
      });
      const result = await resp.json();
      if (resp.ok) {
        const data = { tracks: result.tracks ?? [], totalPlays: result.totalPlays ?? 0 };
        trackCache.current.set(name, data);
        setGenreTracks(data.tracks);
        setGenreTotalPlays(data.totalPlays);
      }
    } catch {
      // network error — leave empty
    } finally {
      setLoadingTracks(false);
    }
  }, []);

  // Album click handler — fetches tracks by artist from server
  const handleAlbumClick = useCallback(async (point: any) => {
    const l = loadedRef.current;
    const extLinks = l?.externalLinksMap?.get(String(point.id));
    console.log('[album-detail] handleAlbumClick:', point.label, point.artist, 'links:', extLinks?.length ?? 0);
    setSelectedAlbum({
      id: String(point.id),
      label: String(point.label),
      artist: String(point.artist ?? ''),
      genres: String(point.genres ?? ''),
      styles: String(point.styles ?? ''),
      releaseYear: point.release_year != null ? Number(point.release_year) : undefined,
      externalLinks: extLinks,
    });
    setSelectedGenre(null);
    setSelectedEdge(null);
    setEdgeDetailA(null);
    setEdgeDetailB(null);

    // Cache key uses album id
    const cacheKey = `album::${point.id}`;
    const cached = trackCache.current.get(cacheKey);
    if (cached) {
      setAlbumTracks(cached.tracks);
      setAlbumTotalPlays(cached.totalPlays);
      return;
    }

    setAlbumTracks([]);
    setAlbumTotalPlays(0);
    setLoadingAlbumTracks(true);
    try {
      const resp = await fetch('/api/album-tracks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ album: point.label, artist: point.artist }),
      });
      const result = await resp.json();
      if (resp.ok) {
        const data = { tracks: result.tracks ?? [], totalPlays: result.totalPlays ?? 0 };
        trackCache.current.set(cacheKey, data);
        setAlbumTracks(data.tracks);
        setAlbumTotalPlays(data.totalPlays);
      }
    } catch {
      // network error — leave empty
    } finally {
      setLoadingAlbumTracks(false);
    }
  }, []);

  // Compute neighbors for the selected node from graph links
  const selectedNeighbors = useMemo(() => {
    if (!loaded) return [];
    // Determine which node ID to look up neighbors for
    let id: string | null = null;
    if (selectedAlbum) {
      id = selectedAlbum.id;
    } else if (selectedGenre) {
      // Genre/style/artist nodes use the label as ID (lowercased tag)
      const pt = loaded.raw.points.find((p: any) =>
        String(p.label).toLowerCase() === selectedGenre.toLowerCase()
        || String(p.id).toLowerCase() === selectedGenre.toLowerCase()
      );
      id = pt ? String(pt.id) : null;
    }
    if (!id) return [];
    const links = loaded.raw.links;
    const idToPoint = new Map(loaded.raw.points.map((p: any) => [p.id, p]));
    const neighbors: { id: string; label: string; weight: number }[] = [];
    for (const lk of links) {
      if (lk.source === id) {
        const pt = idToPoint.get(lk.target);
        if (pt) neighbors.push({ id: lk.target, label: String(pt.label), weight: lk.weight ?? 1 });
      } else if (lk.target === id) {
        const pt = idToPoint.get(lk.source);
        if (pt) neighbors.push({ id: lk.source, label: String(pt.label), weight: lk.weight ?? 1 });
      }
    }
    neighbors.sort((a, b) => b.weight - a.weight);
    return neighbors;
  }, [selectedAlbum, selectedGenre, loaded]);

  // Edge selection: select edge from results, fetch tracks for both endpoints
  const handleEdgeSelect = useCallback(async (edge: EdgeResult | null) => {
    if (!edge || (selectedEdge?.source === edge.source && selectedEdge?.target === edge.target)) {
      setSelectedEdge(null);
      setEdgeDetailA(null);
      setEdgeDetailB(null);
      return;
    }
    setSelectedEdge(edge);
    setSelectedGenre(null); // clear node selection
    setSelectedAlbum(null);
    // Auto-open detail panel
    setOpenTools(prev => {
      if (prev.has('detail')) return prev;
      const next = new Set(prev);
      next.add('detail');
      return next;
    });

    const srcLabel = loaded?.raw.points.find((p: any) => p.id === edge.source)?.label ?? edge.source;
    const tgtLabel = loaded?.raw.points.find((p: any) => p.id === edge.target)?.label ?? edge.target;

    setEdgeDetailA({ label: String(srcLabel), tracks: [], totalPlays: 0 });
    setEdgeDetailB({ label: String(tgtLabel), tracks: [], totalPlays: 0 });
    setLoadingEdgeDetail(true);

    const fetchTracks = async (genre: string) => {
      const cached = trackCache.current.get(genre);
      if (cached) return cached;
      try {
        const resp = await fetch('/api/genre-tracks', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ genre }),
        });
        const result = await resp.json();
        const data = { tracks: result.tracks ?? [], totalPlays: result.totalPlays ?? 0 };
        trackCache.current.set(genre, data);
        return data;
      } catch {
        return { tracks: [], totalPlays: 0 };
      }
    };

    const [resA, resB] = await Promise.all([fetchTracks(String(srcLabel)), fetchTracks(String(tgtLabel))]);
    setEdgeDetailA({ label: String(srcLabel), ...resA });
    setEdgeDetailB({ label: String(tgtLabel), ...resB });
    setLoadingEdgeDetail(false);
  }, [loaded, selectedEdge]);

  const handleSaveEdge = useCallback((edge: EdgeResult) => {
    if (!loaded) return;
    const srcLabel = String(loaded.raw.points.find((p: any) => p.id === edge.source)?.label ?? edge.source);
    const tgtLabel = String(loaded.raw.points.find((p: any) => p.id === edge.target)?.label ?? edge.target);
    setSavedEdges(prev => {
      if (prev.some(s => s.source === edge.source && s.target === edge.target)) return prev;
      return [...prev, {
        source: edge.source,
        target: edge.target,
        sourceLabel: srcLabel,
        targetLabel: tgtLabel,
        score: edge.score,
        algorithm: edge.algorithm,
        savedAt: Date.now(),
      }];
    });
  }, [loaded]);

  const handleUnsaveEdge = useCallback((source: string, target: string) => {
    setSavedEdges(prev => prev.filter(s => !(s.source === source && s.target === target)));
  }, []);

  const isEdgeSaved = useCallback((source: string, target: string) =>
    savedEdges.some(s => s.source === source && s.target === target),
  [savedEdges]);

  // ── Corrections helpers ──────────────────────────────────────────
  const fetchCorrections = useCallback(async () => {
    try {
      const resp = await fetch('/api/corrections');
      if (resp.ok) {
        const data = await resp.json();
        setPendingCorrections(data);
      }
    } catch { /* ignore */ }
  }, []);

  // Fetch pending corrections on mount
  useEffect(() => { fetchCorrections(); }, [fetchCorrections]);

  const handleSubmitCorrection = useCallback(async () => {
    if (!correctionForm) return;
    try {
      const resp = await fetch('/api/corrections', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          trackId: correctionForm.trackId,
          trackName: correctionForm.trackName,
          artistName: correctionForm.artistName,
          currentGenres: correctionForm.currentGenres,
          currentStyles: correctionForm.currentStyles,
          newGenres: correctionForm.newGenres,
          newStyles: correctionForm.newStyles,
        }),
      });
      if (resp.ok) {
        setCorrectionForm(null);
        fetchCorrections();
      }
    } catch { /* ignore */ }
  }, [correctionForm, fetchCorrections]);

  const handleApproveCorrection = useCallback(async (trackId: string) => {
    try {
      const resp = await fetch('/api/corrections/approve', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ trackId }),
      });
      if (resp.ok) {
        fetchCorrections();
        // Invalidate genre-tracks cache so updated tags show on next click
        trackCache.current.clear();
      }
    } catch { /* ignore */ }
  }, [fetchCorrections]);

  const handleDenyCorrection = useCallback(async (trackId: string) => {
    try {
      const resp = await fetch('/api/corrections/deny', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ trackId }),
      });
      if (resp.ok) fetchCorrections();
    } catch { /* ignore */ }
  }, [fetchCorrections]);

  const handleApproveAll = useCallback(async () => {
    try {
      const resp = await fetch('/api/corrections/approve-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });
      if (resp.ok) {
        fetchCorrections();
        trackCache.current.clear();
      }
    } catch { /* ignore */ }
  }, [fetchCorrections]);

  // Stable refs for Cosmograph callbacks (declared early so they can be used in handlers below)
  const loadedRef = useRef(loaded);
  loadedRef.current = loaded;

  // ── Select neighbors via Cosmograph API ──────────────────────────────
  const handleSelectNeighbors = useCallback((pointIndex: number) => {
    const inst = cosmoRef.current;
    if (!inst) return;
    const indices = inst.getConnectedPointIndices(pointIndex);
    if (indices && indices.length > 0) inst.selectPoints(indices);
  }, []);

  // ── Graph right-click → find nearest node ────────────────────────────
  const onGraphContextMenu = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const inst = cosmoRef.current;
    const l = loadedRef.current;
    if (!inst || !l) return;
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    const sx = e.clientX - rect.left;
    const sy = e.clientY - rect.top;
    const spacePos = inst.screenToSpacePosition([sx, sy]);
    if (!spacePos) return;
    const positions = inst.getPointPositions();
    if (!positions || positions.length === 0) return;
    let minDist = Infinity, minIdx = -1;
    for (let i = 0; i < positions.length; i += 2) {
      const dx = positions[i] - spacePos[0];
      const dy = positions[i + 1] - spacePos[1];
      const d = dx * dx + dy * dy;
      if (d < minDist) { minDist = d; minIdx = i >> 1; }
    }
    if (minIdx < 0) return;
    const nearestScreen = inst.spaceToScreenPosition([positions[minIdx * 2], positions[minIdx * 2 + 1]]);
    if (!nearestScreen) return;
    const screenDist = Math.hypot(sx - nearestScreen[0], sy - nearestScreen[1]);
    const threshold = Math.max(inst.getPointScreenRadiusByIndex(minIdx) * 2, 16);
    if (screenDist > threshold) return;
    const point = l.raw.points[minIdx];
    if (point) setNodeContextMenu({ x: e.clientX, y: e.clientY, pointIndex: minIdx, label: String(point.label ?? point.id) });
  }, []);

  // ── Freehand lasso handlers ──────────────────────────────────────────
  const onLassoMouseDown = useCallback((e: React.MouseEvent<HTMLCanvasElement>) => {
    if (e.button !== 0) return;
    const canvas = lassoCanvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    lassoActiveRef.current = true;
    const x = e.clientX - rect.left, y = e.clientY - rect.top;
    lassoPointsRef.current = [[x, y]];
    const ctx = canvas.getContext('2d');
    if (ctx) {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.beginPath();
      ctx.moveTo(x, y);
      ctx.strokeStyle = '#00e5ff';
      ctx.lineWidth = 2;
      ctx.setLineDash([5, 3]);
    }
    e.preventDefault();
    e.stopPropagation();
  }, []);

  const onLassoMouseMove = useCallback((e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!lassoActiveRef.current) return;
    const canvas = lassoCanvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left, y = e.clientY - rect.top;
    lassoPointsRef.current.push([x, y]);
    const ctx = canvas.getContext('2d');
    if (ctx) { ctx.lineTo(x, y); ctx.stroke(); }
  }, []);

  const onLassoMouseUp = useCallback(() => {
    if (!lassoActiveRef.current) return;
    lassoActiveRef.current = false;
    const inst = cosmoRef.current;
    const canvas = lassoCanvasRef.current;
    if (canvas) { const ctx = canvas.getContext('2d'); ctx?.clearRect(0, 0, canvas.width, canvas.height); }
    const pts = lassoPointsRef.current;
    lassoPointsRef.current = [];
    if (!inst || pts.length < 3) return;
    const spacePts = pts.map(([sx, sy]) => inst.screenToSpacePosition([sx, sy])).filter((p): p is [number, number] => p != null);
    if (spacePts.length >= 3) inst.selectPointsInPolygon(spacePts);
  }, []);

  // Stable click refs for Cosmograph callbacks — the memo'd component may not
  // re-apply inline arrow functions properly on config updates.
  const handleGenreClickRef = useRef(handleGenreClick);
  handleGenreClickRef.current = handleGenreClick;
  const handleAlbumClickRef = useRef(handleAlbumClick);
  handleAlbumClickRef.current = handleAlbumClick;

  // Route click to album or genre handler based on point data
  const routeNodeClick = useCallback((point: any) => {
    if (!point) return;
    // Look up external links from the preserved map (survives DuckDB processing)
    const l = loadedRef.current;
    const links = l?.externalLinksMap?.get(String(point.id)) ?? null;
    setSelectedNodeLinks(links);
    if (point.artist != null && point.artist !== '') {
      // Album node — has an explicit artist field
      handleAlbumClickRef.current(point);
    } else if (point.genres || point.styles) {
      // Artist node — no artist field, but has genre/style tags
      // Route through album handler with the label as the artist name
      handleAlbumClickRef.current({ ...point, artist: point.label });
    } else {
      // Genre/style tag node
      handleGenreClickRef.current(String(point.label));
    }
    // Auto-open detail panel when a node is clicked
    setOpenTools(prev => {
      if (prev.has('detail')) return prev;
      const next = new Set(prev);
      next.add('detail');
      return next;
    });
  }, []);

  // Stable onClick: fires on every canvas click; index is defined when a point was clicked
  const onGraphClick = useCallback(
    (index: number | undefined, _pos: [number, number] | undefined, _event: MouseEvent) => {
      if (index == null) return;
      const l = loadedRef.current;
      if (!l) return;
      const point = l.raw.points[index];
      console.log('[node-click] onGraphClick index=', index, 'label=', point?.label);
      if (point) routeNodeClick(point);
    },
    [routeNodeClick],
  );

  // Stable onLabelClick
  const onGraphLabelClick = useCallback(
    (_index: number, id: string, _event: MouseEvent) => {
      console.log('[node-click] onLabelClick id=', id);
      const l = loadedRef.current;
      if (!l || !id) return;
      const point = l.raw.points.find((p: any) => String(p.id) === id);
      if (point) routeNodeClick(point);
    },
    [routeNodeClick],
  );

  // Build final config using pointColorByFn (the correct Cosmograph API for custom coloring).
  // activePointColorStrategy is a read-only getter, NOT a config callback.
  // Link colors are pre-computed as _color with direct strategy.
  const finalCfg = useMemo(() => {
    if (!cfg) return cfg;

    // Always apply click callbacks (link colors are pre-computed as _color)
    // Explicitly clear linkColorByFn/linkWidthByFn so deselection reverts to plasma
    const base = {
      ...cfg,
      onClick: onGraphClick,
      onLabelClick: onGraphLabelClick,
      linkColorByFn: undefined,
      linkWidthByFn: undefined,
    };

    // Path selected → highlight path nodes + edges, dim everything else
    if (selectedPath && loaded) {
      const pathSet = new Set(selectedPath.nodes);
      // Build set of link indices on the path
      const pathLinkIndices = new Set<number>();
      for (let i = 0; i < selectedPath.nodes.length - 1; i++) {
        const a = selectedPath.nodes[i], b = selectedPath.nodes[i + 1];
        const key = a < b ? `${a}→${b}` : `${b}→${a}`;
        const idx = loaded.edgeToIndex.get(key);
        if (idx != null) pathLinkIndices.add(idx);
      }
      return {
        ...base,
        pointGreyoutOpacity: 0.1,
        linkGreyoutOpacity: 0.03,
        selectedPointRingColor: '#ffffff',
        showLabels: true,
        showDynamicLabels: true,
        showTopLabels: true,
        linkColorBy: '_color',
        linkColorStrategy: 'direct',
        linkColorByFn: (_value: any, index?: number) =>
          index != null && pathLinkIndices.has(index) ? [255, 255, 255, 1.0] as [number, number, number, number] : undefined,
        linkWidthByFn: (_value: any, index?: number) =>
          index != null && pathLinkIndices.has(index) ? 3 : undefined,
        pointLabelClassName: (_text: string, _idx: number, pointId?: string) =>
          pointId != null && pathSet.has(String(pointId)) ? 'genre-label' : 'genre-label-hidden',
      };
    }

    // Edge selected → highlight the two endpoints + the edge, dim everything else
    if (selectedEdge && loaded) {
      const edgeSet = new Set([selectedEdge.source, selectedEdge.target]);
      const a = selectedEdge.source, b = selectedEdge.target;
      const key = a < b ? `${a}→${b}` : `${b}→${a}`;
      const selIdx = loaded.edgeToIndex.get(key);
      return {
        ...base,
        pointGreyoutOpacity: 0.1,
        linkGreyoutOpacity: 0.03,
        selectedPointRingColor: '#ffffff',
        showLabels: true,
        showDynamicLabels: true,
        showTopLabels: true,
        linkColorBy: '_color',
        linkColorStrategy: 'direct',
        linkColorByFn: (_value: any, index?: number) =>
          index != null && index === selIdx ? [255, 255, 255, 1.0] as [number, number, number, number] : undefined,
        linkWidthByFn: (_value: any, index?: number) =>
          index != null && index === selIdx ? 3 : undefined,
        pointLabelClassName: (_text: string, _idx: number, pointId?: string) =>
          pointId != null && edgeSet.has(String(pointId)) ? 'genre-label' : 'genre-label-hidden',
      };
    }

    // Genre search highlight → dim non-matching nodes
    if (genreSearchMatches && genreSearchMatches.size > 0 && genreSearchHighlight) {
      let colorFn: (value: any) => string;
      if (genreSearchHeatmap && genreSearchCounts) {
        let maxCount = 1;
        for (const c of genreSearchCounts.values()) {
          if (c > maxCount) maxCount = c;
        }
        const logMax = Math.log1p(maxCount);
        colorFn = (value: any) => {
          const c = genreSearchCounts.get(String(value ?? ''));
          return c ? plasmaColor(Math.log1p(c) / logMax) : DIM_COLOR;
        };
      } else {
        const MATCH_COLOR = '#89b4fa';
        colorFn = (value: any) =>
          genreSearchMatches.has(String(value ?? '')) ? MATCH_COLOR : DIM_COLOR;
      }
      return {
        ...base,
        pointColorBy: 'id',
        pointColorStrategy: undefined,
        pointColorByFn: colorFn,
        showLabels: true,
        showDynamicLabels: true,
        showTopLabels: true,
        pointLabelClassName: (_text: string, _idx: number, pointId?: string) =>
          pointId && genreSearchMatches.has(String(pointId)) ? 'genre-label' : 'genre-label-hidden',
      };
    }

    // Overlap highlight → dim unmatched nodes, same visual as community selection
    if (overlapMatches && overlapMatches.size > 0 && overlapHighlight) {
      const matchedSet = overlapMatches;
      // Heatmap sub-mode: color by play count (plasma)
      let colorFn: (value: any) => string;
      if (overlapHeatmap) {
        let maxPlays = 1;
        for (const m of matchedSet.values()) {
          if (m.playCount > maxPlays) maxPlays = m.playCount;
        }
        const logMax = Math.log1p(maxPlays);
        colorFn = (value: any) => {
          const m = matchedSet.get(String(value ?? ''));
          return m ? plasmaColor(Math.log1p(m.playCount) / logMax) : DIM_COLOR;
        };
      } else {
        // Default: matched nodes keep a highlight color, unmatched dim
        const MATCH_COLOR = '#cba6f7';
        colorFn = (value: any) =>
          matchedSet.has(String(value ?? '')) ? MATCH_COLOR : DIM_COLOR;
      }
      return {
        ...base,
        pointColorBy: 'id',
        pointColorStrategy: undefined,
        pointColorByFn: colorFn,
        showLabels: true,
        showDynamicLabels: true,
        showTopLabels: true,
        pointLabelClassName: (_text: string, _idx: number, pointId?: string) =>
          pointId && matchedSet.has(String(pointId)) ? 'genre-label' : 'genre-label-hidden',
      };
    }

    if (!loaded?.hasCluster || !loaded?.clusterField || !clusterColorMap) return base;

    // Homogeneity mode → plasma gradient by community style entropy
    // Purple (homogeneous) → yellow (diverse)
    if (homogeneityMode && homogeneityMap) {
      return {
        ...base,
        pointColorBy: loaded.clusterField,
        pointColorStrategy: undefined,
        pointColorByFn: (value: any) => {
          const t = homogeneityMap.get(String(value ?? '')) ?? 0;
          return plasmaColor(t);
        },
      };
    }

    if (selectedCluster != null) {
      // Cluster selected → highlight it, dim others, show only selected labels
      const selColor = clusterColorMap.get(selectedCluster) ?? '#ffffff';
      const selectedSet = selectedPointIds ? new Set(selectedPointIds) : new Set<string>();
      return {
        ...base,
        pointColorBy: loaded.clusterField,
        pointColorStrategy: undefined,
        pointColorByFn: (value: any) =>
          String(value ?? '') === selectedCluster ? selColor : DIM_COLOR,
        // Use pointLabelClassName to hide non-selected labels (persists through drag/zoom)
        showLabels: true,
        showDynamicLabels: true,
        showTopLabels: true,
        pointLabelClassName: (_text: string, _idx: number, pointId?: string) =>
          pointId != null && selectedSet.has(String(pointId)) ? 'genre-label' : 'genre-label-hidden',
      };
    }

    if (!colorCommunities) return base;

    // No selection, coloring on → apply our color map using same pattern as homogeneity
    return {
      ...base,
      pointColorBy: loaded.clusterField,
      pointColorStrategy: undefined,
      pointColorByFn: (value: any) =>
        clusterColorMap.get(String(value ?? '')) ?? UNKNOWN_COLOR,
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cfg, loaded, selectedCluster, selectedEdge, selectedPath, clusterColorMap, clusterOn, colorCommunities, selectedPointIds, onGraphClick, onGraphLabelClick, homogeneityMode, homogeneityMap, recolorKick, overlapMatches, overlapHighlight, overlapOpacity, overlapHeatmap, genreSearchMatches, genreSearchHighlight, genreSearchHeatmap, genreSearchCounts]);

  // Imperative selectPoints for Cosmograph's internal selection state
  useEffect(() => {
    const inst = cosmoRef.current;
    if (!inst || !loaded) return;

    // Path selection takes priority
    if (selectedPath) {
      inst.getPointIndicesByIds(selectedPath.nodes).then((indices: number[] | undefined) => {
        if (indices && indices.length > 0) inst.selectPoints(indices);
      });
      return;
    }

    // Edge selection takes priority
    if (selectedEdge) {
      const edgePointIds = [selectedEdge.source, selectedEdge.target];
      inst.getPointIndicesByIds(edgePointIds).then((indices: number[] | undefined) => {
        if (indices && indices.length > 0) inst.selectPoints(indices);
      });
      return;
    }

    // Genre search highlight → select matched nodes
    if (genreSearchMatches && genreSearchMatches.size > 0 && genreSearchHighlight) {
      const matchedIds = Array.from(genreSearchMatches);
      inst.getPointIndicesByIds(matchedIds).then((indices: number[] | undefined) => {
        if (indices && indices.length > 0) inst.selectPoints(indices);
      });
      return;
    }

    // Overlap highlight → select matched nodes so Cosmograph greys out the rest
    if (overlapMatches && overlapMatches.size > 0 && overlapHighlight) {
      const matchedIds = Array.from(overlapMatches.keys());
      inst.getPointIndicesByIds(matchedIds).then((indices: number[] | undefined) => {
        if (indices && indices.length > 0) inst.selectPoints(indices);
      });
      return;
    }

    if (selectedCluster == null) {
      inst.selectPoints(null);
      return;
    }

    const group = loaded.clusterGroups.get(selectedCluster);
    if (!group) return;

    // Use getPointIndicesByIds for robust index mapping
    const pointIds = group.indices
      .map(i => loaded.raw.points[i]?.id)
      .filter((id: any): id is string => id != null)
      .map(String);

    inst.getPointIndicesByIds(pointIds).then((indices: number[] | undefined) => {
      if (indices && indices.length > 0) inst.selectPoints(indices);
    });
  }, [loaded, selectedCluster, selectedEdge, selectedPath, overlapMatches, overlapHighlight, genreSearchMatches, genreSearchHighlight]);


  // ── Apply clustering from server ────────────────────────────────
  const handleApplyClustering = useCallback(async () => {
    if (!loaded) return;
    setClustering(true);
    setClusterError(null);

    // Build params object — only include non-empty values
    const params: Record<string, number> = {};
    for (const p of activeAlgo.params) {
      const raw = algoParams[p];
      if (raw != null && raw !== '') {
        params[p] = Number(raw);
      }
    }

    // Strip leading slash from dataUrl for the server path
    const dataFile = dataUrl.replace(/^\//, '');

    try {
      const resp = await fetch('/api/cluster', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dataFile, algorithm: selectedAlgo, params }),
      });
      const result = await resp.json();
      if (!resp.ok) {
        setClusterError(result.error ?? `Server error ${resp.status}`);
        return;
      }

      const assignments: Record<string, number> = result.assignments;
      const clusterField = 'community';

      // Update raw points with new cluster assignments
      const newPoints = loaded.raw.points.map(p => {
        const cid = assignments[p.id];
        return { ...p, [clusterField]: cid != null ? cid : undefined };
      });
      const links = loaded.raw.links;

      // Build cluster groups first so we can generate colors before data prep
      const tmp0 = new Map<string, number[]>();
      newPoints.forEach((p, i) => {
        const val = p[clusterField];
        if (val == null) return;
        const key = String(val);
        let arr = tmp0.get(key);
        if (!arr) { arr = []; tmp0.set(key, arr); }
        arr.push(i);
      });

      // Generate stable colors for each community, store in ref so
      // clusterColorMap useMemo produces the same colors later.
      generatedColorsRef.current.clear();
      for (const key of tmp0.keys()) {
        generatedColorsRef.current.set(key, randomVisibleColor());
      }

      // Bake _pointColor into every point so Cosmograph renders our exact colors
      const coloredPoints = newPoints.map(p => {
        const key = p[clusterField] != null ? String(p[clusterField]) : undefined;
        return { ...p, _pointColor: key ? (generatedColorsRef.current.get(key) ?? UNKNOWN_COLOR) : UNKNOWN_COLOR };
      });

      // Re-prepare data so Cosmograph sees updated cluster values in
      // its internal columnar store — this drives both coloring and
      // the physics clustering force.
      const dataConfig: any = {
        points: {
          pointIdBy: 'id',
          pointLabelBy: 'label',
          pointIncludeColumns: ['label', clusterField, '_pointColor'],
          ...(loaded.hasPointTime ? { pointTimeBy: 'first_seen_ts' } : {}),
          pointClusterBy: clusterField,
          pointColorBy: '_pointColor',
          pointColorStrategy: 'direct',
        },
        links: {
          linkSourceBy: 'source',
          linkTargetsBy: ['target'],
          linkIncludeColumns: [
            ...(links.some((l: any) => typeof l.weight === 'number') ? ['weight'] : []),
            '_color',
            ...(loaded.hasLinkTime ? ['first_seen_ts'] : []),
          ],
          ...(loaded.hasLinkTime ? { linkTimeBy: 'first_seen_ts' } : {}),
          ...(links.some((l: any) => typeof l.weight === 'number') ? {
            linkColorBy: '_color',
            linkColorStrategy: 'direct',
            linkWidthBy: 'weight',
          } : {}),
        },
        labels: { enabled: true, maxLabelCount: 10000 },
        timeline: (loaded.hasPointTime || loaded.hasLinkTime) ? { enabled: true } : undefined,
      };

      computeLinkColors(links, edgeGradient, edgeOpacity);
      const prepared = await prepareCosmographData(dataConfig, coloredPoints, links);

      // Use the groups we already built (tmp0)
      const sorted = [...tmp0.entries()].sort((a, b) => b[1].length - a[1].length);
      const newGroups = new Map<string, ClusterGroup>();
      for (const [key, indices] of sorted) {
        newGroups.set(key, { count: indices.length, indices });
      }

      // Update loaded state with new cluster info (store coloredPoints so
      // downstream re-prepares can re-bake _pointColor)
      setLoaded(prev => prev ? {
        ...prev,
        raw: { ...prev.raw, points: coloredPoints },
        hasCluster: true,
        clusterField,
        clusterGroups: newGroups,
      } : prev);

      // Replace config with re-prepared data — new points/links refs
      // trigger Cosmograph to re-render, re-color, and re-simulate.
      setCfg(prev => {
        if (!prev) return prev;
        return {
          ...prev,
          points: prepared?.points,
          links: prepared?.links,
          simulationCluster: 0.8,
          showClusterLabels: true,
          scaleClusterLabels: true,
          pointColorBy: clusterField,
          linkColorBy: '_color',
          linkWidthBy: 'weight',
          linkWidthRange: [edgeWidthMin, edgeWidthMax],
        };
      });

      setClusterOn(true);
      setColorCommunities(true);
      setSelectedCluster(null);

      // Delayed recolor kick — forces finalCfg to recompute pointColorByFn
      // in a SEPARATE render frame after Cosmograph finishes loading data.
      setTimeout(() => setRecolorKick(k => k + 1), 80);

      // Clear stale community names/saved/colors (colors already in ref above)
      setCommunityNames(new Map());
      setSavedCommunities([]);
      localStorage.removeItem(`traverse:communityNames:${dataUrl}`);
      localStorage.removeItem(`traverse:savedCommunities:${dataUrl}`);
    } catch (e: any) {
      setClusterError(e?.message ?? 'Network error');
    } finally {
      setClustering(false);
    }
  }, [loaded, activeAlgo, algoParams, selectedAlgo, dataUrl]);

  const getDisplayName = useCallback((value: string) => communityNames.get(value) ?? value, [communityNames]);

  const handleRenameConfirm = useCallback((clusterValue: string, newName: string) => {
    const trimmed = newName.trim();
    setCommunityNames(prev => {
      const next = new Map(prev);
      if (trimmed === '' || trimmed === clusterValue) {
        next.delete(clusterValue);
      } else {
        next.set(clusterValue, trimmed);
      }
      return next;
    });
    // Update name in saved communities too
    setSavedCommunities(prev => prev.map(s =>
      s.clusterValue === clusterValue ? { ...s, name: trimmed || clusterValue } : s
    ));
    setRenamingCluster(null);
  }, []);

  const handleSaveCommunity = useCallback((clusterValue: string) => {
    if (!loaded) return;
    const group = loaded.clusterGroups.get(clusterValue);
    setSavedCommunities(prev => {
      if (prev.some(s => s.clusterValue === clusterValue)) return prev;
      return [...prev, {
        clusterValue,
        name: communityNames.get(clusterValue) ?? clusterValue,
        nodeCount: group?.count ?? 0,
        savedAt: Date.now(),
      }];
    });
  }, [loaded, communityNames]);

  const handleUnsaveCommunity = useCallback((clusterValue: string) => {
    setSavedCommunities(prev => prev.filter(s => s.clusterValue !== clusterValue));
  }, []);

  const isSaved = useCallback((clusterValue: string) =>
    savedCommunities.some(s => s.clusterValue === clusterValue),
  [savedCommunities]);

  const handleClusterClick = useCallback((clusterValue: string) => {
    // Clear any node/edge selection so the detail panel doesn't show stale content
    setSelectedGenre(null);
    setSelectedAlbum(null);
    setSelectedEdge(null);
    setEdgeDetailA(null);
    setEdgeDetailB(null);
    // On mobile, always switch to the clicked community (no deselect — full graph would crash)
    if (isMobile) {
      setSelectedCluster(clusterValue);
    } else {
      setSelectedCluster(prev => prev === clusterValue ? null : clusterValue);
    }
  }, [isMobile]);

  // Run edge analysis on the selected community
  const handleRunEdgeAnalysis = useCallback(async (clusterValue?: string) => {
    const target = clusterValue ?? selectedCluster;
    if (!loaded || !target) return;
    const group = loaded.clusterGroups.get(target);
    if (!group) return;

    setEdgeLoading(true);
    setEdgeError(null);
    setEdgeResults([]);
    setOpenTools(prev => new Set(prev).add('edges'));

    const nodeIds = group.indices
      .map(i => loaded.raw.points[i]?.id)
      .filter((id: any): id is string => id != null)
      .map(String);

    const dataFile = dataUrl.replace(/^\//, '');

    try {
      const resp = await fetch('/api/edge-analysis', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dataFile, algorithm: edgeAlgo, nodeIds, topK: 50 }),
      });
      const result = await resp.json();
      if (!resp.ok) {
        setEdgeError(result.error ?? `Server error ${resp.status}`);
        return;
      }
      setEdgeResults(result.edges ?? []);
    } catch (e: any) {
      setEdgeError(e?.message ?? 'Network error');
    } finally {
      setEdgeLoading(false);
    }
  }, [loaded, selectedCluster, edgeAlgo, dataUrl]);

  // ── Path finding ──────────────────────────────────────────────────
  const handleFindPaths = useCallback(async () => {
    if (!loaded) return;

    let aIds: string[];
    let bIds: string[];

    if (pathMode === 'node') {
      // Node-to-node: resolve search strings to point IDs
      const aQ = pathNodeA.trim().toLowerCase();
      const bQ = pathNodeB.trim().toLowerCase();
      if (!aQ || !bQ) return;
      const findId = (q: string) => {
        const pt = loaded.raw.points.find((p: any) =>
          String(p.label ?? '').toLowerCase() === q || String(p.id ?? '').toLowerCase() === q
        );
        return pt ? String(pt.id) : null;
      };
      const aId = findId(aQ);
      const bId = findId(bQ);
      if (!aId) { setPathError(`Node not found: "${pathNodeA.trim()}"`); return; }
      if (!bId) { setPathError(`Node not found: "${pathNodeB.trim()}"`); return; }
      if (aId === bId) { setPathError('Both endpoints are the same node'); return; }
      aIds = [aId];
      bIds = [bId];
    } else {
      // Community-to-community
      if (!pathCommunityA || !pathCommunityB || pathCommunityA === pathCommunityB) return;
      const groupA = loaded.clusterGroups.get(pathCommunityA);
      const groupB = loaded.clusterGroups.get(pathCommunityB);
      if (!groupA || !groupB) return;
      aIds = groupA.indices.map(i => loaded.raw.points[i]?.id).filter((id: any): id is string => id != null).map(String);
      bIds = groupB.indices.map(i => loaded.raw.points[i]?.id).filter((id: any): id is string => id != null).map(String);
    }

    setPathLoading(true);
    setPathError(null);
    setPathResults([]);
    setSelectedPath(null);

    const dataFile = dataUrl.replace(/^\//, '');

    try {
      const resp = await fetch('/api/paths', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dataFile,
          communityAIds: aIds,
          communityBIds: bIds,
          restrictToCommunities: pathRestrict,
          maxDiverseAttempts: 200,
        }),
      });
      const result = await resp.json();
      if (!resp.ok) {
        setPathError(result.error ?? `Server error ${resp.status}`);
        return;
      }
      if (result.pathCount === 0) {
        setPathError(result.message ?? 'No path found');
      } else {
        setPathResults(result.paths ?? []);
      }
    } catch (e: any) {
      setPathError(e?.message ?? 'Network error');
    } finally {
      setPathLoading(false);
    }
  }, [loaded, pathMode, pathNodeA, pathNodeB, pathCommunityA, pathCommunityB, pathRestrict, dataUrl]);

  const handlePathSelect = useCallback((path: PathResult | null) => {
    if (!path || selectedPath === path) {
      setSelectedPath(null);
      return;
    }
    setSelectedPath(path);
    // Clear other selections
    setSelectedGenre(null);
    setSelectedAlbum(null);
    setSelectedEdge(null);
    setEdgeDetailA(null);
    setEdgeDetailB(null);
    setSelectedCluster(null);
  }, [selectedPath]);

  // Dismiss context menu on click/Escape/scroll
  useEffect(() => {
    if (!contextMenu) return;
    const dismiss = () => setContextMenu(null);
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') dismiss(); };
    document.addEventListener('click', dismiss);
    document.addEventListener('keydown', onKey);
    document.addEventListener('scroll', dismiss, true);
    return () => {
      document.removeEventListener('click', dismiss);
      document.removeEventListener('keydown', onKey);
      document.removeEventListener('scroll', dismiss, true);
    };
  }, [contextMenu]);

  // Dismiss track context menu on click/Escape/scroll
  useEffect(() => {
    if (!trackContextMenu) return;
    const dismiss = () => setTrackContextMenu(null);
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') dismiss(); };
    document.addEventListener('click', dismiss);
    document.addEventListener('keydown', onKey);
    document.addEventListener('scroll', dismiss, true);
    return () => {
      document.removeEventListener('click', dismiss);
      document.removeEventListener('keydown', onKey);
      document.removeEventListener('scroll', dismiss, true);
    };
  }, [trackContextMenu]);

  // Resize lasso canvas when it becomes active
  useEffect(() => {
    const canvas = lassoCanvasRef.current;
    if (!canvas || !lassoMode) return;
    const container = canvas.parentElement;
    if (!container) return;
    canvas.width = container.clientWidth;
    canvas.height = container.clientHeight;
  }, [lassoMode]);

  // Dismiss node context menu on click/Escape
  useEffect(() => {
    if (!nodeContextMenu) return;
    const dismiss = () => setNodeContextMenu(null);
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') dismiss(); };
    document.addEventListener('click', dismiss);
    document.addEventListener('keydown', onKey);
    return () => { document.removeEventListener('click', dismiss); document.removeEventListener('keydown', onKey); };
  }, [nodeContextMenu]);

  // Focus on a single community subgraph
  const handleFocusCommunity = useCallback(async (clusterValue: string) => {
    if (!loaded) return;

    // Save full data on first focus
    if (!fullData) {
      setFullData({ points: loaded.raw.points, links: loaded.raw.links });
    }
    const sourcePoints = fullData ? fullData.points : loaded.raw.points;
    const sourceLinks = fullData ? fullData.links : loaded.raw.links;

    // Get point IDs for the target community from the current cluster groups
    // When focusing from full data, rebuild groups from source points
    const clusterField = loaded.clusterField ?? 'community';
    const nodeSet = new Set<string>();
    sourcePoints.forEach(p => {
      if (String(p[clusterField] ?? '') === clusterValue) {
        nodeSet.add(String(p.id));
      }
    });

    const filteredPoints = sourcePoints.filter(p => nodeSet.has(String(p.id)));
    const filteredLinks = sourceLinks.filter((l: any) =>
      nodeSet.has(String(l.source)) && nodeSet.has(String(l.target))
    );

    // Re-bake _pointColor from current clusterColorMap before re-preparing
    const coloredFiltered = filteredPoints.map(p => {
      const key = p[clusterField] != null ? String(p[clusterField]) : undefined;
      return { ...p, _pointColor: key ? (generatedColorsRef.current.get(key) ?? UNKNOWN_COLOR) : UNKNOWN_COLOR };
    });

    // Re-prepare data
    const dataConfig: any = {
      points: {
        pointIdBy: 'id',
        pointLabelBy: 'label',
        pointIncludeColumns: ['label', clusterField, '_pointColor'],
        ...(loaded.hasPointTime ? { pointTimeBy: 'first_seen_ts' } : {}),
        pointClusterBy: clusterField,
        pointColorBy: '_pointColor',
        pointColorStrategy: 'direct',
      },
      links: {
        linkSourceBy: 'source',
        linkTargetsBy: ['target'],
        linkIncludeColumns: [
          ...(filteredLinks.some((l: any) => typeof l.weight === 'number') ? ['weight'] : []),
          '_color',
          ...(loaded.hasLinkTime ? ['first_seen_ts'] : []),
        ],
        ...(loaded.hasLinkTime ? { linkTimeBy: 'first_seen_ts' } : {}),
        ...(filteredLinks.some((l: any) => typeof l.weight === 'number') ? {
          linkColorBy: '_color',
          linkColorStrategy: 'direct',
          linkWidthBy: 'weight',
        } : {}),
      },
      labels: { enabled: true, maxLabelCount: 10000 },
      timeline: (loaded.hasPointTime || loaded.hasLinkTime) ? { enabled: true } : undefined,
    };

    computeLinkColors(filteredLinks, edgeGradient, edgeOpacity);
    const prepared = await prepareCosmographData(dataConfig, coloredFiltered, filteredLinks);

    // Rebuild cluster groups from filtered points
    const tmp = new Map<string, number[]>();
    filteredPoints.forEach((p, i) => {
      const val = p[clusterField];
      if (val == null) return;
      const key = String(val);
      let arr = tmp.get(key);
      if (!arr) { arr = []; tmp.set(key, arr); }
      arr.push(i);
    });
    const sorted = [...tmp.entries()].sort((a, b) => b[1].length - a[1].length);
    const newGroups = new Map<string, ClusterGroup>();
    for (const [key, indices] of sorted) {
      newGroups.set(key, { count: indices.length, indices });
    }

    setLoaded(prev => prev ? {
      ...prev,
      raw: { ...prev.raw, points: filteredPoints, links: filteredLinks },
      hasCluster: true,
      clusterField,
      clusterGroups: newGroups,
    } : prev);

    setCfg(prev => {
      if (!prev) return prev;
      return {
        ...prev,
        points: prepared?.points,
        links: prepared?.links,
        simulationCluster: 0.8,
        showClusterLabels: true,
        scaleClusterLabels: true,
        pointColorBy: clusterField,
        linkColorBy: '_color',
        linkWidthBy: 'weight',
        linkWidthRange: [edgeWidthMin, edgeWidthMax],
      };
    });

    setFocusedCluster(clusterValue);
    setSelectedCluster(null);
    setContextMenu(null);
    setTimeout(() => setRecolorKick(k => k + 1), 80);
  }, [loaded, fullData, edgeGradient, edgeOpacity]);

  // Restore the full graph from saved data
  const handleRestoreFullGraph = useCallback(async () => {
    if (!fullData || !loaded) return;

    const clusterField = loaded.clusterField ?? 'community';

    // Re-bake _pointColor from current colors
    const coloredFull = fullData.points.map(p => {
      const key = p[clusterField] != null ? String(p[clusterField]) : undefined;
      return { ...p, _pointColor: key ? (generatedColorsRef.current.get(key) ?? UNKNOWN_COLOR) : UNKNOWN_COLOR };
    });

    const dataConfig: any = {
      points: {
        pointIdBy: 'id',
        pointLabelBy: 'label',
        pointIncludeColumns: ['label', clusterField, '_pointColor'],
        ...(loaded.hasPointTime ? { pointTimeBy: 'first_seen_ts' } : {}),
        pointClusterBy: clusterField,
        pointColorBy: '_pointColor',
        pointColorStrategy: 'direct',
      },
      links: {
        linkSourceBy: 'source',
        linkTargetsBy: ['target'],
        linkIncludeColumns: [
          ...(fullData.links.some((l: any) => typeof l.weight === 'number') ? ['weight'] : []),
          '_color',
          ...(loaded.hasLinkTime ? ['first_seen_ts'] : []),
        ],
        ...(loaded.hasLinkTime ? { linkTimeBy: 'first_seen_ts' } : {}),
        ...(fullData.links.some((l: any) => typeof l.weight === 'number') ? {
          linkColorBy: '_color',
          linkColorStrategy: 'direct',
          linkWidthBy: 'weight',
        } : {}),
      },
      labels: { enabled: true, maxLabelCount: 10000 },
      timeline: (loaded.hasPointTime || loaded.hasLinkTime) ? { enabled: true } : undefined,
    };

    computeLinkColors(fullData.links, edgeGradient, edgeOpacity);
    const prepared = await prepareCosmographData(dataConfig, coloredFull, fullData.links);

    // Rebuild cluster groups
    const tmp = new Map<string, number[]>();
    fullData.points.forEach((p, i) => {
      const val = p[clusterField];
      if (val == null) return;
      const key = String(val);
      let arr = tmp.get(key);
      if (!arr) { arr = []; tmp.set(key, arr); }
      arr.push(i);
    });
    const sorted = [...tmp.entries()].sort((a, b) => b[1].length - a[1].length);
    const newGroups = new Map<string, ClusterGroup>();
    for (const [key, indices] of sorted) {
      newGroups.set(key, { count: indices.length, indices });
    }

    setLoaded(prev => prev ? {
      ...prev,
      raw: { ...prev.raw, points: fullData.points, links: fullData.links },
      hasCluster: true,
      clusterField,
      clusterGroups: newGroups,
    } : prev);

    setCfg(prev => {
      if (!prev) return prev;
      return {
        ...prev,
        points: prepared?.points,
        links: prepared?.links,
        simulationCluster: 0.8,
        showClusterLabels: true,
        scaleClusterLabels: true,
        pointColorBy: clusterField,
        linkColorBy: '_color',
        linkWidthBy: 'weight',
        linkWidthRange: [edgeWidthMin, edgeWidthMax],
      };
    });

    setFocusedCluster(null);
    setFullData(null);
    setSelectedCluster(null);
    setTimeout(() => setRecolorKick(k => k + 1), 80);
  }, [loaded, fullData, edgeGradient, edgeOpacity]);


  return (
    <div style={{ height: '100%', width: '100%', position: 'relative', '--panel-bg-opacity': panelOpacity } as CSSProperties}>
      <div style={{ position: 'absolute', inset: 0 }} onContextMenu={onGraphContextMenu}>
        {finalCfg ? (
          <CosmographProvider>
            <Cosmograph
              {...(finalCfg as any)}
              onMount={(inst: any) => { cosmoRef.current = inst; }}
            />
            {/* Freehand lasso canvas overlay */}
            {lassoMode && (
              <canvas
                ref={lassoCanvasRef}
                style={{
                  position: 'absolute',
                  inset: 0,
                  width: '100%',
                  height: '100%',
                  cursor: 'crosshair',
                  zIndex: 5,
                  pointerEvents: 'all',
                }}
                onMouseDown={onLassoMouseDown}
                onMouseMove={onLassoMouseMove}
                onMouseUp={onLassoMouseUp}
              />
            )}


            {/* Mobile community selector */}
            {isMobile && loaded?.hasCluster && loaded.clusterGroups.size > 0 && (
              <div style={{
                position: 'absolute',
                top: 40,
                left: 8,
                right: 8,
                zIndex: 20,
              }}>
                <select
                  value={selectedCluster ?? ''}
                  onChange={e => setSelectedCluster(e.target.value)}
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    background: 'rgba(30,30,30,0.92)',
                    color: '#fff',
                    border: '1px solid rgba(255,255,255,0.2)',
                    borderRadius: 8,
                    fontSize: 14,
                    fontFamily: 'system-ui',
                    appearance: 'auto',
                  }}
                >
                  {[...loaded.clusterGroups.entries()].map(([value, group]) => (
                    <option key={value} value={value}>
                      {getDisplayName(value)} ({group.count} nodes)
                    </option>
                  ))}
                </select>
              </div>
            )}

            {/* Credit overlay */}
            <div className="app-credit">
              Powered by Traverse &middot; Visualized with Cosmograph
            </div>

            <div className="app-build">v0.8.52</div>

            {hasTimeline && (
              <div
                style={{
                  position: 'absolute',
                  left: 0,
                  right: 0,
                  bottom: 60,
                  padding: '8px 12px',
                  background: 'rgba(20,20,20,0.85)',
                  borderTop: '1px solid rgba(255,255,255,0.12)',
                  zIndex: 11,
                }}
              >
                <CosmographTimeline
                  accessor="first_seen_ts"
                  useLinksData={loaded!.hasLinkTime}
                  brush={{ sticky: true }}
                />
              </div>
            )}

            {/* Floating dock toolbar */}
            <div className="toolbar">
              {showClusteringTool && (
                <button className={`toolbar-btn ${openTools.has('clustering') ? 'active' : ''}`}
                        onClick={() => toggleTool('clustering')} title="Clustering">&#8862;</button>
              )}
              <button className={`toolbar-btn ${openTools.has('communities') ? 'active' : ''}`}
                      onClick={() => toggleTool('communities')} title="Communities">&#9673;</button>
              {showEdgeTool && (
                <button className={`toolbar-btn ${openTools.has('edges') ? 'active' : ''}`}
                        onClick={() => toggleTool('edges')} title="Edge Analysis">&#10231;</button>
              )}
              <button className={`toolbar-btn ${openTools.has('detail') ? 'active' : ''}`}
                      onClick={() => toggleTool('detail')} title="Detail">&#9776;</button>
              <button className={`toolbar-btn ${openTools.has('corrections') ? 'active' : ''}`}
                      onClick={() => toggleTool('corrections')} title="Corrections">&#9998;</button>
              <button className={`toolbar-btn ${openTools.has('search') ? 'active' : ''}`}
                      onClick={() => toggleTool('search')} title="Search">&#8981;</button>
              <button className={`toolbar-btn ${openTools.has('selection') ? 'active' : ''}`}
                      onClick={() => toggleTool('selection')} title="Selection">&#11034;</button>
              <button className={`toolbar-btn ${openTools.has('paths') ? 'active' : ''}`}
                      onClick={() => toggleTool('paths')} title="Paths">&#10548;</button>
              <button className={`toolbar-btn ${openTools.has('genre-search') ? 'active' : ''}`}
                      onClick={() => toggleTool('genre-search')} title="Genre Search">&#9830;</button>
              <button className={`toolbar-btn ${openTools.has('overlap') ? 'active' : ''}`}
                      onClick={() => toggleTool('overlap')} title="My Listening">&#9835;</button>
              <div className="toolbar-separator" />
              <button className={`toolbar-btn ${openTools.has('customize') ? 'active' : ''}`}
                      onClick={() => toggleTool('customize')} title="Settings">&#9881;</button>
            </div>

            {/* Clustering algorithm panel */}
            {loaded && openTools.has('clustering') && (
              <div
                className="tool-panel cluster-algo-panel draggable-panel"
                style={{ ...clusterDrag.dragStyle, ...(clusterResize.size.h != null ? { height: clusterResize.size.h } : {}), ...(clusterResize.size.w != null ? { width: clusterResize.size.w } : {}) }}
              >
                <div
                  className="drag-handle cluster-algo-panel-toggle"
                  onMouseDown={clusterDrag.onDragStart}
                >
                  Clustering
                  <button className="genre-detail-close" onClick={() => toggleTool('clustering')}>&times;</button>
                </div>
                <div className="cluster-algo-panel-body">
                  <label>
                    Algorithm
                    <select
                      value={selectedAlgo}
                      onChange={e => { setSelectedAlgo(e.target.value); setAlgoParams({}); setClusterError(null); }}
                    >
                      {ALGORITHMS.map(a => (
                        <option key={a.value} value={a.value}>{a.label}</option>
                      ))}
                    </select>
                  </label>

                  {activeAlgo.params.map(p => (
                    <label key={p}>
                      {PARAM_LABELS[p]}
                      <input
                        type="number"
                        step={p === 'resolution' ? 0.1 : 1}
                        placeholder={p === 'resolution' ? '1.0' : ''}
                        value={algoParams[p] ?? ''}
                        onChange={e => setAlgoParams(prev => ({ ...prev, [p]: e.target.value }))}
                      />
                    </label>
                  ))}

                  <button
                    className="cluster-algo-apply-btn"
                    disabled={clustering}
                    onClick={handleApplyClustering}
                  >
                    {clustering ? 'Clustering...' : 'Apply'}
                  </button>

                  {clusterError && (
                    <div className="cluster-algo-error">{clusterError}</div>
                  )}
                </div>
                <div className="panel-resize-handle" onMouseDown={clusterResize.onVStart} />
                <div className="panel-resize-handle-h" onMouseDown={clusterResize.onHStart} />
              </div>
            )}

            {/* Community selector panel */}
            {loaded?.hasCluster && loaded.clusterGroups.size > 0 && openTools.has('communities') && (
              <div
                className="tool-panel community-panel draggable-panel"
                style={{ ...communityDrag.dragStyle, ...(communityResize.size.h != null ? { height: communityResize.size.h } : {}), ...(communityResize.size.w != null ? { width: communityResize.size.w } : {}) }}
              >
                <div
                  className="drag-handle community-panel-toggle"
                  onMouseDown={communityDrag.onDragStart}
                >
                  Communities
                  <button className="genre-detail-close" onClick={() => toggleTool('communities')}>&times;</button>
                </div>
                <div className="community-panel-body">
                  {focusedCluster != null && !isMobile && (
                    <button
                      className="community-clear-btn"
                      onClick={handleRestoreFullGraph}
                    >
                      Back to full graph
                    </button>
                  )}
                  {selectedCluster != null && !isMobile && (
                    <button
                      className="community-clear-btn"
                      onClick={() => setSelectedCluster(null)}
                    >
                      Clear selection
                    </button>
                  )}
                  <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, opacity: 0.85, margin: '4px 0 2px', cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={colorCommunities}
                      onChange={e => { setColorCommunities(e.target.checked); setTimeout(() => setRecolorKick(k => k + 1), 80); }}
                    />
                    Color communities
                  </label>
                  <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, opacity: 0.85, margin: '2px 0 6px', cursor: 'pointer' }}>
                    <input
                      type="checkbox"
                      checked={homogeneityMode}
                      onChange={e => setHomogeneityMode(e.target.checked)}
                    />
                    Style homogeneity
                    <span style={{ fontSize: 10, opacity: 0.6 }} title="Purple = uniform styles, Yellow = diverse">(purple→yellow)</span>
                  </label>
                  <ul className="community-list">
                    {[...loaded.clusterGroups.entries()].map(([value, group]) => (
                      <li
                        key={value}
                        className={`community-row ${selectedCluster === value ? 'selected' : ''}`}
                        onClick={() => handleClusterClick(value)}
                        onContextMenu={(e) => {
                          e.preventDefault();
                          setContextMenu({ x: e.clientX, y: e.clientY, clusterValue: value });
                        }}
                      >
                        <span
                          className="community-dot"
                          style={{ background: homogeneityMode
                            ? plasmaColor(homogeneityMap?.get(value) ?? 0)
                            : clusterColorMap?.get(value) ?? UNKNOWN_COLOR }}
                        />
                        {renamingCluster === value ? (
                          <input
                            className="community-rename-input"
                            autoFocus
                            value={renameValue}
                            onChange={e => setRenameValue(e.target.value)}
                            onKeyDown={e => {
                              if (e.key === 'Enter') handleRenameConfirm(value, renameValue);
                              if (e.key === 'Escape') setRenamingCluster(null);
                            }}
                            onBlur={() => handleRenameConfirm(value, renameValue)}
                            onClick={e => e.stopPropagation()}
                          />
                        ) : (
                          <span className="community-label">{getDisplayName(value)}</span>
                        )}
                        <span className="community-count">{group.count}</span>
                      </li>
                    ))}
                  </ul>
                  {savedCommunities.length > 0 && (
                    <div className="saved-section">
                      <div className="saved-section-header">
                        Saved ({savedCommunities.length})
                      </div>
                      {savedCommunities.map(s => (
                        <div
                          key={s.clusterValue}
                          className="saved-row"
                          onClick={() => handleClusterClick(s.clusterValue)}
                        >
                          <span
                            className="community-dot"
                            style={{ background: homogeneityMode
                              ? plasmaColor(homogeneityMap?.get(s.clusterValue) ?? 0)
                              : clusterColorMap?.get(s.clusterValue) ?? UNKNOWN_COLOR }}
                          />
                          <span className="community-label">{getDisplayName(s.clusterValue)}</span>
                          <span className="community-count">{s.nodeCount}</span>
                          <button
                            className="saved-row-remove"
                            title="Remove from favorites"
                            onClick={e => { e.stopPropagation(); handleUnsaveCommunity(s.clusterValue); }}
                          >
                            &times;
                          </button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                <div className="panel-resize-handle" onMouseDown={communityResize.onVStart} />
                <div className="panel-resize-handle-h" onMouseDown={communityResize.onHStart} />
              </div>
            )}

            {/* Community node list panel */}
            {selectedNodes && selectedCluster != null && loaded && (
              <div
                className="tool-panel community-node-panel draggable-panel"
                style={{
                  ...nodeListDrag.dragStyle,
                  ...(nodeListResize.size.h != null ? { height: nodeListResize.size.h } : { height: 350 }),
                  ...(nodeListResize.size.w != null ? { width: nodeListResize.size.w } : {}),
                  display: 'flex', flexDirection: 'column',
                }}
              >
                <div
                  className="drag-handle community-panel-toggle"
                  onMouseDown={nodeListDrag.onDragStart}
                >
                  {getDisplayName(selectedCluster)} ({selectedNodes.length})
                  <button className="genre-detail-close" onClick={() => setSelectedCluster(null)}>&times;</button>
                </div>
                <ul className="community-node-list-items" style={{ flex: 1, overflowY: 'auto', margin: 0, padding: '4px 0' }}>
                  {selectedNodes.map(n => (
                    <li
                      key={n.id}
                      className="community-node-item"
                      onClick={(e) => { e.stopPropagation(); handleGenreClick(n.label); }}
                      onContextMenu={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        const idx = loaded.idToIndex.get(String(n.id)) ?? -1;
                        if (idx >= 0) setNodeContextMenu({ x: e.clientX, y: e.clientY, pointIndex: idx, label: n.label });
                      }}
                    >
                      {n.label}
                    </li>
                  ))}
                </ul>
                <div className="panel-resize-handle" onMouseDown={nodeListResize.onVStart} />
                <div className="panel-resize-handle-h" onMouseDown={nodeListResize.onHStart} />
              </div>
            )}

            {/* Edge analysis panel */}
            {loaded?.hasCluster && openTools.has('edges') && (
              <div
                className="tool-panel edge-analysis-panel draggable-panel"
                style={{ ...edgeDrag.dragStyle, ...(edgeResize.size.h != null ? { height: edgeResize.size.h } : {}), ...(edgeResize.size.w != null ? { width: edgeResize.size.w } : {}) }}
              >
                <div
                  className="drag-handle edge-analysis-toggle"
                  onMouseDown={edgeDrag.onDragStart}
                >
                  Edge Analysis
                  <button className="genre-detail-close" onClick={() => toggleTool('edges')}>&times;</button>
                </div>
                <div className="edge-analysis-body">
                  <label>
                    Algorithm
                    <select
                      value={edgeAlgo}
                      onChange={e => { setEdgeAlgo(e.target.value); setEdgeError(null); }}
                    >
                      {EDGE_ALGORITHMS.map(a => (
                        <option key={a.value} value={a.value}>{a.label}</option>
                      ))}
                    </select>
                  </label>

                  <button
                    className="cluster-algo-apply-btn"
                    disabled={edgeLoading || selectedCluster == null}
                    onClick={() => handleRunEdgeAnalysis()}
                    title={selectedCluster == null ? 'Select a community first' : ''}
                  >
                    {edgeLoading ? 'Analyzing...' : selectedCluster == null ? 'Select a community' : 'Run'}
                  </button>

                  {edgeError && (
                    <div className="cluster-algo-error">{edgeError}</div>
                  )}

                  {edgeResults.length > 0 && (
                    <div className="edge-results">
                      <div className="edge-results-header">
                        Top {edgeResults.length} edges
                        {selectedEdge && (
                          <button
                            className="edge-clear-btn"
                            onClick={() => handleEdgeSelect(null)}
                          >
                            Clear
                          </button>
                        )}
                      </div>
                      <ul className="edge-results-list">
                        {edgeResults.map((e, i) => {
                          const isSelected = selectedEdge?.source === e.source && selectedEdge?.target === e.target;
                          return (
                            <li
                              key={i}
                              className={`edge-result-row ${isSelected ? 'selected' : ''}`}
                              onClick={() => handleEdgeSelect(e)}
                            >
                              <span className="edge-result-rank">#{i + 1}</span>
                              <span className="edge-result-nodes">
                                {loaded.raw.points.find((p: any) => p.id === e.source)?.label ?? e.source}
                                {' — '}
                                {loaded.raw.points.find((p: any) => p.id === e.target)?.label ?? e.target}
                              </span>
                              <span className="edge-result-score">{e.score.toFixed(4)}</span>
                              <button
                                className="saved-row-remove"
                                title={isEdgeSaved(e.source, e.target) ? 'Remove from saved' : 'Save edge'}
                                onClick={ev => {
                                  ev.stopPropagation();
                                  if (isEdgeSaved(e.source, e.target)) handleUnsaveEdge(e.source, e.target);
                                  else handleSaveEdge(e);
                                }}
                              >
                                {isEdgeSaved(e.source, e.target) ? '★' : '☆'}
                              </button>
                            </li>
                          );
                        })}
                      </ul>
                    </div>
                  )}
                  {savedEdges.length > 0 && (
                    <div className="saved-section">
                      <div className="saved-section-header">
                        Saved Edges ({savedEdges.length})
                      </div>
                      {savedEdges.map(s => {
                        const isSelected = selectedEdge?.source === s.source && selectedEdge?.target === s.target;
                        return (
                          <div
                            key={`${s.source}-${s.target}`}
                            className={`saved-row ${isSelected ? 'selected' : ''}`}
                            onClick={() => handleEdgeSelect({ source: s.source, target: s.target, score: s.score, algorithm: s.algorithm })}
                          >
                            <span className="edge-result-nodes" style={{ flex: 1 }}>
                              {s.sourceLabel} — {s.targetLabel}
                            </span>
                            <span className="edge-result-score">{s.score.toFixed(4)}</span>
                            <button
                              className="saved-row-remove"
                              title="Remove from saved"
                              onClick={ev => { ev.stopPropagation(); handleUnsaveEdge(s.source, s.target); }}
                            >
                              &times;
                            </button>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
                <div className="panel-resize-handle" onMouseDown={edgeResize.onVStart} />
                <div className="panel-resize-handle-h" onMouseDown={edgeResize.onHStart} />
              </div>
            )}

            {/* Neighborhood Paths panel */}
            {loaded?.hasCluster && loaded.clusterGroups.size > 1 && openTools.has('paths') && (
              <div
                className="tool-panel paths-panel draggable-panel"
                style={{ ...pathsDrag.dragStyle, ...(pathsResize.size.h != null ? { height: pathsResize.size.h } : {}), ...(pathsResize.size.w != null ? { width: pathsResize.size.w } : {}) }}
              >
                <div
                  className="drag-handle paths-panel-toggle"
                  onMouseDown={pathsDrag.onDragStart}
                >
                  Neighborhood Paths
                  <button className="genre-detail-close" onClick={() => toggleTool('paths')}>&times;</button>
                </div>
                <div className="paths-panel-body">
                  {/* Mode tabs */}
                  <div className="overlap-tabs" style={{ marginBottom: 6 }}>
                    <button
                      className={`overlap-tab ${pathMode === 'node' ? 'active' : ''}`}
                      onClick={() => { setPathMode('node'); setPathError(null); }}
                    >Node</button>
                    <button
                      className={`overlap-tab ${pathMode === 'community' ? 'active' : ''}`}
                      onClick={() => { setPathMode('community'); setPathError(null); }}
                    >Community</button>
                  </div>

                  {pathMode === 'node' ? (
                    <>
                      <label>
                        From
                        <input
                          type="text"
                          className="search-input"
                          placeholder="Node name..."
                          value={pathNodeA}
                          onChange={e => { setPathNodeA(e.target.value); setPathError(null); }}
                          list="path-node-a-suggestions"
                        />
                        <datalist id="path-node-a-suggestions">
                          {pathNodeA.trim().length >= 2 && loaded?.raw.points
                            .filter((p: any) => String(p.label ?? '').toLowerCase().includes(pathNodeA.trim().toLowerCase()))
                            .slice(0, 15)
                            .map((p: any) => <option key={p.id} value={p.label} />)}
                        </datalist>
                      </label>
                      <label>
                        To
                        <input
                          type="text"
                          className="search-input"
                          placeholder="Node name..."
                          value={pathNodeB}
                          onChange={e => { setPathNodeB(e.target.value); setPathError(null); }}
                          list="path-node-b-suggestions"
                        />
                        <datalist id="path-node-b-suggestions">
                          {pathNodeB.trim().length >= 2 && loaded?.raw.points
                            .filter((p: any) => String(p.label ?? '').toLowerCase().includes(pathNodeB.trim().toLowerCase()))
                            .slice(0, 15)
                            .map((p: any) => <option key={p.id} value={p.label} />)}
                        </datalist>
                      </label>
                    </>
                  ) : (
                    <>
                      <label>
                        Community A
                        <select
                          value={pathCommunityA ?? ''}
                          onChange={e => { setPathCommunityA(e.target.value || null); setPathError(null); }}
                        >
                          <option value="">Select...</option>
                          {[...loaded.clusterGroups.entries()].map(([key, group]) => (
                            <option key={key} value={key} disabled={key === pathCommunityB}>
                              {communityNames.get(key) || `Community ${key}`} ({group.indices.length})
                            </option>
                          ))}
                        </select>
                      </label>
                      <label>
                        Community B
                        <select
                          value={pathCommunityB ?? ''}
                          onChange={e => { setPathCommunityB(e.target.value || null); setPathError(null); }}
                        >
                          <option value="">Select...</option>
                          {[...loaded.clusterGroups.entries()].map(([key, group]) => (
                            <option key={key} value={key} disabled={key === pathCommunityA}>
                              {communityNames.get(key) || `Community ${key}`} ({group.indices.length})
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="paths-restrict-toggle">
                        <input
                          type="checkbox"
                          checked={pathRestrict}
                          onChange={e => setPathRestrict(e.target.checked)}
                        />
                        Restrict to selected communities only
                      </label>
                    </>
                  )}

                  <button
                    className="cluster-algo-apply-btn"
                    disabled={pathLoading || (pathMode === 'node'
                      ? !pathNodeA.trim() || !pathNodeB.trim()
                      : !pathCommunityA || !pathCommunityB || pathCommunityA === pathCommunityB)}
                    onClick={handleFindPaths}
                  >
                    {pathLoading ? 'Finding...' : 'Find Paths'}
                  </button>

                  {pathError && (
                    <div className="cluster-algo-error">{pathError}</div>
                  )}

                  {pathResults.length > 0 && (
                    <div className="paths-results">
                      <div className="edge-results-header">
                        {pathResults.length} path{pathResults.length > 1 ? 's' : ''} found
                        {selectedPath && (
                          <button className="edge-clear-btn" onClick={() => handlePathSelect(null)}>Clear</button>
                        )}
                      </div>
                      <ul className="paths-results-list">
                        {pathResults.map((p, i) => {
                          const isSelected = selectedPath === p;
                          return (
                            <li
                              key={i}
                              className={`paths-result-row ${isSelected ? 'selected' : ''}`}
                              onClick={() => handlePathSelect(p)}
                            >
                              <div className="paths-result-header">
                                <span className={`paths-type-badge ${p.pathType === 'shortest' ? 'shortest' : 'longest'}`}>
                                  {p.pathType === 'shortest' ? 'Shortest' : 'Longest'}
                                </span>
                                <span className="paths-result-stats">
                                  {p.length} hop{p.length !== 1 ? 's' : ''} &middot; wt {p.totalWeight}
                                </span>
                              </div>
                              <div className="paths-result-chain">
                                {p.labels.join(' \u2192 ')}
                              </div>
                            </li>
                          );
                        })}
                      </ul>
                    </div>
                  )}
                </div>
                <div className="panel-resize-handle" onMouseDown={pathsResize.onVStart} />
                <div className="panel-resize-handle-h" onMouseDown={pathsResize.onHStart} />
              </div>
            )}

            {/* Detail panel — shows node or edge detail */}
            {openTools.has('detail') && (
            <div className="tool-panel detail-tool-panel genre-detail-panel draggable-panel" style={{ ...genreDrag.dragStyle, ...(genreResize.size.h != null ? { height: genreResize.size.h } : {}), ...(genreResize.size.w != null ? { width: genreResize.size.w } : {}) }}>
              <div className="genre-detail-header drag-handle" onMouseDown={genreDrag.onDragStart}>
                <span className="genre-detail-title" onMouseDown={e => e.stopPropagation()}>
                  {selectedEdge
                    ? `Edge: ${loaded?.raw.points.find((p: any) => p.id === selectedEdge.source)?.label ?? selectedEdge.source} — ${loaded?.raw.points.find((p: any) => p.id === selectedEdge.target)?.label ?? selectedEdge.target}`
                    : selectedAlbum ? selectedAlbum.label
                    : selectedGenre ?? 'Detail'}
                </span>
                <button className="genre-detail-close" onClick={() => {
                  setSelectedGenre(null);
                  setSelectedAlbum(null);
                  setSelectedEdge(null);
                  setEdgeDetailA(null);
                  setEdgeDetailB(null);
                  toggleTool('detail');
                }}>
                  &times;
                </button>
              </div>
              {selectedEdge ? (
                // Edge detail: show tracks for both endpoints
                loadingEdgeDetail ? (
                  <div className="genre-detail-loading">{loadingMessage}</div>
                ) : (
                  <div className="genre-detail-body">
                    {[edgeDetailA, edgeDetailB].filter(Boolean).map((side, idx) => (
                      <div key={idx} className="edge-detail-side">
                        <div className="edge-detail-side-header">{side!.label}</div>
                        {side!.tracks.length === 0 ? (
                          <div className="genre-detail-empty">No tracks found</div>
                        ) : (
                          <>
                            <div className="genre-detail-summary">
                              {side!.tracks.length} track{side!.tracks.length !== 1 ? 's' : ''} &middot; {side!.totalPlays.toLocaleString()} plays
                            </div>
                            <ul className="genre-detail-track-list">
                              {side!.tracks.slice(0, 20).map((t: any, i: number) => (
                                <li
                                  key={i}
                                  className="genre-detail-track"
                                  onContextMenu={(e) => {
                                    e.preventDefault();
                                    const id = t.trackId || `${t.trackName}::${t.artistName}`;
                                    setTrackContextMenu({
                                      x: e.clientX, y: e.clientY,
                                      track: { trackId: id, trackName: t.trackName, artistName: t.artistName, genres: t.genres ?? '', styles: t.styles ?? '' },
                                    });
                                  }}
                                >
                                  <span className="genre-detail-rank">#{i + 1}</span>
                                  <div className="genre-detail-track-info">
                                    <div className="genre-detail-track-name">{t.trackName}</div>
                                    <div className="genre-detail-artist">{t.artistName}</div>
                                  </div>
                                  <span className="genre-detail-plays">{t.playCount}x</span>
                                  <a href={discogsTrackUrl(t.trackName, t.artistName)} target="_blank" rel="noopener noreferrer" className="ext-link track-ext-link" onClick={(e) => e.stopPropagation()}>Discogs</a>
                                </li>
                              ))}
                            </ul>
                          </>
                        )}
                      </div>
                    ))}
                  </div>
                )
              ) : selectedAlbum ? (
                // Album/Artist detail: metadata + sectioned content
                <div className="genre-detail-body">
                  {/* ── Top metadata ── */}
                  <div className="album-detail-meta">
                    {selectedAlbum.artist && selectedAlbum.artist !== selectedAlbum.label && (
                      <div className="album-detail-row">
                        <span className="album-detail-label">Artist</span>
                        <span className="album-detail-value">{selectedAlbum.artist}</span>
                      </div>
                    )}
                    {selectedAlbum.releaseYear != null && (
                      <div className="album-detail-row">
                        <span className="album-detail-label">Year</span>
                        <span className="album-detail-value">{selectedAlbum.releaseYear}</span>
                      </div>
                    )}
                    {selectedAlbum.genres && (
                      <div className="album-detail-row">
                        <span className="album-detail-label">Genres</span>
                        <span className="album-detail-value">{selectedAlbum.genres}</span>
                      </div>
                    )}
                    {selectedAlbum.styles && (
                      <div className="album-detail-row">
                        <span className="album-detail-label">Styles</span>
                        <span className="album-detail-value">{selectedAlbum.styles}</span>
                      </div>
                    )}
                    {(selectedAlbum.externalLinks ?? selectedNodeLinks) && (
                      <div className="album-detail-row">
                        <span className="album-detail-label">Links</span>
                        <span className="album-detail-value external-links">
                          {(selectedAlbum.externalLinks ?? selectedNodeLinks ?? []).map((lk, i) => (
                            <a key={i} href={lk.url} target="_blank" rel="noopener noreferrer"
                               className={`ext-link ext-link-${lk.platform}`}>
                              {lk.label}
                            </a>
                          ))}
                        </span>
                      </div>
                    )}
                    <button
                      className="cluster-algo-apply-btn"
                      style={{ marginTop: 6 }}
                      onClick={() => {
                        setCorrectionForm({
                          trackId: selectedAlbum.id,
                          trackName: selectedAlbum.label,
                          artistName: selectedAlbum.artist,
                          currentGenres: selectedAlbum.genres,
                          currentStyles: selectedAlbum.styles,
                          newGenres: selectedAlbum.genres.replace(/ \| /g, ', '),
                          newStyles: selectedAlbum.styles.replace(/ \| /g, ', '),
                        });
                      }}
                    >
                      Edit Tags
                    </button>
                    {/* Listening overlap stats */}
                    {overlapMatches?.has(selectedAlbum.id) && (() => {
                      const om = overlapMatches.get(selectedAlbum.id)!;
                      return (
                        <div className="overlap-detail-section">
                          <div className="overlap-detail-header">My Listening</div>
                          <div className="album-detail-row">
                            <span className="album-detail-label">Plays</span>
                            <span className="album-detail-value">{om.playCount} ({Math.round(om.totalMs / 60000)}m)</span>
                          </div>
                          {om.firstListenEpochMs && (
                            <div className="album-detail-row">
                              <span className="album-detail-label">First</span>
                              <span className="album-detail-value">{new Date(om.firstListenEpochMs).toLocaleDateString()}</span>
                            </div>
                          )}
                          {om.lastListenEpochMs && (
                            <div className="album-detail-row">
                              <span className="album-detail-label">Last</span>
                              <span className="album-detail-value">{new Date(om.lastListenEpochMs).toLocaleDateString()}</span>
                            </div>
                          )}
                          {om.topTracks.length > 0 && (
                            <ul className="overlap-top-tracks">
                              {om.topTracks.map((t, i) => (
                                <li key={i} className="overlap-top-track">
                                  <span className="genre-detail-rank">#{i + 1}</span>
                                  <span className="overlap-top-track-name">{t.trackName}</span>
                                  <span className="genre-detail-plays">{t.playCount}x</span>
                                </li>
                              ))}
                            </ul>
                          )}
                        </div>
                      );
                    })()}
                  </div>

                  {/* ── Tracks section ── */}
                  {loadingAlbumTracks ? (
                    <div className="genre-detail-loading">{loadingMessage}</div>
                  ) : albumTracks.length > 0 && (
                    <div className="detail-section">
                      <div className="detail-section-header">
                        Tracks &middot; {albumTracks.length} &middot; {albumTotalPlays.toLocaleString()} plays
                      </div>
                      <ul className="genre-detail-track-list">
                        {albumTracks.map((t, i) => (
                          <li
                            key={i}
                            className="genre-detail-track"
                            onContextMenu={(e) => {
                              e.preventDefault();
                              const id = t.trackId || `${t.trackName}::${t.artistName}`;
                              setTrackContextMenu({
                                x: e.clientX, y: e.clientY,
                                track: { trackId: id, trackName: t.trackName, artistName: t.artistName, genres: t.genres ?? '', styles: t.styles ?? '' },
                              });
                            }}
                          >
                            <span className="genre-detail-rank">#{i + 1}</span>
                            <div className="genre-detail-track-info">
                              <div className="genre-detail-track-name">{t.trackName}</div>
                              <div className="genre-detail-artist">{t.artistName}</div>
                            </div>
                            <span className="genre-detail-plays">{t.playCount}x</span>
                            <a href={discogsTrackUrl(t.trackName, t.artistName)} target="_blank" rel="noopener noreferrer" className="ext-link track-ext-link" onClick={(e) => e.stopPropagation()}>Discogs</a>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {/* ── Similar nodes section ── */}
                  {selectedNeighbors.length > 0 && (
                    <div className="detail-section">
                      <div className="detail-section-header">
                        Similar &middot; {selectedNeighbors.length}
                      </div>
                      <ul className="genre-detail-track-list">
                        {selectedNeighbors.slice(0, 30).map((n, i) => (
                          <li key={n.id} className="genre-detail-track" style={{ cursor: 'pointer' }}
                              onClick={() => {
                                const pt = loaded?.raw.points.find((p: any) => p.id === n.id);
                                if (pt) routeNodeClick(pt);
                              }}>
                            <span className="genre-detail-rank">#{i + 1}</span>
                            <div className="genre-detail-track-info">
                              <div className="genre-detail-track-name">{n.label}</div>
                            </div>
                            <span className="genre-detail-plays">{n.weight} shared</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              ) : !selectedGenre && communityAggregation ? (
                <div className="genre-detail-body">
                  <div className="genre-detail-summary">
                    {communityAggregation.total} node{communityAggregation.total !== 1 ? 's' : ''} in community
                  </div>

                  {/* ── Genres section ── */}
                  {communityAggregation.genres.length > 0 && (
                    <div className="detail-section">
                      <div className="detail-section-header">
                        Genres &middot; {communityAggregation.genres.length}
                      </div>
                      <ul className="genre-detail-track-list">
                        {communityAggregation.genres.map(([g, count], i) => (
                          <li key={g} className="genre-detail-track" onClick={() => handleGenreClick(g)} style={{ cursor: 'pointer' }}>
                            <span className="genre-detail-rank">#{i + 1}</span>
                            <div className="genre-detail-track-info"><div className="genre-detail-track-name">{g}</div></div>
                            <span className="genre-detail-plays">{count}</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {/* ── Styles section ── */}
                  {communityAggregation.styles.length > 0 && (
                    <div className="detail-section">
                      <div className="detail-section-header">
                        Styles &middot; {communityAggregation.styles.length}
                      </div>
                      <ul className="genre-detail-track-list">
                        {communityAggregation.styles.map(([s, count], i) => (
                          <li key={s} className="genre-detail-track">
                            <span className="genre-detail-rank">#{i + 1}</span>
                            <div className="genre-detail-track-info"><div className="genre-detail-track-name">{s}</div></div>
                            <span className="genre-detail-plays">{count}</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              ) : !selectedGenre ? (
                <div className="genre-detail-empty">No node or edge selected</div>
              ) : (
                <div className="genre-detail-body">
                  {/* ── Top metadata ── */}
                  {selectedNodeLinks && selectedNodeLinks.length > 0 && (
                    <div className="album-detail-meta" style={{ padding: '6px 12px' }}>
                      <div className="album-detail-row">
                        <span className="album-detail-label">Links</span>
                        <span className="album-detail-value external-links">
                          {selectedNodeLinks.map((lk, i) => (
                            <a key={i} href={lk.url} target="_blank" rel="noopener noreferrer"
                               className={`ext-link ext-link-${lk.platform}`}>
                              {lk.label}
                            </a>
                          ))}
                        </span>
                      </div>
                    </div>
                  )}

                  {/* ── Tracks section ── */}
                  {loadingTracks ? (
                    <div className="genre-detail-loading">{loadingMessage}</div>
                  ) : genreTracks.length > 0 && (
                    <div className="detail-section">
                      <div className="detail-section-header">
                        Tracks &middot; {genreTracks.length} &middot; {genreTotalPlays.toLocaleString()} plays
                      </div>
                      <ul className="genre-detail-track-list">
                        {genreTracks.map((t, i) => (
                          <li
                            key={i}
                            className="genre-detail-track"
                            onContextMenu={(e) => {
                              e.preventDefault();
                              const id = t.trackId || `${t.trackName}::${t.artistName}`;
                              setTrackContextMenu({
                                x: e.clientX, y: e.clientY,
                                track: { trackId: id, trackName: t.trackName, artistName: t.artistName, genres: t.genres ?? '', styles: t.styles ?? '' },
                              });
                            }}
                          >
                            <span className="genre-detail-rank">#{i + 1}</span>
                            <div className="genre-detail-track-info">
                              <div className="genre-detail-track-name">{t.trackName}</div>
                              <div className="genre-detail-artist">{t.artistName}</div>
                            </div>
                            <span className="genre-detail-plays">{t.playCount}x</span>
                            <a href={discogsTrackUrl(t.trackName, t.artistName)} target="_blank" rel="noopener noreferrer" className="ext-link track-ext-link" onClick={(e) => e.stopPropagation()}>Discogs</a>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {/* ── Artists section (extracted from tracks) ── */}
                  {genreTracks.length > 0 && (() => {
                    const artistCounts = new Map<string, number>();
                    for (const t of genreTracks) {
                      const a = t.artistName || '';
                      if (a) artistCounts.set(a, (artistCounts.get(a) ?? 0) + t.playCount);
                    }
                    const sorted = [...artistCounts.entries()].sort((a, b) => b[1] - a[1]);
                    if (sorted.length === 0) return null;
                    return (
                      <div className="detail-section">
                        <div className="detail-section-header">
                          Artists &middot; {sorted.length}
                        </div>
                        <ul className="genre-detail-track-list">
                          {sorted.slice(0, 20).map(([artist, plays], i) => (
                            <li key={artist} className="genre-detail-track">
                              <span className="genre-detail-rank">#{i + 1}</span>
                              <div className="genre-detail-track-info">
                                <div className="genre-detail-track-name">{artist}</div>
                              </div>
                              <span className="genre-detail-plays">{plays}x</span>
                              <a href={`https://www.discogs.com/search/?q=${encodeURIComponent(artist)}&type=artist`} target="_blank" rel="noopener noreferrer" className="ext-link track-ext-link" onClick={(e) => e.stopPropagation()}>Discogs</a>
                            </li>
                          ))}
                        </ul>
                      </div>
                    );
                  })()}

                  {/* ── Similar nodes section ── */}
                  {selectedNeighbors.length > 0 && (
                    <div className="detail-section">
                      <div className="detail-section-header">
                        Similar &middot; {selectedNeighbors.length}
                      </div>
                      <ul className="genre-detail-track-list">
                        {selectedNeighbors.slice(0, 30).map((n, i) => (
                          <li key={n.id} className="genre-detail-track" style={{ cursor: 'pointer' }}
                              onClick={() => {
                                const pt = loaded?.raw.points.find((p: any) => p.id === n.id);
                                if (pt) routeNodeClick(pt);
                              }}>
                            <span className="genre-detail-rank">#{i + 1}</span>
                            <div className="genre-detail-track-info">
                              <div className="genre-detail-track-name">{n.label}</div>
                            </div>
                            <span className="genre-detail-plays">{n.weight} shared</span>
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {/* Empty state when nothing at all */}
                  {!loadingTracks && genreTracks.length === 0 && selectedNeighbors.length === 0 && (
                    <div className="genre-detail-empty">No data found for this node.</div>
                  )}
                </div>
              )}
              <div className="panel-resize-handle" onMouseDown={genreResize.onVStart} />
              <div className="panel-resize-handle-h" onMouseDown={genreResize.onHStart} />
            </div>
            )}
            {/* Right-click context menu */}
            {contextMenu && (
              <div
                className="context-menu"
                style={{ top: contextMenu.y, left: contextMenu.x }}
                onClick={(e) => e.stopPropagation()}
              >
                <div
                  className="context-menu-item"
                  onClick={() => {
                    setRenameValue(communityNames.get(contextMenu.clusterValue) ?? contextMenu.clusterValue);
                    setRenamingCluster(contextMenu.clusterValue);
                    setContextMenu(null);
                  }}
                >
                  Rename community
                </div>
                <div
                  className="context-menu-item"
                  onClick={() => {
                    if (isSaved(contextMenu.clusterValue)) {
                      handleUnsaveCommunity(contextMenu.clusterValue);
                    } else {
                      handleSaveCommunity(contextMenu.clusterValue);
                    }
                    setContextMenu(null);
                  }}
                >
                  {isSaved(contextMenu.clusterValue) ? 'Remove from favorites' : 'Save to favorites'}
                </div>
                <div
                  className="context-menu-item"
                  onClick={() => {
                    handleRunEdgeAnalysis(contextMenu.clusterValue);
                    setContextMenu(null);
                  }}
                >
                  Analyze edges
                </div>
                <div
                  className="context-menu-item"
                  onClick={() => {
                    const target = contextMenu.clusterValue;
                    setColorPickerTarget(target);
                    setContextMenu(null);
                    setTimeout(() => colorInputRef.current?.click(), 30);
                  }}
                >
                  {communityColors.has(contextMenu.clusterValue) ? '🎨 Change color' : 'Change color'}
                </div>
                {communityColors.has(contextMenu.clusterValue) && (
                  <div
                    className="context-menu-item"
                    onClick={() => {
                      setCommunityColors(prev => {
                        const next = new Map(prev);
                        next.delete(contextMenu.clusterValue);
                        return next;
                      });
                      setContextMenu(null);
                    }}
                  >
                    Reset color
                  </div>
                )}
                <div className="context-menu-separator" />
                <div
                  className="context-menu-item"
                  onClick={() => handleFocusCommunity(contextMenu.clusterValue)}
                >
                  Focus on this community
                </div>
              </div>
            )}
            {/* Track right-click context menu */}
            {trackContextMenu && (
              <div
                className="context-menu"
                style={{ top: trackContextMenu.y, left: trackContextMenu.x }}
                onClick={(e) => e.stopPropagation()}
              >
                <div
                  className="context-menu-item"
                  onClick={() => {
                    const t = trackContextMenu.track;
                    setCorrectionForm({
                      trackId: t.trackId,
                      trackName: t.trackName,
                      artistName: t.artistName,
                      currentGenres: t.genres,
                      currentStyles: t.styles,
                      newGenres: t.genres.replace(/ \| /g, ', '),
                      newStyles: t.styles.replace(/ \| /g, ', '),
                    });
                    setTrackContextMenu(null);
                  }}
                >
                  Edit genres / styles
                </div>
              </div>
            )}

            {/* Correction form overlay */}
            {correctionForm && (
              <div className="correction-form draggable-panel" style={correctionFormDrag.dragStyle}>
                <div className="correction-form-header drag-handle" onMouseDown={correctionFormDrag.onDragStart}>
                  <span>Edit Tags</span>
                  <button className="genre-detail-close" onClick={() => setCorrectionForm(null)}>&times;</button>
                </div>
                <div className="correction-form-field">
                  <span className="correction-form-label">Track</span>
                  <span className="correction-form-readonly">{correctionForm.trackName}</span>
                </div>
                <div className="correction-form-field">
                  <span className="correction-form-label">Artist</span>
                  <span className="correction-form-readonly">{correctionForm.artistName}</span>
                </div>
                <div className="correction-form-field">
                  <span className="correction-form-label">Current Genres</span>
                  <span className="correction-form-readonly">{correctionForm.currentGenres || '(none)'}</span>
                </div>
                <div className="correction-form-field">
                  <span className="correction-form-label">Current Styles</span>
                  <span className="correction-form-readonly">{correctionForm.currentStyles || '(none)'}</span>
                </div>
                <div className="correction-form-field">
                  <label className="correction-form-label">New Genres</label>
                  <input
                    type="text"
                    value={correctionForm.newGenres}
                    onChange={(e) => setCorrectionForm(prev => prev ? { ...prev, newGenres: e.target.value } : prev)}
                    placeholder="Comma-separated genres"
                  />
                </div>
                <div className="correction-form-field">
                  <label className="correction-form-label">New Styles</label>
                  <input
                    type="text"
                    value={correctionForm.newStyles}
                    onChange={(e) => setCorrectionForm(prev => prev ? { ...prev, newStyles: e.target.value } : prev)}
                    placeholder="Comma-separated styles"
                  />
                </div>
                <div className="correction-form-actions">
                  <button className="cluster-algo-apply-btn" onClick={handleSubmitCorrection}>Submit</button>
                  <button className="community-clear-btn" onClick={() => setCorrectionForm(null)}>Cancel</button>
                </div>
              </div>
            )}

            {/* Corrections review panel */}
            {openTools.has('corrections') && (
            <div
              className="tool-panel corrections-panel draggable-panel"
              style={{ ...correctionsDrag.dragStyle, ...(correctionsResize.size.h != null ? { height: correctionsResize.size.h } : {}), ...(correctionsResize.size.w != null ? { width: correctionsResize.size.w } : {}) }}
            >
              <div
                className="drag-handle corrections-panel-toggle"
                onMouseDown={correctionsDrag.onDragStart}
              >
                Corrections ({pendingCorrections.length})
                <button className="genre-detail-close" onClick={() => toggleTool('corrections')}>&times;</button>
              </div>
              <div className="corrections-panel-body">
                {pendingCorrections.length === 0 ? (
                  <div className="genre-detail-empty">No pending corrections</div>
                ) : (
                  <>
                    {pendingCorrections.map((c: any) => (
                      <div key={c.track_id} className="correction-row">
                        <div className="correction-row-info">
                          <div className="correction-row-track">{c.track_name}</div>
                          <div className="correction-row-artist">{c.artist_name}</div>
                          {c.current_genres !== c.new_genres && (
                            <div className="correction-tag-diff">
                              <span className="correction-tag-old">{c.current_genres || '(none)'}</span>
                              <span className="correction-tag-arrow">&rarr;</span>
                              <span className="correction-tag-new">{c.new_genres || '(none)'}</span>
                            </div>
                          )}
                          {c.current_styles !== c.new_styles && (
                            <div className="correction-tag-diff">
                              <span className="correction-tag-old">{c.current_styles || '(none)'}</span>
                              <span className="correction-tag-arrow">&rarr;</span>
                              <span className="correction-tag-new">{c.new_styles || '(none)'}</span>
                            </div>
                          )}
                        </div>
                        <div className="correction-row-actions">
                          <button
                            className="correction-approve-btn"
                            title="Approve"
                            onClick={() => handleApproveCorrection(c.track_id)}
                          >&#10003;</button>
                          <button
                            className="correction-deny-btn"
                            title="Deny"
                            onClick={() => handleDenyCorrection(c.track_id)}
                          >&times;</button>
                        </div>
                      </div>
                    ))}
                    <button className="cluster-algo-apply-btn" style={{ margin: '8px 0' }} onClick={handleApproveAll}>
                      Approve All ({pendingCorrections.length})
                    </button>
                  </>
                )}
              </div>
              <div className="panel-resize-handle" onMouseDown={correctionsResize.onVStart} />
              <div className="panel-resize-handle-h" onMouseDown={correctionsResize.onHStart} />
            </div>
            )}

            {/* Search panel */}
            {openTools.has('search') && (
            <div
              className="tool-panel search-panel draggable-panel"
              style={{ ...searchDrag.dragStyle, ...(searchResize.size.h != null ? { height: searchResize.size.h } : {}), ...(searchResize.size.w != null ? { width: searchResize.size.w } : {}) }}
            >
              <div
                className="drag-handle search-panel-toggle"
                onMouseDown={searchDrag.onDragStart}
              >
                Search
                <button className="genre-detail-close" onClick={() => toggleTool('search')}>&times;</button>
              </div>
              <div className="search-panel-body">
                <input
                  type="text"
                  className="search-input"
                  placeholder="Search nodes..."
                  value={searchQuery}
                  onChange={e => setSearchQuery(e.target.value)}
                  autoFocus
                />
                {searchQuery.trim() && (
                  <div className="search-results-info">
                    {searchResults.length === 0
                      ? 'No matches'
                      : `${searchResults.length}${searchResults.length >= 50 ? '+' : ''} matches`}
                  </div>
                )}
                <ul className="search-results-list">
                  {searchResults.map((p: any) => (
                    <li
                      key={p.id}
                      className="search-result-row"
                      onClick={() => {
                        const inst = cosmoRef.current;
                        if (inst && loaded) {
                          inst.getPointIndicesByIds([String(p.id)]).then((indices: number[] | undefined) => {
                            if (indices && indices.length > 0) {
                              inst.selectPoints(indices);
                              inst.zoomToPoint(indices[0], 500, 1.5);
                            }
                          });
                        }
                      }}
                    >
                      <span className="search-result-label">{p.label || p.id}</span>
                      {p.artist && <span className="search-result-artist">{p.artist}</span>}
                    </li>
                  ))}
                </ul>
              </div>
              <div className="panel-resize-handle" onMouseDown={searchResize.onVStart} />
              <div className="panel-resize-handle-h" onMouseDown={searchResize.onHStart} />
            </div>
            )}

            {/* Genre search panel */}
            {openTools.has('genre-search') && (
            <div
              className="tool-panel genre-search-panel draggable-panel"
              style={{ ...genreSearchDrag.dragStyle, ...(genreSearchResize.size.h != null ? { height: genreSearchResize.size.h } : {}), ...(genreSearchResize.size.w != null ? { width: genreSearchResize.size.w } : {}) }}
            >
              <div
                className="drag-handle search-panel-toggle"
                onMouseDown={genreSearchDrag.onDragStart}
              >
                Genre Search
                <button className="genre-detail-close" onClick={() => toggleTool('genre-search')}>&times;</button>
              </div>
              <div className="search-panel-body">
                <input
                  type="text"
                  className="search-input"
                  placeholder="Search genres/styles... (comma for OR)"
                  value={genreSearchQuery}
                  onChange={e => setGenreSearchQuery(e.target.value)}
                  list="genre-search-suggestions"
                  autoFocus
                />
                <datalist id="genre-search-suggestions">
                  {allTags
                    .filter(t => genreSearchQuery.trim() && t.includes(genreSearchQuery.trim().toLowerCase().split(',').pop()?.trim() ?? ''))
                    .slice(0, 20)
                    .map(t => <option key={t} value={t} />)}
                </datalist>

                {genreSearchQuery.trim() && (
                  <div className="search-results-info">
                    {genreSearchMatches
                      ? `${genreSearchMatches.size} node${genreSearchMatches.size !== 1 ? 's' : ''} match`
                      : 'No matches'}
                    {loaded && genreSearchMatches && (
                      <span style={{ opacity: 0.6 }}>
                        {' '}({((genreSearchMatches.size / loaded.raw.points.length) * 100).toFixed(1)}%)
                      </span>
                    )}
                  </div>
                )}

                {genreSearchMatches && genreSearchMatches.size > 0 && (
                  <div className="overlap-controls">
                    <button
                      className={`overlap-mode-btn ${genreSearchHighlight ? 'active' : ''}`}
                      onClick={() => setGenreSearchHighlight(h => !h)}
                      title="Highlight matched nodes, dim the rest"
                    >Highlight</button>
                    <button
                      className={`overlap-mode-btn ${genreSearchHeatmap ? 'active' : ''}`}
                      onClick={() => {
                        setGenreSearchHeatmap(h => !h);
                        if (!genreSearchHighlight) setGenreSearchHighlight(true);
                      }}
                      title="Color by number of matching tags (plasma)"
                    >Heatmap</button>
                    <button
                      className="overlap-clear-btn"
                      onClick={() => {
                        setGenreSearchQuery('');
                        setGenreSearchHighlight(false);
                        setGenreSearchHeatmap(false);
                      }}
                    >Clear</button>
                  </div>
                )}

                {genreSearchSorted.length > 0 && (
                  <ul className="search-results-list">
                    {genreSearchSorted
                      .slice(0, 100)
                      .map(({ point: p, pct, matchCount, totalCount }) => (
                        <li
                          key={p.id}
                          className="search-result-row"
                          onClick={() => {
                            const inst = cosmoRef.current;
                            if (inst && loaded) {
                              inst.getPointIndicesByIds([String(p.id)]).then((indices: number[] | undefined) => {
                                if (indices && indices.length > 0) {
                                  inst.selectPoints(indices);
                                  inst.zoomToPoint(indices[0], 500, 1.5);
                                }
                              });
                            }
                          }}
                        >
                          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 8 }}>
                            <span className="search-result-label">{p.label || p.id}</span>
                            <span style={{ fontSize: '0.75em', opacity: 0.9, whiteSpace: 'nowrap', color: pct >= 0.5 ? '#a6e3a1' : pct >= 0.2 ? '#f9e2af' : '#f38ba8' }}>
                              {(pct * 100).toFixed(0)}%
                              <span style={{ opacity: 0.5 }}> ({matchCount}/{totalCount})</span>
                            </span>
                          </div>
                          {p.genres && <span className="search-result-artist" style={{ fontSize: '0.75em', opacity: 0.5 }}>{p.genres}</span>}
                        </li>
                      ))}
                    {genreSearchSorted.length > 100 && (
                      <li className="search-result-row" style={{ opacity: 0.5, pointerEvents: 'none' }}>
                        …and {genreSearchSorted.length - 100} more
                      </li>
                    )}
                  </ul>
                )}
              </div>
              <div className="panel-resize-handle" onMouseDown={genreSearchResize.onVStart} />
              <div className="panel-resize-handle-h" onMouseDown={genreSearchResize.onHStart} />
            </div>
            )}

            {/* User listening overlap panel */}
            {openTools.has('overlap') && (
            <div
              className="tool-panel overlap-panel draggable-panel"
              style={{ ...overlapDrag.dragStyle, ...(overlapResize.size.h != null ? { height: overlapResize.size.h } : {}), ...(overlapResize.size.w != null ? { width: overlapResize.size.w } : {}) }}
            >
              <div
                className="drag-handle overlap-panel-toggle"
                onMouseDown={overlapDrag.onDragStart}
              >
                My Listening
                <button className="genre-detail-close" onClick={() => toggleTool('overlap')}>&times;</button>
              </div>
              <div className="overlap-panel-body">
                {/* Input mode tabs */}
                <div className="overlap-tabs">
                  <button
                    className={`overlap-tab ${overlapInputMode === 'path' ? 'active' : ''}`}
                    onClick={() => setOverlapInputMode('path')}
                  >Path</button>
                  <button
                    className={`overlap-tab ${overlapInputMode === 'upload' ? 'active' : ''}`}
                    onClick={() => setOverlapInputMode('upload')}
                  >Upload</button>
                </div>

                {overlapInputMode === 'path' ? (
                  <div className="overlap-input-section">
                    <input
                      type="text"
                      className="search-input"
                      placeholder="Spotify history directory..."
                      value={overlapPath}
                      onChange={e => setOverlapPath(e.target.value)}
                    />
                    <button
                      className="cluster-algo-apply-btn"
                      disabled={overlapLoading || !overlapPath.trim()}
                      onClick={async () => {
                        setOverlapLoading(true);
                        setOverlapError(null);
                        try {
                          const dataFile = dataUrl.replace(/^\//, '');
                          const resp = await fetch('/api/user-overlap', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ dataFile, historyDir: overlapPath.trim() }),
                          });
                          const result = await resp.json();
                          if (!resp.ok) { setOverlapError(result.error ?? `Error ${resp.status}`); return; }
                          const map = new Map<string, OverlapMatch>();
                          for (const m of result.matches) map.set(m.nodeId, m);
                          setOverlapMatches(map);
                          setOverlapSummary({ matched: result.totalMatched, total: result.totalNodes });
                          // Inject first_seen_ts for timeline
                          if (loaded) {
                            for (const pt of loaded.raw.points) {
                              const match = map.get(pt.id);
                              if (match && match.firstListenEpochMs) {
                                (pt as any).first_seen_ts = match.firstListenEpochMs;
                              } else {
                                delete (pt as any).first_seen_ts;
                              }
                            }
                          }
                        } catch (err: any) {
                          setOverlapError(err.message ?? 'Request failed');
                        } finally {
                          setOverlapLoading(false);
                        }
                      }}
                    >
                      {overlapLoading ? 'Loading...' : 'Load'}
                    </button>
                  </div>
                ) : (
                  <div className="overlap-input-section">
                    <input
                      type="file"
                      multiple
                      accept=".json"
                      className="overlap-file-input"
                      onChange={async (e) => {
                        const files = e.target.files;
                        if (!files || files.length === 0) return;
                        setOverlapLoading(true);
                        setOverlapError(null);
                        try {
                          const allRecords: any[] = [];
                          for (const file of Array.from(files)) {
                            const text = await file.text();
                            const parsed = JSON.parse(text);
                            if (Array.isArray(parsed)) allRecords.push(...parsed);
                          }
                          const dataFile = dataUrl.replace(/^\//, '');
                          const resp = await fetch('/api/user-overlap', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ dataFile, historyData: allRecords }),
                          });
                          const result = await resp.json();
                          if (!resp.ok) { setOverlapError(result.error ?? `Error ${resp.status}`); return; }
                          const map = new Map<string, OverlapMatch>();
                          for (const m of result.matches) map.set(m.nodeId, m);
                          setOverlapMatches(map);
                          setOverlapSummary({ matched: result.totalMatched, total: result.totalNodes });
                          // Inject first_seen_ts for timeline
                          if (loaded) {
                            for (const pt of loaded.raw.points) {
                              const match = map.get(pt.id);
                              if (match && match.firstListenEpochMs) {
                                (pt as any).first_seen_ts = match.firstListenEpochMs;
                              } else {
                                delete (pt as any).first_seen_ts;
                              }
                            }
                          }
                        } catch (err: any) {
                          setOverlapError(err.message ?? 'Failed to parse files');
                        } finally {
                          setOverlapLoading(false);
                        }
                      }}
                    />
                  </div>
                )}

                {overlapError && (
                  <div className="cluster-algo-error">{overlapError}</div>
                )}

                {/* Results */}
                {overlapSummary && overlapMatches && (
                  <div className="overlap-results">
                    <div className="overlap-summary">
                      {overlapSummary.matched} of {overlapSummary.total} nodes match
                      ({overlapSummary.total > 0 ? ((overlapSummary.matched / overlapSummary.total) * 100).toFixed(1) : '0'}%)
                    </div>

                    {overlapMatches.size > 0 ? (
                      <>
                        <div className="overlap-controls">
                          <button
                            className={`overlap-mode-btn ${overlapHighlight ? 'active' : ''}`}
                            onClick={() => setOverlapHighlight(h => !h)}
                            title="Highlight matched nodes, dim the rest"
                          >Highlight</button>
                          <button
                            className={`overlap-mode-btn ${overlapHeatmap ? 'active' : ''}`}
                            onClick={() => {
                              setOverlapHeatmap(h => !h);
                              if (!overlapHighlight) setOverlapHighlight(true);
                            }}
                            title="Color nodes by listen intensity (plasma)"
                          >Heatmap</button>
                          <button
                            className="overlap-clear-btn"
                            onClick={() => {
                              setOverlapMatches(null);
                              setOverlapSummary(null);
                              setOverlapError(null);
                              setOverlapHighlight(false);
                              setOverlapHeatmap(false);
                              // Remove injected first_seen_ts
                              if (loaded) {
                                for (const pt of loaded.raw.points) {
                                  delete (pt as any).first_seen_ts;
                                }
                              }
                            }}
                          >Clear</button>
                        </div>

                        <ul className="overlap-match-list">
                          {Array.from(overlapMatches.values())
                            .sort((a, b) => b.playCount - a.playCount)
                            .map(m => (
                              <li
                                key={m.nodeId}
                                className="overlap-match-row"
                                onClick={() => {
                                  const inst = cosmoRef.current;
                                  if (inst && loaded) {
                                    inst.getPointIndicesByIds([String(m.nodeId)]).then((indices: number[] | undefined) => {
                                      if (indices && indices.length > 0) {
                                        inst.selectPoints(indices);
                                        inst.zoomToPoint(indices[0], 500, 1.5);
                                      }
                                    });
                                    // Open detail panel and select the node
                                    const pt = loaded.raw.points.find((p: any) => p.id === m.nodeId);
                                    if (pt) routeNodeClick(pt);
                                  }
                                }}
                              >
                                <div className="overlap-match-info">
                                  <div className="overlap-match-name">{loaded?.raw.points.find((p: any) => p.id === m.nodeId)?.label ?? m.nodeId}</div>
                                  <div className="overlap-match-stats">
                                    {m.playCount} plays &middot; {Math.round(m.totalMs / 60000)}m
                                  </div>
                                </div>
                                <span className="overlap-match-count">{m.playCount}x</span>
                              </li>
                            ))}
                        </ul>
                      </>
                    ) : (
                      <div className="overlap-empty">No matches found. Your listening history doesn't overlap with this graph.</div>
                    )}
                  </div>
                )}
              </div>
              <div className="panel-resize-handle" onMouseDown={overlapResize.onVStart} />
              <div className="panel-resize-handle-h" onMouseDown={overlapResize.onHStart} />
            </div>
            )}

            {/* Selection tools panel */}
            {openTools.has('selection') && (
            <div
              className="tool-panel selection-panel draggable-panel"
              style={selectionDrag.dragStyle}
            >
              <div
                className="drag-handle selection-panel-toggle"
                onMouseDown={selectionDrag.onDragStart}
              >
                Selection Tools
                <button className="genre-detail-close" onClick={() => toggleTool('selection')}>&times;</button>
              </div>
              <div className="selection-panel-body" style={{ flexDirection: 'column', gap: 6 }}>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <div className="cosmograph-selection-btn-wrap">
                    <CosmographButtonRectangularSelection />
                  </div>
                  <span style={{ fontSize: 11, color: '#aaa' }}>Rectangle</span>
                </div>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <div className="cosmograph-selection-btn-wrap">
                    <CosmographButtonPolygonalSelection />
                  </div>
                  <span style={{ fontSize: 11, color: '#aaa' }}>Polygon</span>
                </div>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <button
                    className={`toolbar-btn ${lassoMode ? 'active' : ''}`}
                    style={{ width: 32, height: 32 }}
                    title="Freehand lasso selection"
                    onClick={() => setLassoMode(m => !m)}
                  >
                    &#8738;
                  </button>
                  <span style={{ fontSize: 11, color: lassoMode ? '#00e5ff' : '#aaa' }}>
                    {lassoMode ? 'Lasso active — draw' : 'Lasso'}
                  </span>
                </div>
              </div>
            </div>
            )}

            {/* Customization panel */}
            {openTools.has('customize') && (
            <div
              className="tool-panel customize-panel draggable-panel"
              style={{ ...customizeDrag.dragStyle, ...(customizeResize.size.h != null ? { height: customizeResize.size.h } : {}), ...(customizeResize.size.w != null ? { width: customizeResize.size.w } : {}) }}
            >
              <div
                className="drag-handle customize-panel-toggle"
                onMouseDown={customizeDrag.onDragStart}
              >
                Customize
                <button className="genre-detail-close" onClick={() => toggleTool('customize')}>&times;</button>
              </div>
              <div className="customize-panel-body">
                <label style={{ flexDirection: 'row', alignItems: 'center', gap: 6 }}>
                  <input
                    type="checkbox"
                    checked={labelsOn}
                    onChange={(e) => setLabelsOn(e.currentTarget.checked)}
                  />
                  Show labels
                </label>
                {loaded?.hasCluster && (
                  <label style={{ flexDirection: 'row', alignItems: 'center', gap: 6 }}>
                    <input
                      type="checkbox"
                      checked={clusterOn}
                      onChange={(e) => setClusterOn(e.currentTarget.checked)}
                    />
                    Cluster by {loaded.clusterField}
                  </label>
                )}

                <label>
                  Edge Gradient
                  <select
                    value={edgeGradient}
                    onChange={e => setEdgeGradient(e.target.value)}
                  >
                    {Object.keys(GRADIENT_PRESETS).map(name => (
                      <option key={name} value={name}>{name.charAt(0).toUpperCase() + name.slice(1)}</option>
                    ))}
                  </select>
                </label>

                <label>
                  Edge Opacity ({edgeOpacity.toFixed(1)})
                  <input
                    type="range"
                    min={0.1}
                    max={1.0}
                    step={0.1}
                    value={edgeOpacity}
                    onChange={e => setEdgeOpacity(Number(e.target.value))}
                  />
                </label>

                <div className="customize-width-row">
                  <label>
                    Width Min
                    <input
                      type="number"
                      min={0.1}
                      max={10}
                      step={0.1}
                      value={edgeWidthMin}
                      onChange={e => setEdgeWidthMin(Number(e.target.value))}
                    />
                  </label>
                  <label>
                    Width Max
                    <input
                      type="number"
                      min={0.1}
                      max={10}
                      step={0.1}
                      value={edgeWidthMax}
                      onChange={e => setEdgeWidthMax(Number(e.target.value))}
                    />
                  </label>
                </div>

                <label>
                  Panel Glass ({Math.round(panelOpacity * 100)}%)
                  <input
                    type="range"
                    min={0.15}
                    max={1.0}
                    step={0.05}
                    value={panelOpacity}
                    onChange={e => setPanelOpacity(Number(e.target.value))}
                  />
                </label>

                <div style={{ borderTop: '1px solid rgba(255,255,255,0.1)', margin: '8px 0 4px', paddingTop: 8 }}>
                  <div style={{ fontSize: 11, color: '#888', marginBottom: 6 }}>Dock Tools</div>
                  <label style={{ flexDirection: 'row', alignItems: 'center', gap: 6 }}>
                    <input
                      type="checkbox"
                      checked={showClusteringTool}
                      onChange={e => setShowClusteringTool(e.currentTarget.checked)}
                    />
                    Clustering
                  </label>
                  <label style={{ flexDirection: 'row', alignItems: 'center', gap: 6 }}>
                    <input
                      type="checkbox"
                      checked={showEdgeTool}
                      onChange={e => setShowEdgeTool(e.currentTarget.checked)}
                    />
                    Edge Analysis
                  </label>
                </div>
              </div>
              <div className="panel-resize-handle" onMouseDown={customizeResize.onVStart} />
              <div className="panel-resize-handle-h" onMouseDown={customizeResize.onHStart} />
            </div>
            )}

            {/* Global activity spinner */}
            {(clustering || edgeLoading || loadingTracks || loadingEdgeDetail || loadingAlbumTracks || overlapLoading) && (
              <div className="global-spinner">
                <div className="global-spinner-ring" />
                <span className="global-spinner-label">
                  {clustering ? 'Clustering...' : edgeLoading ? 'Analyzing edges...' : overlapLoading ? 'Scanning history...' : loadingMessage}
                </span>
              </div>
            )}

            {/* Hidden color input for community color picker */}
            <input
              ref={colorInputRef}
              type="color"
              style={{ position: 'fixed', opacity: 0, pointerEvents: 'none', width: 0, height: 0 }}
              value={colorPickerTarget
                ? (communityColors.get(colorPickerTarget) ?? (clusterColorMap?.get(colorPickerTarget) ?? '#00e5ff'))
                : '#00e5ff'}
              onChange={e => {
                if (colorPickerTarget) {
                  setCommunityColors(prev => new Map(prev).set(colorPickerTarget, e.target.value));
                }
              }}
              onBlur={() => setColorPickerTarget(null)}
            />

            {/* Node right-click context menu */}
            {nodeContextMenu && (
              <div
                className="context-menu"
                style={{ top: nodeContextMenu.y, left: nodeContextMenu.x }}
                onClick={(e) => e.stopPropagation()}
              >
                <div style={{ padding: '4px 14px 6px', fontSize: 11, color: '#888', borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
                  {nodeContextMenu.label}
                </div>
                <div
                  className="context-menu-item"
                  onClick={() => {
                    handleSelectNeighbors(nodeContextMenu.pointIndex);
                    setNodeContextMenu(null);
                  }}
                >
                  Select neighbors
                </div>
              </div>
            )}
          </CosmographProvider>
        ) : (
          <div style={{ position: 'absolute', top: 12, left: 12, padding: '6px 12px', background: 'rgba(30,30,30,0.88)', border: '1px solid rgba(255,255,255,0.15)', borderRadius: 8, fontFamily: 'system-ui', fontSize: 12, color: '#ccc' }}>Loading…</div>
        )}
      </div>

      {/* Top toolbar - graph selector (always visible) */}
      {availableGraphs.length > 0 && (
        <div className="top-toolbar">
          <select
            className="graph-select"
            value={selectedGraph}
            onChange={e => setSelectedGraph(e.target.value)}
          >
            {availableGraphs.map(g => (
              <option key={g.filename} value={g.filename}>
                {g.label} ({g.sizeMB} MB)
              </option>
            ))}
          </select>
          <button
            className="graph-load-btn"
            onClick={handleLoadGraph}
            disabled={graphLoading}
          >
            {graphLoading ? 'Loading…' : 'Load'}
          </button>
          <label className="auto-cluster-toggle" title="Auto-run Louvain clustering on load">
            <input type="checkbox" checked={autoCluster} onChange={e => setAutoCluster(e.target.checked)} />
            Auto-cluster
          </label>
        </div>
      )}
    </div>
  );
}
