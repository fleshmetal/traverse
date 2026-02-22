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
import { loadAndPrepare, computeLinkColors, GRADIENT_PRESETS, type LoadedInputs, type ClusterGroup } from './DataLoader';

interface SavedCommunity {
  clusterValue: string;
  name: string;
  nodeCount: number;
  savedAt: number;
}

const CLUSTER_PALETTE = [
  '#00e5ff', '#ff4081', '#76ff03', '#ffea00', '#e040fb', '#ff6e40',
];

/** Generate a random HSL color with good saturation and visibility. */
function randomVisibleColor(): string {
  const h = Math.floor(Math.random() * 360);
  const s = 65 + Math.floor(Math.random() * 25);
  const l = 50 + Math.floor(Math.random() * 20);
  return `hsl(${h}, ${s}%, ${l}%)`;
}

const UNKNOWN_COLOR = 'rgba(205, 207, 213, 0.9)';
const DIM_COLOR = 'rgba(138, 138, 138, 0.2)';

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
  const dataUrl = useMemo(
    () => new URLSearchParams(window.location.search).get('data') ?? '/cosmo_genres_timeline.json',
    []
  );

  const [loaded, setLoaded] = useState<LoadedInputs | null>(null);
  const [cfg, setCfg] = useState<CosmographConfig | null>(null);
  const [labelsOn, setLabelsOn] = useState(true);
  const [clusterOn, setClusterOn] = useState(false);
  const [status, setStatus] = useState('Loading…');

  const cosmoRef = useRef<CosmographInstance | null>(null);
  const [selectedCluster, setSelectedCluster] = useState<string | null>(null);

  // Toolbar state
  type ToolId = 'clustering' | 'communities' | 'edges' | 'detail' | 'corrections' | 'customize' | 'search' | 'selection';
  const [toolbarOpen, setToolbarOpen] = useState(true);
  const [openTools, setOpenTools] = useState<Set<ToolId>>(new Set(['detail']));

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

  // Drag handles for movable panels
  const clusterDrag = useDrag();
  const communityDrag = useDrag();
  const genreDrag = useDrag();
  const edgeDrag = useDrag();
  const correctionsDrag = useDrag();
  const correctionFormDrag = useDrag();
  const customizeDrag = useDrag();
  const searchDrag = useDrag();
  const selectionDrag = useDrag();

  // Panel resize handles (vertical + horizontal)
  const clusterResize = usePanelResize();
  const communityResize = usePanelResize();
  const edgeResize = usePanelResize();
  const genreResize = usePanelResize();
  const correctionsResize = usePanelResize();
  const customizeResize = usePanelResize();
  const searchResize = usePanelResize();

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

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        setStatus('Loading JSON…');
        const inputs = await loadAndPrepare(dataUrl);
        if (!alive) return;

        const base: CosmographConfig = {
          ...(inputs.prepared?.cosmographConfig ?? {}),
          points: inputs.prepared?.points,
          links:  inputs.prepared?.links,
          // Label config — flat keys, not nested (Cosmograph ignores nested `labels` obj)
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
          curvedLinks: true,
          curvedLinkSegments: 19,
          curvedLinkWeight: 0.8,
          curvedLinkControlPointDistance: 0.5,
          // Link weight visual encoding (colors pre-computed as _color)
          linkColorBy: '_color',
          linkWidthBy: 'weight',
          linkWidthRange: [edgeWidthMin, edgeWidthMax],
          ...(inputs.hasCluster ? {
            simulationCluster: 0.1,
            showClusterLabels: false,
            scaleClusterLabels: true,
          } : {}),
        };

        setLoaded(inputs);
        setCfg(base);
        setStatus('Ready');

        // Restore persisted community names and saved communities
        try {
          const namesJson = localStorage.getItem(`traverse:communityNames:${dataUrl}`);
          if (namesJson) setCommunityNames(new Map(JSON.parse(namesJson)));
          const savedJson = localStorage.getItem(`traverse:savedCommunities:${dataUrl}`);
          if (savedJson) setSavedCommunities(JSON.parse(savedJson));
          const edgesJson = localStorage.getItem(`traverse:savedEdges:${dataUrl}`);
          if (edgesJson) setSavedEdges(JSON.parse(edgesJson));
        } catch { /* ignore corrupt localStorage */ }
        console.log('App: time present? points=', inputs.hasPointTime, 'links=', inputs.hasLinkTime);
      } catch (e: any) {
        console.error(e);
        setStatus(`Error: ${e?.message ?? e}`);
      }
    })();
    return () => { alive = false; };
  }, [dataUrl]);

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

    const dataConfig: any = {
      points: {
        pointIdBy: 'id',
        pointLabelBy: 'label',
        pointIncludeColumns: pointIncludeCols,
        ...(loaded.hasPointTime ? { pointTimeBy: 'first_seen_ts' } : {}),
        ...(hasCluster && clusterField ? {
          pointClusterBy: clusterField,
          pointColorBy: clusterField,
          pointColorStrategy: 'categorical',
          pointColorPalette: CLUSTER_PALETTE,
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

    prepareCosmographData(dataConfig, points, links).then(prepared => {
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
    console.log('[album-detail] handleAlbumClick called:', point.label, point.artist);
    setSelectedAlbum({
      id: String(point.id),
      label: String(point.label),
      artist: String(point.artist ?? ''),
      genres: String(point.genres ?? ''),
      styles: String(point.styles ?? ''),
      releaseYear: point.release_year != null ? Number(point.release_year) : undefined,
      externalLinks: Array.isArray(point.external_links) ? point.external_links : undefined,
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
    const links = Array.isArray(point.external_links) ? point.external_links : null;
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
    const base = {
      ...cfg,
      onClick: onGraphClick,
      onLabelClick: onGraphLabelClick,
    };

    // Edge selected → highlight the two endpoints, dim everything else
    if (selectedEdge && loaded) {
      const edgeNodeIds = [selectedEdge.source, selectedEdge.target];
      return {
        ...base,
        pointGreyoutOpacity: 0.1,
        linkGreyoutOpacity: 0.08,
        selectedPointRingColor: '#ffffff',
        showLabels: true,
        showDynamicLabels: false,
        showTopLabels: false,
        showLabelsFor: edgeNodeIds,
      };
    }

    if (!loaded?.hasCluster || !loaded?.clusterField || !clusterColorMap) return base;

    if (selectedCluster != null) {
      // Cluster selected → highlight it, dim others, show only selected labels
      const selColor = clusterColorMap.get(selectedCluster) ?? '#ffffff';
      return {
        ...base,
        pointColorBy: loaded.clusterField,
        pointColorStrategy: undefined,
        pointColorByFn: (value: any) =>
          String(value ?? '') === selectedCluster ? selColor : DIM_COLOR,
        // Disable auto label placement, show only selected cluster's labels
        showLabels: true,
        showDynamicLabels: false,
        showTopLabels: false,
        showLabelsFor: selectedPointIds,
      };
    }

    if (!clusterOn) return base; // clustering toggled off, no custom coloring

    // No selection, clustering on → use our own color map so panel dots match exactly
    return {
      ...base,
      pointColorStrategy: undefined,
      pointColorByFn: (value: any) =>
        clusterColorMap.get(String(value ?? '')) ?? UNKNOWN_COLOR,
    };
  }, [cfg, loaded, selectedCluster, selectedEdge, clusterColorMap, clusterOn, selectedPointIds, onGraphClick, onGraphLabelClick]);

  // Imperative selectPoints for Cosmograph's internal selection state
  useEffect(() => {
    const inst = cosmoRef.current;
    if (!inst || !loaded) return;

    // Edge selection takes priority
    if (selectedEdge) {
      const edgePointIds = [selectedEdge.source, selectedEdge.target];
      inst.getPointIndicesByIds(edgePointIds).then((indices: number[] | undefined) => {
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
  }, [loaded, selectedCluster, selectedEdge]);

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

      // Re-prepare data so Cosmograph sees updated cluster values in
      // its internal columnar store — this drives both coloring and
      // the physics clustering force.
      const dataConfig: any = {
        points: {
          pointIdBy: 'id',
          pointLabelBy: 'label',
          pointIncludeColumns: ['label', clusterField],
          ...(loaded.hasPointTime ? { pointTimeBy: 'first_seen_ts' } : {}),
          pointClusterBy: clusterField,
          pointColorBy: clusterField,
          pointColorStrategy: 'categorical',
          pointColorPalette: CLUSTER_PALETTE,
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
      const prepared = await prepareCosmographData(dataConfig, newPoints, links);

      // Rebuild cluster groups
      const tmp = new Map<string, number[]>();
      newPoints.forEach((p, i) => {
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

      // Update loaded state with new cluster info
      setLoaded(prev => prev ? {
        ...prev,
        raw: { ...prev.raw, points: newPoints },
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
      setSelectedCluster(null);

      // Clear stale community names/saved/colors from previous clustering
      generatedColorsRef.current.clear();
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
    setSelectedCluster(prev => prev === clusterValue ? null : clusterValue);
  }, []);

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

    // Re-prepare data
    const dataConfig: any = {
      points: {
        pointIdBy: 'id',
        pointLabelBy: 'label',
        pointIncludeColumns: ['label', clusterField],
        ...(loaded.hasPointTime ? { pointTimeBy: 'first_seen_ts' } : {}),
        pointClusterBy: clusterField,
        pointColorBy: clusterField,
        pointColorStrategy: 'categorical',
        pointColorPalette: CLUSTER_PALETTE,
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
    const prepared = await prepareCosmographData(dataConfig, filteredPoints, filteredLinks);

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
  }, [loaded, fullData, edgeGradient, edgeOpacity]);

  // Restore the full graph from saved data
  const handleRestoreFullGraph = useCallback(async () => {
    if (!fullData || !loaded) return;

    const clusterField = loaded.clusterField ?? 'community';

    const dataConfig: any = {
      points: {
        pointIdBy: 'id',
        pointLabelBy: 'label',
        pointIncludeColumns: ['label', clusterField],
        ...(loaded.hasPointTime ? { pointTimeBy: 'first_seen_ts' } : {}),
        pointClusterBy: clusterField,
        pointColorBy: clusterField,
        pointColorStrategy: 'categorical',
        pointColorPalette: CLUSTER_PALETTE,
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
    const prepared = await prepareCosmographData(dataConfig, fullData.points, fullData.links);

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
  }, [loaded, fullData, edgeGradient, edgeOpacity]);


  return (
    <div style={{ height: '100%', width: '100%', position: 'relative' }}>
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

            {/* Title overlay */}
            <div className="app-title">
              {loaded?.meta?.title ?? 'Fuzz Archives'}
            </div>

            {/* Credit overlay */}
            <div className="app-credit">
              Powered by Traverse &middot; Visualized with Cosmograph
            </div>

            {hasTimeline && (
              <div
                style={{
                  position: 'absolute',
                  left: 0,
                  right: 0,
                  bottom: 0,
                  padding: '8px 12px',
                  background: 'rgba(20,20,20,0.85)',
                  borderTop: '1px solid rgba(255,255,255,0.12)'
                }}
              >
                <CosmographTimeline
                  accessor="first_seen_ts"
                  useLinksData={loaded!.hasLinkTime}
                  brush={{ sticky: true }}
                />
              </div>
            )}
            {!hasTimeline && (
              <div
                style={{
                  position: 'absolute',
                  left: 56,
                  bottom: 12,
                  padding: '4px 8px',
                  background: 'rgba(30,30,30,0.85)',
                  border: '1px solid rgba(255,255,255,0.12)',
                  borderRadius: 6,
                  fontFamily: 'system-ui',
                  fontSize: 12,
                  color: '#888'
                }}
              >
                Timeline: disabled (no time fields detected)
              </div>
            )}

            {/* Left toolbar */}
            <div className={`toolbar ${toolbarOpen ? '' : 'toolbar-collapsed'}`}>
              {!toolbarOpen && (
                <button className="toolbar-expand-btn" onClick={() => setToolbarOpen(true)} title="Expand toolbar">&#9776;</button>
              )}
              {toolbarOpen && (
                <>
                  <button className="toolbar-collapse-btn" onClick={() => setToolbarOpen(false)} title="Collapse toolbar">&#9776;</button>
                  <button className={`toolbar-btn ${openTools.has('clustering') ? 'active' : ''}`}
                          onClick={() => toggleTool('clustering')} title="Clustering">&#8862;</button>
                  <button className={`toolbar-btn ${openTools.has('communities') ? 'active' : ''}`}
                          onClick={() => toggleTool('communities')} title="Communities">&#9673;</button>
                  <button className={`toolbar-btn ${openTools.has('edges') ? 'active' : ''}`}
                          onClick={() => toggleTool('edges')} title="Edge Analysis">&#10231;</button>
                  <button className={`toolbar-btn ${openTools.has('detail') ? 'active' : ''}`}
                          onClick={() => toggleTool('detail')} title="Detail">&#9776;</button>
                  <button className={`toolbar-btn ${openTools.has('corrections') ? 'active' : ''}`}
                          onClick={() => toggleTool('corrections')} title="Corrections">&#9998;</button>
                  <button className={`toolbar-btn ${openTools.has('search') ? 'active' : ''}`}
                          onClick={() => toggleTool('search')} title="Search">&#8981;</button>
                  <button className={`toolbar-btn ${openTools.has('selection') ? 'active' : ''}`}
                          onClick={() => toggleTool('selection')} title="Selection">&#11034;</button>
                  <button className={`toolbar-btn ${openTools.has('customize') ? 'active' : ''}`}
                          style={{ marginTop: 'auto' }}
                          onClick={() => toggleTool('customize')} title="Customize">&#9881;</button>
                </>
              )}
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
                  {focusedCluster != null && (
                    <button
                      className="community-clear-btn"
                      onClick={handleRestoreFullGraph}
                    >
                      Back to full graph
                    </button>
                  )}
                  {selectedCluster != null && (
                    <button
                      className="community-clear-btn"
                      onClick={() => setSelectedCluster(null)}
                    >
                      Clear selection
                    </button>
                  )}
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
                          style={{ background: clusterColorMap?.get(value) ?? UNKNOWN_COLOR }}
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
                  {selectedNodes && (
                    <div className="community-node-list">
                      <div className="community-node-list-header">
                        Nodes in community {getDisplayName(selectedCluster!)} ({selectedNodes.length})
                      </div>
                      <ul className="community-node-list-items">
                        {selectedNodes.map(n => (
                          <li
                            key={n.id}
                            className="community-node-item"
                            onClick={(e) => { e.stopPropagation(); handleGenreClick(n.label); }}
                            onContextMenu={(e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              const idx = loaded?.idToIndex.get(String(n.id)) ?? -1;
                              if (idx >= 0) setNodeContextMenu({ x: e.clientX, y: e.clientY, pointIndex: idx, label: n.label });
                            }}
                          >
                            {n.label}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
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
                            style={{ background: clusterColorMap?.get(s.clusterValue) ?? UNKNOWN_COLOR }}
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

            {/* Detail panel — shows node or edge detail */}
            {openTools.has('detail') && (
            <div className="tool-panel detail-tool-panel genre-detail-panel draggable-panel" style={{ ...genreDrag.dragStyle, ...(genreResize.size.h != null ? { height: genreResize.size.h } : {}), ...(genreResize.size.w != null ? { width: genreResize.size.w } : {}) }}>
              <div className="genre-detail-header drag-handle" onMouseDown={genreDrag.onDragStart}>
                <span className="genre-detail-title">
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
              </div>
              <div className="panel-resize-handle" onMouseDown={customizeResize.onVStart} />
              <div className="panel-resize-handle-h" onMouseDown={customizeResize.onHStart} />
            </div>
            )}

            {/* Global activity spinner */}
            {(clustering || edgeLoading || loadingTracks || loadingEdgeDetail || loadingAlbumTracks) && (
              <div className="global-spinner">
                <div className="global-spinner-ring" />
                <span className="global-spinner-label">
                  {clustering ? 'Clustering...' : edgeLoading ? 'Analyzing edges...' : loadingMessage}
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
          <div style={{ padding: 16, fontFamily: 'system-ui' }}>Loading…</div>
        )}
      </div>
    </div>
  );
}
