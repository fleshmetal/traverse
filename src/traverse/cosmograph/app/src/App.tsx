import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import './labels.css';
import {
  Cosmograph,
  CosmographProvider,
  CosmographTimeline,
  prepareCosmographData,
  type CosmographConfig,
  type Cosmograph as CosmographInstance
} from '@cosmograph/react';
import { loadAndPrepare, type LoadedInputs, type ClusterGroup } from './DataLoader';

interface SavedCommunity {
  clusterValue: string;
  name: string;
  nodeCount: number;
  savedAt: number;
}

const CLUSTER_PALETTE = [
  '#00e5ff', '#ff4081', '#76ff03', '#ffea00', '#e040fb', '#ff6e40',
];

const UNKNOWN_COLOR = 'rgba(205, 207, 213, 0.9)';
const DIM_COLOR = 'rgba(138, 138, 138, 0.2)';

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

// ── Draggable panel hook ─────────────────────────────────────────────
function useDrag() {
  const [pos, setPos] = useState<{ x: number; y: number } | null>(null);
  const dragRef = useRef<{ offsetX: number; offsetY: number } | null>(null);

  const onDragStart = useCallback((e: React.MouseEvent) => {
    if (e.button !== 0) return;
    const tag = (e.target as HTMLElement).tagName;
    if (tag === 'INPUT' || tag === 'SELECT' || tag === 'BUTTON') return;
    const panel = (e.currentTarget as HTMLElement).closest('.draggable-panel') as HTMLElement;
    if (!panel) return;
    const rect = panel.getBoundingClientRect();
    dragRef.current = { offsetX: e.clientX - rect.left, offsetY: e.clientY - rect.top };
    const onMove = (ev: MouseEvent) => {
      if (!dragRef.current) return;
      setPos({ x: ev.clientX - dragRef.current.offsetX, y: ev.clientY - dragRef.current.offsetY });
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
    ? { position: 'fixed', top: pos.y, left: pos.x, right: 'auto', bottom: 'auto' }
    : undefined;

  return { dragStyle, onDragStart };
}

export default function App() {
  const dataUrl = useMemo(
    () => new URLSearchParams(window.location.search).get('data') ?? '/cosmo_genres_timeline.json',
    []
  );

  const [loaded, setLoaded] = useState<LoadedInputs | null>(null);
  const [cfg, setCfg] = useState<CosmographConfig | null>(null);
  const [labelsOn, setLabelsOn] = useState(true);
  const [clusterOn, setClusterOn] = useState(true);
  const [status, setStatus] = useState('Loading…');

  const cosmoRef = useRef<CosmographInstance | null>(null);
  const [selectedCluster, setSelectedCluster] = useState<string | null>(null);
  const [panelOpen, setPanelOpen] = useState(true);

  // Context menu + community focus state
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; clusterValue: string } | null>(null);
  const [fullData, setFullData] = useState<{ points: any[]; links: any[] } | null>(null);
  const [focusedCluster, setFocusedCluster] = useState<string | null>(null);

  // Genre detail panel state
  const [selectedGenre, setSelectedGenre] = useState<string | null>(null);
  const [genreTracks, setGenreTracks] = useState<any[]>([]);
  const [genreTotalPlays, setGenreTotalPlays] = useState(0);
  const [loadingTracks, setLoadingTracks] = useState(false);

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
  const [edgePanelOpen, setEdgePanelOpen] = useState(false);

  // Edge selection + saved edges
  const [selectedEdge, setSelectedEdge] = useState<EdgeResult | null>(null);
  const [savedEdges, setSavedEdges] = useState<SavedEdge[]>([]);
  const [edgeDetailA, setEdgeDetailA] = useState<{ label: string; tracks: any[]; totalPlays: number } | null>(null);
  const [edgeDetailB, setEdgeDetailB] = useState<{ label: string; tracks: any[]; totalPlays: number } | null>(null);
  const [loadingEdgeDetail, setLoadingEdgeDetail] = useState(false);

  // Clustering algorithm panel state
  const [clusterPanelOpen, setClusterPanelOpen] = useState(false);
  const [selectedAlgo, setSelectedAlgo] = useState<string>(ALGORITHMS[0].value);
  const [algoParams, setAlgoParams] = useState<Record<string, string>>({});
  const [clustering, setClustering] = useState(false);
  const [clusterError, setClusterError] = useState<string | null>(null);

  const activeAlgo = useMemo(() => ALGORITHMS.find(a => a.value === selectedAlgo)!, [selectedAlgo]);

  // Drag handles for movable panels
  const clusterDrag = useDrag();
  const communityDrag = useDrag();
  const genreDrag = useDrag();
  const edgeDrag = useDrag();

  // Build a stable color map: cluster value → palette color (frequency-desc order,
  // matching Cosmograph's internal categorical assignment)
  const clusterColorMap = useMemo(() => {
    if (!loaded?.hasCluster) return null;
    const map = new Map<string, string>();
    [...loaded.clusterGroups.entries()].forEach(([key], i) => {
      map.set(key, CLUSTER_PALETTE[i % CLUSTER_PALETTE.length]);
    });
    return map;
  }, [loaded]);

  // Link weight → plasma colormap (purple → pink → orange → yellow)
  // Uses rank-based normalization so colors distribute uniformly
  // regardless of weight skew.
  const linkColorByFn = useMemo(() => {
    if (!loaded) return undefined;
    // Sort all weights for percentile lookup
    const sorted = loaded.raw.links
      .map((l: any) => typeof l.weight === 'number' ? l.weight : 0)
      .sort((a: number, b: number) => a - b);
    const n = sorted.length || 1;
    // Plasma stops: [t, r, g, b]
    const stops: [number, number, number, number][] = [
      [0.00,  13,   8, 135],  // deep purple
      [0.33, 126,   3, 168],  // purple-magenta
      [0.66, 204,  71, 120],  // pink
      [1.00, 248, 149,  64],  // orange
    ];
    return (value: any) => {
      const w = typeof value === 'number' ? value : 0;
      // Binary search → percentile rank in [0, 1]
      let lo = 0, hi = sorted.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (sorted[mid] < w) lo = mid + 1; else hi = mid;
      }
      const t = lo / n;
      // Lerp between surrounding plasma stops
      let i = 0;
      while (i < stops.length - 2 && stops[i + 1][0] < t) i++;
      const [t0, r0, g0, b0] = stops[i];
      const [t1, r1, g1, b1] = stops[i + 1];
      const f = (t - t0) / (t1 - t0 || 1);
      const r = Math.round(r0 + (r1 - r0) * f);
      const g = Math.round(g0 + (g1 - g0) * f);
      const b = Math.round(b0 + (b1 - b0) * f);
      return `rgba(${r},${g},${b},0.8)`;
    };
  }, [loaded]);

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
          // Link weight visual encoding
          linkColorBy: 'weight',
          linkWidthBy: 'weight',
          linkWidthRange: [0.3, 2],
          ...(inputs.hasCluster ? {
            simulationCluster: 0.8,
            showClusterLabels: true,
            scaleClusterLabels: true,
            pointColorBy: inputs.clusterField ?? undefined,
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
    setSelectedEdge(null); // clear edge selection
    setEdgeDetailA(null);
    setEdgeDetailB(null);
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
        setGenreTracks(result.tracks ?? []);
        setGenreTotalPlays(result.totalPlays ?? 0);
      }
    } catch {
      // network error — leave empty
    } finally {
      setLoadingTracks(false);
    }
  }, []);

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

    const srcLabel = loaded?.raw.points.find((p: any) => p.id === edge.source)?.label ?? edge.source;
    const tgtLabel = loaded?.raw.points.find((p: any) => p.id === edge.target)?.label ?? edge.target;

    setEdgeDetailA({ label: String(srcLabel), tracks: [], totalPlays: 0 });
    setEdgeDetailB({ label: String(tgtLabel), tracks: [], totalPlays: 0 });
    setLoadingEdgeDetail(true);

    const fetchTracks = async (genre: string) => {
      try {
        const resp = await fetch('/api/genre-tracks', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ genre }),
        });
        const result = await resp.json();
        return { tracks: result.tracks ?? [], totalPlays: result.totalPlays ?? 0 };
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

  // Stable refs for Cosmograph callbacks — the memo'd component may not
  // re-apply inline arrow functions properly on config updates.
  const loadedRef = useRef(loaded);
  loadedRef.current = loaded;
  const handleGenreClickRef = useRef(handleGenreClick);
  handleGenreClickRef.current = handleGenreClick;

  // Stable onClick: fires on every canvas click; index is defined when a point was clicked
  const onGraphClick = useCallback(
    (index: number | undefined, _pos: [number, number] | undefined, _event: MouseEvent) => {
      if (index == null) return;
      const l = loadedRef.current;
      if (!l) return;
      const label = l.raw.points[index]?.label;
      console.log('[genre-detail] onGraphClick index=', index, 'label=', label);
      if (label) handleGenreClickRef.current(String(label));
    },
    [],
  );

  // Stable onLabelClick
  const onGraphLabelClick = useCallback(
    (_index: number, id: string, _event: MouseEvent) => {
      console.log('[genre-detail] onLabelClick id=', id);
      if (id) handleGenreClickRef.current(String(id));
    },
    [],
  );

  // Build final config using pointColorByFn (the correct Cosmograph API for custom coloring).
  // activePointColorStrategy is a read-only getter, NOT a config callback.
  // Link weight gradient is always applied via linkColorByFn.
  const finalCfg = useMemo(() => {
    if (!cfg) return cfg;

    // Always apply link weight color gradient + click callbacks
    const base = {
      ...cfg,
      ...(linkColorByFn ? { linkColorByFn } : {}),
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
  }, [cfg, loaded, selectedCluster, selectedEdge, clusterColorMap, clusterOn, selectedPointIds, linkColorByFn, onGraphClick, onGraphLabelClick]);

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
            ...(loaded.hasLinkTime ? ['first_seen_ts'] : []),
          ],
          ...(loaded.hasLinkTime ? { linkTimeBy: 'first_seen_ts' } : {}),
          ...(links.some((l: any) => typeof l.weight === 'number') ? {
            linkColorBy: 'weight',
            linkWidthBy: 'weight',
          } : {}),
        },
        labels: { enabled: true, maxLabelCount: 10000 },
        timeline: (loaded.hasPointTime || loaded.hasLinkTime) ? { enabled: true } : undefined,
      };

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
          ...(prepared?.cosmographConfig ?? {}),
          points: prepared?.points,
          links: prepared?.links,
          simulationCluster: 0.8,
          showClusterLabels: true,
          scaleClusterLabels: true,
          pointColorBy: clusterField,
          linkColorBy: 'weight',
          linkWidthBy: 'weight',
          linkWidthRange: [0.3, 2],
        };
      });

      setClusterOn(true);
      setSelectedCluster(null);
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
    setEdgePanelOpen(true);

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
          ...(loaded.hasLinkTime ? ['first_seen_ts'] : []),
        ],
        ...(loaded.hasLinkTime ? { linkTimeBy: 'first_seen_ts' } : {}),
        ...(filteredLinks.some((l: any) => typeof l.weight === 'number') ? {
          linkColorBy: 'weight',
          linkWidthBy: 'weight',
        } : {}),
      },
      labels: { enabled: true, maxLabelCount: 10000 },
      timeline: (loaded.hasPointTime || loaded.hasLinkTime) ? { enabled: true } : undefined,
    };

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
        ...(prepared?.cosmographConfig ?? {}),
        points: prepared?.points,
        links: prepared?.links,
        simulationCluster: 0.8,
        showClusterLabels: true,
        scaleClusterLabels: true,
        pointColorBy: clusterField,
        linkColorBy: 'weight',
        linkWidthBy: 'weight',
        linkWidthRange: [0.3, 2],
      };
    });

    setFocusedCluster(clusterValue);
    setSelectedCluster(null);
    setContextMenu(null);
  }, [loaded, fullData]);

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
          ...(loaded.hasLinkTime ? ['first_seen_ts'] : []),
        ],
        ...(loaded.hasLinkTime ? { linkTimeBy: 'first_seen_ts' } : {}),
        ...(fullData.links.some((l: any) => typeof l.weight === 'number') ? {
          linkColorBy: 'weight',
          linkWidthBy: 'weight',
        } : {}),
      },
      labels: { enabled: true, maxLabelCount: 10000 },
      timeline: (loaded.hasPointTime || loaded.hasLinkTime) ? { enabled: true } : undefined,
    };

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
        ...(prepared?.cosmographConfig ?? {}),
        points: prepared?.points,
        links: prepared?.links,
        simulationCluster: 0.8,
        showClusterLabels: true,
        scaleClusterLabels: true,
        pointColorBy: clusterField,
        linkColorBy: 'weight',
        linkWidthBy: 'weight',
        linkWidthRange: [0.3, 2],
      };
    });

    setFocusedCluster(null);
    setFullData(null);
    setSelectedCluster(null);
  }, [loaded, fullData]);


  return (
    <div style={{ height: '100vh', width: '100vw', display: 'flex', flexDirection: 'column' }}>
      <header
        style={{
          padding: '8px 12px',
          borderBottom: '1px solid #ddd',
          display: 'flex',
          alignItems: 'center',
          gap: 16,
          fontFamily: 'system-ui, sans-serif',
          fontSize: 14,
        }}
      >
        <strong style={{ color: '#e040fb' }}>Traverse</strong>
        <span style={{ color: '#666' }}>{status}</span>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <input
            type="checkbox"
            checked={labelsOn}
            onChange={(e) => setLabelsOn(e.currentTarget.checked)}
          />
          Show labels
        </label>
        {loaded?.hasCluster && (
          <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <input
              type="checkbox"
              checked={clusterOn}
              onChange={(e) => setClusterOn(e.currentTarget.checked)}
            />
            Cluster by {loaded.clusterField}
          </label>
        )}
        <span style={{ marginLeft: 'auto', color: '#666', fontSize: 13 }}>
          Visualized with <strong style={{ color: '#000' }}>Cosmograph</strong>
        </span>
      </header>

      <div style={{ position: 'relative', flex: 1, minHeight: 0 }}>
        {finalCfg ? (
          <CosmographProvider>
            <Cosmograph
              {...(finalCfg as any)}
              onMount={(inst: any) => { cosmoRef.current = inst; }}
            />
            {hasTimeline && (
              <div
                style={{
                  position: 'absolute',
                  left: 0,
                  right: 0,
                  bottom: 0,
                  padding: '8px 12px',
                  background: 'rgba(255,255,255,0.85)',
                  borderTop: '1px solid #ddd'
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
                  left: 12,
                  bottom: 12,
                  padding: '4px 8px',
                  background: 'rgba(255,255,255,0.85)',
                  border: '1px solid #ddd',
                  borderRadius: 6,
                  fontFamily: 'system-ui',
                  fontSize: 12,
                  color: '#555'
                }}
              >
                Timeline: disabled (no time fields detected)
              </div>
            )}

            {/* Clustering algorithm panel */}
            {loaded && (
              <div
                className={`cluster-algo-panel draggable-panel ${clusterPanelOpen ? 'open' : 'collapsed'}`}
                style={clusterDrag.dragStyle}
              >
                <div
                  className="drag-handle cluster-algo-panel-toggle"
                  onMouseDown={clusterDrag.onDragStart}
                  onClick={() => setClusterPanelOpen(p => !p)}
                  title={clusterPanelOpen ? 'Collapse panel' : 'Re-cluster graph'}
                >
                  {clusterPanelOpen ? '▼' : '▶'} Clustering
                </div>
                {clusterPanelOpen && (
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
                      {clustering ? 'Clustering…' : 'Apply'}
                    </button>

                    {clusterError && (
                      <div className="cluster-algo-error">{clusterError}</div>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* Community selector panel */}
            {loaded?.hasCluster && loaded.clusterGroups.size > 0 && (
              <div
                className={`community-panel draggable-panel ${panelOpen ? 'open' : 'collapsed'}`}
                style={communityDrag.dragStyle}
              >
                <div
                  className="drag-handle community-panel-toggle"
                  onMouseDown={communityDrag.onDragStart}
                  onClick={() => setPanelOpen(p => !p)}
                  title={panelOpen ? 'Collapse panel' : 'Show communities'}
                >
                  {panelOpen ? '▶' : '◀'} Communities
                </div>
                {panelOpen && (
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
                )}
              </div>
            )}
            {/* Edge analysis panel */}
            {loaded?.hasCluster && (
              <div
                className={`edge-analysis-panel draggable-panel ${edgePanelOpen ? 'open' : 'collapsed'}`}
                style={edgeDrag.dragStyle}
              >
                <div
                  className="drag-handle edge-analysis-toggle"
                  onMouseDown={edgeDrag.onDragStart}
                  onClick={() => setEdgePanelOpen(p => !p)}
                >
                  {edgePanelOpen ? '▼' : '▶'} Edge Analysis
                </div>
                {edgePanelOpen && (
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
                      {edgeLoading ? 'Analyzing…' : selectedCluster == null ? 'Select a community' : 'Run'}
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
                )}
              </div>
            )}

            {/* Detail panel — shows node or edge detail */}
            <div className="genre-detail-panel draggable-panel" style={genreDrag.dragStyle}>
              <div className="genre-detail-header drag-handle" onMouseDown={genreDrag.onDragStart}>
                <span className="genre-detail-title">
                  {selectedEdge
                    ? `Edge: ${loaded?.raw.points.find((p: any) => p.id === selectedEdge.source)?.label ?? selectedEdge.source} — ${loaded?.raw.points.find((p: any) => p.id === selectedEdge.target)?.label ?? selectedEdge.target}`
                    : selectedGenre ?? 'Detail'}
                </span>
                {(selectedGenre || selectedEdge) && (
                  <button className="genre-detail-close" onClick={() => {
                    setSelectedGenre(null);
                    setSelectedEdge(null);
                    setEdgeDetailA(null);
                    setEdgeDetailB(null);
                  }}>
                    &times;
                  </button>
                )}
              </div>
              {selectedEdge ? (
                // Edge detail: show tracks for both endpoints
                loadingEdgeDetail ? (
                  <div className="genre-detail-loading">Loading tracks for both nodes...</div>
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
                                <li key={i} className="genre-detail-track">
                                  <span className="genre-detail-rank">#{i + 1}</span>
                                  <div className="genre-detail-track-info">
                                    <div className="genre-detail-track-name">{t.trackName}</div>
                                    <div className="genre-detail-artist">{t.artistName}</div>
                                  </div>
                                  <span className="genre-detail-plays">{t.playCount}x</span>
                                </li>
                              ))}
                            </ul>
                          </>
                        )}
                      </div>
                    ))}
                  </div>
                )
              ) : !selectedGenre ? (
                <div className="genre-detail-empty">No node or edge selected</div>
              ) : loadingTracks ? (
                <div className="genre-detail-loading">Loading tracks...</div>
              ) : genreTracks.length === 0 ? (
                <div className="genre-detail-empty">No tracks found for this genre.</div>
              ) : (
                <>
                  <div className="genre-detail-summary">
                    {genreTracks.length} track{genreTracks.length !== 1 ? 's' : ''} &middot; {genreTotalPlays.toLocaleString()} total plays
                  </div>
                  <div className="genre-detail-body">
                    <ul className="genre-detail-track-list">
                      {genreTracks.map((t, i) => (
                        <li key={i} className="genre-detail-track">
                          <span className="genre-detail-rank">#{i + 1}</span>
                          <div className="genre-detail-track-info">
                            <div className="genre-detail-track-name">{t.trackName}</div>
                            <div className="genre-detail-artist">{t.artistName}</div>
                          </div>
                          <span className="genre-detail-plays">{t.playCount}x</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </>
              )}
            </div>
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
                <div className="context-menu-separator" />
                <div
                  className="context-menu-item"
                  onClick={() => handleFocusCommunity(contextMenu.clusterValue)}
                >
                  Focus on this community
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
