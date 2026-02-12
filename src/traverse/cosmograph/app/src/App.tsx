import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import './labels.css';
import {
  Cosmograph,
  CosmographProvider,
  CosmographTimeline,
  type CosmographConfig,
  type Cosmograph as CosmographInstance
} from '@cosmograph/react';
import { loadAndPrepare, type LoadedInputs } from './DataLoader';

const CLUSTER_PALETTE = [
  '#00e5ff', '#ff4081', '#76ff03', '#ffea00', '#e040fb', '#ff6e40',
];

const UNKNOWN_COLOR = 'rgba(205, 207, 213, 0.9)';
const DIM_COLOR = 'rgba(138, 138, 138, 0.2)';

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

  // Build final config using pointColorByFn (the correct Cosmograph API for custom coloring).
  // activePointColorStrategy is a read-only getter, NOT a config callback.
  const finalCfg = useMemo(() => {
    if (!cfg || !loaded?.hasCluster || !loaded?.clusterField || !clusterColorMap) return cfg;

    if (selectedCluster != null) {
      // Cluster selected → highlight it, dim others, show only selected labels
      const selColor = clusterColorMap.get(selectedCluster) ?? '#ffffff';
      return {
        ...cfg,
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

    if (!clusterOn) return cfg; // clustering toggled off, no custom coloring

    // No selection, clustering on → use our own color map so panel dots match exactly
    return {
      ...cfg,
      pointColorStrategy: undefined,
      pointColorByFn: (value: any) =>
        clusterColorMap.get(String(value ?? '')) ?? UNKNOWN_COLOR,
    };
  }, [cfg, loaded, selectedCluster, clusterColorMap, clusterOn, selectedPointIds]);

  // Imperative selectPoints for Cosmograph's internal selection state
  useEffect(() => {
    const inst = cosmoRef.current;
    if (!inst || !loaded) return;

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
  }, [loaded, selectedCluster]);

  const handleClusterClick = useCallback((clusterValue: string) => {
    setSelectedCluster(prev => prev === clusterValue ? null : clusterValue);
  }, []);

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
        <strong>Traverse</strong>
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
      </header>

      <div style={{ position: 'relative', flex: 1, minHeight: 0 }}>
        {finalCfg ? (
          <CosmographProvider>
            <Cosmograph
              {...(finalCfg as any)}
              onReady={(inst: CosmographInstance) => { cosmoRef.current = inst; }}
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

            {/* Community selector panel */}
            {loaded?.hasCluster && loaded.clusterGroups.size > 0 && (
              <div className={`community-panel ${panelOpen ? 'open' : 'collapsed'}`}>
                <button
                  className="community-panel-toggle"
                  onClick={() => setPanelOpen(p => !p)}
                  title={panelOpen ? 'Collapse panel' : 'Show communities'}
                >
                  {panelOpen ? '▶' : '◀'} Communities
                </button>
                {panelOpen && (
                  <div className="community-panel-body">
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
                        >
                          <span
                            className="community-dot"
                            style={{ background: clusterColorMap?.get(value) ?? UNKNOWN_COLOR }}
                          />
                          <span className="community-label">{value}</span>
                          <span className="community-count">{group.count}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
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
