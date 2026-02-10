import { useEffect, useMemo, useRef, useState } from 'react';
import './labels.css';
import {
  Cosmograph,
  CosmographProvider,
  CosmographTimeline,
  type CosmographConfig,
  type Cosmograph as CosmographInstance
} from '@cosmograph/react';
import { loadAndPrepare, type LoadedInputs } from './DataLoader';

function parseSelectionsFromURL() {
  const q = new URLSearchParams(window.location.search);
  const selNodes = (q.get('sel_nodes') ?? '')
    .split(',').map(s => s.trim()).filter(Boolean);
  const selEdges = (q.get('sel_edges') ?? '')
    .split(';').map(s => s.trim()).filter(Boolean)
    .map(pair => {
      const [a, b] = pair.split('|').map(t => t.trim());
      return (a && b) ? [a, b] as [string, string] : null;
    })
    .filter((x): x is [string, string] => Array.isArray(x));
  return { selNodes, selEdges };
}
const edgeKey = (a: string, b: string) => (a < b ? `${a}→${b}` : `${b}→${a}`);

export default function App() {
  const dataUrl = useMemo(
    () => new URLSearchParams(window.location.search).get('data') ?? '/cosmo_genres_timeline.json',
    []
  );
  const { selNodes, selEdges } = useMemo(parseSelectionsFromURL, []);

  const [loaded, setLoaded] = useState<LoadedInputs | null>(null);
  const [cfg, setCfg] = useState<CosmographConfig | null>(null);
  const [labelsOn, setLabelsOn] = useState(true);
  const [status, setStatus] = useState('Loading…');

  const cosmoRef = useRef<CosmographInstance | null>(null);

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
          labels: { enabled: true, maxLabelCount: 10000 },
          // keep your css + white labels
          pointLabelClassName: 'genre-label',
          clusterLabelClassName: 'cluster-label',
          pointLabelColor: '#ffffff',
          clusterLabelColor: '#ffffff',
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
    setCfg(prev => prev ? { ...prev, labels: { ...(prev.labels as any ?? {}), enabled: labelsOn } } : prev);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [labelsOn]);

  const hasTimeline = !!loaded && (loaded.hasPointTime || loaded.hasLinkTime);

  // Apply URL-driven selections after the instance is ready
  useEffect(() => {
    async function applySelections() {
      const inst = cosmoRef.current;
      const inputs = loaded;
      if (!inst || !inputs) return;

      console.log('[Debug] Applying selections...', { selNodes, selEdges });

      // Map ids → indices
      const nodeIdxs = selNodes
        .map(id => inputs.idToIndex.get(id))
        .filter((x): x is number => typeof x === 'number');
      
      console.log('[Debug] Mapped node IDs to indices:', { nodeIdxs });

      // Select points via API (native)
      if (nodeIdxs.length) inst.selectPoints(nodeIdxs);
      else inst.unselectAllPoints?.();

      const nodeIdxSet = new Set(nodeIdxs);
      console.log('[Debug] Created node index set:', nodeIdxSet);

      // Precompute explicitly selected edge indices if available
      const selectedEdgeIdxs = new Set<number>();
      for (const [a, b] of selEdges) {
        const idx = inputs.edgeToIndex.get(edgeKey(a, b));
        if (typeof idx === 'number') selectedEdgeIdxs.add(idx);
      }
      console.log('[Debug] Created edge index set:', selectedEdgeIdxs);

      // Strategies react to current selection
      const hasAnySelection = nodeIdxSet.size > 0 || selectedEdgeIdxs.size > 0;
      console.log('[Debug] Setting config with hasAnySelection=', hasAnySelection);

      inst.setConfig({
        activePointColorStrategy: hasAnySelection
          ? (_row: any, idx: number) => (nodeIdxSet.has(idx) ? 'rgba(255, 255, 255, 1)' : 'rgba(138, 138, 138, 0.2)')
          : undefined,
        activePointSizeStrategy: hasAnySelection
          ? (_row: any, idx: number) => (nodeIdxSet.has(idx) ? 5 : 3)
          : undefined,
        activePointLabelColorStrategy: hasAnySelection
          ? (_row: any, idx: number) => (nodeIdxSet.has(idx) ? 'rgba(255, 255, 255, 1)' : 'transparent')
          : undefined,
        activeLinkColorStrategy: hasAnySelection
          ? (row: any) => {
              const s = row['sourceidx'] as number | undefined;
              const t = row['targetidx'] as number | undefined;
              const i = row['idx'] as number | undefined; // may exist in recent builds
              const touchesSel = (typeof s === 'number' && nodeIdxSet.has(s)) ||
                                 (typeof t === 'number' && nodeIdxSet.has(t));
              const explicitlySel = (typeof i === 'number') && selectedEdgeIdxs.has(i);
              return (touchesSel || explicitlySel) ? 'rgba(255, 255, 255, 1)' : 'rgba(96, 96, 96, 0.2)';
            }
          : undefined,
      });
    }
    applySelections();
  }, [loaded, selNodes, selEdges]);

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
        <strong>Cosmograph Smoke</strong>
        <span style={{ color: '#666' }}>{status}</span>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <input
            type="checkbox"
            checked={labelsOn}
            onChange={(e) => setLabelsOn(e.currentTarget.checked)}
          />
          Show labels
        </label>
      </header>

      <div style={{ position: 'relative', flex: 1, minHeight: 0 }}>
        {cfg ? (
          <CosmographProvider>
            <Cosmograph
              {...(cfg as any)}
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
          </CosmographProvider>
        ) : (
          <div style={{ padding: 16, fontFamily: 'system-ui' }}>Loading…</div>
        )}
      </div>
    </div>
  );
}
