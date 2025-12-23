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

const COLOR_DEFAULT_POINT = '#9fb3c8';
const COLOR_SELECTED_POINT = '#ffd54a';
const COLOR_NEIGHBOR_POINT = '#91e0ff';

const COLOR_DEFAULT_LINK = '#8090a0';
const COLOR_SELECTED_LINK = '#ffd54a';

const SIZE_DEFAULT_POINT = 3;
const SIZE_SELECTED_POINT = 8;

export default function App() {
  const dataUrl = useMemo(
    () => new URLSearchParams(window.location.search).get('data') ?? '/cosmo_genres_timeline.json',
    []
  );

  const selParam = useMemo(() => {
    const raw = new URLSearchParams(window.location.search).get('sel_nodes');
    if (!raw) return [] as string[];
    return raw.split(/[|,;]/).map(s => s.trim()).filter(Boolean);
  }, []);

  const [loaded, setLoaded] = useState<LoadedInputs | null>(null);
  const [cfg, setCfg] = useState<CosmographConfig | null>(null);
  const [labelsOn, setLabelsOn] = useState(true);
  const [status, setStatus] = useState('Loading…');

  const graphRef = useRef<CosmographInstance | null>(null);
  const selectedSetRef = useRef<Set<number>>(new Set());
  const neighborSetRef = useRef<Set<number>>(new Set());

  function* iterLinkBatches() {
    const links = loaded?.prepared?.links as any;
    if (!links) return;
    for (const batch of links.batches ?? []) yield batch;
  }

  const recomputeNeighbors = () => {
    neighborSetRef.current.clear();
    const sel = selectedSetRef.current;
    if (!sel.size) return;

    for (const batch of iterLinkBatches()) {
      const srcCol = batch.getChild('sourceidx') ?? batch.getChild('sourceIndex') ?? batch.getChild('sourceidxs');
      const dstCol = batch.getChild('targetidx') ?? batch.getChild('targetIndex') ?? batch.getChild('targetidxs');
      if (!srcCol || !dstCol) continue;
      const n = batch.length;
      for (let i = 0; i < n; i++) {
        const s = srcCol.get(i);
        const t = dstCol.get(i);
        if (sel.has(s)) neighborSetRef.current.add(t);
        if (sel.has(t)) neighborSetRef.current.add(s);
      }
    }
    for (const i of sel) neighborSetRef.current.delete(i);
  };

  const applySelectionVisuals = () => {
    const g = graphRef.current;
    if (!g) return;
    const selectedSet = selectedSetRef.current;
    const neighborSet = neighborSetRef.current;

    g.setConfig({
      pointColorByFn: (_row: any, idx: number) => {
        if (selectedSet.has(idx)) return COLOR_SELECTED_POINT;
        if (neighborSet.has(idx)) return COLOR_NEIGHBOR_POINT;
        return COLOR_DEFAULT_POINT;
      },
      pointSizeByFn: (_row: any, idx: number) =>
        selectedSet.has(idx) ? SIZE_SELECTED_POINT : SIZE_DEFAULT_POINT,
      linkColorByFn: (row: any) => {
        const s = row?.sourceidx ?? row?.sourceIndex;
        const t = row?.targetidx ?? row?.targetIndex;
        if (selectedSet.has(s) || selectedSet.has(t)) return COLOR_SELECTED_LINK;
        return COLOR_DEFAULT_LINK;
      },
      // keep labels white and independent from point color
      pointLabelColor: '#ffffff',
      usePointColorStrategyForLabels: false,
      usePointColorStrategyForClusterLabels: false,
    });
  };

  const selectByIdsOrLabels = (names: string[]) => {
    if (!loaded) return;
    const { idToIdx, labelToIdLower } = loaded;
    const selectedIdx: number[] = [];

    for (const token of names) {
      const byId = idToIdx[token];
      if (typeof byId === 'number') { selectedIdx.push(byId); continue; }
      const idByLabel = labelToIdLower[token.toLowerCase()];
      if (idByLabel && typeof idToIdx[idByLabel] === 'number') {
        selectedIdx.push(idToIdx[idByLabel]);
      }
    }

    selectedSetRef.current = new Set(selectedIdx);
    recomputeNeighbors();
    applySelectionVisuals();
    graphRef.current?.selectPoints(selectedIdx);
  };

  useEffect(() => {
    (window as any).cosmoSelect = (arr: string[]) => selectByIdsOrLabels(arr);
  }, [loaded]);

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        setStatus('Loading JSON…');
        const inputs = await loadAndPrepare(dataUrl);
        if (!alive) return;

        const base: CosmographConfig = {
          ...(inputs.prepared.cosmographConfig ?? {}),
          points: inputs.prepared.points,
          links: inputs.prepared.links,
          labels: { enabled: true, maxLabelCount: 10000 },
        };

        setLoaded(inputs);
        setCfg(base);
        setStatus('Ready');
        console.debug('App: time present? points=', inputs.hasPointTime, 'links=', inputs.hasLinkTime);
      } catch (e: any) {
        console.error(e);
        setStatus(`Error: ${e?.message ?? e}`);
      }
    })();
    return () => { alive = false; };
  }, [dataUrl]);

  useEffect(() => {
    setCfg(prev => prev ? { ...prev, labels: { ...(prev.labels as any ?? {}), enabled: labelsOn } } : prev);
  }, [labelsOn]);

  const hasTimeline = !!loaded && (loaded.hasPointTime || loaded.hasLinkTime);

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
              onReady={(inst) => {
                graphRef.current = inst;
                applySelectionVisuals();

                if (selParam.length) selectByIdsOrLabels(selParam);

                inst.setConfig({
                  onLabelClick: (_row: any, idx: number) => {
                    const sel = selectedSetRef.current;
                    if (sel.has(idx)) sel.delete(idx); else sel.add(idx);
                    recomputeNeighbors();
                    applySelectionVisuals();
                    inst.selectPoints([...sel]);
                  },
                });
              }}
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
