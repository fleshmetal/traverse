import { prepareCosmographData, type CosmographDataPrepResult } from '@cosmograph/react';

export type LoadedInputs = {
  prepared: CosmographDataPrepResult;
  hasPointTime: boolean;
  hasLinkTime: boolean;
  idToIdx: Record<string, number>;
  labelToIdLower: Record<string, string>;
};

type Row = Record<string, any>;

function normalizeTimes(rows: Row[] | undefined) {
  if (!rows || !rows.length) return { rows: rows ?? [], has: false, min: null as number | null, max: null as number | null };
  let has = false;
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  for (const r of rows) {
    let ts: number | undefined;
    const a = r['first_seen_ts'];
    const b = r['first_seen'];
    if (typeof a === 'number' && Number.isFinite(a)) ts = a;
    else if (typeof b === 'number' && Number.isFinite(b)) ts = b;
    if (typeof ts === 'number') {
      r['first_seen_ts'] = ts;
      has = true;
      if (ts < min) min = ts;
      if (ts > max) max = ts;
    } else {
      if (typeof r['first_seen_ts'] !== 'number') delete r['first_seen_ts'];
      if (typeof r['first_seen'] !== 'number') delete r['first_seen'];
    }
  }
  return { rows, has, min: has ? min : null, max: has ? max : null };
}

function ensurePointLabels(points: Row[]): void {
  for (const p of points) if (p['label'] == null) p['label'] = p['id'];
}

export async function loadAndPrepare(url: string): Promise<LoadedInputs> {
  const res = await fetch(url, { cache: 'no-store' });
  if (!res.ok) throw new Error(`Failed to load ${url}: ${res.status} ${res.statusText}`);
  const json = await res.json();

  const rawPoints: Row[] = Array.isArray(json.points) ? [...json.points] : [];
  const rawLinks: Row[]  = Array.isArray(json.links)  ? [...json.links]  : [];

  ensurePointLabels(rawPoints);
  rawPoints.forEach((p, i) => { p.idx = i; });

  const pt = normalizeTimes(rawPoints);
  const lk = normalizeTimes(rawLinks);

  const idToIdx: Record<string, number> = {};
  const labelToIdLower: Record<string, string> = {};
  for (const p of pt.rows) {
    if (typeof p.id === 'string') idToIdx[p.id] = p.idx as number;
    if (typeof p.label === 'string' && typeof p.id === 'string') {
      labelToIdLower[p.label.toLowerCase()] = p.id;
    }
  }

  const hasWeight = rawLinks.some(l => typeof l.weight === 'number');

  // IMPORTANT: this build expects linkTargetsBy (array), not linkTargetBy
  const dataConfig: any = {
    points: {
      pointIdBy: 'id',
      pointIndexBy: 'idx',
      pointLabelBy: 'label',
      pointIncludeColumns: pt.has ? ['label', 'first_seen_ts', 'idx'] : ['label', 'idx'],
      ...(pt.has ? { pointTimeBy: 'first_seen_ts' } : {}),
    },
    links: {
      linkSourceBy: 'source',
      linkTargetsBy: ['target'],
      ...(hasWeight ? { linkWeightBy: 'weight' } : {}),
      linkIncludeColumns: [
        'source', 'target',
        ...(lk.has ? ['first_seen_ts'] : []),
        ...(hasWeight ? ['weight'] : []),
      ],
      ...(lk.has ? { linkTimeBy: 'first_seen_ts' } : {}),
    },
    timeline: { enabled: pt.has || lk.has },
    labels: { enabled: true, maxLabelCount: 10000 },
  };

  console.debug('[DBG] Raw sizes', { points: rawPoints.length, links: rawLinks.length });
  console.debug('[DBG] points time detection: detected=%s min=%s max=%s',
    pt.has ? rawPoints.length : 0, pt.min ? new Date(pt.min).toISOString() : '—', pt.max ? new Date(pt.max).toISOString() : '—');
  console.debug('[DBG] links time detection: detected=%s min=%s max=%s',
    lk.has ? rawLinks.length : 0, lk.min ? new Date(lk.min).toISOString() : '—', lk.max ? new Date(lk.max).toISOString() : '—');

  const prepared = await prepareCosmographData(dataConfig, pt.rows, lk.rows);
  if (!prepared) throw new Error('prepareCosmographData returned null/undefined');

  console.debug('[DBG] Prepared cosmographConfig.timeline:', (prepared as any).cosmographConfig?.timeline ?? '—');
  // Guard against undefined links when DataKit drops them
  const ptsKeys = prepared.points ? Object.keys(prepared.points) : [];
  const lnkKeys = prepared.links ? Object.keys(prepared.links) : [];
  console.debug('[DBG] Prepared points/links keys:', ptsKeys, lnkKeys);

  return {
    prepared,
    hasPointTime: !!pt.has,
    hasLinkTime: !!lk.has,
    idToIdx,
    labelToIdLower,
  };
}
