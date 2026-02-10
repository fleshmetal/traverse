import { prepareCosmographData } from '@cosmograph/react';

export type LoadedInputs = {
  prepared: any;
  hasPointTime: boolean;
  hasLinkTime: boolean;
  raw: { points: any[]; links: any[] };
  idToIndex: Map<string, number>;
  edgeToIndex: Map<string, number>;
};

function toEpochMs(v: unknown): number | null {
  if (v == null) return null;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    // year like "1987"
    const yy = Number(v);
    if (Number.isFinite(yy) && yy >= 1800 && yy <= 2200) {
      return Date.UTC(yy, 0, 1);
    }
    const t = Date.parse(v);
    return Number.isFinite(t) ? t : null;
  }
  return null;
}

function normalizeTimeField(rows: any[], candidateNames: string[]): { has: boolean; field?: string } {
  if (!Array.isArray(rows) || rows.length === 0) return { has: false };
  let chosen: string | undefined;
  for (const name of candidateNames) {
    if (rows[0] && Object.prototype.hasOwnProperty.call(rows[0], name)) { chosen = name; break; }
  }
  if (!chosen) return { has: false };

  let anyValid = false;
  for (const r of rows) {
    const ms = toEpochMs(r[chosen]);
    if (ms != null) {
      r.first_seen_ts = ms; // normalize
      anyValid = true;
    } else {
      // strip unusable time bits to avoid duckdb errors
      delete r.first_seen_ts;
      delete r.first_seen;
    }
  }
  return anyValid ? { has: true, field: 'first_seen_ts' } : { has: false };
}

function ensurePointLabels(points: any[]) {
  for (const p of points) {
    if (p && (p.label == null)) p.label = p.id;
  }
}

function edgeKey(a: string, b: string) {
  return a < b ? `${a}→${b}` : `${b}→${a}`;
}

export async function loadAndPrepare(url: string): Promise<LoadedInputs> {
  const resp = await fetch(url, { cache: 'no-store' });
  if (!resp.ok) throw new Error(`Failed to fetch ${url}: ${resp.status}`);
  const json = await resp.json() as { points?: any[]; links?: any[]; cosmographConfig?: any };

  const points = Array.isArray(json.points) ? [...json.points] : [];
  const links  = Array.isArray(json.links)  ? [...json.links]  : [];

  console.debug('[DBG] Raw sizes', { points: points.length, links: links.length });

  // labels always present
  ensurePointLabels(points);

  // normalize/attach time as first_seen_ts if available
  const ptTime = normalizeTimeField(points, ['first_seen_ts', 'first_seen', 'ts', 'time', 'date']);
  const lkTime = normalizeTimeField(links,  ['first_seen_ts', 'first_seen', 'ts', 'time', 'date']);
  console.debug('[DBG] points time detection:', { detected: ptTime.has });
  console.debug('[DBG] links time detection:',  { detected: lkTime.has });

  // Build DataKit config (THIS is the correct signature!)
  const dataConfig: any = {
    points: {
      pointIdBy: 'id',
      pointLabelBy: 'label',
      pointIncludeColumns: ['label', ...(ptTime.has ? ['first_seen_ts'] : [])],
      ...(ptTime.has ? { pointTimeBy: 'first_seen_ts' } : {}),
    },
    links: {
      linkSourceBy: 'source',
      linkTargetsBy: ['target'],
      linkIncludeColumns: [
        ...(links.some(l => typeof l.weight === 'number') ? ['weight'] : []),
        ...(lkTime.has ? ['first_seen_ts'] : []),
      ],
      ...(lkTime.has ? { linkTimeBy: 'first_seen_ts' } : {}),
    },
    labels: { enabled: true, maxLabelCount: 10000 },
    timeline: (ptTime.has || lkTime.has) ? { enabled: true } : undefined,
  };

  // IMPORTANT: use (config, points, links) form
  const prepared = await prepareCosmographData(dataConfig, points, links);

  console.debug('[DBG] Prepared cosmographConfig.timeline:', prepared?.cosmographConfig?.timeline);
  console.debug('[DBG] Prepared points/links keys:',
    Object.keys(prepared?.points ?? {}), Object.keys(prepared?.links ?? {})
  );

  // Lookup maps (raw array order is preserved)
  const idToIndex = new Map<string, number>();
  points.forEach((p, i) => { if (p?.id != null) idToIndex.set(String(p.id), i); });

  const edgeToIndex = new Map<string, number>();
  links.forEach((e, i) => {
    if (e?.source != null && e?.target != null) {
      edgeToIndex.set(edgeKey(String(e.source), String(e.target)), i);
    }
  });

  return {
    prepared,
    hasPointTime: !!ptTime.has,
    hasLinkTime:  !!lkTime.has,
    raw: { points, links },
    idToIndex,
    edgeToIndex,
  };
}
