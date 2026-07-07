// ── Phase 5 §5.2 shared helpers for the strategy editor panels ────────────────

/** Read a dotted path ("exits.k_sl") out of a nested object. */
export function getAtPath(obj: unknown, path: string): unknown {
  if (!obj || typeof obj !== 'object') return undefined;
  let cur: any = obj;
  for (const part of path.split('.')) {
    if (cur === null || cur === undefined || typeof cur !== 'object') return undefined;
    cur = cur[part];
  }
  return cur;
}

/** Set a dotted path inside a nested object (mutates), creating blocks as needed. */
export function setAtPath(obj: Record<string, any>, path: string, value: unknown): void {
  const parts = path.split('.');
  let cur = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    if (!cur[p] || typeof cur[p] !== 'object') cur[p] = {};
    cur = cur[p];
  }
  cur[parts[parts.length - 1]] = value;
}

/** Reassemble a map of dotted-path → value into a nested block dict for PUT patches. */
export function buildNestedPatch(dotted: Record<string, unknown>): Record<string, any> {
  const patch: Record<string, any> = {};
  for (const [path, value] of Object.entries(dotted)) setAtPath(patch, path, value);
  return patch;
}

/**
 * LIVE-guard check: true when /api/status reports mode==live && running.
 * Fails safe — if the status endpoint is unreachable we return false (the
 * backend still enforces its own 409 live guard, which callers surface).
 */
export async function isLiveRunning(baseUrl = ''): Promise<boolean> {
  try {
    const res = await fetch(`${baseUrl}/api/status`, { cache: 'no-store' });
    if (!res.ok) return false;
    const data = await res.json().catch(() => null);
    if (!data || typeof data !== 'object' || data.error) return false;
    // Some deployments nest the status block ({status:{mode,running}}).
    const s = (data.status && typeof data.status === 'object') ? data.status : data;
    return s.mode === 'live' && Boolean(s.running);
  } catch {
    return false;
  }
}

/** Short config hash for display (first 8 chars). */
export function shortHash(hash: unknown): string {
  return typeof hash === 'string' && hash.length > 0 ? hash.slice(0, 8) : '—';
}

/** Render any config value compactly for diff / read-only display. */
export function fmtValue(v: unknown): string {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'boolean') return v ? 'on' : 'off';
  if (typeof v === 'number') return Number.isInteger(v) ? String(v) : String(Number(v.toFixed(6)));
  if (typeof v === 'string') return v;
  try { return JSON.stringify(v); } catch { return String(v); }
}
