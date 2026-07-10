import { useCallback, useEffect, useState } from 'react';

// ── WolfBot S5 — EV-score model hooks/types ────────────────────────────────
// Shared types + polling hooks for the EV (expected-value / win-probability)
// model surface. Every field is optional and read defensively: older backends
// return {available:false}, 404, or omit sections entirely, and the UI must
// degrade to a muted "EV model not available" state rather than crash.
//
// Endpoints:
//   GET  /api/ev/scores     → per-coin win-probability scores + ranking + next_buy
//   GET  /api/ev/model      → version list + status + counts
//   POST /api/ev/train      → kick a retrain (409 if already running)
//   GET  /api/ev/train      → last held-out report + running flag
//   POST /api/ev/activate   → activate a version (body {version, confirm:'LIVE'})
//   GET  /api/ev/expectancy → live (slot-limited) vs paper-shadow expectancy

// ── Scores ─────────────────────────────────────────────────────────────────

export interface EvReason {
  feature: string;
  points: number;
}

export interface EvScore {
  pct?: number;               // 0-100 win-probability score
  probability?: number;       // 0-1 raw probability (fallback for pct)
  trained?: boolean;
  version?: string | number;
  top_reasons?: EvReason[];
}

export interface EvModelBadge {
  version?: string | number;
  trained?: boolean;
  n_trades?: number;
  min_clean?: number;
  floor_active?: boolean;
  note?: string;
}

export interface EvScoresPayload {
  available?: boolean;
  model?: EvModelBadge;
  scores?: Record<string, EvScore>;
  ranking?: string[];
  next_buy?: string | null;
}

// ── Model / versions ───────────────────────────────────────────────────────

export interface EvModelStatus {
  version?: string | number;
  trained?: boolean;
  n_trades?: number;
  min_clean?: number;
  floor_active?: boolean;
  running?: boolean;
  note?: string;
  [k: string]: unknown;
}

export interface EvVersion {
  version: string | number;
  trained?: boolean;
  n_trades?: number;
  created?: string | number;
  active?: boolean;
  report?: EvReport | null;
}

export interface EvCounts {
  total?: number;
  live?: number;
  paper_shadow?: number;
  wins?: number;
}

export interface EvModelPayload {
  available?: boolean;
  status?: EvModelStatus;
  versions?: EvVersion[];
  counts?: EvCounts;
}

// ── Train report ───────────────────────────────────────────────────────────

export interface EvReport {
  version?: string | number;
  n_trades?: number;
  n_train?: number;
  n_test?: number;
  auc?: number;
  accuracy?: number;
  brier?: number;
  log_loss?: number;
  finished?: string | number;
  note?: string;
  [k: string]: unknown;
}

export interface EvTrainPayload {
  available?: boolean;
  running?: boolean;
  report?: EvReport | null;
  started?: string | number;
  error?: string;
}

// ── Expectancy ─────────────────────────────────────────────────────────────

export interface EvExpectancyLeg {
  expectancy?: number;
  win_rate?: number;
  avg_win_r?: number;
  avg_loss_r?: number;
  n?: number;
  n_total?: number;
  open_positions?: number;
  trades_today?: number;
}

export interface EvExpectancyPayload {
  available?: boolean;
  live?: EvExpectancyLeg;
  paper_shadow?: EvExpectancyLeg;
}

// ── Generic defensive poller ───────────────────────────────────────────────

interface PollState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  /** 404 / not-deployed → treat as "model not available" rather than an error. */
  notFound: boolean;
}

function usePoll<T>(path: string, intervalMs: number, baseUrl = ''): PollState<T> & { refetch: () => void } {
  const [state, setState] = useState<PollState<T>>({
    data: null, error: null, loading: true, notFound: false,
  });
  const [tick, setTick] = useState(0);

  const run = useCallback(async (cancelledRef: { v: boolean }) => {
    try {
      const r = await fetch(`${baseUrl}${path}`, { cache: 'no-store' });
      if (r.status === 404) {
        if (!cancelledRef.v) setState({ data: null, error: null, loading: false, notFound: true });
        return;
      }
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = (await r.json()) as T;
      if (!cancelledRef.v) setState({ data: d, error: null, loading: false, notFound: false });
    } catch (e: unknown) {
      if (!cancelledRef.v) {
        setState(s => ({ ...s, error: (e as Error).message ?? 'fetch failed', loading: false }));
      }
    }
  }, [baseUrl, path]);

  useEffect(() => {
    const cancelledRef = { v: false };
    run(cancelledRef);
    const id = setInterval(() => run(cancelledRef), intervalMs);
    return () => { cancelledRef.v = true; clearInterval(id); };
  }, [run, intervalMs, tick]);

  const refetch = useCallback(() => { setTick(t => t + 1); }, []);
  return { ...state, refetch };
}

// A payload is "unavailable" when missing, explicitly available:false, or 404.
export function isUnavailable(p: { available?: boolean } | null, notFound: boolean): boolean {
  return notFound || p == null || p.available === false;
}

export function useEvScores(baseUrl = '') {
  return usePoll<EvScoresPayload>('/api/ev/scores', 5000, baseUrl);
}

export function useEvModel(baseUrl = '') {
  return usePoll<EvModelPayload>('/api/ev/model', 30000, baseUrl);
}

export function useEvExpectancy(baseUrl = '') {
  return usePoll<EvExpectancyPayload>('/api/ev/expectancy', 30000, baseUrl);
}

// Train-status poll runs fast (5s) but only while a retrain is running; the
// component supplies `active` so we don't hammer the endpoint at rest.
export function useEvTrainStatus(baseUrl = '', active = false) {
  const [data, setData] = useState<EvTrainPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notFound, setNotFound] = useState(false);

  const fetchOnce = useCallback(async () => {
    try {
      const r = await fetch(`${baseUrl}/api/ev/train`, { cache: 'no-store' });
      if (r.status === 404) { setNotFound(true); return; }
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = (await r.json()) as EvTrainPayload;
      setData(d); setError(null); setNotFound(false);
    } catch (e: unknown) {
      setError((e as Error).message ?? 'fetch failed');
    }
  }, [baseUrl]);

  useEffect(() => {
    let cancelled = false;
    const guarded = async () => { if (!cancelled) await fetchOnce(); };
    guarded();
    // Poll fast only while a train is active; otherwise a slow 30s refresh.
    const id = setInterval(guarded, active ? 5000 : 30000);
    return () => { cancelled = true; clearInterval(id); };
  }, [fetchOnce, active]);

  return { data, error, notFound, refetch: fetchOnce };
}

// ── Score → red-to-green colour scale ──────────────────────────────────────
// Reuses the app's gain/loss semantics: 0% ≈ loss-red, 100% ≈ gain-green, with
// a hue sweep through amber in the middle. Returns an hsl() string usable as an
// inline colour so the exact 0-100 value maps continuously (Tailwind classes
// only give a few buckets).
export function scoreColor(pct: number | null | undefined): string {
  if (pct == null || !isFinite(pct)) return 'hsl(var(--muted-foreground))';
  const clamped = Math.max(0, Math.min(100, pct));
  // 0 → hue 0 (red), 100 → hue 140 (green)
  const hue = (clamped / 100) * 140;
  return `hsl(${hue.toFixed(0)} 72% 50%)`;
}

export function pctOf(s: EvScore | undefined): number | null {
  if (!s) return null;
  if (typeof s.pct === 'number' && isFinite(s.pct)) return s.pct;
  if (typeof s.probability === 'number' && isFinite(s.probability)) return s.probability * 100;
  return null;
}
