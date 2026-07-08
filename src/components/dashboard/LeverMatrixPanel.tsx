import { useCallback, useEffect, useRef, useState } from 'react';

// ── L3 — edge-report (lever matrix) view ────────────────────────────────────
// Ranked variants table: variant/label, expectancy, PF, max drawdown, trade
// count, confidence — with the baseline row pinned on top and each variant's
// delta vs baseline shown. A "Run lever matrix" button POSTs
// /api/backtest/lever-matrix?months=3, disables while running, and shows a
// "running… (may take minutes)" note. The GET is polled while running. Coded
// defensively — before the first run the backend returns {available:false} or
// an empty payload, which renders a muted state.

interface Metrics {
  expectancy?: number;
  profit_factor?: number;
  max_drawdown?: number;
  trade_count?: number;
  win_rate?: number;
}

interface Variant extends Metrics {
  variant?: string;
  label?: string;
  confidence?: number | string;
}

interface LeverMatrix {
  available?: boolean;
  running?: boolean;
  last_run_ts?: number;
  generated_ts?: number;
  window_months?: number;
  symbols_n?: number;
  baseline?: Metrics;
  variants?: Variant[];
  notes?: string[];
}

const POLL_MS = 8_000;
const num = (v: unknown): number => (typeof v === 'number' && isFinite(v) ? v : 0);

const fmtExp = (v: unknown) => {
  const n = num(v);
  return `${n > 0 ? '+' : ''}${n.toFixed(2)}`;
};
const fmtPf = (v: unknown) => num(v).toFixed(2);
const fmtDd = (v: unknown) => `${num(v).toFixed(1)}%`;
const pnlClass = (v: unknown) =>
  num(v) > 0 ? 'text-gain' : num(v) < 0 ? 'text-loss' : 'text-muted-foreground';

const fmtTs = (ts: unknown) => {
  const n = num(ts);
  if (!n) return '—';
  return new Date(n > 1e12 ? n : n * 1000).toLocaleString();
};

const fmtConfidence = (c: unknown): string => {
  if (typeof c === 'string') return c;
  const n = num(c);
  if (!n) return '—';
  // Treat 0–1 as a fraction, otherwise a raw score.
  return n <= 1 ? `${(n * 100).toFixed(0)}%` : n.toFixed(2);
};

// Signed delta vs baseline (variant − baseline), with colour.
function Delta({ value, digits = 2, invert = false }: { value: number; digits?: number; invert?: boolean }) {
  if (!isFinite(value) || value === 0) {
    return <span className="text-[8px] text-muted-foreground/60">·</span>;
  }
  // invert=true → lower is better (e.g. drawdown), so a negative delta is good.
  const good = invert ? value < 0 : value > 0;
  return (
    <span className={`text-[8px] ${good ? 'text-gain' : 'text-loss'}`}>
      {value > 0 ? '+' : ''}{value.toFixed(digits)}
    </span>
  );
}

const COLS = 'grid grid-cols-[minmax(6rem,1.6fr)_4rem_3rem_4rem_3rem_3.5rem] gap-x-2';

export function useLeverMatrix(baseUrl = '') {
  const [matrix, setMatrix] = useState<LeverMatrix | null>(null);
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${baseUrl}/api/backtest/lever-matrix`, { cache: 'no-store' });
      if (!r.ok) throw new Error(`http ${r.status}`);
      const d = await r.json();
      setMatrix(d as LeverMatrix);
      setError(false);
      return d as LeverMatrix;
    } catch {
      setError(true);
      return null;
    } finally {
      setLoading(false);
    }
  }, [baseUrl]);

  const running = matrix?.running === true;

  // Poll continuously while a run is in flight; otherwise a slow idle poll so a
  // run kicked off elsewhere is still noticed.
  useEffect(() => {
    load();
    const interval = running ? POLL_MS : 60_000;
    const id = setInterval(load, interval);
    return () => clearInterval(id);
  }, [load, running]);

  return { matrix, error, loading, running, reload: load };
}

export function LeverMatrixPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const { matrix, error, loading, running, reload } = useLeverMatrix(baseUrl);
  const [starting, setStarting] = useState(false);
  const [runMsg, setRunMsg] = useState<string | null>(null);
  const mounted = useRef(true);
  useEffect(() => () => { mounted.current = false; }, []);

  const runMatrix = useCallback(async () => {
    setStarting(true);
    setRunMsg(null);
    try {
      const r = await fetch(`${baseUrl}/api/backtest/lever-matrix?months=3`, { method: 'POST' });
      if (r.status === 409) {
        setRunMsg('Already running');
      } else if (!r.ok) {
        throw new Error(`http ${r.status}`);
      } else {
        setRunMsg('Started');
      }
      await reload();
    } catch {
      if (mounted.current) setRunMsg('Failed to start');
    } finally {
      if (mounted.current) setStarting(false);
    }
  }, [baseUrl, reload]);

  const unavailable = (!matrix || matrix.available === false || error) && !loading;
  const baseline = matrix?.baseline ?? {};
  const variants = matrix?.variants ?? [];
  const notes = matrix?.notes ?? [];
  const runBtnDisabled = starting || running;

  // Rank variants by expectancy (best first).
  const ranked = [...variants].sort((a, b) => num(b.expectancy) - num(a.expectancy));

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between flex-wrap gap-1">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
          Lever Matrix (edge report)
        </p>
        <button
          onClick={runMatrix}
          disabled={runBtnDisabled}
          className="text-[9px] font-semibold px-2 py-0.5 rounded border border-accent/40 text-accent hover:bg-accent/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          {running ? 'Running…' : starting ? 'Starting…' : 'Run lever matrix'}
        </button>
      </div>

      {/* Run status / meta line */}
      <div className="flex items-center justify-between flex-wrap gap-x-2 gap-y-0.5">
        <p className="text-[8px] text-muted-foreground">
          {matrix?.generated_ts
            ? <>generated {fmtTs(matrix.generated_ts)}</>
            : matrix?.last_run_ts
              ? <>last run {fmtTs(matrix.last_run_ts)}</>
              : 'not yet run'}
          {matrix?.window_months != null && <> · {num(matrix.window_months)}mo window</>}
          {matrix?.symbols_n != null && <> · {num(matrix.symbols_n)} symbols</>}
        </p>
        {runMsg && <span className="text-[8px] text-muted-foreground/70">{runMsg}</span>}
      </div>

      {running && (
        <p className="text-[9px] text-accent">running… (may take minutes)</p>
      )}

      {loading && !matrix ? (
        <p className="text-[9px] text-muted-foreground py-1">Loading lever matrix…</p>
      ) : unavailable ? (
        <p className="text-[9px] text-muted-foreground/70 italic py-1">
          Lever matrix unavailable — run it or wait for the backend to deploy
        </p>
      ) : (
        <>
          <div className="overflow-x-auto">
            {/* Header */}
            <div className={`${COLS} pb-0.5 border-b border-border/60 min-w-[24rem]`}>
              <span className="text-[8px] text-muted-foreground font-semibold">VARIANT</span>
              <span className="text-[8px] text-muted-foreground text-right">EXP</span>
              <span className="text-[8px] text-muted-foreground text-right">PF</span>
              <span className="text-[8px] text-muted-foreground text-right">MAX DD</span>
              <span className="text-[8px] text-muted-foreground text-right">TRADES</span>
              <span className="text-[8px] text-muted-foreground text-right">CONF</span>
            </div>

            {/* Baseline row — pinned on top */}
            <div className={`${COLS} py-0.5 border-b border-border/40 items-center bg-muted/20 min-w-[24rem]`}>
              <span className="text-[9px] font-mono font-semibold text-foreground truncate">baseline</span>
              <span className={`text-[9px] font-mono text-right ${pnlClass(baseline.expectancy)}`}>{fmtExp(baseline.expectancy)}</span>
              <span className="text-[9px] font-mono text-right text-muted-foreground">{fmtPf(baseline.profit_factor)}</span>
              <span className="text-[9px] font-mono text-right text-loss">{fmtDd(baseline.max_drawdown)}</span>
              <span className="text-[9px] font-mono text-right text-muted-foreground">{num(baseline.trade_count)}</span>
              <span className="text-[9px] font-mono text-right text-muted-foreground/60">—</span>
            </div>

            {/* Variant rows with deltas vs baseline */}
            {ranked.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/60 italic py-1">no variants in report</p>
            ) : (
              ranked.map((v, i) => {
                const dExp = num(v.expectancy) - num(baseline.expectancy);
                const dPf = num(v.profit_factor) - num(baseline.profit_factor);
                const dDd = num(v.max_drawdown) - num(baseline.max_drawdown);
                return (
                  <div key={v.variant ?? v.label ?? i} className={`${COLS} py-0.5 border-b border-border/20 items-center min-w-[24rem]`}>
                    <span className="text-[9px] font-mono text-foreground truncate" title={v.variant ?? v.label}>
                      {v.label ?? v.variant ?? `variant ${i + 1}`}
                    </span>
                    <span className={`text-[9px] font-mono text-right ${pnlClass(v.expectancy)}`}>
                      {fmtExp(v.expectancy)}
                      <span className="block leading-none"><Delta value={dExp} /></span>
                    </span>
                    <span className="text-[9px] font-mono text-right text-muted-foreground">
                      {fmtPf(v.profit_factor)}
                      <span className="block leading-none"><Delta value={dPf} /></span>
                    </span>
                    <span className="text-[9px] font-mono text-right text-loss">
                      {fmtDd(v.max_drawdown)}
                      <span className="block leading-none"><Delta value={dDd} digits={1} invert /></span>
                    </span>
                    <span className="text-[9px] font-mono text-right text-muted-foreground">{num(v.trade_count)}</span>
                    <span className="text-[9px] font-mono text-right text-muted-foreground">{fmtConfidence(v.confidence)}</span>
                  </div>
                );
              })
            )}
          </div>

          {/* Notes — e.g. unsupported levers */}
          {notes.length > 0 && (
            <div className="border-t border-border/40 pt-1 space-y-0.5">
              {notes.map((n, i) => (
                <p key={i} className="text-[8px] text-muted-foreground/80 leading-tight break-words">• {n}</p>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default LeverMatrixPanel;
