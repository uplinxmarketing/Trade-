import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts';

// ── Types (tolerant — backend may still be deploying) ──────────────────────
interface BacktestResult {
  stats?: Record<string, unknown>;
  equity_curve?: Array<[number, number]>;
  trades?: Array<Record<string, unknown>>;
}

interface BacktestJob {
  status?: 'queued' | 'running' | 'done' | 'error' | string;
  progress_pct?: number;
  result?: BacktestResult;
  error?: string;
}

interface CoverageEntry {
  first_ms?: number;
  last_ms?: number;
  count?: number;
}

const num = (v: unknown): number => (typeof v === 'number' && isFinite(v) ? v : 0);

const isoDate = (d: Date) => d.toISOString().slice(0, 10);

const fmtStatValue = (v: unknown): string => {
  if (typeof v === 'number') {
    if (!isFinite(v)) return String(v);
    if (Number.isInteger(v)) return String(v);
    return Math.abs(v) >= 100 ? v.toFixed(1) : v.toFixed(2);
  }
  if (v === null || v === undefined) return '—';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
};

const statClass = (key: string, v: unknown): string => {
  if (typeof v !== 'number') return 'text-foreground';
  const k = key.toLowerCase();
  if (k.includes('pnl') || k.includes('profit') || k.includes('return') || k.includes('expectancy')) {
    return v > 0 ? 'text-gain' : v < 0 ? 'text-loss' : 'text-muted-foreground';
  }
  if (k.includes('drawdown') && v !== 0) return 'text-loss';
  return 'text-foreground';
};

// Columns we try first for the trades table; anything missing falls back to '—'.
const TRADE_COLUMNS: Array<{ key: string; aliases: string[]; label: string; align: 'left' | 'right' }> = [
  { key: 'symbol',     aliases: ['symbol', 'coin', 'pair'],                       label: 'SYMBOL', align: 'left' },
  { key: 'entry_ts',   aliases: ['entry_ts', 'entry_time', 'open_ts', 'ts'],      label: 'ENTRY',  align: 'right' },
  { key: 'entry_price',aliases: ['entry_price', 'entry', 'buy_price'],            label: 'IN',     align: 'right' },
  { key: 'exit_price', aliases: ['exit_price', 'exit', 'sell_price'],             label: 'OUT',    align: 'right' },
  { key: 'net_pnl',    aliases: ['net_pnl', 'pnl', 'profit', 'net'],              label: 'PNL',    align: 'right' },
  { key: 'exit_label', aliases: ['exit_label', 'label', 'exit_reason', 'reason'], label: 'EXIT',   align: 'right' },
];

const pickField = (trade: Record<string, unknown>, aliases: string[]): unknown => {
  for (const a of aliases) {
    if (trade[a] !== undefined && trade[a] !== null) return trade[a];
  }
  return undefined;
};

const fmtTradeCell = (key: string, v: unknown): string => {
  if (v === undefined || v === null) return '—';
  if (key === 'entry_ts' && typeof v === 'number') {
    const ms = v > 1e12 ? v : v * 1000;
    const d = new Date(ms);
    return `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
  }
  if (typeof v === 'number') return Number.isInteger(v) ? String(v) : v.toFixed(v < 1 && v > -1 ? 6 : 4);
  return String(v);
};

function tradesToCsv(trades: Array<Record<string, unknown>>): string {
  const keys = Array.from(new Set(trades.flatMap(t => Object.keys(t))));
  const esc = (v: unknown): string => {
    if (v === null || v === undefined) return '';
    const s = typeof v === 'object' ? JSON.stringify(v) : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [keys.join(',')];
  for (const t of trades) lines.push(keys.map(k => esc(t[k])).join(','));
  return lines.join('\n');
}

export function BacktestPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const defaults = useMemo(() => {
    const end = new Date();
    const start = new Date(end.getTime() - 90 * 24 * 3600 * 1000);
    return { start: isoDate(start), end: isoDate(end) };
  }, []);

  const [start, setStart] = useState(defaults.start);
  const [end, setEnd] = useState(defaults.end);
  const [running, setRunning] = useState(false);
  const [job, setJob] = useState<BacktestJob | null>(null);
  const [runError, setRunError] = useState<string | null>(null);
  const [coverage, setCoverage] = useState<{ symbols: number; oldest: number | null } | null>(null);
  const [coverageError, setCoverageError] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
  }, []);

  useEffect(() => stopPolling, [stopPolling]);

  // ── Coverage hint ────────────────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${baseUrl}/api/klines/coverage?symbols=approved`, { cache: 'no-store' });
        if (!res.ok) throw new Error(`http ${res.status}`);
        const data = await res.json();
        if (data && typeof data === 'object' && 'error' in data && data.error) throw new Error(String(data.error));
        // Shape is per-symbol; entries may be {first_ms,...} directly or keyed by interval ('1m'/'5m').
        const perSymbol: Record<string, unknown> =
          (data && typeof data === 'object' && !Array.isArray(data))
            ? ((data as any).coverage ?? (data as any).symbols ?? data)
            : {};
        let symbols = 0;
        let oldest: number | null = null;
        for (const v of Object.values(perSymbol)) {
          if (!v || typeof v !== 'object') continue;
          const entries: CoverageEntry[] = ('first_ms' in (v as any) || 'count' in (v as any))
            ? [v as CoverageEntry]
            : Object.values(v as Record<string, CoverageEntry>).filter(e => e && typeof e === 'object');
          const hasData = entries.some(e => num(e?.count) > 0);
          if (!hasData) continue;
          symbols += 1;
          for (const e of entries) {
            const f = num(e?.first_ms);
            if (f > 0 && (oldest === null || f < oldest)) oldest = f;
          }
        }
        if (!cancelled) { setCoverage({ symbols, oldest }); setCoverageError(false); }
      } catch {
        if (!cancelled) setCoverageError(true);
      }
    })();
    return () => { cancelled = true; };
  }, [baseUrl]);

  // ── Run + poll ───────────────────────────────────────────────────────────
  const pollJob = useCallback((jobId: string) => {
    stopPolling();
    pollRef.current = setInterval(async () => {
      try {
        const res = await fetch(`${baseUrl}/api/backtest/${jobId}`, { cache: 'no-store' });
        if (!res.ok) throw new Error(`http ${res.status}`);
        const data: BacktestJob = await res.json();
        if (data && typeof data === 'object' && 'error' in data && data.error && !data.status) {
          throw new Error(String(data.error));
        }
        setJob(data);
        if (data.status === 'done' || data.status === 'error') {
          stopPolling();
          setRunning(false);
          if (data.status === 'error') setRunError(data.error ?? 'backtest failed');
        }
      } catch (e: any) {
        stopPolling();
        setRunning(false);
        setRunError(e?.message ?? 'lost contact with backtest job');
      }
    }, 2000);
  }, [baseUrl, stopPolling]);

  const run = useCallback(async () => {
    setRunning(true);
    setRunError(null);
    setJob(null);
    try {
      const res = await fetch(`${baseUrl}/api/backtest`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ start, end, symbols: 'approved' }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || (data && data.error)) {
        throw new Error(data?.error ?? `backtest endpoint unavailable (http ${res.status})`);
      }
      if (!data.job_id) throw new Error('no job_id returned');
      setJob({ status: 'queued', progress_pct: 0 });
      pollJob(String(data.job_id));
    } catch (e: any) {
      setRunning(false);
      setRunError(e?.message ?? 'failed to start backtest');
    }
  }, [baseUrl, start, end, pollJob]);

  const exportCsv = useCallback(() => {
    const trades = job?.result?.trades ?? [];
    if (trades.length === 0) return;
    const blob = new Blob([tradesToCsv(trades)], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `backtest_trades_${start}_${end}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }, [job, start, end]);

  const result = job?.result;
  const stats = result?.stats ?? {};
  const statEntries = Object.entries(stats).filter(([, v]) => typeof v !== 'object' || v === null);
  const equityData = (result?.equity_curve ?? [])
    .filter(p => Array.isArray(p) && p.length >= 2)
    .map(([ts, equity]) => ({ ts: num(ts), equity: num(equity) }));
  const trades = result?.trades ?? [];
  const shownTrades = trades.slice(-50);

  return (
    <div className="space-y-3">
      {/* Controls */}
      <div className="flex items-end gap-2 flex-wrap">
        <div>
          <p className="text-[8px] uppercase tracking-wider text-muted-foreground mb-0.5">Start</p>
          <input
            type="date" value={start} max={end}
            onChange={e => setStart(e.target.value)}
            className="text-[9px] bg-background border border-border rounded px-2 py-1 font-mono outline-none focus:border-accent [color-scheme:dark]"
          />
        </div>
        <div>
          <p className="text-[8px] uppercase tracking-wider text-muted-foreground mb-0.5">End</p>
          <input
            type="date" value={end} min={start}
            onChange={e => setEnd(e.target.value)}
            className="text-[9px] bg-background border border-border rounded px-2 py-1 font-mono outline-none focus:border-accent [color-scheme:dark]"
          />
        </div>
        <div>
          <p className="text-[8px] uppercase tracking-wider text-muted-foreground mb-0.5">Symbols</p>
          <span className="inline-block text-[9px] font-mono px-2 py-1 border border-border rounded bg-muted/20 text-muted-foreground">
            approved
          </span>
        </div>
        <button
          onClick={run}
          disabled={running || !start || !end}
          className="text-[9px] font-semibold px-3 py-1 rounded border border-accent/40 text-accent hover:bg-accent/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
        >
          {running ? 'Running…' : 'Run backtest'}
        </button>
      </div>

      {/* Coverage hint */}
      {coverageError ? (
        <p className="text-[9px] text-muted-foreground/70 italic">
          Backfill coverage unavailable — endpoint not deployed yet
        </p>
      ) : coverage ? (
        <p className="text-[9px] text-muted-foreground">
          Backfill coverage: {coverage.symbols} symbol{coverage.symbols === 1 ? '' : 's'} with data
          {coverage.oldest ? `, oldest ${new Date(coverage.oldest).toLocaleDateString()}` : ''}.
          {coverage.symbols === 0 || (coverage.oldest && coverage.oldest > new Date(start).getTime()) ? (
            <span className="text-orange-400"> Coverage looks thin for this range — run backfill.py on the server first.</span>
          ) : null}
        </p>
      ) : null}

      {/* Progress / errors */}
      {running && job && (
        <div>
          <div className="flex items-center justify-between mb-0.5">
            <p className="text-[9px] text-muted-foreground capitalize">{job.status ?? 'queued'}…</p>
            <span className="text-[9px] font-mono text-accent">{num(job.progress_pct).toFixed(0)}%</span>
          </div>
          <div className="h-1.5 bg-muted/20 rounded overflow-hidden">
            <div className="h-full bg-accent/70 rounded transition-all" style={{ width: `${Math.min(100, num(job.progress_pct))}%` }} />
          </div>
        </div>
      )}
      {runError && <p className="text-[9px] text-loss">{runError}</p>}

      {/* Results */}
      {job?.status === 'done' && result && (
        <div className="space-y-3">
          {/* Stats grid */}
          {statEntries.length > 0 && (
            <div className="grid grid-cols-3 sm:grid-cols-4 gap-1.5">
              {statEntries.map(([k, v]) => (
                <div key={k} className="border border-border rounded bg-muted/10 px-2 py-1.5 min-w-0">
                  <p className="text-[8px] uppercase tracking-wider text-muted-foreground truncate" title={k}>
                    {k.replace(/_/g, ' ')}
                  </p>
                  <p className={`text-xs font-mono font-semibold ${statClass(k, v)}`}>{fmtStatValue(v)}</p>
                </div>
              ))}
            </div>
          )}

          {/* Equity curve */}
          {equityData.length > 1 && (
            <div>
              <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground mb-1">Equity curve</p>
              <div className="h-40 border border-border rounded bg-muted/10 p-1">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={equityData} margin={{ top: 6, right: 8, bottom: 0, left: 0 }}>
                    <CartesianGrid strokeDasharray="2 4" stroke="hsl(var(--border))" strokeOpacity={0.4} />
                    <XAxis
                      dataKey="ts" type="number" domain={['dataMin', 'dataMax']}
                      tickFormatter={(t: number) => new Date(t > 1e12 ? t : t * 1000).toLocaleDateString(undefined, { month: 'numeric', day: 'numeric' })}
                      tick={{ fontSize: 8, fill: 'hsl(var(--muted-foreground))' }}
                      stroke="hsl(var(--border))" tickLine={false}
                    />
                    <YAxis
                      domain={['auto', 'auto']} width={44}
                      tickFormatter={(v: number) => v.toFixed(0)}
                      tick={{ fontSize: 8, fill: 'hsl(var(--muted-foreground))' }}
                      stroke="hsl(var(--border))" tickLine={false}
                    />
                    <Tooltip
                      contentStyle={{
                        background: 'hsl(var(--background))',
                        border: '1px solid hsl(var(--border))',
                        borderRadius: 4, fontSize: 9, padding: '4px 8px',
                      }}
                      labelStyle={{ color: 'hsl(var(--muted-foreground))', fontSize: 8 }}
                      labelFormatter={(t: number) => new Date(t > 1e12 ? t : t * 1000).toLocaleString()}
                      formatter={(v: number) => [v.toFixed(2), 'equity']}
                    />
                    <Line
                      type="monotone" dataKey="equity" dot={false} strokeWidth={1.5}
                      stroke={equityData[equityData.length - 1].equity >= equityData[0].equity
                        ? 'hsl(var(--gain))' : 'hsl(var(--loss))'}
                      isAnimationActive={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {/* Trades table */}
          <div>
            <div className="flex items-center justify-between mb-1">
              <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
                Trades ({trades.length}{trades.length > 50 ? `, showing last ${shownTrades.length}` : ''})
              </p>
              <button
                onClick={exportCsv}
                disabled={trades.length === 0}
                className="text-[9px] font-semibold px-2 py-0.5 rounded border border-border text-muted-foreground hover:border-accent/50 hover:text-accent disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                Export CSV ({trades.length})
              </button>
            </div>
            {shownTrades.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/60 italic">no trades in result</p>
            ) : (
              <div className="overflow-x-auto max-h-64 overflow-y-auto scrollbar-thin">
                <div className="grid grid-cols-[4.5rem_5.5rem_4.5rem_4.5rem_4rem_4rem] gap-x-2 pb-0.5 border-b border-border/60 min-w-[28rem] sticky top-0 bg-background">
                  {TRADE_COLUMNS.map(c => (
                    <span key={c.key} className={`text-[8px] text-muted-foreground font-semibold ${c.align === 'right' ? 'text-right' : ''}`}>
                      {c.label}
                    </span>
                  ))}
                </div>
                {shownTrades.map((t, i) => {
                  const pnl = num(pickField(t, ['net_pnl', 'pnl', 'profit', 'net']));
                  return (
                    <div key={i} className="grid grid-cols-[4.5rem_5.5rem_4.5rem_4.5rem_4rem_4rem] gap-x-2 py-0.5 border-b border-border/20 min-w-[28rem]">
                      {TRADE_COLUMNS.map(c => {
                        const v = pickField(t, c.aliases);
                        const cls = c.key === 'net_pnl'
                          ? (pnl > 0 ? 'text-gain' : pnl < 0 ? 'text-loss' : 'text-muted-foreground')
                          : c.key === 'symbol' ? 'text-foreground' : 'text-muted-foreground';
                        return (
                          <span key={c.key} className={`text-[9px] font-mono truncate ${cls} ${c.align === 'right' ? 'text-right' : ''}`}>
                            {c.key === 'net_pnl' && v !== undefined ? `${pnl > 0 ? '+' : ''}${pnl.toFixed(2)}` : fmtTradeCell(c.key, v)}
                          </span>
                        );
                      })}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
