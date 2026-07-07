import { useState, useEffect, useCallback } from 'react';
import { isLowSample, LowSampleNA } from '@/lib/sampleGuard';

// ── Types (tolerant — backend may still be deploying) ──────────────────────
interface SymbolRow {
  symbol: string;
  trades: number;
  net_pnl: number;
  win_rate: number;
  low_sample?: boolean;
}

interface ExitLabelStats {
  count: number;
  net_pnl: number;
}

interface ExpectancyStats {
  trades: number;
  win_rate: number;
  avg_win: number;
  avg_loss: number;
  expectancy_per_trade: number;
  profit_factor: number;
  total_fees: number;
  fee_share_of_gross: number;
  avg_hold_time_sec: number;
  per_symbol: SymbolRow[];
  exit_labels: Record<string, ExitLabelStats>;
  low_sample?: boolean;
}

interface SignalSide {
  trades?: number;
  expectancy?: number;
  win_rate?: number;
}

interface SignalAttribution {
  fired?: SignalSide;
  not_fired?: SignalSide;
  lift?: number;
  low_sample?: boolean;
}

interface VetoAttribution {
  blocked?: number;
  hypothetical_expectancy?: number;
  saved_usdt?: number;
}

interface AttributionReport {
  ts?: number;
  report?: {
    signals?: Record<string, SignalAttribution>;
    vetoes?: Record<string, VetoAttribution>;
  };
}

type Mode = 'paper' | 'live' | 'all';
const MODES: Mode[] = ['paper', 'live', 'all'];
const DAY_OPTIONS = [7, 30, 90];

// ── Formatting helpers ─────────────────────────────────────────────────────
const num = (v: unknown): number => (typeof v === 'number' && isFinite(v) ? v : 0);

const fmtUsd = (v: unknown) => {
  const n = num(v);
  const sign = n > 0 ? '+' : '';
  return `${sign}${n.toFixed(2)}`;
};

const fmtPct = (v: unknown, digits = 1) => `${(num(v) * 100).toFixed(digits)}%`;

const fmtDuration = (sec: unknown) => {
  const s = num(sec);
  if (s <= 0) return '—';
  if (s < 60) return `${s.toFixed(0)}s`;
  if (s < 3600) return `${(s / 60).toFixed(0)}m`;
  return `${(s / 3600).toFixed(1)}h`;
};

const pnlClass = (v: unknown) =>
  num(v) > 0 ? 'text-gain' : num(v) < 0 ? 'text-loss' : 'text-muted-foreground';

// Exit-label display order — 'recycler' visibility is a spec goal, keep it prominent.
const EXIT_LABEL_ORDER = ['tp', 'sl', 'trail', 'recycler', 'force', 'manual', 'delist', 'unlabeled'];

const EXIT_LABEL_TITLES: Record<string, string> = {
  tp: 'Take profit',
  sl: 'Stop loss',
  trail: 'Trailing stop',
  recycler: 'Recycler channel exit',
  force: 'Forced exit',
  manual: 'Manual close',
  delist: 'Delisting exit',
  unlabeled: 'No exit label recorded',
};

function StatCard({ label, value, sub, valueClass = 'text-foreground' }: {
  label: string;
  value: string;
  sub?: string;
  valueClass?: string;
}) {
  return (
    <div className="border border-border rounded bg-muted/10 px-2 py-1.5 min-w-0">
      <p className="text-[8px] uppercase tracking-wider text-muted-foreground truncate">{label}</p>
      <p className={`text-xs font-mono font-semibold ${valueClass}`}>{value}</p>
      {sub && <p className="text-[8px] text-muted-foreground truncate">{sub}</p>}
    </div>
  );
}

function Unavailable({ what }: { what: string }) {
  return (
    <p className="text-[9px] text-muted-foreground/70 italic py-1">
      {what} unavailable — endpoint not deployed yet
    </p>
  );
}

export function AnalyticsPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const [mode, setMode] = useState<Mode>('all');
  const [days, setDays] = useState(30);
  // When on, append ?since_deploy=1 so expectancy reflects only post-fix trades.
  const [sinceDeploy, setSinceDeploy] = useState(false);

  const [stats, setStats] = useState<ExpectancyStats | null>(null);
  const [statsError, setStatsError] = useState(false);
  const [statsLoading, setStatsLoading] = useState(true);

  const [attribution, setAttribution] = useState<AttributionReport | null>(null);
  const [attribError, setAttribError] = useState(false);
  const [attribLoading, setAttribLoading] = useState(true);
  const [rebuilding, setRebuilding] = useState(false);

  // ── Expectancy (polled every 30s) ────────────────────────────────────────
  const loadStats = useCallback(async () => {
    try {
      const res = await fetch(
        `${baseUrl}/api/stats/expectancy?days=${days}&mode=${mode}${sinceDeploy ? '&since_deploy=1' : ''}`,
        { cache: 'no-store' },
      );
      if (!res.ok) throw new Error(`http ${res.status}`);
      const data = await res.json();
      if (data && typeof data === 'object' && 'error' in data && data.error) throw new Error(String(data.error));
      setStats(data as ExpectancyStats);
      setStatsError(false);
    } catch {
      setStatsError(true);
    } finally {
      setStatsLoading(false);
    }
  }, [baseUrl, days, mode, sinceDeploy]);

  useEffect(() => {
    setStatsLoading(true);
    loadStats();
    const t = setInterval(loadStats, 30_000);
    return () => clearInterval(t);
  }, [loadStats]);

  // ── Attribution ──────────────────────────────────────────────────────────
  const loadAttribution = useCallback(async (rebuild = false) => {
    if (rebuild) setRebuilding(true);
    try {
      const res = await fetch(`${baseUrl}/api/stats/attribution${rebuild ? '?rebuild=1' : ''}`, { cache: 'no-store' });
      if (!res.ok) throw new Error(`http ${res.status}`);
      const data = await res.json();
      if (data && typeof data === 'object' && 'error' in data && data.error) throw new Error(String(data.error));
      setAttribution(data as AttributionReport);
      setAttribError(false);
    } catch {
      setAttribError(true);
    } finally {
      setAttribLoading(false);
      if (rebuild) setRebuilding(false);
    }
  }, [baseUrl]);

  useEffect(() => { loadAttribution(); }, [loadAttribution]);

  const feeShare = num(stats?.fee_share_of_gross);
  const feeShareHigh = feeShare > 0.30;

  const exitLabels: Array<[string, ExitLabelStats]> = stats?.exit_labels
    ? Object.entries(stats.exit_labels)
        .sort(([a], [b]) => {
          const ia = EXIT_LABEL_ORDER.indexOf(a);
          const ib = EXIT_LABEL_ORDER.indexOf(b);
          return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib);
        })
    : [];
  const maxExitCount = Math.max(1, ...exitLabels.map(([, v]) => num(v?.count)));

  const perSymbol = (stats?.per_symbol ?? [])
    .slice()
    .sort((a, b) => num(b?.net_pnl) - num(a?.net_pnl))
    .slice(0, 12);

  const signals: Array<[string, SignalAttribution]> = attribution?.report?.signals
    ? Object.entries(attribution.report.signals)
    : [];
  const vetoes: Array<[string, VetoAttribution]> = attribution?.report?.vetoes
    ? Object.entries(attribution.report.vetoes)
    : [];

  return (
    <div className="space-y-3">
      {/* Mode / days selectors */}
      <div className="flex items-center justify-between flex-wrap gap-1">
        <div className="flex gap-1">
          {MODES.map(m => (
            <button key={m} onClick={() => setMode(m)}
              className={`text-[8px] px-1.5 py-0.5 border rounded capitalize transition-colors ${
                mode === m ? 'border-accent text-accent bg-accent/10' : 'border-border text-muted-foreground hover:border-accent/50'
              }`}>
              {m}
            </button>
          ))}
        </div>
        <div className="flex gap-1 items-center">
          <button
            onClick={() => setSinceDeploy(v => !v)}
            title="Show expectancy for trades since the last deploy only (post-fix). Ignored gracefully if the backend doesn't support it."
            className={`text-[8px] px-1.5 py-0.5 border rounded transition-colors ${
              sinceDeploy ? 'border-accent text-accent bg-accent/10' : 'border-border text-muted-foreground hover:border-accent/50'
            }`}>
            Since deploy
          </button>
          {DAY_OPTIONS.map(d => (
            <button key={d} onClick={() => setDays(d)}
              disabled={sinceDeploy}
              className={`text-[8px] px-1.5 py-0.5 border rounded transition-colors disabled:opacity-40 ${
                days === d ? 'border-accent text-accent bg-accent/10' : 'border-border text-muted-foreground hover:border-accent/50'
              }`}>
              {d}d
            </button>
          ))}
        </div>
      </div>

      {/* Expectancy cards */}
      {statsLoading && !stats ? (
        <p className="text-[9px] text-muted-foreground py-1">Loading expectancy…</p>
      ) : statsError && !stats ? (
        <Unavailable what="Expectancy stats" />
      ) : stats ? (
        <>
          <div className="grid grid-cols-3 sm:grid-cols-6 gap-1.5">
            <StatCard label="Win rate"
              value={isLowSample(num(stats.trades), stats.low_sample) ? `N/A (n=${num(stats.trades)})` : fmtPct(stats.win_rate)}
              valueClass={isLowSample(num(stats.trades), stats.low_sample) ? 'text-muted-foreground/50' : 'text-foreground'}
              sub={`${num(stats.trades)} trades`} />
            <StatCard label="Avg win" value={fmtUsd(stats.avg_win)} valueClass="text-gain" />
            <StatCard label="Avg loss" value={fmtUsd(stats.avg_loss)} valueClass="text-loss" />
            <StatCard label="Profit factor" value={num(stats.profit_factor).toFixed(2)}
              valueClass={num(stats.profit_factor) >= 1 ? 'text-gain' : 'text-loss'} />
            <StatCard label="Expectancy / trade" value={fmtUsd(stats.expectancy_per_trade)}
              valueClass={pnlClass(stats.expectancy_per_trade)}
              sub={`hold ${fmtDuration(stats.avg_hold_time_sec)}`} />
            <StatCard label="Fees % of gross" value={fmtPct(stats.fee_share_of_gross)}
              valueClass={feeShareHigh ? 'text-loss' : 'text-foreground'}
              sub={`${num(stats.total_fees).toFixed(2)} USDT`} />
          </div>
          {feeShareHigh && (
            <p className="text-[9px] text-loss">
              Fees are eating {fmtPct(feeShare)} of gross profit — position sizes may be too small.
            </p>
          )}

          {/* Exit-label breakdown */}
          <div>
            <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground mb-1">
              Exit labels
            </p>
            {exitLabels.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/60 italic">no labeled exits yet</p>
            ) : (
              <div className="space-y-1">
                {exitLabels.map(([label, v]) => {
                  const count = num(v?.count);
                  const pnl = num(v?.net_pnl);
                  const isRecycler = label === 'recycler';
                  return (
                    <div key={label} className="flex items-center gap-2" title={EXIT_LABEL_TITLES[label] ?? label}>
                      <span className={`w-16 shrink-0 text-[9px] font-mono ${
                        isRecycler ? 'text-accent font-semibold' : 'text-muted-foreground'
                      }`}>
                        {label}{isRecycler ? ' ♻' : ''}
                      </span>
                      <div className="flex-1 h-2.5 bg-muted/20 rounded overflow-hidden">
                        <div
                          className={`h-full rounded ${pnl > 0 ? 'bg-gain/60' : pnl < 0 ? 'bg-loss/60' : 'bg-muted/60'}`}
                          style={{ width: `${Math.max(2, (count / maxExitCount) * 100)}%` }}
                        />
                      </div>
                      <span className="w-8 shrink-0 text-right text-[9px] font-mono text-muted-foreground">{count}</span>
                      <span className={`w-16 shrink-0 text-right text-[9px] font-mono ${pnlClass(pnl)}`}>
                        {fmtUsd(pnl)}
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* Per-symbol table */}
          <div>
            <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground mb-1">
              Per symbol
            </p>
            {perSymbol.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/60 italic">no closed trades in window</p>
            ) : (
              <div className="overflow-x-auto">
                <div className="grid grid-cols-[5rem_3rem_1fr_3.5rem] gap-x-2 pb-0.5 border-b border-border/60">
                  <span className="text-[8px] text-muted-foreground font-semibold">SYMBOL</span>
                  <span className="text-[8px] text-muted-foreground text-right">TRADES</span>
                  <span className="text-[8px] text-muted-foreground text-right">NET PNL</span>
                  <span className="text-[8px] text-muted-foreground text-right">WIN %</span>
                </div>
                {perSymbol.map(row => (
                  <div key={row.symbol} className="grid grid-cols-[5rem_3rem_1fr_3.5rem] gap-x-2 py-0.5 border-b border-border/20">
                    <span className="text-[9px] font-mono text-foreground truncate">{row.symbol}</span>
                    <span className="text-[9px] font-mono text-muted-foreground text-right">{num(row.trades)}</span>
                    <span className={`text-[9px] font-mono text-right ${pnlClass(row.net_pnl)}`}>{fmtUsd(row.net_pnl)}</span>
                    <span className="text-[9px] font-mono text-muted-foreground text-right">
                      {isLowSample(num(row.trades), row.low_sample)
                        ? <LowSampleNA count={num(row.trades)} className="text-muted-foreground/50 font-mono" />
                        : fmtPct(row.win_rate, 0)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </>
      ) : null}

      {/* Attribution */}
      <div className="border-t border-border/50 pt-2 space-y-2">
        <div className="flex items-center justify-between">
          <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
            Signal attribution
            {attribution?.ts ? (
              <span className="ml-2 normal-case tracking-normal font-normal text-muted-foreground/60">
                built {new Date(num(attribution.ts) * (num(attribution.ts) > 1e12 ? 1 : 1000)).toLocaleString()}
              </span>
            ) : null}
          </p>
          <button
            onClick={() => loadAttribution(true)}
            disabled={rebuilding}
            className="text-[9px] font-semibold px-2 py-0.5 rounded border border-accent/40 text-accent hover:bg-accent/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            {rebuilding ? 'Rebuilding…' : 'Rebuild'}
          </button>
        </div>

        {attribLoading ? (
          <p className="text-[9px] text-muted-foreground py-1">Loading attribution…</p>
        ) : attribError && !attribution ? (
          <Unavailable what="Attribution report" />
        ) : (
          <>
            {signals.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/60 italic">no signal attribution data yet</p>
            ) : (
              <div className="overflow-x-auto">
                <div className="grid grid-cols-[minmax(6rem,1.4fr)_3rem_4rem_4rem_3.5rem] gap-x-2 pb-0.5 border-b border-border/60 min-w-[22rem]">
                  <span className="text-[8px] text-muted-foreground font-semibold">SIGNAL</span>
                  <span className="text-[8px] text-muted-foreground text-right">FIRED</span>
                  <span className="text-[8px] text-muted-foreground text-right">EXP FIRED</span>
                  <span className="text-[8px] text-muted-foreground text-right">EXP NOT</span>
                  <span className="text-[8px] text-muted-foreground text-right">LIFT</span>
                </div>
                {signals.map(([id, s]) => {
                  const lift = num(s?.lift);
                  const firedTrades = num(s?.fired?.trades);
                  // Lift computed off a handful of fired trades is not meaningful.
                  const lowSample = isLowSample(firedTrades, s?.low_sample);
                  return (
                    <div key={id} className="grid grid-cols-[minmax(6rem,1.4fr)_3rem_4rem_4rem_3.5rem] gap-x-2 py-0.5 border-b border-border/20 min-w-[22rem]">
                      <span className="text-[9px] font-mono text-foreground truncate" title={id}>{id}</span>
                      <span className="text-[9px] font-mono text-muted-foreground text-right">{firedTrades}</span>
                      <span className={`text-[9px] font-mono text-right ${pnlClass(s?.fired?.expectancy)}`}>
                        {fmtUsd(s?.fired?.expectancy)}
                      </span>
                      <span className={`text-[9px] font-mono text-right ${pnlClass(s?.not_fired?.expectancy)}`}>
                        {fmtUsd(s?.not_fired?.expectancy)}
                      </span>
                      <span className={`text-[9px] font-mono font-semibold text-right ${lowSample ? 'text-muted-foreground/50' : lift > 0 ? 'text-gain' : lift < 0 ? 'text-loss' : 'text-muted-foreground'}`}>
                        {lowSample ? <LowSampleNA count={firedTrades} className="text-muted-foreground/50 font-mono" /> : fmtUsd(lift)}
                      </span>
                    </div>
                  );
                })}
              </div>
            )}

            <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground pt-1">
              Veto saves
            </p>
            {vetoes.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/60 italic">no veto attribution data yet</p>
            ) : (
              <div className="overflow-x-auto">
                <div className="grid grid-cols-[minmax(6rem,1.4fr)_3.5rem_4.5rem_4.5rem] gap-x-2 pb-0.5 border-b border-border/60 min-w-[20rem]">
                  <span className="text-[8px] text-muted-foreground font-semibold">VETO</span>
                  <span className="text-[8px] text-muted-foreground text-right">BLOCKED</span>
                  <span className="text-[8px] text-muted-foreground text-right">HYP EXP</span>
                  <span className="text-[8px] text-muted-foreground text-right">SAVED</span>
                </div>
                {vetoes.map(([reason, v]) => {
                  const saved = num(v?.saved_usdt);
                  return (
                    <div key={reason} className="grid grid-cols-[minmax(6rem,1.4fr)_3.5rem_4.5rem_4.5rem] gap-x-2 py-0.5 border-b border-border/20 min-w-[20rem]">
                      <span className="text-[9px] font-mono text-foreground truncate" title={reason}>{reason}</span>
                      <span className="text-[9px] font-mono text-muted-foreground text-right">{num(v?.blocked)}</span>
                      <span className={`text-[9px] font-mono text-right ${pnlClass(v?.hypothetical_expectancy)}`}>
                        {fmtUsd(v?.hypothetical_expectancy)}
                      </span>
                      <span className={`text-[9px] font-mono font-semibold text-right ${saved > 0 ? 'text-gain' : saved < 0 ? 'text-loss' : 'text-muted-foreground'}`}>
                        {fmtUsd(saved)}
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
