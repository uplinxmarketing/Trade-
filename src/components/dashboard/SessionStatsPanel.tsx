import { useCallback, useEffect, useState } from 'react';
import { isLowSample } from '@/lib/sampleGuard';

// ── L2.3 — session / hour expectancy panel ──────────────────────────────────
// Two small tables: by session (Asia/EU/US) and by hour (0–23 UTC). Columns:
// trades, win %, expectancy, avg slippage (bps). Any bucket flagged low_sample
// (n<20) is greyed and shows "n=k" instead of a confident number (min-sample
// honesty). The best/worst session are subtly highlighted. Fetches on mount and
// polls ~30s. Coded defensively — older backends may 404 or omit sections.

interface SessionRow {
  name: string;
  utc_range?: string;
  trade_count?: number;
  win_rate?: number;
  expectancy?: number;
  avg_slippage_bps?: number;
  low_sample?: boolean;
}

interface HourRow {
  hour: number;
  trade_count?: number;
  win_rate?: number;
  expectancy?: number;
  avg_slippage_bps?: number;
  low_sample?: boolean;
}

interface SessionStats {
  available?: boolean;
  sessions?: SessionRow[];
  hours?: HourRow[];
  data_start?: string | number;
  n_total?: number;
}

const POLL_MS = 30_000;
const num = (v: unknown): number => (typeof v === 'number' && isFinite(v) ? v : 0);
const fmtPct = (v: unknown) => `${(num(v) * 100).toFixed(0)}%`;
const fmtExp = (v: unknown) => {
  const n = num(v);
  return `${n > 0 ? '+' : ''}${n.toFixed(2)}`;
};
const fmtBps = (v: unknown) => `${num(v).toFixed(1)}`;
const pnlClass = (v: unknown) =>
  num(v) > 0 ? 'text-gain' : num(v) < 0 ? 'text-loss' : 'text-muted-foreground';

export function useSessionStats(baseUrl = '') {
  const [stats, setStats] = useState<SessionStats | null>(null);
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      const r = await fetch(`${baseUrl}/api/analytics/sessions`, { cache: 'no-store' });
      if (!r.ok) throw new Error(`http ${r.status}`);
      const d = await r.json();
      setStats(d as SessionStats);
      setError(false);
    } catch {
      setError(true);
    } finally {
      setLoading(false);
    }
  }, [baseUrl]);

  useEffect(() => {
    load();
    const id = setInterval(load, POLL_MS);
    return () => clearInterval(id);
  }, [load]);

  return { stats, error, loading };
}

// A single stat cell that greys + shows n=k when the sample is too small.
function GuardedCell({ count, lowSample, children, className = '' }: {
  count: number;
  lowSample?: boolean;
  children: React.ReactNode;
  className?: string;
}) {
  if (isLowSample(count, lowSample)) {
    return (
      <span className="text-[9px] font-mono text-muted-foreground/50" title="insufficient sample (<20)">
        n={count}
      </span>
    );
  }
  return <span className={`text-[9px] font-mono ${className}`}>{children}</span>;
}

const SESSION_COLS = 'grid grid-cols-[minmax(3.5rem,1fr)_2.5rem_2.5rem_3.5rem_3rem] gap-x-2';
const HOUR_COLS = 'grid grid-cols-[2.5rem_2.5rem_2.5rem_3.5rem_3rem] gap-x-2';

function TableHeader({ cols, first }: { cols: string; first: string }) {
  return (
    <div className={`${cols} pb-0.5 border-b border-border/60`}>
      <span className="text-[8px] text-muted-foreground font-semibold">{first}</span>
      <span className="text-[8px] text-muted-foreground text-right">TRADES</span>
      <span className="text-[8px] text-muted-foreground text-right">WIN%</span>
      <span className="text-[8px] text-muted-foreground text-right">EXP</span>
      <span className="text-[8px] text-muted-foreground text-right">SLIP</span>
    </div>
  );
}

export function SessionStatsPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const { stats, error, loading } = useSessionStats(baseUrl);

  const unavailable = (!stats || stats.available === false || error) && !loading;
  const sessions = stats?.sessions ?? [];
  const hours = stats?.hours ?? [];

  // Best/worst session by expectancy — only among sufficiently-sampled buckets.
  const confident = sessions.filter(s => !isLowSample(num(s.trade_count), s.low_sample));
  let bestName = '';
  let worstName = '';
  if (confident.length >= 2) {
    const sorted = [...confident].sort((a, b) => num(b.expectancy) - num(a.expectancy));
    bestName = sorted[0].name;
    worstName = sorted[sorted.length - 1].name;
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
          Session Expectancy
        </p>
        {stats?.n_total != null && !unavailable && (
          <span className="text-[8px] text-muted-foreground font-mono">
            n={num(stats.n_total)}
            {stats.data_start ? ` · since ${stats.data_start}` : ''}
          </span>
        )}
      </div>

      {loading && !stats ? (
        <p className="text-[9px] text-muted-foreground py-1">Loading session stats…</p>
      ) : unavailable ? (
        <p className="text-[9px] text-muted-foreground/70 italic py-1">
          Session analytics unavailable — endpoint not deployed yet
        </p>
      ) : (
        <>
          {/* By session */}
          <div>
            <p className="text-[8px] uppercase tracking-wider text-muted-foreground mb-0.5">By session</p>
            {sessions.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/60 italic">no session data yet</p>
            ) : (
              <div className="overflow-x-auto">
                <TableHeader cols={SESSION_COLS} first="SESSION" />
                {sessions.map(s => {
                  const count = num(s.trade_count);
                  const isBest = s.name === bestName;
                  const isWorst = s.name === worstName;
                  const rowCls = isBest
                    ? 'bg-gain/5'
                    : isWorst
                      ? 'bg-loss/5'
                      : '';
                  return (
                    <div key={s.name} className={`${SESSION_COLS} py-0.5 border-b border-border/20 items-center ${rowCls}`}>
                      <span className="text-[9px] font-mono text-foreground truncate" title={s.utc_range ?? s.name}>
                        {s.name}
                        {isBest && <span className="text-gain ml-0.5" title="best expectancy">▲</span>}
                        {isWorst && <span className="text-loss ml-0.5" title="worst expectancy">▼</span>}
                      </span>
                      <span className="text-[9px] font-mono text-muted-foreground text-right">{count}</span>
                      <span className="text-right">
                        <GuardedCell count={count} lowSample={s.low_sample} className="text-muted-foreground">
                          {fmtPct(s.win_rate)}
                        </GuardedCell>
                      </span>
                      <span className="text-right">
                        <GuardedCell count={count} lowSample={s.low_sample} className={pnlClass(s.expectancy)}>
                          {fmtExp(s.expectancy)}
                        </GuardedCell>
                      </span>
                      <span className="text-[9px] font-mono text-muted-foreground text-right">{fmtBps(s.avg_slippage_bps)}</span>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* By hour */}
          <div>
            <p className="text-[8px] uppercase tracking-wider text-muted-foreground mb-0.5">By hour (UTC)</p>
            {hours.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/60 italic">no hourly data yet</p>
            ) : (
              <div className="overflow-x-auto max-h-64 overflow-y-auto">
                <TableHeader cols={HOUR_COLS} first="HOUR" />
                {hours.map(h => {
                  const count = num(h.trade_count);
                  return (
                    <div key={h.hour} className={`${HOUR_COLS} py-0.5 border-b border-border/20 items-center`}>
                      <span className="text-[9px] font-mono text-foreground text-left">
                        {String(num(h.hour)).padStart(2, '0')}h
                      </span>
                      <span className="text-[9px] font-mono text-muted-foreground text-right">{count}</span>
                      <span className="text-right">
                        <GuardedCell count={count} lowSample={h.low_sample} className="text-muted-foreground">
                          {fmtPct(h.win_rate)}
                        </GuardedCell>
                      </span>
                      <span className="text-right">
                        <GuardedCell count={count} lowSample={h.low_sample} className={pnlClass(h.expectancy)}>
                          {fmtExp(h.expectancy)}
                        </GuardedCell>
                      </span>
                      <span className="text-[9px] font-mono text-muted-foreground text-right">{fmtBps(h.avg_slippage_bps)}</span>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}

export default SessionStatsPanel;
