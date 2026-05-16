import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';

// ── Types ──────────────────────────────────────────────────────────────────

interface Alert {
  id: number;
  timestamp: string;
  severity: 'info' | 'warn' | 'critical';
  category: string;
  message: string;
  acknowledged: boolean;
}

interface SignalRate {
  evaluations: number;
  fires: number;
  fire_rate_pct: number;
  category?: string;
  description?: string;
}

interface SignalWinStat {
  wins: number;
  losses: number;
  total_trades_with_signal: number;
  win_rate_pct: number;
  avg_pnl_per_trade: number;
  total_pnl: number;
}

interface CoinTrace {
  symbol: string;
  evaluations_count: number;
  buy_allowed_count: number;
  rejection_reasons: Record<string, number>;
  recent_snapshots: any[];
  window_hours?: number;
}

interface SellTiming {
  sells_count: number;
  stats: {
    min_ms: number; max_ms: number;
    p50_ms: number; p90_ms: number;
    p95_ms: number; p99_ms: number;
  };
  slow_sells: Array<{ symbol: string; timing_ms: number; reason: string; ts: string }>;
}

interface AuditEntry {
  timestamp: string;
  field_key: string;
  old_value: string;
  new_value: string;
  source: string;
}

// ── Helpers ───────────────────────────────────────────────────────────────

function fmt_ms(ms: number | null | undefined): string {
  if (ms == null) return '—';
  return ms < 1000 ? `${ms.toFixed(0)}ms` : `${(ms / 1000).toFixed(2)}s`;
}

function ms_color(ms: number | null | undefined): string {
  if (ms == null) return 'text-muted-foreground';
  if (ms < 500) return 'text-gain';
  if (ms < 2000) return 'text-yellow-400';
  return 'text-loss';
}

// ── Alerts Banner ─────────────────────────────────────────────────────────

function AlertsBanner({ baseUrl }: { baseUrl: string }) {
  const [alerts, setAlerts] = useState<Alert[]>([]);

  const load = useCallback(() => {
    fetch(`${baseUrl}/api/alerts?only_unacknowledged=true&limit=10`)
      .then(r => r.json())
      .then(d => setAlerts(d.alerts || []))
      .catch(() => {});
  }, [baseUrl]);

  useEffect(() => {
    load();
    const iv = setInterval(load, 30_000);
    return () => clearInterval(iv);
  }, [load]);

  const acknowledge = async (id: number) => {
    await fetch(`${baseUrl}/api/alerts/${id}/acknowledge`, { method: 'POST' }).catch(() => {});
    setAlerts(prev => prev.filter(a => a.id !== id));
    toast.success('Alert dismissed');
  };

  if (alerts.length === 0) {
    return (
      <div className="flex items-center gap-2 text-[9px] text-gain bg-gain/10 border border-gain/30 rounded px-2 py-1">
        <span className="w-1.5 h-1.5 rounded-full bg-gain shrink-0" />
        No active alerts
      </div>
    );
  }

  return (
    <div className="space-y-1">
      {alerts.map(alert => {
        const cls = alert.severity === 'critical'
          ? 'bg-loss/10 border-loss/30 text-loss'
          : alert.severity === 'warn'
          ? 'bg-orange-500/10 border-orange-500/30 text-orange-400'
          : 'bg-accent/10 border-accent/30 text-accent';
        return (
          <div key={alert.id} className={`flex items-center justify-between border rounded px-2 py-1 ${cls}`}>
            <div className="min-w-0 flex-1">
              <p className="text-[9px] font-semibold">[{alert.category.toUpperCase()}] {alert.message}</p>
              <p className="text-[8px] opacity-70">{new Date(alert.timestamp).toLocaleString()}</p>
            </div>
            <button onClick={() => acknowledge(alert.id)}
              className="shrink-0 ml-2 text-[8px] px-1.5 py-0.5 border rounded hover:bg-muted/20 border-current">
              Dismiss
            </button>
          </div>
        );
      })}
    </div>
  );
}

// ── Signal Fire Rates ──────────────────────────────────────────────────────

function SignalFireRates({ baseUrl }: { baseUrl: string }) {
  const [rates, setRates] = useState<Record<string, SignalRate>>({});
  const [windowHours, setWindowHours] = useState(1);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch(`${baseUrl}/api/diagnostics/signal-rates?window_hours=${windowHours}`)
      .then(r => r.json())
      .then(d => { setRates(d.signals || {}); setLoading(false); })
      .catch(() => setLoading(false));
    const iv = setInterval(() => {
      fetch(`${baseUrl}/api/diagnostics/signal-rates?window_hours=${windowHours}`)
        .then(r => r.json())
        .then(d => setRates(d.signals || {}))
        .catch(() => {});
    }, 30_000);
    return () => clearInterval(iv);
  }, [baseUrl, windowHours]);

  const sorted = Object.entries(rates).sort(([, a], [, b]) => b.fire_rate_pct - a.fire_rate_pct);

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Signal Fire Rates</p>
        <select value={windowHours} onChange={e => setWindowHours(parseFloat(e.target.value))}
          className="text-[8px] bg-background border border-border rounded px-1 py-0.5 text-foreground">
          <option value={0.25}>15 min</option>
          <option value={1}>1 hour</option>
          <option value={4}>4 hours</option>
          <option value={24}>24 hours</option>
        </select>
      </div>

      {loading ? (
        <p className="text-[9px] text-muted-foreground">Loading…</p>
      ) : sorted.length === 0 ? (
        <p className="text-[9px] text-muted-foreground italic">No data yet — wait a few minutes</p>
      ) : (
        <div className="space-y-1">
          {sorted.map(([sigId, stats]) => {
            const pct = Math.min(100, stats.fire_rate_pct);
            const barColor = pct > 60 ? 'bg-gain' : pct > 30 ? 'bg-yellow-500' : pct > 10 ? 'bg-orange-500' : 'bg-loss';
            return (
              <div key={sigId} className="flex items-center gap-2">
                <p className="text-[8px] font-mono w-36 truncate shrink-0" title={stats.description}>{sigId}</p>
                <div className="flex-1 h-3 bg-muted/30 rounded overflow-hidden relative">
                  <div className={`h-full ${barColor} rounded`} style={{ width: `${pct}%` }} />
                  <span className="absolute inset-0 flex items-center px-1 text-[7px] font-bold text-foreground">
                    {stats.fire_rate_pct.toFixed(1)}%
                  </span>
                </div>
                <p className="text-[8px] text-muted-foreground w-14 text-right shrink-0">
                  {stats.fires}/{stats.evaluations}
                </p>
              </div>
            );
          })}
          <p className="text-[7px] text-muted-foreground pt-1">
            &lt;10% too strict · &gt;80% too loose
          </p>
        </div>
      )}
    </div>
  );
}

// ── Signal Win Rates ───────────────────────────────────────────────────────

function SignalWinRates({ baseUrl }: { baseUrl: string }) {
  const [stats, setStats] = useState<Record<string, SignalWinStat>>({});
  const [days, setDays] = useState(7);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch(`${baseUrl}/api/diagnostics/signal-win-rates?days=${days}`)
      .then(r => r.json())
      .then(d => { setStats(d.signals || {}); setTotal(d.total_trades_analyzed || 0); setLoading(false); })
      .catch(() => setLoading(false));
  }, [baseUrl, days]);

  const sorted = Object.entries(stats).sort(([, a], [, b]) => b.win_rate_pct - a.win_rate_pct);

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
          Signal Win Rates <span className="normal-case font-normal">({total} trades)</span>
        </p>
        <select value={days} onChange={e => setDays(parseInt(e.target.value))}
          className="text-[8px] bg-background border border-border rounded px-1 py-0.5 text-foreground">
          <option value={1}>1 day</option>
          <option value={7}>7 days</option>
          <option value={30}>30 days</option>
        </select>
      </div>

      {loading ? (
        <p className="text-[9px] text-muted-foreground">Loading…</p>
      ) : sorted.length === 0 ? (
        <p className="text-[9px] text-muted-foreground italic">No data yet (need completed trades with signal snapshots)</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-border/50">
                {['Signal', 'Trades', 'Win %', 'Avg PnL', 'Total PnL'].map(h => (
                  <th key={h} className="text-[8px] text-muted-foreground font-semibold text-right first:text-left pb-0.5">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sorted.map(([sigId, s]) => (
                <tr key={sigId} className="border-b border-border/20 last:border-0">
                  <td className="text-[8px] font-mono py-0.5 pr-2 truncate max-w-[8rem]">{sigId}</td>
                  <td className="text-[8px] text-right text-muted-foreground">{s.total_trades_with_signal}</td>
                  <td className={`text-[8px] text-right font-bold ${s.win_rate_pct >= 60 ? 'text-gain' : s.win_rate_pct >= 40 ? 'text-yellow-400' : 'text-loss'}`}>
                    {s.win_rate_pct.toFixed(1)}%
                  </td>
                  <td className={`text-[8px] text-right font-mono ${s.avg_pnl_per_trade >= 0 ? 'text-gain' : 'text-loss'}`}>
                    ${s.avg_pnl_per_trade.toFixed(3)}
                  </td>
                  <td className={`text-[8px] text-right font-mono ${s.total_pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                    ${s.total_pnl.toFixed(2)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Coin Trace ─────────────────────────────────────────────────────────────

function CoinTraceLookup({ baseUrl }: { baseUrl: string }) {
  const [symbol, setSymbol] = useState('');
  const [hours, setHours] = useState(1);
  const [trace, setTrace] = useState<CoinTrace | null>(null);
  const [loading, setLoading] = useState(false);

  const lookup = async () => {
    if (!symbol.trim()) return;
    setLoading(true);
    try {
      const r = await fetch(`${baseUrl}/api/diagnostics/coin-trace/${symbol.toUpperCase().replace('USDT', '') + 'USDT'}?hours=${hours}`);
      const d = await r.json();
      setTrace(d);
    } catch { toast.error('Lookup failed'); }
    finally { setLoading(false); }
  };

  return (
    <div className="space-y-2">
      <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Coin Trace</p>
      <div className="flex gap-1">
        <input type="text" value={symbol} onChange={e => setSymbol(e.target.value.toUpperCase())}
          onKeyDown={e => e.key === 'Enter' && lookup()}
          placeholder="e.g. BTC or BTCUSDT"
          className="flex-1 text-[9px] font-mono bg-background border border-border rounded px-2 py-0.5 outline-none focus:border-accent" />
        <select value={hours} onChange={e => setHours(parseFloat(e.target.value))}
          className="text-[8px] bg-background border border-border rounded px-1 text-foreground">
          <option value={0.5}>30m</option>
          <option value={1}>1h</option>
          <option value={4}>4h</option>
        </select>
        <button onClick={lookup} disabled={loading}
          className="text-[9px] px-2 py-0.5 border border-accent/40 text-accent rounded hover:bg-accent/10 disabled:opacity-50">
          {loading ? '…' : 'Lookup'}
        </button>
      </div>

      {trace && (
        <div className="space-y-2 text-[9px]">
          <div className="flex gap-4">
            <span className="text-muted-foreground">Symbol: <span className="text-foreground font-mono">{trace.symbol}</span></span>
            <span className="text-muted-foreground">Evals: <span className="text-foreground">{trace.evaluations_count}</span></span>
            <span className="text-muted-foreground">Buy OK: <span className="text-gain">{trace.buy_allowed_count}</span></span>
          </div>

          {Object.keys(trace.rejection_reasons).length > 0 && (
            <div>
              <p className="text-[8px] font-semibold text-muted-foreground mb-1">Rejection reasons:</p>
              <div className="space-y-0.5">
                {Object.entries(trace.rejection_reasons)
                  .sort(([, a], [, b]) => b - a)
                  .map(([reason, count]) => (
                    <div key={reason} className="flex gap-2">
                      <span className="text-muted-foreground w-6 text-right shrink-0">{count}×</span>
                      <span className="font-mono text-foreground">{reason}</span>
                    </div>
                  ))}
              </div>
            </div>
          )}

          {trace.recent_snapshots?.length > 0 && (
            <details className="text-[8px]">
              <summary className="cursor-pointer text-muted-foreground hover:text-foreground">Recent evaluations ({trace.recent_snapshots.length})</summary>
              <pre className="mt-1 text-[7px] bg-muted/20 rounded p-2 overflow-auto max-h-40 text-muted-foreground">
                {JSON.stringify(trace.recent_snapshots, null, 2)}
              </pre>
            </details>
          )}
        </div>
      )}
    </div>
  );
}

// ── Sell Timing ───────────────────────────────────────────────────────────

function SellTimingHistogram({ baseUrl }: { baseUrl: string }) {
  const [data, setData] = useState<SellTiming | null>(null);
  const [hours, setHours] = useState(24);

  useEffect(() => {
    fetch(`${baseUrl}/api/diagnostics/sell-timing?hours=${hours}`)
      .then(r => r.json())
      .then(setData)
      .catch(() => {});
  }, [baseUrl, hours]);

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Sell Execution Timing</p>
        <select value={hours} onChange={e => setHours(parseInt(e.target.value))}
          className="text-[8px] bg-background border border-border rounded px-1 py-0.5 text-foreground">
          <option value={4}>4h</option>
          <option value={24}>24h</option>
          <option value={168}>7d</option>
        </select>
      </div>

      {!data || data.sells_count === 0 ? (
        <p className="text-[9px] text-muted-foreground italic">No sells in window</p>
      ) : (
        <>
          <div className="grid grid-cols-3 gap-2">
            {[
              { label: 'p50', val: data.stats?.p50_ms },
              { label: 'p95', val: data.stats?.p95_ms },
              { label: 'p99', val: data.stats?.p99_ms },
            ].map(({ label, val }) => (
              <div key={label} className="bg-muted/20 rounded px-2 py-1 text-center">
                <p className="text-[8px] text-muted-foreground">{label}</p>
                <p className={`text-[11px] font-bold ${ms_color(val)}`}>{fmt_ms(val)}</p>
              </div>
            ))}
          </div>
          <p className="text-[8px] text-muted-foreground">{data.sells_count} sells · range {fmt_ms(data.stats?.min_ms)}–{fmt_ms(data.stats?.max_ms)}</p>

          {data.slow_sells?.length > 0 && (
            <div>
              <p className="text-[8px] font-semibold text-muted-foreground mb-0.5">Slow sells (&gt;1s):</p>
              <div className="space-y-0.5">
                {data.slow_sells.map((s, i) => (
                  <div key={i} className="flex gap-2 text-[8px]">
                    <span className="font-mono w-20 shrink-0">{s.symbol}</span>
                    <span className={`font-bold ${ms_color(s.timing_ms)}`}>{fmt_ms(s.timing_ms)}</span>
                    <span className="text-muted-foreground truncate">{s.reason}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ── Strategy Audit ────────────────────────────────────────────────────────

function StrategyAuditLog({ baseUrl }: { baseUrl: string }) {
  const [changes, setChanges] = useState<AuditEntry[]>([]);

  useEffect(() => {
    fetch(`${baseUrl}/api/diagnostics/strategy-audit?limit=30`)
      .then(r => r.json())
      .then(d => setChanges(d.changes || []))
      .catch(() => {});
  }, [baseUrl]);

  return (
    <div className="space-y-2">
      <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Strategy Change History</p>
      {changes.length === 0 ? (
        <p className="text-[9px] text-muted-foreground italic">No recorded changes</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-border/50">
                {['When', 'Field', 'Was', 'Now', 'Source'].map(h => (
                  <th key={h} className="text-[8px] text-muted-foreground font-semibold text-left pb-0.5 pr-2">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {changes.map((c, i) => (
                <tr key={i} className="border-b border-border/20 last:border-0">
                  <td className="text-[7px] text-muted-foreground pr-2 py-0.5 whitespace-nowrap">
                    {new Date(c.timestamp).toLocaleString()}
                  </td>
                  <td className="text-[8px] font-mono pr-2 truncate max-w-[6rem]">{c.field_key}</td>
                  <td className="text-[8px] font-mono text-muted-foreground pr-2">{c.old_value ?? '—'}</td>
                  <td className="text-[8px] font-mono text-accent pr-2">{c.new_value ?? '—'}</td>
                  <td className="text-[7px] text-muted-foreground">{c.source}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Main export ───────────────────────────────────────────────────────────

export function DiagnosticsTab({ baseUrl = '' }: { baseUrl?: string }) {
  return (
    <div className="space-y-4">
      <AlertsBanner baseUrl={baseUrl} />
      <div className="border-t border-border/50 pt-3">
        <SignalFireRates baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <SignalWinRates baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <CoinTraceLookup baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <SellTimingHistogram baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <StrategyAuditLog baseUrl={baseUrl} />
      </div>
    </div>
  );
}
