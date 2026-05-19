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

interface SellTimingResponse {
  window_hours: number;
  trades_count: number;
  target_to_trigger_count: number;
  trigger_to_filled_count: number;
  target_to_trigger_ms: { min: number|null; max: number|null; p50: number|null; p90: number|null; p95: number|null; p99: number|null; };
  trigger_to_filled_ms: { min: number|null; max: number|null; p50: number|null; p90: number|null; p95: number|null; p99: number|null; };
  slow_target_to_trigger: Array<{ coin: string; target_to_trigger_ms: number; reason: string; ts: string }>;
  slow_trigger_to_filled: Array<{ coin: string; trigger_to_filled_ms: number; reason: string; ts: string }>;
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

// ── Thread Health ─────────────────────────────────────────────────────────

interface ThreadStatus {
  last_beat_ago_seconds: number;
  alive: boolean;
  status: 'alive' | 'warn' | 'stuck';
}

function ThreadHealth({ baseUrl }: { baseUrl: string }) {
  const [health, setHealth] = useState<Record<string, ThreadStatus>>({});
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    fetch(`${baseUrl}/api/diagnostics/thread_health`)
      .then(r => r.json())
      .then(d => {
        if (d.error) { setError(d.error); return; }
        setError(null);
        setHealth(d);
      })
      .catch(e => setError(String(e)));
  }, [baseUrl]);

  useEffect(() => {
    load();
    const iv = setInterval(load, 10_000);
    return () => clearInterval(iv);
  }, [load]);

  const dotColor = (status: string) =>
    status === 'alive' ? 'bg-gain' : status === 'warn' ? 'bg-yellow-400' : 'bg-loss';
  const textColor = (status: string) =>
    status === 'alive' ? 'text-gain' : status === 'warn' ? 'text-yellow-400' : 'text-loss';

  const entries = Object.entries(health);

  return (
    <div className="space-y-2">
      <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Thread Health</p>
      {error ? (
        <p className="text-[9px] text-loss">{error}</p>
      ) : entries.length === 0 ? (
        <p className="text-[9px] text-muted-foreground italic">No heartbeats received yet</p>
      ) : (
        <div className="grid grid-cols-2 gap-1">
          {entries.map(([name, stat]) => (
            <div key={name} className="flex items-center gap-2 bg-muted/20 rounded px-2 py-1">
              <span className={`w-2 h-2 rounded-full shrink-0 ${dotColor(stat.status)}`} />
              <span className="text-[8px] font-mono flex-1 truncate">{name}</span>
              <span className={`text-[8px] font-bold shrink-0 ${textColor(stat.status)}`}>
                {stat.last_beat_ago_seconds}s ago
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
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
  const [data, setData] = useState<SellTimingResponse | null>(null);
  const [hours, setHours] = useState(24);

  useEffect(() => {
    fetch(`${baseUrl}/api/diagnostics/sell-timing?hours=${hours}`)
      .then(r => r.json())
      .then(setData)
      .catch(() => {});
  }, [baseUrl, hours]);

  const StatGrid = ({ stats }: { stats: SellTimingResponse['target_to_trigger_ms'] }) => (
    <div className="grid grid-cols-4 gap-2">
      {(['p50','p90','p95','p99'] as const).map(k => (
        <div key={k} className="bg-muted/20 rounded px-2 py-1 text-center">
          <p className="text-[8px] text-muted-foreground">{k}</p>
          <p className={`text-[11px] font-bold ${ms_color((stats as any)[k])}`}>{fmt_ms((stats as any)[k])}</p>
        </div>
      ))}
    </div>
  );

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Sell Execution Timing</p>
        <select value={hours} onChange={e => setHours(parseInt(e.target.value))}
          className="text-[8px] bg-background border border-border rounded px-1 py-0.5 text-foreground">
          <option value={4}>4h</option>
          <option value={24}>24h</option>
          <option value={168}>7d</option>
        </select>
      </div>

      {!data || data.trades_count === 0 ? (
        <p className="text-[9px] text-muted-foreground italic">No trades in window</p>
      ) : (
        <>
          <p className="text-[8px] text-muted-foreground">{data.trades_count} trades · latency data on {data.target_to_trigger_count} sells</p>

          <div className="space-y-2">
            <p className="text-[8px] font-semibold text-muted-foreground">Stage 1: Target hit → executor dispatch</p>
            {data.target_to_trigger_count === 0
              ? <p className="text-[8px] text-muted-foreground italic">No data yet (populates after next sell)</p>
              : <StatGrid stats={data.target_to_trigger_ms} />
            }
            <p className="text-[7px] text-muted-foreground">High = event-loop blocking before sell dispatched</p>
          </div>

          <div className="space-y-2 pt-1 border-t border-border/40">
            <p className="text-[8px] font-semibold text-muted-foreground">Stage 2: Executor dispatch → Binance fill</p>
            {data.trigger_to_filled_count === 0
              ? <p className="text-[8px] text-muted-foreground italic">No data yet</p>
              : <StatGrid stats={data.trigger_to_filled_ms} />
            }
            <p className="text-[7px] text-muted-foreground">High = Binance API or network latency</p>
          </div>

          {(data.slow_target_to_trigger.length > 0 || data.slow_trigger_to_filled.length > 0) && (
            <details className="text-[8px]">
              <summary className="cursor-pointer text-muted-foreground hover:text-foreground">Slow sells (&gt;1s)</summary>
              <div className="mt-1 space-y-1">
                {data.slow_target_to_trigger.map((s, i) => (
                  <div key={i} className="flex gap-2">
                    <span className="font-mono w-20 shrink-0">{s.coin}</span>
                    <span className={`font-bold ${ms_color(s.target_to_trigger_ms)}`}>{fmt_ms(s.target_to_trigger_ms)}</span>
                    <span className="text-muted-foreground truncate">dispatch lag · {s.reason}</span>
                  </div>
                ))}
                {data.slow_trigger_to_filled.map((s, i) => (
                  <div key={i} className="flex gap-2">
                    <span className="font-mono w-20 shrink-0">{s.coin}</span>
                    <span className={`font-bold ${ms_color(s.trigger_to_filled_ms)}`}>{fmt_ms(s.trigger_to_filled_ms)}</span>
                    <span className="text-muted-foreground truncate">fill lag · {s.reason}</span>
                  </div>
                ))}
              </div>
            </details>
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

// ── Orphan Check ──────────────────────────────────────────────────────────

interface OrphanIssue {
  type: 'orphan_on_binance' | 'orphan_in_db' | 'qty_mismatch';
  symbol: string;
  binance_qty: number;
  db_qty: number | null;
  diff_pct?: number;
  value_usdt: number | null;
}

interface OrphanCheckResponse {
  issues_count: number;
  total_value_usdt: number;
  issues: OrphanIssue[];
  error?: string;
}

function OrphanCheck({ baseUrl }: { baseUrl: string }) {
  const [data, setData] = useState<OrphanCheckResponse | null>(null);
  const [loading, setLoading] = useState(false);

  const run = useCallback(() => {
    setLoading(true);
    fetch(`${baseUrl}/api/diagnostics/orphan-check`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [baseUrl]);

  const TYPE_LABEL: Record<string, string> = {
    orphan_on_binance: 'On Binance, not in bot DB',
    orphan_in_db: 'In bot DB, not on Binance',
    qty_mismatch: 'Qty mismatch',
  };
  const TYPE_COLOR: Record<string, string> = {
    orphan_on_binance: 'bg-yellow-500/10 border-yellow-500/30 text-yellow-400',
    orphan_in_db: 'bg-loss/10 border-loss/30 text-loss',
    qty_mismatch: 'bg-orange-500/10 border-orange-500/30 text-orange-400',
  };

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Position Sync Check</p>
        <button onClick={run} disabled={loading}
          className="text-[8px] px-2 py-0.5 border border-accent/40 text-accent rounded hover:bg-accent/10 disabled:opacity-50">
          {loading ? 'Checking…' : 'Run Check'}
        </button>
      </div>

      {data?.error && (
        <p className="text-[9px] text-loss">{data.error}</p>
      )}

      {data && !data.error && (
        data.issues_count === 0 ? (
          <div className="flex items-center gap-2 text-[9px] text-gain bg-gain/10 border border-gain/30 rounded px-2 py-1">
            <span className="w-1.5 h-1.5 rounded-full bg-gain shrink-0" />
            All positions synced &mdash; no orphans detected
          </div>
        ) : (
          <div className="space-y-1">
            <p className="text-[8px] text-muted-foreground">{data.issues_count} issue(s) &middot; ~${data.total_value_usdt.toFixed(2)} stranded</p>
            {data.issues.map((issue, i) => (
              <div key={i} className={`flex justify-between border rounded px-2 py-1 ${TYPE_COLOR[issue.type]}`}>
                <div>
                  <p className="text-[8px] font-semibold font-mono">{issue.symbol}</p>
                  <p className="text-[7px] opacity-80">{TYPE_LABEL[issue.type]}</p>
                </div>
                <div className="text-right">
                  <p className="text-[8px] font-mono">B:{issue.binance_qty?.toFixed?.(4) ?? '—'} DB:{issue.db_qty?.toFixed?.(4) ?? '—'}</p>
                  {issue.value_usdt != null && <p className="text-[7px] opacity-70">${issue.value_usdt.toFixed(2)}</p>}
                </div>
              </div>
            ))}
          </div>
        )
      )}

      {!data && !loading && (
        <p className="text-[9px] text-muted-foreground italic">Click &ldquo;Run Check&rdquo; to compare Binance balances vs bot DB</p>
      )}
    </div>
  );
}

// ── Fill Quality ──────────────────────────────────────────────────────────

interface FillQualityData {
  window_hours: number;
  trade_count: number;
  buy_slippage: { p50: number|null; p90: number|null; p99: number|null; avg: number|null };
  sell_slippage: { p50: number|null; p90: number|null; p99: number|null; avg: number|null };
  worst_fills: Array<{ coin: string; buy_slippage_pct: number|null; sell_slippage_pct: number|null; timestamp_sell: string }>;
  error?: string;
}

function FillQuality({ baseUrl }: { baseUrl: string }) {
  const [hours, setHours] = useState(24);
  const [data, setData] = useState<FillQualityData | null>(null);

  useEffect(() => {
    fetch(`${baseUrl}/api/diagnostics/fill_quality?hours=${hours}`)
      .then(r => r.json())
      .then(setData)
      .catch(() => {});
  }, [baseUrl, hours]);

  const slip_color = (v: number | null) => {
    if (v == null) return 'text-muted-foreground';
    if (Math.abs(v) < 0.05) return 'text-gain';
    if (Math.abs(v) < 0.2) return 'text-yellow-400';
    return 'text-loss';
  };

  const SlipTile = ({ label, val }: { label: string; val: number | null }) => (
    <div className="bg-muted/20 rounded px-2 py-1 text-center">
      <p className="text-[8px] text-muted-foreground">{label}</p>
      <p className={`text-[11px] font-bold ${slip_color(val)}`}>
        {val == null ? '—' : `${val > 0 ? '+' : ''}${val.toFixed(3)}%`}
      </p>
    </div>
  );

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Fill Quality</p>
        <select value={hours} onChange={e => setHours(parseInt(e.target.value))}
          className="text-[8px] bg-background border border-border rounded px-1 py-0.5 text-foreground">
          <option value={4}>4h</option>
          <option value={24}>24h</option>
          <option value={168}>7d</option>
        </select>
      </div>

      {data?.error ? (
        <p className="text-[9px] text-loss">{data.error}</p>
      ) : !data ? (
        <p className="text-[9px] text-muted-foreground">Loading…</p>
      ) : data.trade_count === 0 ? (
        <p className="text-[9px] text-muted-foreground italic">No trades in window</p>
      ) : (
        <div className="space-y-3">
          <p className="text-[8px] text-muted-foreground">{data.trade_count} trades</p>
          <div>
            <p className="text-[8px] font-semibold text-muted-foreground mb-1">Buy Slippage</p>
            <div className="grid grid-cols-4 gap-1">
              <SlipTile label="avg" val={data.buy_slippage.avg} />
              <SlipTile label="p50" val={data.buy_slippage.p50} />
              <SlipTile label="p90" val={data.buy_slippage.p90} />
              <SlipTile label="p99" val={data.buy_slippage.p99} />
            </div>
          </div>
          <div>
            <p className="text-[8px] font-semibold text-muted-foreground mb-1">Sell Slippage</p>
            <div className="grid grid-cols-4 gap-1">
              <SlipTile label="avg" val={data.sell_slippage.avg} />
              <SlipTile label="p50" val={data.sell_slippage.p50} />
              <SlipTile label="p90" val={data.sell_slippage.p90} />
              <SlipTile label="p99" val={data.sell_slippage.p99} />
            </div>
          </div>
          {data.worst_fills.length > 0 && (
            <details>
              <summary className="cursor-pointer text-[8px] text-muted-foreground hover:text-foreground">
                Worst fills ({data.worst_fills.length})
              </summary>
              <div className="mt-1 overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-border/50">
                      {['Coin', 'Buy slip', 'Sell slip', 'When'].map(h => (
                        <th key={h} className="text-[8px] text-muted-foreground font-semibold text-left pb-0.5 pr-2">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.worst_fills.map((r, i) => (
                      <tr key={i} className="border-b border-border/20 last:border-0">
                        <td className="text-[8px] font-mono py-0.5 pr-2">{r.coin?.replace('USDT', '')}</td>
                        <td className={`text-[8px] pr-2 ${slip_color(r.buy_slippage_pct)}`}>
                          {r.buy_slippage_pct == null ? '—' : `${r.buy_slippage_pct > 0 ? '+' : ''}${r.buy_slippage_pct?.toFixed(3)}%`}
                        </td>
                        <td className={`text-[8px] pr-2 ${slip_color(r.sell_slippage_pct)}`}>
                          {r.sell_slippage_pct == null ? '—' : `${r.sell_slippage_pct > 0 ? '+' : ''}${r.sell_slippage_pct?.toFixed(3)}%`}
                        </td>
                        <td className="text-[7px] text-muted-foreground">{r.timestamp_sell ? new Date(r.timestamp_sell).toLocaleString() : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </details>
          )}
        </div>
      )}
    </div>
  );
}

// ── Buy Decision Telemetry ────────────────────────────────────────────────

interface BuyRejectionData {
  window_hours: number;
  total_rejections: number;
  by_reason: Array<{ reason: string; count: number; pct: number }>;
  by_coin: Array<{ coin: string; rejections: number }>;
  recent: Array<{ timestamp: string; coin: string; reason: string; detail: string | null; score: number | null; rsi_value: number | null }>;
  error?: string;
}

function BuyDecisionTelemetry({ baseUrl }: { baseUrl: string }) {
  const [hours, setHours] = useState(1);
  const [data, setData] = useState<BuyRejectionData | null>(null);
  const [loading, setLoading] = useState(true);
  const [showRecent, setShowRecent] = useState(false);

  const load = useCallback(() => {
    setLoading(true);
    fetch(`${baseUrl}/api/diagnostics/buy_rejections?hours=${hours}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, [baseUrl, hours]);

  useEffect(() => { load(); }, [load]);

  const windowLabel = (h: number) => h < 1 ? `${h * 60 | 0}min` : `${h}h`;

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Buy Decision Telemetry</p>
        <div className="flex gap-1">
          {[0.25, 1, 4, 24].map(h => (
            <button key={h} onClick={() => setHours(h)}
              className={`text-[8px] px-1.5 py-0.5 border rounded ${hours === h ? 'border-accent text-accent' : 'border-border text-muted-foreground hover:border-accent/50'}`}>
              {windowLabel(h)}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <p className="text-[9px] text-muted-foreground">Loading…</p>
      ) : data?.error ? (
        <p className="text-[9px] text-loss">{data.error}</p>
      ) : !data ? null : (
        <div className="space-y-3">
          <p className="text-[8px] text-muted-foreground">{data.total_rejections} rejections in last {hours}h</p>

          {data.by_reason.length > 0 && (
            <div>
              <p className="text-[8px] font-semibold text-muted-foreground mb-1">By Reason</p>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-border/50">
                      {['Reason', 'Count', '%'].map(h => (
                        <th key={h} className="text-[8px] text-muted-foreground font-semibold text-left pb-0.5 pr-2">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.by_reason.map((r, i) => (
                      <tr key={i} className="border-b border-border/20 last:border-0">
                        <td className="text-[8px] font-mono py-0.5 pr-2 truncate max-w-[10rem]">{r.reason}</td>
                        <td className="text-[8px] text-right pr-2">{r.count}</td>
                        <td className="text-[8px] text-right text-muted-foreground">{r.pct}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {data.by_coin.length > 0 && (
            <div>
              <p className="text-[8px] font-semibold text-muted-foreground mb-1">Most Rejected Coins</p>
              <div className="flex flex-wrap gap-1">
                {data.by_coin.slice(0, 10).map((c, i) => (
                  <span key={i} className="text-[8px] font-mono bg-muted/20 rounded px-1.5 py-0.5">
                    {c.coin.replace('USDT', '')} <span className="text-muted-foreground">{c.rejections}</span>
                  </span>
                ))}
              </div>
            </div>
          )}

          {data.recent.length > 0 && (
            <details open={showRecent} onToggle={e => setShowRecent((e.target as HTMLDetailsElement).open)}>
              <summary className="cursor-pointer text-[8px] text-muted-foreground hover:text-foreground">
                Recent Rejections ({data.recent.length})
              </summary>
              <div className="mt-1 space-y-0.5 max-h-40 overflow-y-auto">
                {data.recent.slice(0, 20).map((r, i) => (
                  <div key={i} className="flex gap-2 text-[7px] border-b border-border/10 py-0.5">
                    <span className="text-muted-foreground shrink-0 w-16 truncate">{new Date(r.timestamp).toLocaleTimeString()}</span>
                    <span className="font-mono w-16 truncate shrink-0">{r.coin.replace('USDT', '')}</span>
                    <span className="text-yellow-400 truncate flex-1">{r.reason}</span>
                    {r.score != null && <span className="text-muted-foreground shrink-0">s:{r.score}</span>}
                  </div>
                ))}
              </div>
            </details>
          )}
        </div>
      )}
    </div>
  );
}

// ── Phantom Position Alerts ───────────────────────────────────────────────

interface PhantomAlert {
  id: number;
  timestamp: string;
  symbol: string;
  db_qty: number;
  binance_qty: number;
  resolved: number;
}

function PhantomAlerts({ baseUrl }: { baseUrl: string }) {
  const [phantoms, setPhantoms] = useState<PhantomAlert[]>([]);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    fetch(`${baseUrl}/api/diagnostics/phantoms`)
      .then(r => r.json())
      .then(d => { setPhantoms(d.phantoms || []); setError(d.error || null); })
      .catch(e => setError(String(e)));
  }, [baseUrl]);

  useEffect(() => {
    load();
    const iv = setInterval(load, 30_000);
    return () => clearInterval(iv);
  }, [load]);

  const resolve = async (id: number) => {
    await fetch(`${baseUrl}/api/diagnostics/phantoms/${id}/resolve`, { method: 'POST' }).catch(() => {});
    setPhantoms(prev => prev.filter(p => p.id !== id));
    toast.success('Phantom alert resolved');
  };

  return (
    <div className="space-y-2">
      <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Phantom Position Alerts</p>
      {error ? (
        <p className="text-[9px] text-loss">{error}</p>
      ) : phantoms.length === 0 ? (
        <div className="flex items-center gap-2 text-[9px] text-gain bg-gain/10 border border-gain/30 rounded px-2 py-1">
          <span className="w-1.5 h-1.5 rounded-full bg-gain shrink-0" />
          No phantom positions detected
        </div>
      ) : (
        <div className="space-y-1">
          <p className="text-[8px] text-muted-foreground">{phantoms.length} unresolved alert(s)</p>
          {phantoms.map(p => (
            <div key={p.id} className="flex items-center justify-between border border-loss/30 bg-loss/10 rounded px-2 py-1">
              <div>
                <p className="text-[8px] font-semibold font-mono text-loss">{p.symbol}</p>
                <p className="text-[7px] text-muted-foreground">
                  DB: {p.db_qty?.toFixed(6)} · Binance: {p.binance_qty?.toFixed(6)} · {new Date(p.timestamp).toLocaleString()}
                </p>
              </div>
              <button onClick={() => resolve(p.id)}
                className="shrink-0 ml-2 text-[8px] px-1.5 py-0.5 border border-accent/40 text-accent rounded hover:bg-accent/10">
                Resolve
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Main export ───────────────────────────────────────────────────────────

export function DiagnosticsTab({ baseUrl = '' }: { baseUrl?: string }) {
  return (
    <div className="space-y-4">
      <ThreadHealth baseUrl={baseUrl} />
      <div className="border-t border-border/50 pt-3">
        <AlertsBanner baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <SignalFireRates baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <SignalWinRates baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <BuyDecisionTelemetry baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <CoinTraceLookup baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <SellTimingHistogram baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <FillQuality baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <StrategyAuditLog baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <OrphanCheck baseUrl={baseUrl} />
      </div>
      <div className="border-t border-border/50 pt-3">
        <PhantomAlerts baseUrl={baseUrl} />
      </div>
    </div>
  );
}
