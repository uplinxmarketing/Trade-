import { useState, useEffect, useRef, useCallback } from 'react';
import { TrendingUp, TrendingDown, Pause, Play, RotateCcw, Settings, ChevronDown, ChevronUp } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { toast } from 'sonner';

// ── Types ─────────────────────────────────────────────────────────────────────

interface FuturesStatus {
  running: boolean;
  balance: number;
  equity: number;
  positions: number;
  total_pnl: number;
  win_rate: number;
  trade_count: number;
}

interface FuturesPosition {
  id: number;
  symbol: string;
  direction: 'LONG' | 'SHORT';
  entry_price: number;
  mark_price: number;
  quantity: number;
  margin_usdt: number;
  leverage: number;
  take_profit: number;
  stop_loss: number;
  unrealized_pnl: number;
  timestamp: string;
}

interface FuturesTrade {
  id: number;
  symbol: string;
  direction: string;
  entry_price: number;
  exit_price: number;
  margin_usdt: number;
  leverage: number;
  net_profit: number;
  profitable: number;
  timestamp_open: string;
  timestamp_close: string;
}

interface FuturesSignal {
  symbol: string;
  mark_price: number;
  score: number;
  funding_rate: number;
  signals: {
    trend?: number;
    rsi?: number;
    macd?: number;
    volume?: number;
    obv?: number;
    funding?: number;
  };
}

interface FuturesSettings {
  leverage: number;
  budget_usdt: number;
  take_profit_pct: number;
  stop_loss_pct: number;
  min_signals: number;
  max_positions: number;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// Use Railway URL env var if set; otherwise fall back to relative paths so the
// component works when served directly from the Railway server without any
// extra configuration.
const RAILWAY_BASE = (import.meta.env.VITE_RAILWAY_URL ?? '').replace(/\/$/, '');

async function apiFetch(path: string, opts?: RequestInit) {
  const url = `${RAILWAY_BASE}${path}`;
  return fetch(url, opts);
}

function SignalDot({ val }: { val: number | undefined }) {
  if (val === undefined || val === null)
    return <span className="inline-block w-2 h-2 rounded-full bg-muted/40" />;
  if (val > 0) return <span className="inline-block w-2 h-2 rounded-full bg-gain" title="Bullish" />;
  if (val < 0) return <span className="inline-block w-2 h-2 rounded-full bg-loss" title="Bearish" />;
  return <span className="inline-block w-2 h-2 rounded-full bg-muted-foreground/40" title="Neutral" />;
}

// ── Component ─────────────────────────────────────────────────────────────────

const POLL_MS = 8_000;
const STARTING_USDT = 3000;

const FuturesAgent = () => {
  const [status, setStatus]       = useState<FuturesStatus | null>(null);
  const [positions, setPositions] = useState<FuturesPosition[]>([]);
  const [trades, setTrades]       = useState<FuturesTrade[]>([]);
  const [signals, setSignals]     = useState<FuturesSignal[]>([]);
  const [settings, setSettings]   = useState<FuturesSettings>({
    leverage: 5,
    budget_usdt: 200,
    take_profit_pct: 0.02,
    stop_loss_pct: 0.01,
    min_signals: 4,
    max_positions: 5,
  });

  const [showSettings, setShowSettings] = useState(false);
  const [showSignals, setShowSignals]   = useState(false);
  const [showTrades, setShowTrades]     = useState(false);
  const [loading, setLoading]           = useState(false);
  const [pollError, setPollError]       = useState(false);
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inFlightRef = useRef(false);

  // Single combined poll — one request, all data
  const poll = useCallback(async () => {
    if (inFlightRef.current) return;
    inFlightRef.current = true;
    try {
      const ctrl  = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 6_000);
      const resp  = await apiFetch('/api/futures/all', { signal: ctrl.signal });
      clearTimeout(timer);

      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const d = await resp.json();

      if (d.status)    setStatus(d.status);
      if (d.positions) setPositions(d.positions);
      if (d.trades)    setTrades(d.trades);
      if (d.signals) {
        const arr: FuturesSignal[] = [...d.signals];
        arr.sort((a, b) => Math.abs(b.score) - Math.abs(a.score));
        setSignals(arr);
      }
      setPollError(false);
    } catch {
      setPollError(true);
    } finally {
      inFlightRef.current = false;
      pollRef.current = setTimeout(poll, POLL_MS);
    }
  }, []);

  useEffect(() => {
    poll();
    return () => { if (pollRef.current) clearTimeout(pollRef.current); };
  }, [poll]);

  // ── API actions ─────────────────────────────────────────────────────────────

  const postAction = async (path: string, body?: object) => {
    setLoading(true);
    try {
      const resp = await apiFetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined,
      });
      const data = await resp.json();
      if (!data.success && data.error) throw new Error(data.error);
      return data;
    } catch (e: any) {
      toast.error(e.message ?? 'Request failed');
      return null;
    } finally {
      setLoading(false);
    }
  };

  const handleToggle = async () => {
    const path = status?.running ? '/api/futures/pause' : '/api/futures/start';
    const res  = await postAction(path);
    if (res) {
      toast[status?.running ? 'info' : 'success'](
        status?.running ? 'Futures agent paused' : 'Futures agent started'
      );
      // Optimistic UI — flip the running flag immediately
      setStatus(s => s ? { ...s, running: !s.running } : s);
    }
  };

  const handleReset = async () => {
    if (!confirm('Reset futures paper wallet? All positions and trade history will be erased.')) return;
    const res = await postAction('/api/futures/reset', { starting_usdt: STARTING_USDT });
    if (res) {
      toast.success(`Futures wallet reset to ${STARTING_USDT} USDT`);
      setPositions([]);
      setTrades([]);
      setStatus(s => s ? { ...s, balance: STARTING_USDT, equity: STARTING_USDT,
        total_pnl: 0, win_rate: 0, trade_count: 0, positions: 0 } : s);
    }
  };

  const handleSaveSettings = async () => {
    const res = await postAction('/api/futures/settings', {
      leverage:        settings.leverage,
      budget_usdt:     settings.budget_usdt,
      take_profit_pct: settings.take_profit_pct,
      stop_loss_pct:   settings.stop_loss_pct,
      min_signals:     settings.min_signals,
      max_positions:   settings.max_positions,
    });
    if (res) {
      toast.success('Futures settings saved');
      setShowSettings(false);
    }
  };

  // ── Derived stats ────────────────────────────────────────────────────────────

  const equity    = status?.equity  ?? 0;
  const balance   = status?.balance ?? 0;
  const totalPnl  = status?.total_pnl ?? 0;
  const sessionPnlPct = STARTING_USDT > 0
    ? (((equity - STARTING_USDT) / STARTING_USDT) * 100).toFixed(2)
    : '0.00';

  const isRunning = status?.running ?? false;

  return (
    <div className="trading-card p-4 space-y-4">
      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className={`w-2.5 h-2.5 rounded-full transition-colors ${
            isRunning ? 'bg-gain animate-pulse' : 'bg-muted-foreground'
          }`} />
          <h3 className="text-sm font-semibold">Futures Agent</h3>
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-accent/20 text-accent font-medium">
            USDT-M Perpetuals · Paper
          </span>
          {pollError && (
            <span className="text-[10px] text-loss">⚠ offline</span>
          )}
        </div>
        <div className="flex gap-1">
          <Button
            variant="ghost" size="icon" className="h-7 w-7"
            onClick={() => setShowSettings(v => !v)}
            title="Settings"
          >
            <Settings className="w-3.5 h-3.5" />
          </Button>
          <Button
            variant="ghost" size="icon" className="h-7 w-7"
            onClick={handleReset} disabled={loading}
            title="Reset wallet"
          >
            <RotateCcw className="w-3.5 h-3.5" />
          </Button>
        </div>
      </div>

      {/* ── Stats row ── */}
      <div className="grid grid-cols-4 gap-2">
        {[
          { label: 'Balance', value: `$${balance.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` },
          { label: 'Equity',  value: `$${equity.toLocaleString('en-US',  { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` },
          {
            label: 'Total P&L',
            value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`,
            color: totalPnl >= 0 ? 'text-gain' : 'text-loss',
          },
          {
            label: 'Session',
            value: `${Number(sessionPnlPct) >= 0 ? '+' : ''}${sessionPnlPct}%`,
            color: Number(sessionPnlPct) >= 0 ? 'text-gain' : 'text-loss',
          },
        ].map(s => (
          <div key={s.label} className="bg-muted/20 rounded-md p-2">
            <div className="text-[10px] uppercase tracking-widest text-muted-foreground">{s.label}</div>
            <div className={`text-xs font-mono font-semibold tabular-nums ${s.color ?? ''}`}>{s.value}</div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-3 gap-2">
        {[
          { label: 'Positions', value: String(status?.positions ?? 0) },
          { label: 'Trades',    value: String(status?.trade_count ?? 0) },
          { label: 'Win Rate',  value: `${(status?.win_rate ?? 0).toFixed(1)}%` },
        ].map(s => (
          <div key={s.label} className="bg-muted/20 rounded-md p-2">
            <div className="text-[10px] uppercase tracking-widest text-muted-foreground">{s.label}</div>
            <div className="text-xs font-mono font-semibold tabular-nums">{s.value}</div>
          </div>
        ))}
      </div>

      {/* ── Start / Pause ── */}
      <Button
        onClick={handleToggle}
        disabled={loading}
        className={`w-full font-semibold ${
          isRunning
            ? 'bg-loss/90 hover:bg-loss text-background'
            : 'bg-gain/90 hover:bg-gain text-background'
        }`}
      >
        {loading ? (
          <span className="animate-spin mr-1.5">⟳</span>
        ) : isRunning ? (
          <Pause className="w-4 h-4 mr-1.5" />
        ) : (
          <Play className="w-4 h-4 mr-1.5" />
        )}
        {loading ? 'Updating…' : isRunning ? 'Pause Futures Agent' : 'Start Futures Agent'}
      </Button>

      {/* ── Settings panel ── */}
      {showSettings && (
        <div className="border border-border/50 rounded-md p-3 space-y-3">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground">Futures Settings</div>
          <div className="grid grid-cols-2 gap-3">

            <div>
              <label className="text-[10px] text-muted-foreground">Leverage</label>
              <div className="flex gap-1 mt-1 flex-wrap">
                {[2, 3, 5, 10, 20].map(lv => (
                  <button
                    key={lv}
                    onClick={() => setSettings(s => ({ ...s, leverage: lv }))}
                    className={`px-2 py-0.5 rounded text-[10px] font-medium border transition-colors ${
                      settings.leverage === lv
                        ? 'bg-accent text-accent-foreground border-accent'
                        : 'border-border text-muted-foreground hover:text-foreground'
                    }`}
                  >{lv}x</button>
                ))}
              </div>
            </div>

            <div>
              <label className="text-[10px] text-muted-foreground">Min Signals (of 6)</label>
              <div className="flex gap-1 mt-1">
                {[3, 4, 5, 6].map(n => (
                  <button
                    key={n}
                    onClick={() => setSettings(s => ({ ...s, min_signals: n }))}
                    className={`px-2 py-0.5 rounded text-[10px] font-medium border transition-colors ${
                      settings.min_signals === n
                        ? 'bg-accent text-accent-foreground border-accent'
                        : 'border-border text-muted-foreground hover:text-foreground'
                    }`}
                  >{n}/6</button>
                ))}
              </div>
            </div>

            <div>
              <label className="text-[10px] text-muted-foreground">Margin / Trade (USDT)</label>
              <input
                type="number" min={10} max={10000} step={10}
                value={settings.budget_usdt}
                onChange={e => setSettings(s => ({ ...s, budget_usdt: Number(e.target.value) }))}
                className="w-full mt-1 bg-muted/30 border border-border rounded px-2 py-1 text-xs font-mono"
              />
            </div>

            <div>
              <label className="text-[10px] text-muted-foreground">Max Open Positions</label>
              <input
                type="number" min={1} max={20}
                value={settings.max_positions}
                onChange={e => setSettings(s => ({ ...s, max_positions: Number(e.target.value) }))}
                className="w-full mt-1 bg-muted/30 border border-border rounded px-2 py-1 text-xs font-mono"
              />
            </div>

            <div>
              <label className="text-[10px] text-muted-foreground">Take Profit %</label>
              <input
                type="number" min={0.1} max={20} step={0.1}
                value={(settings.take_profit_pct * 100).toFixed(1)}
                onChange={e => setSettings(s => ({ ...s, take_profit_pct: Number(e.target.value) / 100 }))}
                className="w-full mt-1 bg-muted/30 border border-border rounded px-2 py-1 text-xs font-mono"
              />
            </div>

            <div>
              <label className="text-[10px] text-muted-foreground">Stop Loss %</label>
              <input
                type="number" min={0.1} max={20} step={0.1}
                value={(settings.stop_loss_pct * 100).toFixed(1)}
                onChange={e => setSettings(s => ({ ...s, stop_loss_pct: Number(e.target.value) / 100 }))}
                className="w-full mt-1 bg-muted/30 border border-border rounded px-2 py-1 text-xs font-mono"
              />
            </div>
          </div>

          <Button onClick={handleSaveSettings} disabled={loading} className="w-full" size="sm">
            Save Settings
          </Button>
        </div>
      )}

      {/* ── Open positions ── */}
      {positions.length > 0 && (
        <div className="space-y-1.5">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground">
            Open Positions ({positions.length})
          </div>
          {positions.map(p => {
            const pnlColor = p.unrealized_pnl >= 0 ? 'text-gain' : 'text-loss';
            const pnlPct   = p.margin_usdt > 0
              ? ((p.unrealized_pnl / p.margin_usdt) * 100).toFixed(1)
              : '0.0';
            return (
              <div key={p.id}
                className="flex items-center justify-between bg-muted/20 rounded-md px-2.5 py-2 text-xs"
              >
                <div className="flex items-center gap-2">
                  {p.direction === 'LONG'
                    ? <TrendingUp  className="w-3.5 h-3.5 text-gain flex-shrink-0" />
                    : <TrendingDown className="w-3.5 h-3.5 text-loss flex-shrink-0" />}
                  <span className="font-mono font-medium">{p.symbol.replace('USDT', '')}</span>
                  <span className={`text-[10px] px-1 rounded font-semibold ${
                    p.direction === 'LONG' ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'
                  }`}>{p.direction} {p.leverage}x</span>
                </div>
                <div className="flex items-center gap-3 text-right">
                  <div>
                    <div className="text-[10px] text-muted-foreground">Entry</div>
                    <div className="font-mono">{Number(p.entry_price).toFixed(4)}</div>
                  </div>
                  <div>
                    <div className="text-[10px] text-muted-foreground">Mark</div>
                    <div className="font-mono">{p.mark_price > 0 ? Number(p.mark_price).toFixed(4) : '—'}</div>
                  </div>
                  <div className={pnlColor}>
                    <div className="text-[10px] text-muted-foreground">uPnL</div>
                    <div className="font-mono font-semibold">
                      {p.unrealized_pnl >= 0 ? '+' : ''}{Number(p.unrealized_pnl).toFixed(3)}
                      <span className="text-[10px] ml-0.5">({pnlPct}%)</span>
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* ── Signal grid ── */}
      {signals.length > 0 && (
        <div>
          <button
            onClick={() => setShowSignals(v => !v)}
            className="flex items-center gap-1 text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 hover:text-foreground transition-colors"
          >
            Signal Scanner ({signals.length} coins)
            {showSignals ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
          </button>
          {showSignals && (
            <div className="overflow-x-auto">
              <table className="w-full text-[10px] border-collapse">
                <thead>
                  <tr className="text-muted-foreground">
                    <th className="text-left pb-1 pr-2">Coin</th>
                    <th className="pb-1 pr-1">Score</th>
                    <th className="pb-1 pr-1" title="Trend">TR</th>
                    <th className="pb-1 pr-1" title="RSI">RSI</th>
                    <th className="pb-1 pr-1" title="MACD">MC</th>
                    <th className="pb-1 pr-1" title="Volume">VOL</th>
                    <th className="pb-1 pr-1" title="OBV">OBV</th>
                    <th className="pb-1 pr-1" title="Funding">FND</th>
                    <th className="pb-1 text-right">Fund%</th>
                  </tr>
                </thead>
                <tbody>
                  {signals.slice(0, 15).map(sig => {
                    const scoreColor = sig.score >= 4
                      ? 'text-gain font-semibold'
                      : sig.score <= -4
                        ? 'text-loss font-semibold'
                        : 'text-muted-foreground';
                    return (
                      <tr key={sig.symbol} className="border-t border-border/30">
                        <td className="py-0.5 pr-2 font-mono">{sig.symbol.replace('USDT', '')}</td>
                        <td className={`py-0.5 pr-1 text-center font-mono ${scoreColor}`}>
                          {sig.score > 0 ? '+' : ''}{sig.score}
                        </td>
                        <td className="py-0.5 pr-1 text-center"><SignalDot val={sig.signals?.trend} /></td>
                        <td className="py-0.5 pr-1 text-center"><SignalDot val={sig.signals?.rsi} /></td>
                        <td className="py-0.5 pr-1 text-center"><SignalDot val={sig.signals?.macd} /></td>
                        <td className="py-0.5 pr-1 text-center"><SignalDot val={sig.signals?.volume} /></td>
                        <td className="py-0.5 pr-1 text-center"><SignalDot val={sig.signals?.obv} /></td>
                        <td className="py-0.5 pr-1 text-center"><SignalDot val={sig.signals?.funding} /></td>
                        <td className={`py-0.5 text-right font-mono ${
                          (sig.funding_rate ?? 0) > 0 ? 'text-loss' : (sig.funding_rate ?? 0) < 0 ? 'text-gain' : ''
                        }`}>
                          {(sig.funding_rate ?? 0) >= 0 ? '+' : ''}{(sig.funding_rate ?? 0).toFixed(4)}%
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              <div className="mt-1 text-[9px] text-muted-foreground">
                TR=Trend · RSI · MC=MACD · VOL=Volume · OBV · FND=Funding &nbsp;
                <span className="text-gain">●</span> Bull &nbsp;
                <span className="text-loss">●</span> Bear &nbsp;
                ○ Neutral
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Recent trades ── */}
      {trades.length > 0 && (
        <div>
          <button
            onClick={() => setShowTrades(v => !v)}
            className="flex items-center gap-1 text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 hover:text-foreground transition-colors"
          >
            Recent Trades ({trades.length})
            {showTrades ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
          </button>
          {showTrades && (
            <div className="space-y-0.5 max-h-48 overflow-y-auto scrollbar-thin">
              {trades.map(t => {
                const pnl      = t.net_profit ?? 0;
                const pnlColor = pnl >= 0 ? 'text-gain' : 'text-loss';
                return (
                  <div key={t.id}
                    className="flex items-center justify-between text-xs py-1 border-b border-border/40 last:border-0"
                  >
                    <div className="flex items-center gap-2">
                      {t.direction === 'LONG'
                        ? <TrendingUp  className="w-3 h-3 text-gain flex-shrink-0" />
                        : <TrendingDown className="w-3 h-3 text-loss flex-shrink-0" />}
                      <span className="font-mono">{String(t.symbol).replace('USDT', '')}</span>
                      <span className={`text-[10px] ${t.direction === 'LONG' ? 'text-gain' : 'text-loss'}`}>
                        {t.direction} {t.leverage}x
                      </span>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="font-mono text-muted-foreground text-[10px]">
                        {Number(t.entry_price).toFixed(3)} → {Number(t.exit_price).toFixed(3)}
                      </span>
                      <span className={`font-mono font-medium ${pnlColor}`}>
                        {pnl >= 0 ? '+' : ''}{pnl.toFixed(3)}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* Loading / empty state */}
      {!status && !pollError && (
        <p className="text-xs text-muted-foreground text-center py-2 animate-pulse">
          Connecting to futures engine…
        </p>
      )}
      {!status && pollError && (
        <p className="text-xs text-loss text-center py-2">
          Could not reach the Railway API. Check the bot is deployed and running.
        </p>
      )}
    </div>
  );
};

export default FuturesAgent;
