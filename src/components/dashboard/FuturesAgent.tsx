import { useState, useEffect, useRef, useCallback } from 'react';
import {
  TrendingUp, TrendingDown, Pause, Play, RotateCcw,
  Settings, ChevronDown, ChevronUp, BarChart2, Shield, ShieldOff,
} from 'lucide-react';
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
  stop_loss_enabled: boolean;
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
  stop_loss: number | null;
  sl_enabled: boolean;
  liquidation_price: number;
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
  duration_seconds: number;
  timestamp_open: string;
  timestamp_close: string;
}

interface FuturesSignal {
  symbol: string;
  mark_price: number;
  score: number;
  funding_rate: number;
  signals: {
    trend?: number; rsi?: number; macd?: number;
    volume?: number; obv?: number; funding?: number;
  };
}

interface FuturesSettings {
  leverage: number;
  budget_usdt: number;
  budget_mode: 'fixed' | 'percent';
  budget_pct: number;
  take_profit_pct: number;
  stop_loss_pct: number;
  stop_loss_enabled: boolean;
  min_signals: number;
  max_positions: number;
}

// ── Constants ─────────────────────────────────────────────────────────────────

const RAILWAY_BASE  = (import.meta.env.VITE_RAILWAY_URL ?? '').replace(/\/$/, '');
const POLL_MS       = 8_000;
const START_BALANCE = 3000;

async function apiFetch(path: string, opts?: RequestInit) {
  return fetch(`${RAILWAY_BASE}${path}`, opts);
}

// ── Sub-components ────────────────────────────────────────────────────────────

function SignalDot({ val }: { val: number | undefined }) {
  if (val === undefined || val === null)
    return <span className="inline-block w-2 h-2 rounded-full bg-muted/40" />;
  if (val > 0) return <span className="inline-block w-2 h-2 rounded-full bg-gain" title="Bullish" />;
  if (val < 0) return <span className="inline-block w-2 h-2 rounded-full bg-loss" title="Bearish" />;
  return <span className="inline-block w-2 h-2 rounded-full bg-muted-foreground/30" title="Neutral" />;
}

/** Visual range bar: SL ──── Entry ──── Mark ──── TP */
function PositionBar({ pos }: { pos: FuturesPosition }) {
  const ep  = pos.entry_price;
  const mp  = pos.mark_price;
  const tp  = pos.take_profit;
  const sl  = pos.stop_loss ?? (pos.direction === 'LONG' ? pos.liquidation_price : pos.liquidation_price);
  const liq = pos.liquidation_price;

  // Range: from sl (or liq) to tp
  const lo = Math.min(sl, liq, ep, mp, tp);
  const hi = Math.max(sl, liq, ep, mp, tp);
  const range = hi - lo || 1;

  const pct = (v: number) => `${((v - lo) / range * 100).toFixed(1)}%`;

  const markPct = ((mp - lo) / range * 100);
  const entryPct = ((ep - lo) / range * 100);
  const tpPct   = ((tp - lo) / range * 100);
  const slPct   = ((sl - lo) / range * 100);

  const isLong  = pos.direction === 'LONG';
  const inProfit = isLong ? mp > ep : mp < ep;

  return (
    <div className="mt-2 px-0.5">
      <div className="relative h-3 bg-muted/30 rounded-full overflow-visible">
        {/* Profit zone fill */}
        {isLong ? (
          <div
            className={`absolute h-full rounded-full transition-all ${inProfit ? 'bg-gain/25' : 'bg-loss/25'}`}
            style={{
              left:  pct(Math.min(ep, mp)),
              width: pct(Math.max(ep, mp)).replace('%','') + '%',
            }}
          />
        ) : (
          <div
            className={`absolute h-full rounded-full transition-all ${inProfit ? 'bg-gain/25' : 'bg-loss/25'}`}
            style={{
              left:  pct(Math.min(ep, mp)),
              right: `${100 - ((Math.max(ep,mp) - lo) / range * 100)}%`,
            }}
          />
        )}

        {/* SL marker */}
        {pos.stop_loss && (
          <div
            className="absolute top-0 h-full w-0.5 bg-loss/70"
            style={{ left: `${slPct}%` }}
            title={`SL ${pos.stop_loss.toFixed(4)}`}
          />
        )}

        {/* Entry marker */}
        <div
          className="absolute top-0 h-full w-0.5 bg-muted-foreground/60"
          style={{ left: `${entryPct}%` }}
          title={`Entry ${ep.toFixed(4)}`}
        />

        {/* TP marker */}
        <div
          className="absolute top-0 h-full w-0.5 bg-gain/70"
          style={{ left: `${tpPct}%` }}
          title={`TP ${tp.toFixed(4)}`}
        />

        {/* Mark price dot */}
        <div
          className={`absolute top-1/2 -translate-y-1/2 w-3 h-3 rounded-full border-2 border-background transition-all ${
            inProfit ? 'bg-gain' : 'bg-loss'
          }`}
          style={{ left: `calc(${markPct}% - 6px)` }}
          title={`Mark ${mp.toFixed(4)}`}
        />
      </div>

      {/* Labels */}
      <div className="flex justify-between text-[9px] text-muted-foreground mt-0.5 px-0.5">
        <span className="text-loss">{pos.stop_loss ? `SL ${pos.stop_loss.toFixed(2)}` : 'No SL'}</span>
        <span>Entry {ep.toFixed(2)}</span>
        <span className="text-gain">TP {tp.toFixed(2)}</span>
      </div>
    </div>
  );
}

/** Aggregated reports panel */
function ReportsPanel({ trades, startBalance }: { trades: FuturesTrade[]; startBalance: number }) {
  if (!trades.length) return (
    <p className="text-xs text-muted-foreground text-center py-3">No closed trades yet</p>
  );

  const profits = trades.map(t => t.net_profit ?? 0);
  const wins    = profits.filter(p => p > 0).length;
  const losses  = profits.filter(p => p <= 0).length;
  const totalPnl = profits.reduce((a, b) => a + b, 0);
  const avgPnl   = totalPnl / trades.length;
  const bestTrade  = Math.max(...profits);
  const worstTrade = Math.min(...profits);
  const winRate    = (wins / trades.length) * 100;
  const avgDur = trades.reduce((a, t) => a + (t.duration_seconds ?? 0), 0) / trades.length;

  const longTrades  = trades.filter(t => t.direction === 'LONG');
  const shortTrades = trades.filter(t => t.direction === 'SHORT');
  const longWins    = longTrades.filter(t => (t.net_profit ?? 0) > 0).length;
  const shortWins   = shortTrades.filter(t => (t.net_profit ?? 0) > 0).length;

  const fmtDur = (s: number) => s < 60 ? `${s.toFixed(0)}s` : s < 3600 ? `${(s/60).toFixed(0)}m` : `${(s/3600).toFixed(1)}h`;

  return (
    <div className="space-y-3">
      {/* Summary grid */}
      <div className="grid grid-cols-3 gap-2">
        {[
          { label: 'Total Trades', value: trades.length },
          { label: 'Wins',  value: wins,   color: 'text-gain' },
          { label: 'Losses', value: losses, color: 'text-loss' },
          { label: 'Win Rate', value: `${winRate.toFixed(1)}%`,
            color: winRate >= 50 ? 'text-gain' : 'text-loss' },
          { label: 'Total P&L',
            value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`,
            color: totalPnl >= 0 ? 'text-gain' : 'text-loss' },
          { label: 'Avg P&L',
            value: `${avgPnl >= 0 ? '+' : ''}$${avgPnl.toFixed(2)}`,
            color: avgPnl >= 0 ? 'text-gain' : 'text-loss' },
          { label: 'Best Trade',  value: `+$${bestTrade.toFixed(2)}`,  color: 'text-gain' },
          { label: 'Worst Trade', value: `$${worstTrade.toFixed(2)}`,  color: 'text-loss' },
          { label: 'Avg Hold',   value: fmtDur(avgDur) },
        ].map(s => (
          <div key={s.label} className="bg-muted/20 rounded-md p-2">
            <div className="text-[9px] uppercase tracking-widest text-muted-foreground">{s.label}</div>
            <div className={`text-xs font-mono font-semibold ${s.color ?? ''}`}>{String(s.value)}</div>
          </div>
        ))}
      </div>

      {/* Long vs Short breakdown */}
      <div className="grid grid-cols-2 gap-2">
        <div className="bg-gain/10 border border-gain/20 rounded-md p-2">
          <div className="text-[9px] uppercase text-gain font-medium mb-1">LONG</div>
          <div className="text-xs font-mono">{longTrades.length} trades · {longTrades.length ? `${(longWins/longTrades.length*100).toFixed(0)}%` : '—'} win</div>
          <div className={`text-xs font-mono font-semibold ${longTrades.reduce((a,t)=>a+(t.net_profit??0),0)>=0?'text-gain':'text-loss'}`}>
            {longTrades.reduce((a,t)=>a+(t.net_profit??0),0)>=0?'+':''}${longTrades.reduce((a,t)=>a+(t.net_profit??0),0).toFixed(2)}
          </div>
        </div>
        <div className="bg-loss/10 border border-loss/20 rounded-md p-2">
          <div className="text-[9px] uppercase text-loss font-medium mb-1">SHORT</div>
          <div className="text-xs font-mono">{shortTrades.length} trades · {shortTrades.length ? `${(shortWins/shortTrades.length*100).toFixed(0)}%` : '—'} win</div>
          <div className={`text-xs font-mono font-semibold ${shortTrades.reduce((a,t)=>a+(t.net_profit??0),0)>=0?'text-gain':'text-loss'}`}>
            {shortTrades.reduce((a,t)=>a+(t.net_profit??0),0)>=0?'+':''}${shortTrades.reduce((a,t)=>a+(t.net_profit??0),0).toFixed(2)}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

const FuturesAgent = () => {
  const [status, setStatus]       = useState<FuturesStatus | null>(null);
  const [positions, setPositions] = useState<FuturesPosition[]>([]);
  const [trades, setTrades]       = useState<FuturesTrade[]>([]);
  const [signals, setSignals]     = useState<FuturesSignal[]>([]);
  const [settings, setSettings]   = useState<FuturesSettings>({
    leverage: 5, budget_usdt: 200, budget_mode: 'fixed', budget_pct: 10,
    take_profit_pct: 0.02, stop_loss_pct: 0.01, stop_loss_enabled: true,
    min_signals: 2, max_positions: 5,
  });

  const [showSettings, setShowSettings] = useState(false);
  const [showSignals, setShowSignals]   = useState(false);
  const [showTrades, setShowTrades]     = useState(false);
  const [showReports, setShowReports]   = useState(false);
  const [loading, setLoading]           = useState(false);
  const [pollError, setPollError]       = useState(false);
  const pollRef    = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inFlightRef = useRef(false);

  // ── Single combined poll ──────────────────────────────────────────────────

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
      // Sync settings toggles from server state
      if (d.status?.stop_loss_enabled !== undefined) {
        setSettings(s => ({
          ...s,
          stop_loss_enabled: d.status.stop_loss_enabled,
          budget_mode: d.status.budget_mode ?? s.budget_mode,
          budget_pct:  d.status.budget_pct  ?? s.budget_pct,
        }));
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
      setStatus(s => s ? { ...s, running: !s.running } : s);
    }
  };

  const handleSlToggle = async () => {
    const next = !settings.stop_loss_enabled;
    const res = await postAction('/api/futures/settings', { stop_loss_enabled: next });
    if (res) {
      setSettings(s => ({ ...s, stop_loss_enabled: next }));
      toast.info(next ? 'Stop loss enabled' : 'Stop loss disabled — TP-only mode');
    }
  };

  const handleReset = async () => {
    if (!confirm('Reset futures paper wallet? All positions and trade history will be erased.')) return;
    const res = await postAction('/api/futures/reset', { starting_usdt: START_BALANCE });
    if (res) {
      toast.success(`Futures wallet reset to ${START_BALANCE} USDT`);
      setPositions([]); setTrades([]);
      setStatus(s => s ? {
        ...s, balance: START_BALANCE, equity: START_BALANCE,
        total_pnl: 0, win_rate: 0, trade_count: 0, positions: 0,
      } : s);
    }
  };

  const handleSaveSettings = async () => {
    const res = await postAction('/api/futures/settings', {
      leverage:          settings.leverage,
      budget_usdt:       settings.budget_usdt,
      budget_mode:       settings.budget_mode,
      budget_pct:        settings.budget_pct,
      take_profit_pct:   settings.take_profit_pct,
      stop_loss_pct:     settings.stop_loss_pct,
      stop_loss_enabled: settings.stop_loss_enabled,
      min_signals:       settings.min_signals,
      max_positions:     settings.max_positions,
    });
    if (res) { toast.success('Futures settings saved'); setShowSettings(false); }
  };

  // ── Derived ──────────────────────────────────────────────────────────────────

  const isRunning  = status?.running ?? false;
  const slEnabled  = settings.stop_loss_enabled;
  const equity     = status?.equity  ?? 0;
  const balance    = status?.balance ?? 0;
  const totalPnl   = status?.total_pnl ?? 0;
  const sessionPct = START_BALANCE > 0
    ? (((equity - START_BALANCE) / START_BALANCE) * 100).toFixed(2)
    : '0.00';

  // ── Render ──────────────────────────────────────────────────────────────────

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
            USDT-M · Paper
          </span>
          {pollError && <span className="text-[10px] text-loss">⚠ offline</span>}
        </div>
        <div className="flex gap-1">
          {/* Stop-loss quick toggle */}
          <button
            onClick={handleSlToggle}
            disabled={loading}
            title={slEnabled ? 'Stop loss ON — click to disable' : 'Stop loss OFF — click to enable'}
            className={`flex items-center gap-1 px-2 py-1 rounded text-[10px] font-medium border transition-colors ${
              slEnabled
                ? 'bg-gain/10 border-gain/30 text-gain hover:bg-gain/20'
                : 'bg-loss/10 border-loss/30 text-loss hover:bg-loss/20'
            }`}
          >
            {slEnabled ? <Shield className="w-3 h-3" /> : <ShieldOff className="w-3 h-3" />}
            SL {slEnabled ? 'ON' : 'OFF'}
          </button>
          <Button variant="ghost" size="icon" className="h-7 w-7"
            onClick={() => setShowSettings(v => !v)} title="Settings">
            <Settings className="w-3.5 h-3.5" />
          </Button>
          <Button variant="ghost" size="icon" className="h-7 w-7"
            onClick={handleReset} disabled={loading} title="Reset wallet">
            <RotateCcw className="w-3.5 h-3.5" />
          </Button>
        </div>
      </div>

      {/* ── Stats ── */}
      <div className="grid grid-cols-4 gap-2">
        {[
          { label: 'Balance', value: `$${balance.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` },
          { label: 'Equity',  value: `$${equity.toLocaleString('en-US',  { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` },
          { label: 'Total P&L', value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`,
            color: totalPnl >= 0 ? 'text-gain' : 'text-loss' },
          { label: 'Session', value: `${Number(sessionPct) >= 0 ? '+' : ''}${sessionPct}%`,
            color: Number(sessionPct) >= 0 ? 'text-gain' : 'text-loss' },
        ].map(s => (
          <div key={s.label} className="bg-muted/20 rounded-md p-2">
            <div className="text-[10px] uppercase tracking-widest text-muted-foreground">{s.label}</div>
            <div className={`text-xs font-mono font-semibold tabular-nums ${s.color ?? ''}`}>{s.value}</div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-3 gap-2">
        {[
          { label: 'Open',     value: String(status?.positions ?? 0) },
          { label: 'Trades',   value: String(status?.trade_count ?? 0) },
          { label: 'Win Rate', value: `${(status?.win_rate ?? 0).toFixed(1)}%` },
        ].map(s => (
          <div key={s.label} className="bg-muted/20 rounded-md p-2">
            <div className="text-[10px] uppercase tracking-widest text-muted-foreground">{s.label}</div>
            <div className="text-xs font-mono font-semibold tabular-nums">{s.value}</div>
          </div>
        ))}
      </div>

      {/* ── Start / Pause ── */}
      <Button
        onClick={handleToggle} disabled={loading}
        className={`w-full font-semibold ${
          isRunning
            ? 'bg-loss/90 hover:bg-loss text-background'
            : 'bg-gain/90 hover:bg-gain text-background'
        }`}
      >
        {loading ? <span className="animate-spin mr-1.5">⟳</span>
          : isRunning ? <Pause className="w-4 h-4 mr-1.5" />
          : <Play className="w-4 h-4 mr-1.5" />}
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
                  <button key={lv}
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
                {[2, 3, 4, 5, 6].map(n => (
                  <button key={n}
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
              <label className="text-[10px] text-muted-foreground">Budget Mode</label>
              <div className="flex gap-1 mt-1">
                {(['fixed', 'percent'] as const).map(m => (
                  <button key={m}
                    onClick={() => setSettings(s => ({ ...s, budget_mode: m }))}
                    className={`flex-1 px-2 py-0.5 rounded text-[10px] font-medium border transition-colors ${
                      settings.budget_mode === m
                        ? 'bg-accent text-accent-foreground border-accent'
                        : 'border-border text-muted-foreground hover:text-foreground'
                    }`}
                  >{m === 'fixed' ? 'Fixed $' : '% Balance'}</button>
                ))}
              </div>
              {settings.budget_mode === 'fixed' ? (
                <input type="number" min={10} max={10000} step={10}
                  value={settings.budget_usdt}
                  onChange={e => setSettings(s => ({ ...s, budget_usdt: Number(e.target.value) }))}
                  className="w-full mt-1 bg-muted/30 border border-border rounded px-2 py-1 text-xs font-mono"
                  placeholder="USDT per trade"
                />
              ) : (
                <div className="flex items-center gap-1 mt-1">
                  <input type="number" min={1} max={100} step={1}
                    value={settings.budget_pct}
                    onChange={e => setSettings(s => ({ ...s, budget_pct: Number(e.target.value) }))}
                    className="flex-1 bg-muted/30 border border-border rounded px-2 py-1 text-xs font-mono"
                    placeholder="% of balance"
                  />
                  <span className="text-[10px] text-muted-foreground">%</span>
                </div>
              )}
            </div>
            <div>
              <label className="text-[10px] text-muted-foreground">Max Open Positions</label>
              <input type="number" min={1} max={20}
                value={settings.max_positions}
                onChange={e => setSettings(s => ({ ...s, max_positions: Number(e.target.value) }))}
                className="w-full mt-1 bg-muted/30 border border-border rounded px-2 py-1 text-xs font-mono"
              />
            </div>
            <div>
              <label className="text-[10px] text-muted-foreground">Take Profit %</label>
              <input type="number" min={0.1} max={50} step={0.1}
                value={(settings.take_profit_pct * 100).toFixed(1)}
                onChange={e => setSettings(s => ({ ...s, take_profit_pct: Number(e.target.value) / 100 }))}
                className="w-full mt-1 bg-muted/30 border border-border rounded px-2 py-1 text-xs font-mono"
              />
            </div>
            <div>
              <label className="text-[10px] text-muted-foreground">Stop Loss %</label>
              <div className="flex gap-2 mt-1 items-center">
                <input type="number" min={0.1} max={50} step={0.1}
                  value={(settings.stop_loss_pct * 100).toFixed(1)}
                  disabled={!settings.stop_loss_enabled}
                  onChange={e => setSettings(s => ({ ...s, stop_loss_pct: Number(e.target.value) / 100 }))}
                  className="flex-1 bg-muted/30 border border-border rounded px-2 py-1 text-xs font-mono disabled:opacity-40"
                />
                {/* Inline SL toggle */}
                <button
                  onClick={() => setSettings(s => ({ ...s, stop_loss_enabled: !s.stop_loss_enabled }))}
                  className={`px-2 py-1 rounded text-[10px] font-medium border transition-colors flex-shrink-0 ${
                    settings.stop_loss_enabled
                      ? 'bg-gain/10 border-gain/30 text-gain'
                      : 'bg-muted/30 border-border text-muted-foreground'
                  }`}
                >{settings.stop_loss_enabled ? 'ON' : 'OFF'}</button>
              </div>
            </div>
          </div>
          <Button onClick={handleSaveSettings} disabled={loading} className="w-full" size="sm">
            Save Settings
          </Button>
        </div>
      )}

      {/* ── Open positions with range bar ── */}
      {positions.length > 0 && (
        <div className="space-y-3">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground">
            Open Positions ({positions.length})
          </div>
          {positions.map(p => {
            const pnlColor = p.unrealized_pnl >= 0 ? 'text-gain' : 'text-loss';
            const roe = p.margin_usdt > 0
              ? ((p.unrealized_pnl / p.margin_usdt) * 100).toFixed(1)
              : '0.0';
            return (
              <div key={p.id} className="bg-muted/20 rounded-md px-3 py-2.5">
                {/* Row 1: coin + direction + P&L */}
                <div className="flex items-center justify-between text-xs">
                  <div className="flex items-center gap-2">
                    {p.direction === 'LONG'
                      ? <TrendingUp  className="w-3.5 h-3.5 text-gain flex-shrink-0" />
                      : <TrendingDown className="w-3.5 h-3.5 text-loss flex-shrink-0" />}
                    <span className="font-mono font-semibold">{p.symbol.replace('USDT', '')}</span>
                    <span className={`text-[10px] px-1.5 py-0.5 rounded font-semibold ${
                      p.direction === 'LONG' ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'
                    }`}>{p.direction} {p.leverage}x</span>
                    {!p.stop_loss && (
                      <span className="text-[9px] text-muted-foreground">No SL</span>
                    )}
                  </div>
                  <div className={`text-right ${pnlColor}`}>
                    <span className="font-mono font-semibold">
                      {p.unrealized_pnl >= 0 ? '+' : ''}{Number(p.unrealized_pnl).toFixed(3)} USDT
                    </span>
                    <span className="text-[10px] ml-1">({roe}% ROE)</span>
                  </div>
                </div>

                {/* Row 2: prices */}
                <div className="flex gap-4 mt-1.5 text-[10px] text-muted-foreground">
                  <span>Entry <span className="font-mono text-foreground">{Number(p.entry_price).toFixed(4)}</span></span>
                  <span>Mark <span className={`font-mono ${pnlColor}`}>{p.mark_price > 0 ? Number(p.mark_price).toFixed(4) : '—'}</span></span>
                  <span>TP <span className="font-mono text-gain">{Number(p.take_profit).toFixed(4)}</span></span>
                  {p.stop_loss && <span>SL <span className="font-mono text-loss">{Number(p.stop_loss).toFixed(4)}</span></span>}
                </div>

                {/* Position range bar */}
                <PositionBar pos={p} />
              </div>
            );
          })}
        </div>
      )}

      {/* ── Reports ── */}
      <div>
        <button
          onClick={() => setShowReports(v => !v)}
          className="flex items-center gap-1 text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 hover:text-foreground transition-colors w-full"
        >
          <BarChart2 className="w-3 h-3" />
          Reports {trades.length > 0 && `(${trades.length} trades)`}
          {showReports ? <ChevronUp className="w-3 h-3 ml-auto" /> : <ChevronDown className="w-3 h-3 ml-auto" />}
        </button>
        {showReports && (
          <ReportsPanel trades={trades} startBalance={START_BALANCE} />
        )}
      </div>

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
                    <th className="pb-1 pr-1" title="Trend (EMA20)">TR</th>
                    <th className="pb-1 pr-1" title="RSI">RSI</th>
                    <th className="pb-1 pr-1" title="MACD histogram">MC</th>
                    <th className="pb-1 pr-1" title="Volume">VOL</th>
                    <th className="pb-1 pr-1" title="OBV slope">OBV</th>
                    <th className="pb-1 pr-1" title="Funding rate">FND</th>
                    <th className="pb-1 text-right">Fund%</th>
                  </tr>
                </thead>
                <tbody>
                  {signals.slice(0, 15).map(sig => {
                    const scoreColor = sig.score >= 3
                      ? 'text-gain font-semibold'
                      : sig.score <= -3 ? 'text-loss font-semibold'
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
                          {(sig.funding_rate ?? 0) >= 0 ? '+' : ''}{Number(sig.funding_rate ?? 0).toFixed(4)}%
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              <div className="mt-1 text-[9px] text-muted-foreground">
                TR=Trend · RSI · MC=MACD · VOL · OBV · FND=Funding &nbsp;
                <span className="text-gain">●</span> Bull &nbsp;
                <span className="text-loss">●</span> Bear &nbsp; ○ Neutral
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Trade history ── */}
      {trades.length > 0 && (
        <div>
          <button
            onClick={() => setShowTrades(v => !v)}
            className="flex items-center gap-1 text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 hover:text-foreground transition-colors"
          >
            Trade History ({trades.length})
            {showTrades ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
          </button>
          {showTrades && (
            <div className="space-y-0.5 max-h-52 overflow-y-auto scrollbar-thin">
              {trades.map(t => {
                const pnl      = t.net_profit ?? 0;
                const pnlColor = pnl >= 0 ? 'text-gain' : 'text-loss';
                const dur      = t.duration_seconds
                  ? t.duration_seconds < 60 ? `${t.duration_seconds}s`
                  : t.duration_seconds < 3600 ? `${Math.round(t.duration_seconds/60)}m`
                  : `${(t.duration_seconds/3600).toFixed(1)}h`
                  : '';
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
                      {dur && <span className="text-[9px] text-muted-foreground">{dur}</span>}
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="font-mono text-muted-foreground text-[10px]">
                        {Number(t.entry_price).toFixed(3)} → {Number(t.exit_price).toFixed(3)}
                      </span>
                      <span className={`font-mono font-semibold ${pnlColor}`}>
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

      {/* Empty / connecting state */}
      {!status && !pollError && (
        <p className="text-xs text-muted-foreground text-center py-2 animate-pulse">
          Connecting to futures engine…
        </p>
      )}
      {!status && pollError && (
        <p className="text-xs text-loss text-center py-2">
          Could not reach the Railway API — check the bot is deployed and running.
        </p>
      )}
    </div>
  );
};

export default FuturesAgent;
