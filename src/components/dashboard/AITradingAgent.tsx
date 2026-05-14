import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Play, Square, Brain, TrendingUp, TrendingDown, Zap,
  RotateCcw, ChevronDown, ChevronUp, FlaskConical,
  Pencil, Check, X, BookOpen, Activity, Eye, EyeOff,
  ShoppingCart, Banknote, RefreshCw, Settings2, Shield,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import { checkExits, TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { calcEMA, calcRSI, calcMACD, calcBollingerBands, calcSMA } from '@/lib/indicators';
import { formatTime, formatPnL } from '@/lib/format';

// ── Simple 4-signal analyser (no API key, Binance public data only) ────────────────────
const BIN = 'https://api.binance.com/api/v3';
const SESSION = 'default';
const MAX_POSITIONS = 3;
const MIN_USDT      = 11;
const PAPER_CFG_KEY = 'paper_wallet_config';

function getPaperCfg() {
  try { return JSON.parse(localStorage.getItem(PAPER_CFG_KEY) ?? '{}'); }
  catch { return {}; }
}

// Compute USDT to allocate for one trade based on wallet's budget mode setting.
function getAllocation(runBal: number, symbol: string): number {
  const cfg = getPaperCfg();
  const mode = cfg.budgetMode ?? 'percent';
  switch (mode) {
    case 'fixed':    return Math.min(cfg.budgetFixed ?? 100, runBal);
    case 'percent':  return Math.min(runBal * (cfg.budgetPct ?? 25) / 100, runBal);
    case 'capped':   return Math.min((cfg.budgetCap ?? 500) / MAX_POSITIONS, runBal);
    case 'per_coin': return Math.min(cfg.budgetPerCoin?.[symbol] ?? 100, runBal);
    case 'coin_pct': return Math.min(runBal * ((cfg.budgetCoinPct?.[symbol] ?? 5) / 100), runBal);
    default:         return Math.min(runBal * 0.25, runBal);
  }
}

// RSI thresholds — match config.py: RSI_BUY_MIN=40, RSI_BUY_MAX=65
// RSI 65 passes (<=), RSI 66 fails (>)
const RSI_BUY_MIN = 40;
const RSI_BUY_MAX = 65;

// evaluateSignals: returns exactly {trend, rsi, macd, volume} — all booleans.
// This is the single authoritative source of truth for signal evaluation.
// bullish_count = Object.values(signals).filter(Boolean).length — all 4 keys included.
// BUY fires only if bullish_count >= 3.
function evaluateSignals(closes: number[], volumes: number[]): {
  trend: boolean; rsi: boolean; macd: boolean; volume: boolean;
} {
  const ema9   = calcEMA(closes, 9);
  const ema21  = calcEMA(closes, 21);
  const rsiVal = calcRSI(closes, 14);
  const macd   = calcMACD(closes);
  const volSma = calcSMA(volumes, 20);
  const curVol = volumes[volumes.length - 1] ?? 0;
  const volRat = volSma > 0 ? curVol / volSma : 1;
  const recentVol = volumes.slice(-10).reduce((a, b) => a + b, 0);
  const prevVol   = volumes.slice(-20, -10).reduce((a, b) => a + b, 0);

  return {
    trend:  ema9 > ema21,
    // RSI_BUY_MIN <= rsi <= RSI_BUY_MAX (both constants — RSI 65 passes, 66 fails)
    rsi:    rsiVal >= RSI_BUY_MIN && rsiVal <= RSI_BUY_MAX,
    macd:   macd.histogram > 0,
    volume: volRat >= 1.5,
  };
}

interface CoinSignal {
  symbol: string;
  price: number;
  signal: 'BUY' | 'HOLD' | 'loading' | 'error';
  emaBullish: boolean;
  rsiOk: boolean;
  macdPos: boolean;
  volUp: boolean;
  rsi: number;
  reason: string;
}

// Fetch one coin sequentially — avoids Binance rate-limit on parallel bulk requests.
// Uses klines only (no depth endpoint) to halve the request count.
async function analyseCoin(sym: string): Promise<CoinSignal> {
  const ctrl = new AbortController();
  const timeout = setTimeout(() => ctrl.abort(), 8000);
  try {
    const res = await fetch(
      `${BIN}/klines?symbol=${sym}&interval=1m&limit=60`,
      { signal: ctrl.signal }
    );
    clearTimeout(timeout);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const klines = await res.json();
    if (!Array.isArray(klines) || klines.length < 30) throw new Error('insufficient data');

    const closes  = klines.map((k: any[]) => parseFloat(k[4]));
    const volumes = klines.map((k: any[]) => parseFloat(k[5]));
    const price   = closes[closes.length - 1] ?? 0;
    const signals = evaluateSignals(closes, volumes);
    const rsiVal  = calcRSI(closes, 14);
    const bullish = Object.values(signals).filter(Boolean).length;
    const reason  = bullish >= 4 ? 'Strong buy signal (4/4)'
                  : bullish === 3 ? 'Moderate buy signal (3/4)'
                  : `Insufficient signals (${bullish}/4)`;
    return {
      symbol: sym, price,
      signal: bullish >= 3 ? 'BUY' : 'HOLD',
      emaBullish: signals.trend, rsiOk: signals.rsi,
      macdPos: signals.macd, volUp: signals.volume,
      rsi: rsiVal, reason,
    };
  } catch (e: any) {
    clearTimeout(timeout);
    return { symbol: sym, price: 0, signal: 'error', emaBullish: false, rsiOk: false, macdPos: false, volUp: false, rsi: 0, reason: e.message };
  }
}

const INSTRUCTIONS_KEY  = 'ai_agent_instructions';
const AGENT_CYCLE_MS    = 30_000;
const MAX_LOG_LINES     = 200;
const isServerMode      = true; // always use VPS bot REST API

interface OpenPosition {
  symbol: string;
  quantity: number;
  avg_entry_price: number;
  exit_target?: number;
  current_price?: number;
  profitable?: boolean;
  hold_human?: string;
  hold_minutes?: number;
  breakeven_price_real?: number;
  real_bep_gap_pct?: number;
  is_trapped?: boolean;
}

interface TradeRow {
  id: string;
  created_at: string;
  symbol: string;
  side: 'BUY' | 'SELL';
  price: number;
  quantity: number;
  pnl: number | null;
  reason: string | null;
  volume_usdt?: number;
}

interface AITradingAgentProps {
  selectedCoins: string[];
  prices: LivePrices;
  binanceConnected?: boolean;
  onConnectBinance?: () => void;
  onCoinsChange?: (coins: string[]) => void;
  onLiveModeDetected?: (isLive: boolean) => void;
  onStateChange?: (
    positions: {symbol:string;quantity:number;avg_entry_price:number}[],
    balance: number,
    initialBalance?: number,
    trades?: {side:'BUY'|'SELL';pnl:number|null;quantity:number;price:number}[]
  ) => void;
}

interface AgentFieldsProps {
  budgetMode: 'fixed'|'percent'|'capped'; setBudgetMode: (m: 'fixed'|'percent'|'capped') => void;
  budgetValue: number;     setBudgetValue: (n: number) => void;
  allocation: number;      setAllocation: (n: number) => void;
  slEnabled: boolean;      setSlEnabled: (v: boolean) => void;
  stopLoss: number;        setStopLoss: (n: number) => void;
  tpEnabled: boolean;      setTpEnabled: (v: boolean) => void;
  takeProfit: number;      setTakeProfit: (n: number) => void;
  smartHold: boolean;      setSmartHold: (v: boolean) => void;
  trailingStop: number;    setTrailingStop: (n: number) => void;
  reinvest: boolean;       setReinvest: (v: boolean) => void;
  maxPositions: number;    setMaxPositions: (n: number) => void;
  minSignals: number;      setMinSignals: (n: number) => void;
}

// Inline toggle switch
const Toggle = ({ on, onChange, color = 'bg-accent/80' }: { on: boolean; onChange: () => void; color?: string }) => (
  <button onClick={onChange}
    className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors shrink-0 ${on ? color : 'bg-muted/60'}`}>
    <span className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform ${on ? 'translate-x-4' : 'translate-x-0.5'}`} />
  </button>
);

const AgentTradingFields = React.memo(({
  budgetMode, setBudgetMode, budgetValue, setBudgetValue,
  allocation, setAllocation,
  slEnabled, setSlEnabled, stopLoss, setStopLoss,
  tpEnabled, setTpEnabled, takeProfit, setTakeProfit,
  smartHold, setSmartHold, trailingStop, setTrailingStop,
  reinvest, setReinvest,
  maxPositions, setMaxPositions, minSignals, setMinSignals,
}: AgentFieldsProps) => (
  <div className="space-y-3">
    {/* ── Trade Size Mode ── */}
    <div className="space-y-1.5">
      <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Trade Size Mode</label>
      <div className="grid grid-cols-3 gap-1">
        {([['fixed','Fixed USDT'],['percent','% of Balance'],['capped','Capped Total']] as const).map(([val, label]) => (
          <button key={val} onClick={() => setBudgetMode(val)}
            className={`py-1.5 text-[11px] font-semibold rounded border transition-colors ${budgetMode === val ? 'bg-accent text-accent-foreground border-accent' : 'border-border text-muted-foreground hover:border-accent/50'}`}>
            {label}
          </button>
        ))}
      </div>
    </div>

    {/* Per-trade size value */}
    <div className="space-y-1">
      <label className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {budgetMode === 'fixed' ? 'Amount per trade (USDT)' : budgetMode === 'percent' ? '% of free balance per trade' : 'Total capital cap (USDT)'}
      </label>
      <div className="flex items-center gap-2">
        <input type="number" min="1" step="1" max={budgetMode === 'percent' ? '100' : '100000'}
          value={budgetValue}
          onChange={e => setBudgetValue(parseFloat(e.target.value) || 10)}
          className="w-32 bg-muted/40 border border-border rounded px-2 py-1.5 text-sm font-mono focus:outline-none focus:border-accent/60" />
        <span className="text-xs text-muted-foreground">{budgetMode === 'percent' ? '%' : 'USDT'}</span>
      </div>
    </div>

    {/* Bot Allocation */}
    <div className="space-y-1">
      <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Bot Allocation (USDT cap, 0 = unlimited)</label>
      <div className="flex items-center gap-2">
        <input type="number" min="0" step="10"
          value={allocation}
          onChange={e => setAllocation(parseFloat(e.target.value) || 0)}
          className="w-32 bg-muted/40 border border-border rounded px-2 py-1.5 text-sm font-mono focus:outline-none focus:border-accent/60" />
        <span className="text-xs text-muted-foreground">USDT</span>
      </div>
    </div>

    {/* ── Stop Loss ── */}
    <div className="bg-muted/30 rounded-md px-3 py-2.5 space-y-2">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs font-semibold">Stop Loss</p>
          <p className="text-[9px] text-muted-foreground">{slEnabled ? 'Auto-exit when position loses this %' : 'OFF — hold through drawdowns'}</p>
        </div>
        <Toggle on={slEnabled} onChange={() => setSlEnabled(!slEnabled)} color="bg-loss/80" />
      </div>
      {slEnabled && (
        <div className="flex items-center gap-2">
          <input type="number" min="0.1" max="50" step="0.1"
            value={stopLoss}
            onChange={e => setStopLoss(parseFloat(e.target.value) || 2)}
            className="w-24 bg-muted/40 border border-border rounded px-2 py-1 text-xs font-mono focus:outline-none focus:border-loss/60" />
          <span className="text-xs text-muted-foreground">% below entry</span>
        </div>
      )}
    </div>

    {/* ── Take Profit ── */}
    <div className="bg-muted/30 rounded-md px-3 py-2.5 space-y-2">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs font-semibold">Take Profit</p>
          <p className="text-[9px] text-muted-foreground">{tpEnabled ? 'Auto-exit when position gains this %' : 'OFF — hold indefinitely'}</p>
        </div>
        <Toggle on={tpEnabled} onChange={() => setTpEnabled(!tpEnabled)} color="bg-gain/80" />
      </div>
      {tpEnabled && (
        <div className="flex items-center gap-2">
          <input type="number" min="0.1" max="100" step="0.1"
            value={takeProfit}
            onChange={e => setTakeProfit(parseFloat(e.target.value) || 0.5)}
            className="w-24 bg-muted/40 border border-border rounded px-2 py-1 text-xs font-mono focus:outline-none focus:border-gain/60" />
          <span className="text-xs text-muted-foreground">% above entry</span>
        </div>
      )}
    </div>

    {/* ── Smart Hold ── */}
    <div className="bg-muted/30 rounded-md px-3 py-2.5 space-y-2">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs font-semibold">Smart Hold</p>
          <p className="text-[9px] text-muted-foreground">{smartHold ? 'Hold while bullish; exit when signals turn or price drops from peak' : 'OFF — exit immediately at profit target'}</p>
        </div>
        <Toggle on={smartHold} onChange={() => setSmartHold(!smartHold)} color="bg-accent/80" />
      </div>
      {smartHold && (
        <div className="flex items-center gap-2">
          <input type="number" min="0.1" max="10" step="0.1"
            value={trailingStop}
            onChange={e => setTrailingStop(parseFloat(e.target.value) || 0.5)}
            className="w-24 bg-muted/40 border border-border rounded px-2 py-1 text-xs font-mono focus:outline-none focus:border-accent/60" />
          <span className="text-xs text-muted-foreground">% trailing drop from peak</span>
        </div>
      )}
    </div>

    {/* ── Reinvest Profits ── */}
    <div className="bg-muted/30 rounded-md px-3 py-2.5">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs font-semibold">Reinvest Profits</p>
          <p className="text-[9px] text-muted-foreground">{reinvest ? 'Trade sizes grow as balance grows' : 'OFF — fixed trade sizes'}</p>
        </div>
        <Toggle on={reinvest} onChange={() => setReinvest(!reinvest)} color="bg-gain/80" />
      </div>
    </div>

    {/* ── Max Positions + Min Signals ── */}
    <div className="grid grid-cols-2 gap-3">
      <div>
        <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Max Positions</label>
        <input type="number" min="1" max="50" step="1"
          value={maxPositions}
          onChange={e => setMaxPositions(parseInt(e.target.value) || 10)}
          className="w-full mt-1 bg-muted/40 border border-border rounded px-2 py-1.5 text-xs font-mono focus:outline-none focus:border-accent/60" />
        <p className="text-[9px] text-muted-foreground mt-0.5">Max concurrent open positions</p>
      </div>
      <div>
        <label className="text-[10px] uppercase tracking-wider text-muted-foreground">Min Signals to Buy</label>
        <div className="flex gap-1 mt-1">
          {[1,2,3,4,5,6].map(n => (
            <button key={n} onClick={() => setMinSignals(n)}
              className={`flex-1 py-1.5 text-xs font-bold rounded border transition-colors ${minSignals === n ? 'bg-accent text-accent-foreground border-accent' : 'border-border text-muted-foreground hover:border-accent/50'}`}>
              {n}/6
            </button>
          ))}
        </div>
        <p className="text-[9px] text-muted-foreground mt-0.5">Higher = fewer, more confident buys</p>
      </div>
    </div>
  </div>
));

// ── Component ────────────────────────────────────────────────────────────────────────────
const AITradingAgent = ({ selectedCoins, prices, binanceConnected, onConnectBinance, onCoinsChange, onStateChange, onLiveModeDetected }: AITradingAgentProps) => {
  const [mode, setMode]           = useState<'test' | 'live'>('test');
  const [isRunning, setIsRunning] = useState(false);
  const [loading, setLoading]     = useState(false);
  const [balance, setBalance]     = useState(0);
  const [initialBalance, setInitialBalance] = useState(0);
  const [positions, setPositions] = useState<OpenPosition[]>([]);
  const [freshPrices, setFreshPrices] = useState<Record<string, number>>({});
  const [trades, setTrades]       = useState<TradeRow[]>([]);
  const [coinSignals, setCoinSignals] = useState<CoinSignal[]>([]);
  const [cycleCountdown, setCycleCountdown] = useState(0);
  const [agentStatus, setAgentStatus]       = useState('');
  const [scanning, setScanning]   = useState(false);
  const [showAllTrades, setShowAllTrades] = useState(false);
  const [showAllPositions, setShowAllPositions] = useState(false);
  const [showPositionsSection, setShowPositionsSection] = useState(true);
  const [showTradesSection, setShowTradesSection] = useState(true);
  const [forcingBuy, setForcingBuy]   = useState<string | null>(null);
  const [forcingSell, setForcingSell] = useState<string | null>(null);
  const [showSettings, setShowSettings] = useState(false);
  const [stopLossEnabled, setStopLossEnabled]         = useState(false);
  const [stopLossPct, setStopLossPct]                 = useState(2.0);
  const [takeProfitEnabled, setTakeProfitEnabled]     = useState(true);
  const [takeProfitPct, setTakeProfitPct]             = useState(0.5);
  const [smartHoldEnabled, setSmartHoldEnabled]       = useState(false);
  const [trailingStopPct, setTrailingStopPct]         = useState(0.5);
  const [reinvestProfits, setReinvestProfits]         = useState(false);
  const [maxPositions, setMaxPositions]               = useState(10);
  const [minSignals, setMinSignals]                   = useState(4);
  const [settingsDraft, setSettingsDraft]             = useState({
    stopLossEnabled: false, stopLossPct: 2.0,
    takeProfitEnabled: true, takeProfitPct: 0.5,
    smartHoldEnabled: false, trailingStopPct: 0.5,
    reinvestProfits: false,
    maxPositions: 10, minSignals: 4,
  });
  const [savingSettings, setSavingSettings]       = useState(false);
  const [instructions, setInstructions]   = useState(() => localStorage.getItem(INSTRUCTIONS_KEY) ?? '');
  const [editingInstr, setEditingInstr]   = useState(false);
  const [instrDraft, setInstrDraft]       = useState('');
  const [actLog, setActLog]       = useState<string[]>([]);
  const [dataPersistent, setDataPersistent] = useState<boolean | null>(null);
  // Server-authoritative P&L stats — avoids double-counting with Supabase
  const [serverRealizedPnl, setServerRealizedPnl]     = useState<number | null>(null);
  const [serverWins, setServerWins]                   = useState<number | null>(null);
  const [serverTotalTrades, setServerTotalTrades]     = useState<number | null>(null);
  const [showLog, setShowLog]     = useState(true);
  // All API calls are relative (same-origin) — bot runs on wolfbot.tech.
  const railwayUrl = '';
  const [liveApiKey, setLiveApiKey]         = useState('');
  const [liveApiSecret, setLiveApiSecret]   = useState('');
  const [showLiveSecret, setShowLiveSecret] = useState(false);
  const [liveSetupLoading, setLiveSetupLoading] = useState(false);

  // ── Setup wizard / Agent Trading Settings ─────────────────────────────────────────
  // Wizard always appears before every bot start — no localStorage persistence.
  // Users must confirm settings each time they start the bot.
  const [setupComplete, setSetupComplete]     = useState(false);
  // settingsSynced: true once settings were successfully POSTed to bot.
  const [settingsSynced, setSettingsSynced]   = useState(false);
  // Trade Size Mode (per-trade sizing) + per-mode value
  const [setupBudgetMode, setSetupBudgetMode]   = useState<'fixed'|'percent'|'capped'>('fixed');
  const [setupBudgetValue, setSetupBudgetValue] = useState(10);
  // Bot Allocation: total USDT from wallet the bot may use (0 = unlimited)
  const [setupAllocation, setSetupAllocation]   = useState(0);
  // Risk settings (all toggles + values)
  const [setupSlEnabled, setSetupSlEnabled]         = useState(false);
  const [setupStopLoss, setSetupStopLoss]           = useState(2.0);
  const [setupTpEnabled, setSetupTpEnabled]         = useState(true);
  const [setupTakeProfit, setSetupTakeProfit]       = useState(0.5);
  const [setupSmartHold, setSetupSmartHold]         = useState(false);
  const [setupTrailingStop, setSetupTrailingStop]   = useState(0.5);
  const [setupReinvest, setSetupReinvest]           = useState(false);
  const [setupMaxPositions, setSetupMaxPositions]   = useState(10);
  const [setupMinSignals, setSetupMinSignals]       = useState(4);

  const isRunningRef     = useRef(false);
  const coinsRestoredRef = useRef<boolean>(false);
  const balanceRef     = useRef(0);
  const timerRef       = useRef<ReturnType<typeof setTimeout> | null>(null);
  const scanTimerRef   = useRef<ReturnType<typeof setTimeout> | null>(null);
  const positionsRef   = useRef<OpenPosition[]>([]);
  const [railwaySignals, setRailwaySignals] = useState<any[]>([]);
  const [usingPaperFallback, setUsingPaperFallback] = useState(false);
  const [liveErrorMsg, setLiveErrorMsg] = useState<string | null>(null);

  const addLog = useCallback((msg: string) => {
    setActLog(prev => [msg, ...prev].slice(0, MAX_LOG_LINES));
  }, []);

  // ── Sync settings from server on mount ────────────────────────────────────
  useEffect(() => {
    // (so users with already-correct settings don't see a false warning).
    fetch(`${railwayUrl}/api/settings`, { cache: 'no-store' })
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (!d) return;
        if (d.stop_loss_enabled   !== undefined) setStopLossEnabled(Boolean(d.stop_loss_enabled));
        if (d.stop_loss_pct       !== undefined) { setStopLossPct(Number(d.stop_loss_pct)); }
        if (d.take_profit_enabled !== undefined) setTakeProfitEnabled(Boolean(d.take_profit_enabled));
        if (d.take_profit_pct     !== undefined) { setTakeProfitPct(Number(d.take_profit_pct)); }
        if (d.smart_hold_enabled  !== undefined) setSmartHoldEnabled(Boolean(d.smart_hold_enabled));
        if (d.trailing_stop_pct   !== undefined) setTrailingStopPct(Number(d.trailing_stop_pct));
        if (d.reinvest_profits    !== undefined) setReinvestProfits(Boolean(d.reinvest_profits));
        if (d.max_positions       !== undefined) setMaxPositions(Number(d.max_positions));
        if (d.min_signals         !== undefined) setMinSignals(Number(d.min_signals));
      })
      .catch(() => {});
    fetch(`${railwayUrl}/api/config`, { cache: 'no-store' })
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (!d) return;
        if (d.budget_mode  !== undefined) setSetupBudgetMode(d.budget_mode as 'fixed'|'percent'|'capped');
        if (d.budget_value !== undefined) setSetupBudgetValue(Number(d.budget_value));
        if (d.allocation   !== undefined) setSetupAllocation(Number(d.allocation));
      })
      .catch(() => {});
  }, [railwayUrl]);

  // ── Data loader (Supabase — local mode only) ──────────────────────────────
  const loadData = useCallback(async () => {
    // Server mode: all data comes from pollRailway (VPS SQLite), not Supabase.
    // Letting loadData run would race with pollRailway and overwrite server data with empty Supabase rows.
    if (isServerMode) return;
    try {
      const { data: cfg } = await supabase.from('bot_config')
        .select('is_running,mode,current_balance,initial_balance,stop_loss_percent,take_profit_percent')
        .eq('user_session', SESSION).maybeSingle();
      if (!cfg) return;
      setIsRunning(Boolean(cfg.is_running));
      isRunningRef.current = Boolean(cfg.is_running);
      setMode((cfg.mode === 'live' ? 'live' : 'test') as 'test' | 'live');
      const b = Number(cfg.current_balance ?? 0);
      setBalance(b); balanceRef.current = b;
      setInitialBalance(Number(cfg.initial_balance ?? b));

      const { data: pos } = await supabase.from('positions')
        .select('*').eq('user_session', SESSION).eq('status', 'open');
      const openPositions: OpenPosition[] = (pos ?? []).map((p: any) => ({
        symbol: p.symbol, quantity: Number(p.quantity),
        avg_entry_price: Number(p.avg_entry_price),
      }));
      setPositions(openPositions); positionsRef.current = openPositions;

      const { data: tr } = await supabase.from('trades')
        .select('*').eq('user_session', SESSION)
        .order('created_at', { ascending: false }).limit(50);
      setTrades((tr ?? []) as TradeRow[]);
    } catch (e: any) { addLog(`[DB] load failed: ${e.message}`); }
  }, [addLog]);

  useEffect(() => { loadData(); }, [loadData]);

  // ── Local trading loop ────────────────────────────────────────────────────
  const runCycle = useCallback(async () => {
    if (isServerMode || !isRunningRef.current) return;
    setScanning(true);
    setAgentStatus(`Scanning ${selectedCoins.length} coins…`);
    try {
      const fresh = await loadData() as any;
      const bal = balanceRef.current;
      const pos = positionsRef.current;
      if (bal < MIN_USDT) { setAgentStatus('Insufficient balance'); setScanning(false); return; }

      // Exit check
      const exitResults = checkExits(pos.map(p => ({
        symbol: p.symbol, quantity: p.quantity,
        avg_entry_price: p.avg_entry_price,
        current_price: prices[p.symbol] ?? p.avg_entry_price,
        stop_loss_pct: stopLossEnabled ? stopLossPct / 100 : null,
        take_profit_pct: takeProfitEnabled ? takeProfitPct / 100 : null,
      })), prices);

      for (const ex of exitResults) {
        if (!ex.should_exit) continue;
        const pos_ = pos.find(p => p.symbol === ex.symbol);
        if (!pos_) continue;
        const exitPrice = prices[ex.symbol] ?? pos_.avg_entry_price;
        const pnl = (exitPrice - pos_.avg_entry_price) * pos_.quantity * (1 - TAKER_FEE);
        const newBal = bal + pos_.avg_entry_price * pos_.quantity + pnl;
        await supabase.from('positions').update({ status: 'closed', updated_at: new Date().toISOString() })
          .eq('user_session', SESSION).eq('symbol', ex.symbol).eq('status', 'open');
        await supabase.from('trades').insert({
          user_session: SESSION, symbol: ex.symbol, side: 'SELL',
          price: exitPrice, quantity: pos_.quantity, pnl,
          reason: ex.reason, created_at: new Date().toISOString(),
        });
        await supabase.from('bot_config').update({ current_balance: newBal, updated_at: new Date().toISOString() })
          .eq('user_session', SESSION);
        balanceRef.current = newBal;
        addLog(`SELL ${ex.symbol} @ ${exitPrice.toFixed(4)} • PnL ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)} USDT`);
      }

      // Exit checker only runs in local mode — server handles exits server-side
      if (pos.length >= MAX_POSITIONS) { setAgentStatus('Max positions held'); setScanning(false); return; }

      // Buy scan
      const signals: CoinSignal[] = [];
      for (const sym of selectedCoins) {
        if (pos.find(p => p.symbol === sym)) continue;
        const sig = await analyseCoin(sym);
        signals.push(sig);
        if (sig.signal === 'BUY') {
          const alloc = getAllocation(balanceRef.current, sym);
          if (alloc < MIN_USDT) continue;
          const price = prices[sym] ?? sig.price;
          const qty = alloc / price;
          const cost = qty * price * (1 + TAKER_FEE);
          if (balanceRef.current < cost) continue;
          await supabase.from('positions').insert({
            user_session: SESSION, symbol: sym, status: 'open',
            quantity: qty, avg_entry_price: price,
            created_at: new Date().toISOString(), updated_at: new Date().toISOString(),
          });
          const nb = balanceRef.current - cost;
          await supabase.from('bot_config').update({ current_balance: nb, updated_at: new Date().toISOString() })
            .eq('user_session', SESSION);
          await supabase.from('trades').insert({
            user_session: SESSION, symbol: sym, side: 'BUY',
            price, quantity: qty, pnl: null,
            reason: sig.reason, created_at: new Date().toISOString(),
          });
          balanceRef.current = nb;
          addLog(`BUY ${sym} @ ${price.toFixed(4)} • ${alloc.toFixed(2)} USDT`);
        }
      }
      setCoinSignals(signals);
    } catch (e: any) { addLog(`[cycle] error: ${e.message}`); }
    setScanning(false);
    await loadData();
  }, [selectedCoins, prices, stopLossEnabled, stopLossPct, takeProfitEnabled, takeProfitPct, addLog, loadData]);

  // ── Countdown timer ───────────────────────────────────────────────────────
  useEffect(() => {
    if (!isRunning || isServerMode) return;
    let remaining = AGENT_CYCLE_MS / 1000;
    setCycleCountdown(remaining);
    const tick = setInterval(() => {
      remaining -= 1;
      setCycleCountdown(remaining);
      if (remaining <= 0) {
        remaining = AGENT_CYCLE_MS / 1000;
        setCycleCountdown(remaining);
        runCycle();
      }
    }, 1000);
    return () => clearInterval(tick);
  }, [isRunning, runCycle]);

  // ── Sync selectedCoins to VPS bot — debounced so rapid clicks don't spam the API ──
  useEffect(() => {
    if (!isServerMode || selectedCoins.length === 0) return;
    const handle = setTimeout(() => {
      fetch(`${railwayUrl}/api/coins`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ coins: selectedCoins }),
      }).catch(() => {});
    }, 800);
    return () => clearTimeout(handle);
  }, [selectedCoins, railwayUrl]); // eslint-disable-line

  // ── VPS bot poller ─────────────────────────────────────────────────────────────────
  // Polls /api/all (same-origin → wolfbot.tech) every 5 s normally, every 1 s
  // when positions are open. Mirrors bot state into React state for live UI.
  const serverPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const fastPollRef   = useRef<ReturnType<typeof setInterval> | null>(null);
  // In-flight guard: both poll intervals call pollRailway. Without this, a slow
  // 5-s response can land after a fast 1-s response and clobber newer data.
  const pollInFlightRef = useRef<boolean>(false);

  const pollRailway = useCallback(async () => {
    if (pollInFlightRef.current) return;
    pollInFlightRef.current = true;
    try {
    // Single /api/all request (status + positions + trades + activity in one round trip)
    // Retry once with a 2s delay if the first attempt fails.
    const attempt = async () => {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 5_000);
      try {
        const res = await fetch(`${railwayUrl}/api/all`, { signal: ctrl.signal, cache: 'no-store' });
        clearTimeout(timer);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return await res.json();
      } catch (e) {
        clearTimeout(timer);
        throw e;
      }
    };

    let data: any;
    try {
      data = await attempt();
    } catch {
      // one immediate retry on failure
      try { data = await attempt(); }
      catch (e: any) {
        if (e.name !== 'AbortError') addLog(`[Bot] poll failed: ${e.message}`);
        return;
      }
    }
    if (!data || typeof data !== 'object') return;
    // Guard against an error envelope from the server
    if (data.error) {
      addLog(`[Bot] server error: ${data.error}`);
      return;
    }

    const s = data.status ?? {};
    const running = Boolean(s.running);
    // Reset wizard when poll detects bot stopped (handles stop from another tab/session).
    if (isRunningRef.current && !running) { setSetupComplete(false); setSettingsSynced(false); }
    if (running) { setSetupComplete(true); setSettingsSynced(true); }
    isRunningRef.current = running;
    setIsRunning(running);
    const bal = Number(s.balance_usdt ?? 0);
    setBalance(bal); balanceRef.current = bal;
    setInitialBalance(Number(s.initial_balance ?? bal));
    // Propagate mode to parent so binanceConnected stays correct even when the
    // page loaded while the bot was restarting and the mount hook failed.
    onLiveModeDetected?.(s.mode === 'live');
    setAgentStatus(`Bot · ${s.mode?.toUpperCase() ?? 'PAPER'} · ${formatTime(new Date())}`);
    if (s.data_persistent !== undefined) setDataPersistent(Boolean(s.data_persistent));
    setUsingPaperFallback(Boolean(s.using_paper_fallback));
    setLiveErrorMsg(s.live_error ?? null);
    // Update committed state from server — never touch settingsDraft here.
    // The draft is only reset when the user opens the settings panel, so
    // in-progress edits are never overwritten by a background poll.
    if (s.stop_loss_enabled   !== undefined) setStopLossEnabled(Boolean(s.stop_loss_enabled));
    if (s.stop_loss_pct       !== undefined) setStopLossPct(Number(s.stop_loss_pct));
    if (s.take_profit_enabled !== undefined) setTakeProfitEnabled(Boolean(s.take_profit_enabled));
    if (s.take_profit_pct     !== undefined) setTakeProfitPct(Number(s.take_profit_pct));
    if (s.smart_hold_enabled  !== undefined) setSmartHoldEnabled(Boolean(s.smart_hold_enabled));
    if (s.trailing_stop_pct   !== undefined) setTrailingStopPct(Number(s.trailing_stop_pct));
    if (s.reinvest_profits    !== undefined) setReinvestProfits(Boolean(s.reinvest_profits));
    if (s.max_positions       !== undefined) setMaxPositions(Number(s.max_positions));
    if (s.min_signals         !== undefined) setMinSignals(Number(s.min_signals));
    if (s.strategy_notes   !== undefined) { setInstructions(s.strategy_notes as string); localStorage.setItem(INSTRUCTIONS_KEY, s.strategy_notes as string); }
    if (s.budget_mode        !== undefined) setSetupBudgetMode(s.budget_mode as 'fixed'|'percent'|'capped');
    if (s.budget_fixed_usdt  !== undefined) setSetupBudgetValue(Number(s.budget_fixed_usdt));
    if (s.bot_allocation_usdt !== undefined) setSetupAllocation(Number(s.bot_allocation_usdt));
    // Server-authoritative stats — use these instead of summing individual trade rows
    // to avoid double-counting with Supabase.
    if (s.realized_pnl  !== undefined) setServerRealizedPnl(Number(s.realized_pnl));
    if (s.wins          !== undefined) setServerWins(Number(s.wins));
    if (s.total_trades  !== undefined) setServerTotalTrades(Number(s.total_trades));

    // Restore coin selection from bot's watchlist ONLY ONCE on mount.
    // After mount, the frontend is the source of truth for selectedCoins —
    // server polls must not overwrite user selections in progress.
    if (Array.isArray(s.watched_coins) && s.watched_coins.length > 0 && !coinsRestoredRef.current) {
      coinsRestoredRef.current = true;
      onCoinsChange?.(s.watched_coins as string[]);
    }

    // Positions
    const rawPos: OpenPosition[] = (data.positions ?? []).map((p: any) => ({
      symbol: p.symbol,
      quantity: Number(p.quantity),
      avg_entry_price: Number(p.avg_entry_price),
      exit_target: p.exit_target !== undefined ? Number(p.exit_target) : undefined,
      current_price: p.current_price !== undefined ? Number(p.current_price) : undefined,
      profitable: p.profitable,
      hold_human: p.hold_human,
      hold_minutes: p.hold_minutes !== undefined ? Number(p.hold_minutes) : undefined,
      breakeven_price_real: p.breakeven_price_real !== undefined ? Number(p.breakeven_price_real) : undefined,
      real_bep_gap_pct: p.real_bep_gap_pct !== undefined ? Number(p.real_bep_gap_pct) : undefined,
      is_trapped: p.is_trapped === true,
    }));
    setPositions(rawPos);
    positionsRef.current = rawPos;
    if (data.fresh_prices && typeof data.fresh_prices === 'object') {
      setFreshPrices(data.fresh_prices as Record<string, number>);
    }

    // Trades
    const railwayTrades: TradeRow[] = [];
    for (const t of (data.trades ?? [])) {
      if (!t.symbol || !t.side) continue;
      railwayTrades.push({
        id: t.id ?? `${t.symbol}-${t.created_at}`,
        created_at: t.created_at ?? new Date().toISOString(),
        symbol: t.symbol, side: t.side,
        price: Number(t.price), quantity: Number(t.quantity),
        pnl: t.pnl !== null && t.pnl !== undefined ? Number(t.pnl) : null,
        reason: t.reason ?? null,
        volume_usdt: t.volume_usdt !== undefined ? Number(t.volume_usdt) : undefined,
      });
    }
    // Merge Supabase history only when bot returned NO trades — this
    // happens after a fresh redeploy without a persistent volume.
    const railwayTrades2 = [...railwayTrades];
    if (railwayTrades.length === 0) {
      try {
        const { data: sbTrades } = await supabase.from('trades')
          .select('*').eq('user_session', SESSION)
          .order('created_at', { ascending: false }).limit(50);
        if (Array.isArray(sbTrades) && sbTrades.length > 0) {
          railwayTrades2.push(...(sbTrades as TradeRow[]));
        }
      } catch { /* Supabase unavailable — bot-only data shown */ }
    }

    const sorted = railwayTrades2.sort((a, b) =>
      new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
    );
    setTrades(sorted);

    // Update bot signal cache snapshot (6-signal system from Python bot)
    if (Array.isArray(data.signals) && data.signals.length > 0) {
      setRailwaySignals(data.signals);
    }

    onStateChange?.(rawPos, bal, Number(s.initial_balance ?? bal),
      sorted.map(t => ({ side: t.side, pnl: t.pnl, quantity: t.quantity, price: t.price }))
    );

    // Fetch debug info only if there is a live error shown
    if (s.live_error) {
      const dbg = await fetch(`${railwayUrl}/api/debug`, { cache: 'no-store' }).then(r => r.ok ? r.json() : null);
      if (dbg?.env?.MODE) {
        addLog(`[debug] MODE=${dbg.env.MODE} live_error=${s.live_error ?? 'none'}`);
      }
    }
    } finally {
      pollInFlightRef.current = false;
    }
  }, [railwayUrl, addLog]);

  // Start / stop server poll
  useEffect(() => {
    if (!isServerMode) return;
    pollRailway();
    serverPollRef.current = setInterval(pollRailway, 3_000);
    return () => { if (serverPollRef.current) clearInterval(serverPollRef.current); };
  }, [isServerMode, pollRailway]); // eslint-disable-line

  // Uses `current_price` from the server API response (not WebSocket prices)
  useEffect(() => {
    if (!isServerMode) return;
    if (fastPollRef.current) clearInterval(fastPollRef.current);
    if (positions.length > 0) {
      fastPollRef.current = setInterval(pollRailway, 500);
    } else {
      fastPollRef.current = null;
    }
    return () => { if (fastPollRef.current) clearInterval(fastPollRef.current); };
  }, [isServerMode, positions, pollRailway]); // eslint-disable-line

  // ── Start / Stop ──────────────────────────────────────────────────────────
  const handleStartStop = useCallback(async () => {
    if (isRunning) {
      if (isServerMode) {
        const res = await fetch(`${railwayUrl}/api/agent/stop`, { method: 'POST' });
        if (res.ok) {
          setIsRunning(false); isRunningRef.current = false;
          setSetupComplete(false); setSettingsSynced(false);
          addLog('Bot stopped');
          toast.success('Bot stopped');
        }
        return;
      }
      setIsRunning(false); isRunningRef.current = false;
    }
  }, [isRunning, isServerMode, railwayUrl, addLog]);

  // ── Switch bot to live mode with API keys ───────────────────────────────────
  const handleGoLive = useCallback(async () => {
    if (!liveApiKey || !liveApiSecret) { toast.error('Enter API key and secret'); return; }
    setLiveSetupLoading(true);
    try {
      const res = await fetch(`${railwayUrl}/api/mode`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'live', api_key: liveApiKey, api_secret: liveApiSecret }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      if (data.live_error) {
        toast.error('Binance connection failed', { description: data.live_error });
        addLog(`[live] Binance error: ${data.live_error}`);
      } else {
        toast.success('Switched to live trading mode');
        addLog('[live] Live mode enabled');
        // Poll until mode confirms
        let attempts = 0;
        const poll = setInterval(async () => {
          attempts++;
          const ping = await fetch(`${railwayUrl}/api/ping`, { cache: 'no-store' });
          if (ping.ok) {
            const pd = await ping.json();
            if (pd.mode === 'live') {
              clearInterval(poll);
              await pollRailway();
            }
          }
          if (attempts > 10) clearInterval(poll);
        }, 1000);
      }
    } catch (e: any) {
      toast.error('Failed to switch mode', { description: e.message });
    } finally {
      setLiveSetupLoading(false);
    }
  }, [liveApiKey, liveApiSecret, railwayUrl, addLog, pollRailway]);

  // ── Sync setup wizard settings to server ─────────────────────────────────
  const syncSettingsToServer = useCallback(async (): Promise<boolean> => {
    try {
      // Map frontend budget fields to the keys /api/config actually accepts.
      const budgetPayload: Record<string,unknown> = {
        budget_mode:           setupBudgetMode,
        bot_allocation_usdt:   setupAllocation,
        budget_fixed_usdt:     setupBudgetValue,
        budget_pct_of_free:    setupBudgetMode === 'percent' ? setupBudgetValue : undefined,
        budget_total_cap_usdt: setupBudgetMode === 'capped'  ? setupBudgetValue : undefined,
      };
      const settingsPayload = {
        stop_loss_enabled:   setupSlEnabled,
        stop_loss_pct:       setupStopLoss,
        take_profit_enabled: setupTpEnabled,
        take_profit_pct:     setupTakeProfit,
        smart_hold_enabled:  setupSmartHold,
        trailing_stop_pct:   setupTrailingStop,
        reinvest_profits:    setupReinvest,
        max_positions:       setupMaxPositions,
        min_signals:         setupMinSignals,
      };
      await Promise.all([
        fetch(`${railwayUrl}/api/config`,   { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(budgetPayload) }),
        fetch(`${railwayUrl}/api/settings`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(settingsPayload) }),
      ]);
      return true;
    } catch {
      return false;
    }
  }, [
    setupBudgetMode, setupBudgetValue, setupAllocation,
    setupSlEnabled, setupStopLoss, setupTpEnabled, setupTakeProfit,
    setupSmartHold, setupTrailingStop, setupReinvest,
    setupMaxPositions, setupMinSignals,
    railwayUrl]);

  // ── Start bot ─────────────────────────────────────────────────────────────
  const handleStartBot = useCallback(async () => {
    if (loading) return;
    setLoading(true);
    try {
      if (isServerMode) {
      // ── Server mode: delegate to VPS bot ──
      let res: Response;
        // Bot may be temporarily unreachable (e.g. during deploy).
        // Sync settings before starting. If settings aren't synced yet
        // (e.g. wizard completed while bot was restarting), force a sync now
        // so the Python bot always starts with the user's latest configuration.
        if (!settingsSynced) {
          const ok = await syncSettingsToServer();
          if (ok) setSettingsSynced(true);
          else {
            toast.error('Could not sync settings to bot — try again in a moment');
            setLoading(false);
            return;
          }
        }
        const endpoint = '/api/agent/start';
        res = await fetch(`${railwayUrl}${endpoint}`, { method: 'POST' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        setIsRunning(true); isRunningRef.current = true;
        addLog(`Bot started · ${data.message ?? 'ok'}`);
        toast.success('Bot started');
        // Re-sync coins after start (bot resets watchlist on start)
        if (selectedCoins.length > 0) {
          fetch(`${railwayUrl}/api/coins`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ coins: selectedCoins }),
          }).catch(() => {});
        }
        pollRailway().catch(() => {});  // fire-and-forget; don't block UI
        return;
      }
      // Local mode
      setIsRunning(true); isRunningRef.current = true;
      addLog('Bot started (local mode)');
      toast.success('Bot started');
      runCycle();
    } catch (e: any) {
      toast.error('Failed to start bot', { description: e.message });
      addLog(`[start] error: ${e.message}`);
    } finally {
      setLoading(false);
    }
  }, [loading, isServerMode, selectedCoins, railwayUrl, settingsSynced, syncSettingsToServer, addLog, pollRailway, runCycle]);

  // ── Force buy ─────────────────────────────────────────────────────────────
  const handleForceBuy = useCallback(async (sym: string) => {
    if (!isServerMode) { toast.error('Force buy only available in server mode'); return; }
    setForcingBuy(sym);
    try {
      // Server mode: delegate to VPS bot's force-buy endpoint
      if (positions.find(p => p.symbol === sym)) { toast.error(`Already holding ${sym}`); return; }
        const res  = await fetch(`${railwayUrl}/api/force-buy/${sym}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ budget: setupBudgetValue }),
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        addLog(`FORCE BUY ${sym} @ ${Number(data.price).toFixed(4)} USDT · ${Number(data.budget).toFixed(2)} USDT`);
        toast.success(`Force-bought ${sym}`);
        await pollRailway();
    } catch (e: any) {
      toast.error(`Force buy failed: ${e.message}`);
      addLog(`[force-buy] ${sym}: ${e.message}`);
    } finally {
      setForcingBuy(null);
    }
  }, [isServerMode, positions, railwayUrl, setupBudgetValue, addLog, pollRailway]);

  // ── Force sell ────────────────────────────────────────────────────────────
  const handleForceSell = useCallback(async (pos: OpenPosition) => {
    setForcingSell(pos.symbol);
    try {
      if (isServerMode) {
        const cur = (pos.current_price && pos.current_price > 0)
          ? pos.current_price
          : (prices[pos.symbol] ?? 0);

        // Optimistic UI: remove position immediately so the panel feels instant.
        // If the backend fails, the next pollRailway restores it automatically.
        const positionBackup = positions.find(p => p.symbol === pos.symbol);
        setPositions(prev => prev.filter(p => p.symbol !== pos.symbol));

        try {
          const res = await fetch(`${railwayUrl}/api/force-sell/${pos.symbol}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ price: cur }),
          });
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          if (data.error) throw new Error(data.error);
          addLog(`FORCE SELL ${pos.symbol} @ ${Number(data.price).toFixed(4)} USDT`);
          toast.success(`Sold ${pos.symbol}`);
          setTimeout(() => pollRailway(), 200);
        } catch (sellErr) {
          // Sell failed — restore position so user can retry
          if (positionBackup) setPositions(prev => [...prev, positionBackup]);
          throw sellErr;
        }
        return;
      }
      // Local mode
      const exitPrice = prices[pos.symbol] ?? pos.avg_entry_price;
      const pnl = (exitPrice - pos.avg_entry_price) * pos.quantity * (1 - TAKER_FEE);
      await supabase.from('positions').update({ status: 'closed', updated_at: new Date().toISOString() })
        .eq('user_session', SESSION).eq('symbol', pos.symbol).eq('status', 'open');
      await supabase.from('trades').insert({
        user_session: SESSION, symbol: pos.symbol, side: 'SELL',
        price: exitPrice, quantity: pos.quantity, pnl,
        reason: 'manual', created_at: new Date().toISOString(),
      });
      const newBal = balanceRef.current + pos.avg_entry_price * pos.quantity + pnl;
      await supabase.from('bot_config').update({ current_balance: newBal, updated_at: new Date().toISOString() })
        .eq('user_session', SESSION);
      balanceRef.current = newBal;
      addLog(`FORCE SELL ${pos.symbol} @ ${exitPrice.toFixed(4)} • PnL ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)} USDT`);
      toast.success(`Sold ${pos.symbol}`);
      await loadData();
    } catch (e: any) {
      toast.error(`Force sell failed: ${e.message}`);
    } finally {
      setForcingSell(null);
    }
  }, [isServerMode, railwayUrl, prices, positions, addLog, loadData, pollRailway]);

  // ── Reset wallet ──────────────────────────────────────────────────────────
  const handleReset = useCallback(async () => {
    try {
      if (isServerMode) {
        const res  = await fetch(`${railwayUrl}/api/reset`, { method: 'POST' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        addLog('=== Bot wallet reset ===');
        toast.success(`Bot reset · ${data.balance_usdt?.toLocaleString()} USDT restored`);
        await pollRailway();
        return;
      }
      // Local mode
      const cfg = getPaperCfg();
      const startBal = cfg.startingBalance ?? 1000;
      await supabase.from('bot_config').update({ current_balance: startBal, updated_at: new Date().toISOString() })
        .eq('user_session', SESSION);
      await supabase.from('positions').update({ status: 'closed', updated_at: new Date().toISOString() })
        .eq('user_session', SESSION).eq('status', 'open');
      balanceRef.current = startBal;
      addLog('=== Wallet reset ===');
      toast.success(`Wallet reset to ${startBal} USDT`);
      await loadData();
    } catch (e: any) {
      toast.error(`Reset failed: ${e.message}`);
    }
  }, [isServerMode, railwayUrl, addLog, pollRailway, loadData]);

  // ── Save bot settings to server ──────────────────────────────────────────────
  const saveSettings = useCallback(async () => {
    setSavingSettings(true);
    try {
      await Promise.all([
        fetch(`${railwayUrl}/api/settings`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            stop_loss_enabled:   settingsDraft.stopLossEnabled,
            stop_loss_pct:       settingsDraft.stopLossPct,
            take_profit_enabled: settingsDraft.takeProfitEnabled,
            take_profit_pct:     settingsDraft.takeProfitPct,
            smart_hold_enabled:  settingsDraft.smartHoldEnabled,
            trailing_stop_pct:   settingsDraft.trailingStopPct,
            reinvest_profits:    settingsDraft.reinvestProfits,
            max_positions:       settingsDraft.maxPositions,
            min_signals:         settingsDraft.minSignals,
          }),
        }),
        fetch(`${railwayUrl}/api/config`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            budget_mode:           setupBudgetMode,
            budget_fixed_usdt:     setupBudgetValue,
            budget_pct_of_free:    setupBudgetMode === 'percent' ? setupBudgetValue : undefined,
            budget_total_cap_usdt: setupBudgetMode === 'capped'  ? setupBudgetValue : undefined,
            bot_allocation_usdt:   setupAllocation,
          }),
        }),
      ]);
      setStopLossEnabled(settingsDraft.stopLossEnabled);
      setStopLossPct(settingsDraft.stopLossPct);
      setTakeProfitEnabled(settingsDraft.takeProfitEnabled);
      setTakeProfitPct(settingsDraft.takeProfitPct);
      setSmartHoldEnabled(settingsDraft.smartHoldEnabled);
      setTrailingStopPct(settingsDraft.trailingStopPct);
      setReinvestProfits(settingsDraft.reinvestProfits);
      setMaxPositions(settingsDraft.maxPositions);
      setMinSignals(settingsDraft.minSignals);
      toast.success('Settings saved');
    } catch (e: any) {
      toast.error(`Save failed: ${e.message}`);
    } finally {
      setSavingSettings(false);
    }
  }, [settingsDraft, setupBudgetMode, setupBudgetValue, setupAllocation, railwayUrl]);

  // ── Computed stats ────────────────────────────────────────────────────────
  const totalValue = positions.reduce((sum, p) => {
    const cur = (isServerMode && p.current_price) ? p.current_price : (prices[p.symbol] ?? p.avg_entry_price);
    return sum + cur * p.quantity;
  }, 0);

  const unrealizedPnl = positions.reduce((sum, p) => {
    const cur = (isServerMode && p.current_price) ? p.current_price : (prices[p.symbol] ?? p.avg_entry_price);
    return sum + (cur - p.avg_entry_price) * p.quantity;
  }, 0);

  // Use server stats when available; fall back to summing trade rows.
  // double-counting that occurs when server and Supabase trade rows are merged.
  const realizedPnl = serverRealizedPnl !== null ? serverRealizedPnl
    : trades.filter(t => t.side === 'SELL' && t.pnl !== null).reduce((s, t) => s + (t.pnl ?? 0), 0);
  const totalPnl = realizedPnl + unrealizedPnl;
  const winningTrades = serverWins !== null ? serverWins
    : trades.filter(t => t.side === 'SELL' && (t.pnl ?? 0) > 0).length;
  const totalClosedTrades = serverTotalTrades !== null ? serverTotalTrades
    : trades.filter(t => t.side === 'SELL').length;
  const winRate = totalClosedTrades > 0 ? (winningTrades / totalClosedTrades) * 100 : 0;
  const roi = initialBalance > 0 ? ((balance + totalValue - initialBalance) / initialBalance) * 100 : 0;

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div className="trading-card">
      {/* Paper-fallback warning banner */}
      {usingPaperFallback && (
        <div className="flex items-center gap-2 px-4 py-2 bg-amber-500/15 border-b border-amber-500/30 text-amber-400 text-xs">
          <span className="font-bold">⚠ PAPER FALLBACK ACTIVE</span>
          <span className="text-amber-400/80">{liveErrorMsg ?? 'Live Binance connection failed — running on paper wallet. Retrying every 60 s.'}</span>
        </div>
      )}
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-border">
        <div className="flex items-center gap-3">
          <div className={`p-2 rounded-lg ${isRunning ? 'bg-gain/20' : 'bg-muted/30'}`}>
            <Brain className={`w-5 h-5 ${isRunning ? 'text-gain' : 'text-muted-foreground'}`} />
          </div>
          <div>
            <h2 className="text-sm font-bold">AI Trading Agent</h2>
            <p className="text-[10px] text-muted-foreground">
              {agentStatus || (isRunning ? 'Running…' : 'Idle')}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {dataPersistent !== null && (
            <span className={`text-[9px] px-1.5 py-0.5 rounded font-bold ${
              dataPersistent ? 'bg-gain/20 text-gain' : 'bg-amber-500/20 text-amber-400'
            }`}>{dataPersistent ? 'Live (DB connected)' : 'Volatile (no DB)'}</span>
          )}
          {isRunning && <span className="w-2 h-2 rounded-full bg-gain animate-pulse" />}
          <button onClick={async () => {
              const next = !showSettings;
              setShowSettings(next);
              if (next) {
                try {
                  const [s, c] = await Promise.all([
                    fetch(`/api/settings`, { cache: 'no-store' }).then(r => r.json()),
                    fetch(`/api/config`,   { cache: 'no-store' }).then(r => r.json()),
                  ]);
                  if (s?.ok) {
                    setSettingsDraft({
                      stopLossEnabled:   Boolean(s.stop_loss_enabled),
                      stopLossPct:       Number(s.stop_loss_pct ?? 2.0),
                      takeProfitEnabled: Boolean(s.take_profit_enabled),
                      takeProfitPct:     Number(s.take_profit_pct ?? 0.1),
                      smartHoldEnabled:  Boolean(s.smart_hold_enabled),
                      trailingStopPct:   Number(s.trailing_stop_pct ?? 0.5),
                      reinvestProfits:   Boolean(s.reinvest_profits),
                      maxPositions:      Number(s.max_positions ?? 20),
                      minSignals:        Number(s.min_signals ?? 3),
                    });
                  }
                  if (c) {
                    if (c.budget_mode) setSetupBudgetMode(c.budget_mode as 'fixed'|'percent'|'capped');
                    if (c.budget_fixed_usdt !== undefined) setSetupBudgetValue(Number(c.budget_fixed_usdt));
                    if (c.bot_allocation_usdt !== undefined) setSetupAllocation(Number(c.bot_allocation_usdt));
                  }
                } catch { }
              }
            }}
            className={`p-1.5 rounded hover:bg-muted/40 transition-colors ${showSettings ? 'text-accent' : 'text-muted-foreground'}`}>
            <Settings2 className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-4 divide-x divide-border border-b border-border">
        {[['Balance', `${(balance).toLocaleString(undefined, {maximumFractionDigits:2})} USDT`, balance > initialBalance ? 'text-gain' : 'text-foreground'],
          ['Positions', `${positions.length}`, 'text-foreground'],
          ['Total PnL', formatPnL(totalPnl, 2), totalPnl >= 0 ? 'text-gain' : 'text-loss'],
          ['Win Rate', `${winRate.toFixed(0)}%`, winRate >= 50 ? 'text-gain' : 'text-muted-foreground'],
        ].map(([label, val, cls]) => (
          <div key={label} className="p-3 text-center">
            <p className="text-[9px] text-muted-foreground uppercase tracking-wider">{label}</p>
            <p className={`text-sm font-bold font-mono mt-0.5 ${cls}`}>{val}</p>
          </div>
        ))}
      </div>

      {/* Mode badge + quick actions */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-border">
        <div className="flex items-center gap-2">
          <span className={`text-[9px] font-bold px-2 py-0.5 rounded-full ${
            binanceConnected ? 'bg-gain/20 text-gain' : 'bg-muted/40 text-muted-foreground'
          }`}>
            {binanceConnected ? 'LIVE' : 'PAPER'}
          </span>
          {!binanceConnected && (
            <button onClick={onConnectBinance}
              className="text-[10px] text-accent hover:underline">Connect Binance</button>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => isServerMode ? pollRailway() : runCycle()}
            disabled={scanning}
            className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground transition-colors disabled:opacity-40"
            title="Refresh">
            <RefreshCw className={`w-3.5 h-3.5 ${scanning ? 'animate-spin' : ''}`} />
          </button>
          <button
            onClick={() => setShowLog(!showLog)}
            className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground transition-colors"
            title={showLog ? 'Hide log' : 'Show log'}>
            {showLog ? <EyeOff className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
          </button>
        </div>
      </div>

      {/* Setup wizard / start button */}
      {!setupComplete ? (
        <div className="px-4 py-3 space-y-3">
          <p className="text-xs text-muted-foreground">Configure settings via the gear icon, then start the bot.</p>
          <Button
            onClick={async () => { setSetupComplete(true); await handleStartBot(); }}
            disabled={loading}
            className="w-full bg-gain hover:bg-gain/90 text-white font-bold">
            {loading ? 'Starting…' : <>Start Bot</>}
          </Button>
        </div>
      ) : (
        <div className="p-4 space-y-3">
          {/* Running control */}
          <div className="flex gap-2">
            {isRunning ? (
              <Button onClick={handleStartStop} variant="destructive" className="flex-1 font-bold">
                <Square className="w-4 h-4 mr-2" />Stop Bot
              </Button>
            ) : (
              <Button onClick={handleStartBot} disabled={loading} className="flex-1 bg-gain hover:bg-gain/90 text-white font-bold">
                {loading ? 'Starting…' : <><Play className="w-4 h-4 mr-2" />Start Bot</>}
              </Button>
            )}
            <Button onClick={handleReset} variant="outline" size="icon" title="Reset wallet">
              <RotateCcw className="w-4 h-4" />
            </Button>
            <Button onClick={() => setSetupComplete(false)} variant="outline" size="icon" title="Re-run wizard">
              <Settings2 className="w-4 h-4" />
            </Button>
          </div>

          {/* Settings panel */}
          {showSettings && (
            <div className="bg-muted/20 border border-border rounded-lg p-4 space-y-4">
              <div className="flex items-center justify-between">
                <span className="text-xs font-semibold">Bot Settings</span>
                <button onClick={() => { setSettingsDraft({ stopLossEnabled, stopLossPct, takeProfitEnabled, takeProfitPct, smartHoldEnabled, trailingStopPct, reinvestProfits, maxPositions, minSignals }); }}
                  className="text-[10px] text-muted-foreground hover:text-foreground">Reload from server</button>
              </div>

              <AgentTradingFields
                budgetMode={setupBudgetMode} setBudgetMode={setSetupBudgetMode}
                budgetValue={setupBudgetValue} setBudgetValue={setSetupBudgetValue}
                allocation={setupAllocation} setAllocation={setSetupAllocation}
                slEnabled={settingsDraft.stopLossEnabled} setSlEnabled={v => setSettingsDraft(d => ({...d, stopLossEnabled: v}))}
                stopLoss={settingsDraft.stopLossPct} setStopLoss={v => setSettingsDraft(d => ({...d, stopLossPct: v}))}
                tpEnabled={settingsDraft.takeProfitEnabled} setTpEnabled={v => setSettingsDraft(d => ({...d, takeProfitEnabled: v}))}
                takeProfit={settingsDraft.takeProfitPct} setTakeProfit={v => setSettingsDraft(d => ({...d, takeProfitPct: v}))}
                smartHold={settingsDraft.smartHoldEnabled} setSmartHold={v => setSettingsDraft(d => ({...d, smartHoldEnabled: v}))}
                trailingStop={settingsDraft.trailingStopPct} setTrailingStop={v => setSettingsDraft(d => ({...d, trailingStopPct: v}))}
                reinvest={settingsDraft.reinvestProfits} setReinvest={v => setSettingsDraft(d => ({...d, reinvestProfits: v}))}
                maxPositions={settingsDraft.maxPositions} setMaxPositions={v => setSettingsDraft(d => ({...d, maxPositions: v}))}
                minSignals={settingsDraft.minSignals} setMinSignals={v => setSettingsDraft(d => ({...d, minSignals: v}))}
              />

              <Button onClick={saveSettings} disabled={savingSettings} size="sm" className="w-full">
                {savingSettings ? 'Saving…' : 'Save Settings'}
              </Button>

              {/* ── Bot Server ── */}
              <div className="bg-muted/20 border border-border rounded-md px-3 py-2.5 space-y-1">
                <div className="flex items-center gap-2">
                  <Activity className="w-3.5 h-3.5 text-accent" />
                  <span className="text-xs font-semibold text-accent">Bot Server</span>
                  {isServerMode && <span className="text-[9px] px-1.5 py-0.5 rounded bg-gain/20 text-gain font-bold">CONNECTED</span>}
                </div>
                <p className="text-[10px] text-gain font-sans">
                  Same-origin · API calls go to <code className="bg-muted px-1 rounded text-foreground">/api/*</code>
                </p>
              </div>

              {/* ── Instructions ── */}
              <div className="bg-muted/20 border border-border rounded-md px-3 py-2.5 space-y-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <BookOpen className="w-3.5 h-3.5 text-accent" />
                    <span className="text-xs font-semibold text-accent">Strategy Notes</span>
                  </div>
                  {!editingInstr ? (
                    <button onClick={() => { setInstrDraft(instructions); setEditingInstr(true); }} className="text-[10px] text-muted-foreground hover:text-foreground flex items-center gap-1">
                      <Pencil className="w-3 h-3" />Edit
                    </button>
                  ) : (
                    <div className="flex gap-2">
                      <button onClick={async () => {
                        setInstructions(instrDraft);
                        localStorage.setItem(INSTRUCTIONS_KEY, instrDraft);
                        setEditingInstr(false);
                        // Sync notes to VPS bot so Claude can use them in strategy decisions
                        if (isServerMode) {
                          await fetch(`${railwayUrl}/api/settings`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ strategy_notes: instrDraft }),
                          }).catch(() => {});
                        }
                      }} className="text-[10px] text-gain flex items-center gap-0.5"><Check className="w-3 h-3" />Save</button>
                      <button onClick={() => setEditingInstr(false)} className="text-[10px] text-loss flex items-center gap-0.5"><X className="w-3 h-3" />Cancel</button>
                    </div>
                  )}
                </div>
                {editingInstr ? (
                  <textarea value={instrDraft} onChange={e => setInstrDraft(e.target.value)} rows={4}
                    placeholder="e.g. Focus on BTC and ETH only. Avoid meme coins."
                    className="w-full text-xs bg-background border border-border rounded px-2 py-1.5 outline-none focus:border-accent resize-none" />
                ) : (
                  <p className="text-[10px] text-muted-foreground whitespace-pre-wrap">
                    {instructions || <span className="italic">No strategy notes set</span>}
                  </p>
                )}
              </div>

              {/* ── Live API setup ── */}
              {!binanceConnected && (
                <div className="bg-muted/20 border border-border rounded-md px-3 py-2.5 space-y-2">
                  <div className="flex items-center gap-2">
                    <Shield className="w-3.5 h-3.5 text-accent" />
                    <span className="text-xs font-semibold text-accent">Live Trading Setup</span>
                  </div>
                  <div className="space-y-1.5">
                    <input value={liveApiKey} onChange={e => setLiveApiKey(e.target.value)}
                      placeholder="Binance API Key"
                      className="w-full text-xs bg-background border border-border rounded px-2 py-1.5 font-mono outline-none focus:border-accent" />
                    <div className="relative">
                      <input value={liveApiSecret} onChange={e => setLiveApiSecret(e.target.value)}
                        type={showLiveSecret ? 'text' : 'password'}
                        placeholder="Binance API Secret"
                        className="w-full text-xs bg-background border border-border rounded px-2 py-1.5 font-mono outline-none focus:border-accent pr-8" />
                      <button onClick={() => setShowLiveSecret(!showLiveSecret)}
                        className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground">
                        {showLiveSecret ? <EyeOff className="w-3 h-3" /> : <Eye className="w-3 h-3" />}
                      </button>
                    </div>
                    <Button onClick={handleGoLive} disabled={liveSetupLoading} size="sm" className="w-full bg-gain hover:bg-gain/90 text-white">
                      {liveSetupLoading ? 'Connecting…' : 'Switch to Live Mode'}
                    </Button>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Positions */}
      <div className="border-t border-border">
        <button
          onClick={() => setShowPositionsSection(!showPositionsSection)}
          className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
          <div className="flex items-center gap-2">
            <ShoppingCart className="w-3.5 h-3.5 text-muted-foreground" />
            <span className="text-xs font-semibold">Open Positions ({positions.length})</span>
            {unrealizedPnl !== 0 && (
              <span className={`text-[10px] font-mono ${unrealizedPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                {formatPnL(unrealizedPnl, 2)} USDT
              </span>
            )}
          </div>
          {showPositionsSection ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" /> : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
        </button>

        {showPositionsSection && (
          <div className="px-4 pb-3">
            {positions.length === 0 ? (
              <p className="text-[10px] text-muted-foreground py-2">No open positions</p>
            ) : (
              <>
                <div className="space-y-2">
                  {(showAllPositions ? positions : positions.slice(0, 5)).map(pos => {
                    const entry = pos.avg_entry_price > 0 ? pos.avg_entry_price : 0;
                    // Priority: fresh_prices (injected per-poll outside cache) > local WS > cached server price
                    const cur   = freshPrices[pos.symbol]
                      ?? (isServerMode && pos.current_price && pos.current_price > 0 ? pos.current_price : undefined)
                      ?? prices[pos.symbol]
                      ?? entry;
                    const pnl = entry > 0 ? (cur - entry) * pos.quantity : 0;
                    const pct = entry > 0 ? ((cur - entry) / entry) * 100 : 0;
                    // exit_target comes from server; fallback to entry × breakeven (0.15% above)
                    const exitTarget = pos.exit_target && pos.exit_target > 0
                      ? pos.exit_target
                      : entry * 1.0017;
                    const profitable = cur >= exitTarget;
                    const progressPct = entry > 0 && exitTarget > entry
                      ? Math.max(0, Math.min(100, (cur - entry) / (exitTarget - entry) * 100))
                      : 0;
                    return (
                      <div key={pos.symbol} className="bg-muted/20 rounded px-3 py-2 space-y-1">
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-2">
                            <div className={`w-1.5 h-1.5 rounded-full ${profitable ? 'bg-gain' : pct < 0 ? 'bg-loss' : 'bg-amber-400'}`} />
                            <div>
                              <p className="text-xs font-bold">{pos.symbol.replace('USDT','')}</p>
                              <p className="text-[9px] text-muted-foreground">
                                {pos.quantity.toFixed(6)} @ <span className="font-mono">${entry > 0 ? entry.toFixed(6) : '—'}</span>
                                {pos.hold_human && <span className="ml-1 text-muted-foreground/60">· {pos.hold_human}</span>}
                              </p>
                            </div>
                          </div>
                          <div className="text-right">
                            <p className="text-[9px] font-mono text-foreground/80">
                              Now: <span
                                key={`${pos.symbol}-${Math.round(cur * 1e5)}`}
                                className={`font-semibold rounded px-0.5 ${pct >= 0 ? 'animate-flash-green' : 'animate-flash-red'}`}
                              >${cur > 0 ? cur.toFixed(6) : '…'}</span>
                            </p>
                            <p className={`text-[9px] font-mono ${pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                              {formatPnL(pnl, 4)} USDT ({pct >= 0 ? '+' : pct < 0 ? '−' : ''}{Math.abs(pct).toFixed(3)}%)
                            </p>
                          </div>
                          <button
                            onClick={() => handleForceSell(pos)}
                            disabled={forcingSell === pos.symbol}
                            className="ml-2 p-1.5 rounded bg-loss/20 hover:bg-loss/30 text-loss transition-colors disabled:opacity-40"
                            title="Force sell">
                            {forcingSell === pos.symbol
                              ? <RefreshCw className="w-3 h-3 animate-spin" />
                              : <Banknote className="w-3 h-3" />}
                          </button>
                        </div>
                        {/* Progress bar: 0% = entry, 100% = exit target */}
                        <div className="w-full h-1.5 bg-muted/40 rounded-full overflow-hidden">
                          <div
                            className={`h-full rounded-full transition-all duration-700 ${
                              profitable ? 'bg-gain' : progressPct > 50 ? 'bg-amber-400' : 'bg-accent/70'
                            }`}
                            style={{ width: `${progressPct.toFixed(1)}%` }}
                          />
                        </div>
                        {/* Exit target row */}
                        <div className="flex items-center justify-between">
                          <span className="text-[8px] text-muted-foreground">
                            Buy <span className="font-mono">${entry > 0 ? entry.toFixed(6) : '—'}</span>
                            {' → '}Sell ≥ <span className="font-mono text-accent">${exitTarget > 0 ? exitTarget.toFixed(6) : '—'}</span>
                          </span>
                          <span className={`text-[8px] font-mono ${profitable ? 'text-gain font-semibold' : 'text-muted-foreground'}`}>
                            {profitable
                              ? '✓ READY TO SELL'
                              : cur > 0
                                ? `${progressPct.toFixed(1)}% — needs +${((exitTarget - cur) / cur * 100).toFixed(3)}%`
                                : 'loading…'}
                          </span>
                        </div>
                        {/* Real breakeven — shown only when it differs meaningfully from trigger price */}
                        {pos.breakeven_price_real && pos.breakeven_price_real > exitTarget * 1.001 && (
                          <div className="text-[8px] text-amber-500/80 font-mono" title="Real breakeven after Binance fees + lot-size rounding. Bot won't sell below this even if trigger fires.">
                            Real target: ${pos.breakeven_price_real.toFixed(6)}
                            {pos.real_bep_gap_pct !== undefined && pos.real_bep_gap_pct > 0 && (
                              <span className="ml-1">(+{pos.real_bep_gap_pct.toFixed(2)}% needed)</span>
                            )}
                          </div>
                        )}
                        {/* Trapped warning — lot-step rounding requires >2% move to break even */}
                        {pos.is_trapped && pos.real_bep_gap_pct !== undefined && (
                          <div className="text-xs text-yellow-500 mt-1 flex items-center gap-1">
                            <span>⚠</span>
                            <span>Trapped: needs +{pos.real_bep_gap_pct.toFixed(2)}% to break even{pos.breakeven_price_real ? ` (real target $${pos.breakeven_price_real.toFixed(6)})` : ''}</span>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
                {positions.length > 5 && (
                  <button onClick={() => setShowAllPositions(!showAllPositions)}
                    className="mt-2 text-[10px] text-accent hover:underline">
                    {showAllPositions ? 'Show less' : `Show all ${positions.length} positions`}
                  </button>
                )}
              </>
            )}
          </div>
        )}
      </div>

      {/* Trades */}
      <div className="border-t border-border">
        <button
          onClick={() => setShowTradesSection(!showTradesSection)}
          className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
          <div className="flex items-center gap-2">
            <Banknote className="w-3.5 h-3.5 text-muted-foreground" />
            <span className="text-xs font-semibold">Trade History ({totalClosedTrades})</span>
            {totalClosedTrades > 0 && (
              <span className={`text-[10px] ${winRate >= 50 ? 'text-gain' : 'text-loss'}`}>{winRate.toFixed(0)}% wins</span>
            )}
          </div>
          {showTradesSection ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" /> : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
        </button>

        {showTradesSection && (
          <div className="px-4 pb-3">
            {trades.length === 0 ? (
              <p className="text-[10px] text-muted-foreground py-2">No trades yet</p>
            ) : (
              <>
                <div className="space-y-1">
                  {(showAllTrades ? trades : trades.slice(0, 10)).map((t, i) => (
                    <div key={t.id || i} className="flex items-center justify-between py-1 border-b border-border/50 last:border-0">
                      <div className="flex items-center gap-2">
                        {t.side === 'BUY'
                          ? <TrendingUp className="w-3 h-3 text-gain" />
                          : <TrendingDown className="w-3 h-3 text-loss" />}
                        <div>
                          <p className="text-[11px] font-bold">{t.symbol.replace('USDT','')}</p>
                          <p className="text-[9px] text-muted-foreground">{formatTime(t.created_at)}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        <p className="text-[11px] font-mono">{t.price.toFixed(4)}</p>
                        {t.pnl !== null && (
                          <p className={`text-[9px] font-mono font-bold ${t.pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                            {formatPnL(t.pnl, 2)}
                          </p>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
                {trades.length > 10 && (
                  <button onClick={() => setShowAllTrades(!showAllTrades)}
                    className="mt-2 text-[10px] text-accent hover:underline">
                    {showAllTrades ? 'Show less' : `Show all ${trades.length} trades`}
                  </button>
                )}
              </>
            )}
          </div>
        )}
      </div>

      {/* Force buy section */}
      {isServerMode && setupComplete && (
        <div className="border-t border-border px-4 py-3">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Manual Buy</p>
          <div className="flex flex-wrap gap-1.5">
            {selectedCoins.slice(0, 20).map(sym => {
              const held = positions.find(p => p.symbol === sym);
              return (
                <button
                  key={sym}
                  onClick={() => !held && handleForceBuy(sym)}
                  disabled={!!held || forcingBuy === sym}
                  className={`text-[10px] px-2 py-1 rounded border transition-colors ${
                    held ? 'border-gain/40 text-gain bg-gain/10 cursor-default'
                    : forcingBuy === sym ? 'border-accent/40 text-accent animate-pulse'
                    : 'border-border hover:border-accent/50 hover:text-accent'
                  }`}>
                  {sym.replace('USDT','')}{held ? ' ✓' : ''}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Activity log */}
      {showLog && actLog.length > 0 && (
        <div className="border-t border-border px-4 py-3">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Activity Log</p>
          <div className="space-y-0.5 max-h-32 overflow-y-auto scrollbar-thin">
            {actLog.map((line, i) => (
              <p key={i} className="text-[10px] font-mono text-muted-foreground leading-relaxed">{line}</p>
            ))}
          </div>
        </div>
      )}

      {/* Signal scanner */}
      <div className="border-t border-border px-4 py-3">
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2">Market Signals</p>
        {/* In server mode: show bot's 6-signal cache if available, else JS scan */}
        {(isServerMode && railwaySignals.length > 0) ? (
          <div className="space-y-0">
            {/* Header row */}
            <div className="grid pb-1 border-b border-border/60" style={{gridTemplateColumns:'4rem 1fr 1fr 1fr 1fr 1fr 1fr 1fr 1fr 2rem 3rem'}}>
              <span className="text-[8px] text-muted-foreground font-semibold">COIN</span>
              {(['EMA','RSI','MACD','VOL','OBV','ATR','BB','5m'] as const).map(lbl => (
                <span key={lbl} className="text-[8px] text-muted-foreground text-center">{lbl}</span>
              ))}
              <span className="text-[8px] text-muted-foreground text-center">SCR</span>
              <span className="text-[8px] text-muted-foreground text-right">PRICE</span>
            </div>
            {railwaySignals.map((sig: any) => {
              const bools = [!!sig.trend, !!sig.rsi_ok, !!sig.macd, !!sig.volume, !!sig.obv, !!sig.atr];
              const score = typeof sig.score === 'number' ? sig.score : bools.filter(Boolean).length;
              const bbBlock   = sig.bb_ok === false;
              const fiveBlock = sig['5m_ok'] === false;
              const isBuy      = score >= 4 && !bbBlock && !fiveBlock;
              const isReady    = score >= 3 && !bbBlock && !fiveBlock;
              const isVetoBb   = score >= 3 && bbBlock;
              const isVetoFive = score >= 3 && !bbBlock && fiveBlock;
              const isDowntrend = !sig.trend && score < 3;

              let label: string;
              let labelColor: string;
              if (isBuy)        { label = 'BUY';     labelColor = 'bg-gain/20 text-gain'; }
              else if (isReady) { label = 'READY';   labelColor = 'bg-gain/10 text-gain'; }
              else if (isVetoBb)   { label = 'VETO·BB'; labelColor = 'bg-orange-500/20 text-orange-400'; }
              else if (isVetoFive) { label = 'VETO·5m'; labelColor = 'bg-orange-500/20 text-orange-400'; }
              else if (isDowntrend){ label = 'DOWN';    labelColor = 'bg-loss/20 text-loss'; }
              else if (score === 2){ label = 'WAIT';    labelColor = 'bg-yellow-500/20 text-yellow-400'; }
              else                 { label = 'HOLD';    labelColor = 'bg-muted/30 text-muted-foreground'; }

              return (
                <div key={sig.symbol} className="grid items-center py-0.5 border-b border-border/20 last:border-0"
                  style={{gridTemplateColumns:'4rem 1fr 1fr 1fr 1fr 1fr 1fr 1fr 1fr 2rem 3rem'}}>
                  <div className="flex items-center gap-1 min-w-0">
                    <span className={`text-[7px] font-bold px-0.5 rounded shrink-0 ${labelColor}`}>{label}</span>
                    <span className="text-[9px] font-mono font-semibold truncate">{sig.symbol?.replace('USDT','')}</span>
                  </div>
                  {bools.map((v, i) => (
                    <div key={i} className="flex justify-center">
                      <div className={`w-2 h-2 rounded-full ${v ? 'bg-gain' : 'bg-muted/40'}`} />
                    </div>
                  ))}
                  <div className="flex justify-center" title="BB veto">
                    <div className={`w-2 h-2 rounded-full ${sig.bb_ok !== false ? 'bg-gain' : 'bg-loss'}`} />
                  </div>
                  <div className="flex justify-center" title="5m trend veto">
                    <div className={`w-2 h-2 rounded-full ${sig['5m_ok'] !== false ? 'bg-gain' : 'bg-loss'}`} />
                  </div>
                  <span className={`text-[9px] font-bold text-center ${score >= 4 ? 'text-gain' : score === 3 ? 'text-yellow-400' : 'text-muted-foreground'}`}>
                    {score}/6
                  </span>
                  <span className="text-[9px] font-mono text-muted-foreground text-right">
                    {sig.price ? (sig.price > 100
                      ? Number(sig.price).toLocaleString('en-US',{maximumFractionDigits:0})
                      : Number(sig.price).toFixed(4)) : ''}
                  </span>
                </div>
              );
            })}
            <p className="text-[7px] text-muted-foreground pt-1 leading-relaxed">EMA=9/21 · RSI=45–65 · MACD=hist↑ · VOL=&gt;1.3× · OBV=pressure · ATR=range · BB=middle/lower · 5m=uptrend</p>
          </div>
        ) : (
          <div className="space-y-0.5">
            {/* Header row */}
            <div className="flex items-center justify-between pb-1 border-b border-border/60">
              <span className="text-[9px] text-muted-foreground w-16">COIN</span>
              <div className="flex items-center gap-3 flex-1 justify-end">
                {(['EMA','RSI','MACD','VOL'] as const).map(lbl => (
                  <span key={lbl} className="text-[8px] text-muted-foreground w-7 text-center">{lbl}</span>
                ))}
                <span className="text-[8px] text-muted-foreground w-16 text-right">RSI val · Price</span>
              </div>
            </div>
            {coinSignals.length === 0 && !scanning && (
              <p className="text-[10px] text-muted-foreground py-2">Click refresh to scan</p>
            )}
            {coinSignals.map(sig => (
              <div key={sig.symbol} className="flex items-center justify-between py-1 border-b border-border/30 last:border-0">
                <div className="flex items-center gap-1.5 w-16 flex-shrink-0">
                  <span className={`text-[8px] font-bold px-1 py-0.5 rounded ${
                    sig.signal === 'BUY' ? 'bg-gain/20 text-gain' : 'bg-muted/40 text-muted-foreground'
                  }`}>{sig.signal === 'loading' ? '…' : sig.signal}</span>
                  <span className="text-[10px] font-mono font-semibold">{sig.symbol.replace('USDT','')}</span>
                </div>
                <div className="flex items-center gap-3 flex-1 justify-end">
                  {[sig.emaBullish, sig.rsiOk, sig.macdPos, sig.volUp].map((v, idx) => (
                    <div key={idx} className="flex flex-col items-center w-7">
                      <div className={`w-2 h-2 rounded-full ${v ? 'bg-gain' : 'bg-muted/50'}`} />
                    </div>
                  ))}
                  <span className="text-[9px] text-muted-foreground w-16 text-right">
                    {sig.rsi.toFixed(0)} · {sig.price > 0 ? sig.price.toLocaleString('en-US',{maximumFractionDigits: sig.price > 100 ? 2 : 4}) : '—'}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default AITradingAgent;
