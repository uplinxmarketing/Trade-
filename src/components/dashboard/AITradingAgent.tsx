import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Play, Square, Brain, TrendingUp, TrendingDown, Zap,
  RotateCcw, ChevronDown, ChevronUp, FlaskConical,
  Pencil, Check, X, BookOpen, Activity, Eye, EyeOff,
  ShoppingCart, Banknote, RefreshCw, Settings2, Shield,
  BarChart3, History, SlidersHorizontal, ListChecks, FileClock,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import { checkExits, TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { calcEMA, calcRSI, calcMACD, calcBollingerBands, calcSMA } from '@/lib/indicators';
import { formatTime, formatPnL } from '@/lib/format';
import { SignalEnginePanel } from './SignalEnginePanel';
import { DiagnosticsTab } from './DiagnosticsTab';
import { AnalyticsPanel } from './AnalyticsPanel';
import { SessionStatsPanel } from './SessionStatsPanel';
import { BacktestPanel } from './BacktestPanel';
import { LeverMatrixPanel } from './LeverMatrixPanel';
import { RiskPanel } from './RiskPanel';
import { StrategySettingsPanel } from './StrategySettingsPanel';
import { EntryGatePanel } from './EntryGatePanel';
import { SignalsEditorPanel } from './SignalsEditorPanel';
import { ConfigHistoryPanel } from './ConfigHistoryPanel';
import UniverseNoticesBanner from './UniverseNoticesBanner';
import { useUniverseHealth } from '@/hooks/useUniverseHealth';
import { resolveCoinStatus, type CoinLifecycle } from '@/lib/coin-status';

// ── Simple 4-signal analyser (no API key, Binance public data only) ────────────────────
const BIN = '/api/proxy/binance';
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

const SIGNAL_SHORT_LABELS: Record<string, string> = {
  T1_ema_short_long:       'EMA',
  M1_rsi_below_threshold:  'RSI',
  M3_macd_rising:          'MACD',
  V1_volume_above_average: 'VOL',
  V2_obv_rising:           'OBV',
  X1_atr_sufficient:       'ATR',
  P1_near_24h_low:         'NLow',
  R1_reversal_confirmed:   'Rev',
  E1_spread_too_wide:      'Sprd',
  TM1_bad_hour:            'Hour',
  M2_stoch_rsi_oversold:   'SRSI',
};

// I4 — status badge for a selected coin that has NO signal-cache entry yet, so
// it still renders in the Market Signals list (operator must always see all N).
function placeholderBadge(lifecycle: CoinLifecycle, successor?: string, backfillPct?: number):
  { text: string; cls: string; title: string } {
  switch (lifecycle) {
    case 'warming':
      return {
        text: `warming up — backfilling${typeof backfillPct === 'number' ? ` ${Math.round(backfillPct)}%` : ''}`,
        cls: 'bg-warn/10 text-warn/80',
        title: 'In your universe but still backfilling history — starts trading automatically once warmed up.',
      };
    case 'halted':
      return {
        text: 'halted — auto-restores',
        cls: 'bg-warn/15 text-warn',
        title: 'Trading on this pair is halted (BREAK/AUCTION). The bot auto-restores it when it resumes.',
      };
    case 'delisted':
      return {
        text: successor ? `delisted → ${successor.replace('USDT', '')}` : 'delisted',
        cls: 'bg-loss/15 text-loss',
        title: 'This pair is no longer tradable. Remove or replace it (see the auto-removed banner above).',
      };
    default:
      return {
        text: 'awaiting signal…',
        cls: 'bg-muted/30 text-muted-foreground',
        title: 'Selected and warm — no signal-cache entry yet this scan cycle.',
      };
  }
}

// Safe numeric price from the WebSocket ticker map (entries are objects, not numbers).
function numPrice(prices: LivePrices, sym: string): number | undefined {
  const v = Number(prices[sym]?.price);
  return Number.isFinite(v) && v > 0 ? v : undefined;
}

// J1 — humane short age formatting for the per-symbol decision trace (Ns / Nm / Nh).
function humanizeAgeSec(sec: number | null | undefined): string {
  if (sec == null || !Number.isFinite(sec)) return '—';
  const s = Math.max(0, Math.round(sec));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  return `${Math.round(s / 3600)}h`;
}

const INSTRUCTIONS_KEY  = 'ai_agent_instructions';
const AGENT_CYCLE_MS    = 30_000;
const MAX_LOG_LINES     = 200;
const isServerMode      = true; // always use VPS bot REST API

function useDataFetcher<T>(url: string, intervalMs: number, initial: T, enabled = true) {
  const [data, setData] = useState<T>(initial);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState(0);

  useEffect(() => {
    if (!enabled) return;
    let cancelled = false;
    const run = async () => {
      try {
        const r = await fetch(url);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = await r.json();
        if (!cancelled) { setData(d); setLoading(false); setLastUpdate(Date.now()); }
      } catch { if (!cancelled) setLoading(false); }
    };
    run();
    const iv = setInterval(run, intervalMs);
    return () => { cancelled = true; clearInterval(iv); };
  }, [url, intervalMs, enabled]);

  return { data, loading, lastUpdate };
}

function FreshnessIndicator({ lastUpdate }: { lastUpdate: number }) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  if (!lastUpdate) return null;
  const age = Math.floor((now - lastUpdate) / 1000);
  const color = age < 4 ? 'text-gain' : age < 10 ? 'text-yellow-400' : 'text-loss';
  return <span className={`text-[8px] ${color}`}>{age}s ago</span>;
}

interface OpenPosition {
  symbol: string;
  quantity: number;
  avg_entry_price: number;
  exit_target?: number;
  current_price?: number;
  profitable?: boolean;
  ready_to_sell?: boolean;
  hold_human?: string;
  hold_minutes?: number;
  breakeven_price_real?: number;
  real_bep_gap_pct?: number;
  is_trapped?: boolean;
  dist_to_exit_pct?: number;
  dist_to_bep_pct?: number;
  net_profit_now?: number;
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

    {/* ── Exit management moved (Part E dead-field removal) ──
        The old Stop Loss / Take Profit / Smart Hold controls wrote the legacy
        root keys (stop_loss_pct, take_profit_pct, smart_hold_enabled,
        trailing_stop_pct) which the engine only reads on its pre-v2 legacy
        path. Since the v2 migration always creates the `exits` block, ALL
        exits are governed by Strategy Settings → Exits (ATR stop k_sl +
        clamps, hard SL, reward:risk TP, breakeven move, trailing, smart-hold
        score gate) — the removed controls saved but changed nothing. */}
    <div className="bg-muted/30 rounded-md px-3 py-2.5">
      <p className="text-xs font-semibold">Exits (stop-loss / take-profit / trailing)</p>
      <p className="text-[9px] text-muted-foreground">
        Configured in <span className="font-semibold">Strategy Settings → Exits</span> below
        (ATR stop, hard stop, reward:risk take-profit, trailing). Changes apply live.
      </p>
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
  const [showSignalEngine, setShowSignalEngine] = useState(false);
  const [showDiagnostics, setShowDiagnostics] = useState(false);
  const [showAnalytics, setShowAnalytics] = useState(false);
  const [showBacktest, setShowBacktest] = useState(false);
  const [showRisk, setShowRisk] = useState(false);
  // Phase 5 §5.2 — strategy editor sections
  const [showStrategySettings, setShowStrategySettings] = useState(false);
  const [showSignalsEditor, setShowSignalsEditor] = useState(false);
  const [showConfigHistory, setShowConfigHistory] = useState(false);
  // Phase 4 §4.4 — compact breaker summary carried on the /api/all poll.
  const [riskSummary, setRiskSummary] = useState<{
    daily_stopped?: boolean; consec_paused?: boolean;
    effective_slots?: number | null; degraded?: boolean;
    slippage_veto_count?: number;
  } | null>(null);
  const [signalRegistry, setSignalRegistry] = useState<{id: string; category: string; description: string; role: string}[]>([]);
  const [forcingBuy, setForcingBuy]   = useState<string | null>(null);
  const [forcingSell, setForcingSell] = useState<string | null>(null);
  const [showSettings, setShowSettings] = useState(false);
  // Part E: legacy exit settings (stop_loss_*, take_profit_*, smart_hold,
  // trailing_stop) were removed from this panel — the v2 `exits` block in
  // Strategy Settings is the single exit-tuning surface.
  const [reinvestProfits, setReinvestProfits]         = useState(false);
  const [maxPositions, setMaxPositions]               = useState(10);
  const [minSignals, setMinSignals]                   = useState(4);
  const [settingsDraft, setSettingsDraft]             = useState({
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
  const [actLogFilter, setActLogFilter] = useState<'all' | 'orders' | 'sells' | 'buys' | 'errors'>('all');
  // All API calls are relative (same-origin) — bot runs on wolfbot.tech.
  const botUrl = '';  // same-origin VPS API — all calls use relative /api/* paths
  const [liveSetupLoading, setLiveSetupLoading] = useState(false);
  const [showModeToggle, setShowModeToggle] = useState(false);
  // Poll health — drives the "Bot Server" badge (CONNECTED vs UNREACHABLE)
  const [lastPollOkAt, setLastPollOkAt] = useState(0);
  const [pollFailCount, setPollFailCount] = useState(0);

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
  // Risk settings (exit tuning lives in Strategy Settings → Exits)
  const [setupReinvest, setSetupReinvest]           = useState(false);
  const [setupMaxPositions, setSetupMaxPositions]   = useState(10);
  const [setupMinSignals, setSetupMinSignals]       = useState(4);

  const isRunningRef     = useRef(false);
  const coinsRestoredRef = useRef<boolean>(false);
  const balanceRef     = useRef(0);
  const timerRef       = useRef<ReturnType<typeof setTimeout> | null>(null);
  const scanTimerRef   = useRef<ReturnType<typeof setTimeout> | null>(null);
  const positionsRef   = useRef<OpenPosition[]>([]);
  const [botSignals, setRailwaySignals] = useState<any[]>([]);
  const [usingPaperFallback, setUsingPaperFallback] = useState(false);
  const [liveErrorMsg, setLiveErrorMsg] = useState<string | null>(null);

  // Independent polling for positions and signals tables
  const { data: positionsData, loading: positionsLoading, lastUpdate: positionsUpdated } =
    useDataFetcher(`${botUrl}/api/positions`, 2000, { positions: [] as any[] }, isServerMode);
  const { data: signalsData, loading: signalsLoading, lastUpdate: signalsUpdated } =
    // I4 — limit raised so every selected coin with a signal-cache entry gets a
    // real row (operator runs ~100 coins). Coins still without an entry are
    // rendered below as status placeholders so the list always shows all N.
    useDataFetcher(`${botUrl}/api/signals-summary?limit=250`, 5000, { signals: [] as any[], total_tracked: 0 }, isServerMode);

  // I4 — universe health drives placeholder status for selected coins that have
  // no signal-cache entry yet, and readiness for the "warming up" state.
  const universe = useUniverseHealth();
  const universeInputs = useMemo(() => ({
    invalid: universe.invalid,
    warming: universe.warming,
    ready: universe.ready,
    backfillPct: universe.backfillPct,
  }), [universe.invalid, universe.warming, universe.ready, universe.backfillPct]);

  // I4 — "Add successor" on an auto-remove notice: append the successor to the
  // watchlist (which the debounced effect syncs to POST /api/coins) and also
  // POST immediately so the operator's universe changes without a round-trip.
  const handleAddSuccessor = useCallback(async (successor: string) => {
    const sym = successor.toUpperCase();
    if (selectedCoins.some(c => c.toUpperCase() === sym)) return;
    const next = [...selectedCoins, sym];
    onCoinsChange?.(next);
    try {
      await fetch(`${botUrl}/api/coins`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ coins: next }),
      });
      toast.success(`Added successor ${sym.replace('USDT', '')}`);
    } catch {
      toast.error(`Failed to add ${sym.replace('USDT', '')}`);
    }
  }, [selectedCoins, onCoinsChange, botUrl]);

  const addLog = useCallback((msg: string) => {
    setActLog(prev => [msg, ...prev].slice(0, MAX_LOG_LINES));
  }, []);

  // ── Sync settings from server on mount ────────────────────────────────────
  useEffect(() => {
    // (so users with already-correct settings don't see a false warning).
    fetch(`${botUrl}/api/settings`, { cache: 'no-store' })
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (!d) return;
        if (d.reinvest_profits    !== undefined) setReinvestProfits(Boolean(d.reinvest_profits));
        if (d.max_positions       !== undefined) setMaxPositions(Number(d.max_positions));
        if (d.min_signals         !== undefined) setMinSignals(Number(d.min_signals));
      })
      .catch(() => {});
    fetch(`${botUrl}/api/config`, { cache: 'no-store' })
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (!d) return;
        if (d.budget_mode  !== undefined) setSetupBudgetMode(d.budget_mode as 'fixed'|'percent'|'capped');
        // /api/config returns budget_fixed_usdt / budget_pct_of_free /
        // budget_total_cap_usdt / bot_allocation_usdt — pick per-mode value.
        const budgetMode = d.budget_mode ?? 'fixed';
        const budgetVal  = budgetMode === 'percent' ? d.budget_pct_of_free
                         : budgetMode === 'capped'  ? d.budget_total_cap_usdt
                         : d.budget_fixed_usdt;
        if (budgetVal            !== undefined) setSetupBudgetValue(Number(budgetVal));
        if (d.bot_allocation_usdt !== undefined) setSetupAllocation(Number(d.bot_allocation_usdt));
      })
      .catch(() => {});
  }, [botUrl]);

  // ── Data loader (Supabase — local mode only) ──────────────────────────────
  const loadData = useCallback(async () => {
    // Server mode: all data comes from pollBot (VPS SQLite), not Supabase.
    // Letting loadData run would race with pollBot and overwrite server data with empty Supabase rows.
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
    // Part E dead-code removal: the browser-local paper-trading loop
    // (Supabase positions + localStorage budget allocation) is permanently
    // unreachable — isServerMode is hardwired true and the VPS bot owns ALL
    // entries/exits/sizing. Leaving it here shadowed the server-side sizing
    // config with the dead localStorage paper_wallet_config path and it no
    // longer type-checked against the current lib/trading-engine API.
    try {
      const signals: CoinSignal[] = [];
      for (const sym of selectedCoins) {
        const sig = await analyseCoin(sym);
        signals.push(sig);
      }
      setCoinSignals(signals);
    } catch (e: any) { addLog(`[cycle] error: ${e.message}`); }
    setScanning(false);
    await loadData();
  }, [selectedCoins, addLog, loadData]);

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
      fetch(`${botUrl}/api/coins`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ coins: selectedCoins }),
      }).catch(() => {});
    }, 800);
    return () => clearTimeout(handle);
  }, [selectedCoins, botUrl]); // eslint-disable-line

  // ── VPS bot poller ─────────────────────────────────────────────────────────────────
  // Polls /api/all (same-origin → wolfbot.tech) every 5 s normally, every 1 s
  // when positions are open. Mirrors bot state into React state for live UI.
  const serverPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const fastPollRef   = useRef<ReturnType<typeof setInterval> | null>(null);
  // In-flight guard: both poll intervals call pollBot. Without this, a slow
  // 5-s response can land after a fast 1-s response and clobber newer data.
  const pollInFlightRef = useRef<boolean>(false);

  const pollBot = useCallback(async () => {
    if (pollInFlightRef.current) return;
    pollInFlightRef.current = true;
    try {
    // Single /api/all request (status + positions + trades + activity in one round trip)
    // Retry once with a 2s delay if the first attempt fails.
    const attempt = async () => {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 5_000);
      try {
        const res = await fetch(`${botUrl}/api/all`, { signal: ctrl.signal, cache: 'no-store' });
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
        setPollFailCount(c => c + 1);
        return;
      }
    }
    if (!data || typeof data !== 'object') return;
    // Guard against an error envelope from the server
    if (data.error) {
      addLog(`[Bot] server error: ${data.error}`);
      return;
    }
    setLastPollOkAt(Date.now());
    setPollFailCount(0);

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
    // Mirror server mode into local state so the mode pill / switcher reflect truth.
    setMode(s.mode === 'live' ? 'live' : 'test');
    setAgentStatus(`Bot · ${s.mode?.toUpperCase() ?? 'PAPER'} · ${formatTime(new Date())}`);
    if (s.data_persistent !== undefined) setDataPersistent(Boolean(s.data_persistent));
    setUsingPaperFallback(Boolean(s.using_paper_fallback));
    setLiveErrorMsg(s.live_error ?? null);
    // Update committed state from server — never touch settingsDraft here.
    // The draft is only reset when the user opens the settings panel, so
    // in-progress edits are never overwritten by a background poll.
    if (s.reinvest_profits    !== undefined) setReinvestProfits(Boolean(s.reinvest_profits));
    if (s.max_positions       !== undefined) setMaxPositions(Number(s.max_positions));
    if (s.min_signals         !== undefined) setMinSignals(Number(s.min_signals));
    if (s.strategy_notes   !== undefined) { setInstructions(s.strategy_notes as string); localStorage.setItem(INSTRUCTIONS_KEY, s.strategy_notes as string); }
    if (s.budget_mode        !== undefined) setSetupBudgetMode(s.budget_mode as 'fixed'|'percent'|'capped');
    if (s.budget_fixed_usdt  !== undefined) setSetupBudgetValue(Number(s.budget_fixed_usdt));
    if (s.bot_allocation_usdt !== undefined) setSetupAllocation(Number(s.bot_allocation_usdt));
    // Server-authoritative stats — use these instead of summing individual trade rows
    // to avoid double-counting with Supabase.
    // Phase 4 §4.4 — breaker-state summary for the "BUYS PAUSED" header badge.
    if (data.risk && typeof data.risk === 'object') setRiskSummary(data.risk);
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
      ready_to_sell: p.ready_to_sell === true,
      hold_human: p.hold_human,
      hold_minutes: p.hold_minutes !== undefined ? Number(p.hold_minutes) : undefined,
      breakeven_price_real: p.breakeven_price_real !== undefined ? Number(p.breakeven_price_real) : undefined,
      real_bep_gap_pct: p.real_bep_gap_pct !== undefined ? Number(p.real_bep_gap_pct) : undefined,
      is_trapped: p.is_trapped === true,
      net_profit_now: p.net_profit_now !== undefined ? Number(p.net_profit_now) : undefined,
    }));
    setPositions(rawPos);
    positionsRef.current = rawPos;
    if (data.fresh_prices && typeof data.fresh_prices === 'object') {
      setFreshPrices(data.fresh_prices as Record<string, number>);
    }

    // Trades
    const botTrades: TradeRow[] = [];
    for (const t of (data.trades ?? [])) {
      if (!t.symbol || !t.side) continue;
      botTrades.push({
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
    const botTrades2 = [...botTrades];
    if (botTrades.length === 0) {
      try {
        const { data: sbTrades } = await supabase.from('trades')
          .select('*').eq('user_session', SESSION)
          .order('created_at', { ascending: false }).limit(50);
        if (Array.isArray(sbTrades) && sbTrades.length > 0) {
          botTrades2.push(...(sbTrades as TradeRow[]));
        }
      } catch { /* Supabase unavailable — bot-only data shown */ }
    }

    const sorted = botTrades2.sort((a, b) =>
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
      const dbg = await fetch(`${botUrl}/api/debug`, { cache: 'no-store' }).then(r => r.ok ? r.json() : null).catch(() => null);
      if (dbg) {
        addLog(`[debug] MODE=${dbg?.env?.MODE ?? 'unknown'} live_error=${s.live_error ?? 'none'}`);
      }
    }
    } finally {
      pollInFlightRef.current = false;
    }
  }, [botUrl, addLog]);

  // Start / stop server poll
  useEffect(() => {
    if (!isServerMode) return;
    pollBot();
    serverPollRef.current = setInterval(pollBot, 3_000);
    return () => { if (serverPollRef.current) clearInterval(serverPollRef.current); };
  }, [isServerMode, pollBot]); // eslint-disable-line

  // Fetch signal registry once on mount (server mode only)
  useEffect(() => {
    if (!isServerMode) return;
    fetch(`${botUrl}/api/signal-registry`)
      .then(r => r.json())
      .then(d => { if (d.signals) setSignalRegistry(d.signals); })
      .catch(() => {});
  }, [isServerMode, botUrl]); // eslint-disable-line

  // Uses `current_price` from the server API response (not WebSocket prices)
  useEffect(() => {
    if (!isServerMode) return;
    if (fastPollRef.current) clearInterval(fastPollRef.current);
    if (positions.length > 0) {
      fastPollRef.current = setInterval(pollBot, 500);
    } else {
      fastPollRef.current = null;
    }
    return () => { if (fastPollRef.current) clearInterval(fastPollRef.current); };
  }, [isServerMode, positions, pollBot]); // eslint-disable-line

  // ── Start / Stop ──────────────────────────────────────────────────────────
  const handleStartStop = useCallback(async () => {
    if (isRunning) {
      if (isServerMode) {
        const res = await fetch(`${botUrl}/api/agent/stop`, { method: 'POST' });
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
  }, [isRunning, isServerMode, botUrl, addLog]);

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
        reinvest_profits:    setupReinvest,
        max_positions:       setupMaxPositions,
        min_signals:         setupMinSignals,
      };
      const [cfgRes, setRes] = await Promise.all([
        fetch(`${botUrl}/api/config`,   { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(budgetPayload) }),
        fetch(`${botUrl}/api/settings`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(settingsPayload) }),
      ]);
      return cfgRes.ok && setRes.ok;
    } catch {
      return false;
    }
  }, [
    setupBudgetMode, setupBudgetValue, setupAllocation, setupReinvest,
    setupMaxPositions, setupMinSignals,
    botUrl]);

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
        res = await fetch(`${botUrl}${endpoint}`, { method: 'POST' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        setIsRunning(true); isRunningRef.current = true;
        addLog(`Bot started · ${data.message ?? 'ok'}`);
        toast.success('Bot started');
        // Re-sync coins after start (bot resets watchlist on start)
        if (selectedCoins.length > 0) {
          fetch(`${botUrl}/api/coins`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ coins: selectedCoins }),
          }).catch(() => {});
        }
        pollBot().catch(() => {});  // fire-and-forget; don't block UI
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
  }, [loading, isServerMode, selectedCoins, botUrl, settingsSynced, syncSettingsToServer, addLog, pollBot, runCycle]);

  // ── Force buy ─────────────────────────────────────────────────────────────
  const handleForceBuy = useCallback(async (sym: string) => {
    if (!isServerMode) { toast.error('Force buy only available in server mode'); return; }
    setForcingBuy(sym);
    try {
      // Server mode: delegate to VPS bot's force-buy endpoint
      if (positions.find(p => p.symbol === sym)) { toast.error(`Already holding ${sym}`); return; }
        const res  = await fetch(`${botUrl}/api/force-buy/${sym}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          // price hint lets the server fill even when its WS cache is cold
          body: JSON.stringify({ budget: setupBudgetValue, price: numPrice(prices, sym) ?? 0 }),
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        addLog(`FORCE BUY ${sym} @ ${Number(data.price).toFixed(4)} USDT · ${Number(data.budget).toFixed(2)} USDT`);
        toast.success(`Force-bought ${sym}`);
        await pollBot();
    } catch (e: any) {
      toast.error(`Force buy failed: ${e.message}`);
      addLog(`[force-buy] ${sym}: ${e.message}`);
    } finally {
      setForcingBuy(null);
    }
  }, [isServerMode, positions, botUrl, setupBudgetValue, prices, addLog, pollBot]);

  // ── Force sell ────────────────────────────────────────────────────────────
  const handleForceSell = useCallback(async (pos: OpenPosition) => {
    setForcingSell(pos.symbol);
    try {
      if (isServerMode) {
        // prices[] entries are ticker OBJECTS — coerce to a number or the
        // backend rejects the request (price: float) with HTTP 422.
        const cur = (pos.current_price && pos.current_price > 0)
          ? pos.current_price
          : (numPrice(prices, pos.symbol) ?? 0);

        // Optimistic UI: remove position immediately so the panel feels instant.
        // If the backend fails, the next pollBot restores it automatically.
        const positionBackup = positions.find(p => p.symbol === pos.symbol);
        setPositions(prev => prev.filter(p => p.symbol !== pos.symbol));

        try {
          const res = await fetch(`${botUrl}/api/force-sell/${pos.symbol}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ price: cur }),
          });
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          if (data.error) throw new Error(data.error);
          addLog(`FORCE SELL ${pos.symbol} @ ${Number(data.price).toFixed(4)} USDT`);
          toast.success(`Sold ${pos.symbol}`);
          setTimeout(() => pollBot(), 200);
        } catch (sellErr) {
          // Sell failed — restore position so user can retry
          if (positionBackup) setPositions(prev => [...prev, positionBackup]);
          throw sellErr;
        }
        return;
      }
      // Local mode
      const exitPrice = numPrice(prices, pos.symbol) ?? pos.avg_entry_price;
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
  }, [isServerMode, botUrl, prices, positions, addLog, loadData, pollBot]);

  // ── Reset wallet ──────────────────────────────────────────────────────────
  const handleReset = useCallback(async () => {
    try {
      if (isServerMode) {
        const res  = await fetch(`${botUrl}/api/reset`, { method: 'POST' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (data.error) throw new Error(data.error);
        addLog('=== Bot wallet reset ===');
        toast.success(`Bot reset · ${data.balance_usdt?.toLocaleString()} USDT restored`);
        await pollBot();
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
  }, [isServerMode, botUrl, addLog, pollBot, loadData]);

  // ── Save bot settings to server ──────────────────────────────────────────────
  const saveSettings = useCallback(async () => {
    setSavingSettings(true);
    try {
      const [settingsRes, configRes] = await Promise.all([
        fetch(`${botUrl}/api/settings`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            reinvest_profits:    settingsDraft.reinvestProfits,
            max_positions:       settingsDraft.maxPositions,
            min_signals:         settingsDraft.minSignals,
          }),
        }),
        fetch(`${botUrl}/api/config`, {
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
      if (!settingsRes.ok) throw new Error(`settings HTTP ${settingsRes.status}`);
      if (!configRes.ok)   throw new Error(`config HTTP ${configRes.status}`);
      setReinvestProfits(settingsDraft.reinvestProfits);
      setMaxPositions(settingsDraft.maxPositions);
      setMinSignals(settingsDraft.minSignals);
      toast.success('Settings saved');
    } catch (e: any) {
      toast.error(`Save failed: ${e.message}`);
    } finally {
      setSavingSettings(false);
    }
  }, [settingsDraft, setupBudgetMode, setupBudgetValue, setupAllocation, botUrl]);

  // ── Computed stats ────────────────────────────────────────────────────────
  const totalValue = positions.reduce((sum, p) => {
    const cur = (isServerMode && p.current_price) ? p.current_price : (numPrice(prices, p.symbol) ?? p.avg_entry_price);
    return sum + cur * p.quantity;
  }, 0);

  const unrealizedPnl = positions.reduce((sum, p) => {
    const cur = (isServerMode && p.current_price) ? p.current_price : (numPrice(prices, p.symbol) ?? p.avg_entry_price);
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

  // Bot reachability derived from actual poll health, not compile-time constants.
  const botReachable = lastPollOkAt > 0 && pollFailCount < 2;

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

      {/* I4 — auto-remove notices: tell the operator when the universe changed
          under them (delisted → successor), with one-click "Add successor". */}
      <UniverseNoticesBanner selectedCoins={selectedCoins} onAddSuccessor={handleAddSuccessor} />

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

      {/* Mode toggle row */}
      <div className="border-b border-border">
        <div className="flex items-center justify-between px-4 py-2">
          <div className="flex items-center gap-2">
            {/* Clickable mode toggle pill */}
            <button
              onClick={() => setShowModeToggle(v => !v)}
              className={`flex items-center gap-1.5 text-[10px] font-bold px-2.5 py-1 rounded-full border transition-all ${
                mode === 'live' && !usingPaperFallback
                  ? 'bg-gain/20 text-gain border-gain/30 hover:bg-gain/30'
                  : usingPaperFallback
                  ? 'bg-amber-500/20 text-amber-400 border-amber-500/30 hover:bg-amber-500/30'
                  : 'bg-muted/40 text-muted-foreground border-border hover:bg-muted/60'
              }`}
            >
              <span className={`w-1.5 h-1.5 rounded-full ${
                mode === 'live' && !usingPaperFallback ? 'bg-gain animate-pulse' :
                usingPaperFallback ? 'bg-amber-400' : 'bg-muted-foreground'
              }`} />
              {mode === 'live' ? (usingPaperFallback ? 'LIVE · PAPER FALLBACK' : 'LIVE') : 'PAPER'}
              <ChevronDown className={`w-2.5 h-2.5 transition-transform ${showModeToggle ? 'rotate-180' : ''}`} />
            </button>
            {/* Phase 4 §4.4 — breaker badge next to the mode pill */}
            {riskSummary?.daily_stopped ? (
              <span className="text-[9px] font-bold px-2 py-0.5 rounded-full bg-loss/20 text-loss border border-loss/30 whitespace-nowrap">
                BUYS PAUSED: daily stop
              </span>
            ) : riskSummary?.consec_paused ? (
              <span className="text-[9px] font-bold px-2 py-0.5 rounded-full bg-amber-500/20 text-amber-400 border border-amber-500/30 whitespace-nowrap">
                BUYS PAUSED: loss streak
              </span>
            ) : null}
            {liveErrorMsg && !showModeToggle && (
              <span className="text-[9px] text-amber-400 truncate max-w-[140px]">{liveErrorMsg}</span>
            )}
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => isServerMode ? pollBot() : runCycle()}
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

        {/* Expandable mode switcher */}
        {showModeToggle && (
          <div className="px-4 pb-3 pt-1 border-t border-border/50 space-y-2">
            {mode === 'live' ? (
              /* Currently LIVE — offer switch to paper */
              <div className="space-y-2">
                <p className="text-[10px] text-muted-foreground">
                  Switch to paper mode to trade with simulated funds. Any open live positions will remain on Binance.
                </p>
                <Button
                  onClick={async () => {
                    setLiveSetupLoading(true);
                    try {
                      await fetch(`${botUrl}/api/mode`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ mode: 'paper' }),
                      });
                      setShowModeToggle(false);
                      toast.success('Switching to paper mode…');
                      setTimeout(() => pollBot(), 3000);
                    } catch (e: any) {
                      toast.error('Failed', { description: e.message });
                    } finally {
                      setLiveSetupLoading(false);
                    }
                  }}
                  disabled={liveSetupLoading}
                  size="sm"
                  variant="outline"
                  className="w-full border-amber-500/40 text-amber-400 hover:bg-amber-500/10">
                  {liveSetupLoading ? 'Switching…' : 'Switch to Paper Mode'}
                </Button>
              </div>
            ) : (
              /* Currently PAPER — direct user to the top-right Binance button */
              <div className="space-y-2">
                <p className="text-[10px] text-muted-foreground">
                  To trade with real funds, connect your Binance API keys using the <strong>Binance</strong> button in the top-right corner.
                </p>
                <Button
                  onClick={() => { setShowModeToggle(false); onConnectBinance?.(); }}
                  size="sm"
                  className="w-full bg-gain hover:bg-gain/90 text-white font-semibold">
                  Open Binance Settings
                </Button>
              </div>
            )}
          </div>
        )}
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
                <button onClick={() => { setSettingsDraft({ reinvestProfits, maxPositions, minSignals }); }}
                  className="text-[10px] text-muted-foreground hover:text-foreground">Reload from server</button>
              </div>
              <p className="text-[9px] text-muted-foreground/70 italic">
                Full strategy editor available in <span className="font-semibold not-italic">Strategy Settings</span> below.
              </p>

              <AgentTradingFields
                budgetMode={setupBudgetMode} setBudgetMode={setSetupBudgetMode}
                budgetValue={setupBudgetValue} setBudgetValue={setSetupBudgetValue}
                allocation={setupAllocation} setAllocation={setSetupAllocation}
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
                  {isServerMode && (botReachable
                    ? <span className="text-[9px] px-1.5 py-0.5 rounded bg-gain/20 text-gain font-bold">CONNECTED</span>
                    : <span className="text-[9px] px-1.5 py-0.5 rounded bg-loss/20 text-loss font-bold">UNREACHABLE</span>)}
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
                          await fetch(`${botUrl}/api/settings`, {
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

              {/* Mode switching is done via the toggle pill at the top of the card */}
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
            <FreshnessIndicator lastUpdate={positionsUpdated} />
          </div>
          {showPositionsSection ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" /> : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
        </button>

        {showPositionsSection && (
          <div className="px-4 pb-3">
            {positionsLoading && positions.length === 0 ? (
              <div className="space-y-2">
                {[1,2,3].map(i => (
                  <div key={i} className="h-16 bg-muted/20 rounded animate-pulse" />
                ))}
              </div>
            ) : positions.length === 0 ? (
              <p className="text-[10px] text-muted-foreground py-2">No open positions</p>
            ) : (
              <>
                <div className="space-y-2">
                  <AnimatePresence initial={false}>
                  {(showAllPositions
                    ? [...positions].sort((a, b) => (b.dist_to_exit_pct ?? -999) - (a.dist_to_exit_pct ?? -999))
                    : [...positions].sort((a, b) => (b.dist_to_exit_pct ?? -999) - (a.dist_to_exit_pct ?? -999)).slice(0, 5)
                  ).map(pos => {
                    const entry = pos.avg_entry_price > 0 ? pos.avg_entry_price : 0;
                    // Priority: fresh_prices (injected per-poll outside cache) > local WS > cached server price
                    const cur   = freshPrices[pos.symbol]
                      ?? (isServerMode && pos.current_price && pos.current_price > 0 ? pos.current_price : undefined)
                      ?? numPrice(prices, pos.symbol)
                      ?? entry;
                    // Real breakeven: server-computed (fees + lot rounding), fallback to ~0.17% above entry
                    const bep = pos.breakeven_price_real ?? (entry > 0 ? entry * 1.0017 : 0);
                    // Fee-inclusive net profit if sold now (server provides this; local fallback)
                    const netPnlNow = pos.net_profit_now
                      ?? (bep > 0 && entry > 0 ? (cur - entry) * pos.quantity - (cur * pos.quantity * 0.001) : 0);
                    // % relative to breakeven — green means actual profit after all fees
                    const pct = bep > 0 ? ((cur - bep) / bep) * 100 : 0;
                    // exit_target comes from server; fallback to entry × breakeven (0.15% above)
                    const exitTarget = pos.exit_target && pos.exit_target > 0
                      ? pos.exit_target
                      : entry * 1.0017;
                    // Use server's profitable (real BEP after fees + lot rounding) not local exit_target check
                    const profitable = pos.profitable ?? false;
                    // ready_to_sell = price above BOTH real BEP and exit_target (bot will sell)
                    const readyToSell = pos.ready_to_sell ?? (profitable && cur >= exitTarget);
                    // Three-zone bar: Entry(0%) → BEP(bepPct%) → Target(100%)
                    const barRange = exitTarget > entry ? exitTarget - entry : entry * 0.003;
                    const bepPct     = entry > 0 && exitTarget > entry && bep > entry
                      ? Math.min(98, Math.max(2, (bep - entry) / barRange * 100))
                      : 20;
                    const progressPct = entry > 0
                      ? Math.max(0, Math.min(100, (cur - entry) / barRange * 100))
                      : 0;
                    return (
                      <motion.div
                        key={pos.symbol}
                        layout
                        initial={{ opacity: 0, y: -8 }}
                        animate={{ opacity: 1, y: 0 }}
                        exit={{ opacity: 0, y: 8 }}
                        transition={{ duration: 0.25, type: 'spring', stiffness: 300, damping: 30 }}
                        className="bg-muted/20 rounded px-3 py-2 space-y-1"
                      >
                        <div className="flex items-center justify-between">
                          <div className="flex items-center gap-2">
                            <div className={`w-1.5 h-1.5 rounded-full ${readyToSell ? 'bg-gain' : profitable ? 'bg-amber-400' : netPnlNow < 0 ? 'bg-loss' : 'bg-amber-400/50'}`} />
                            <div>
                              <p className="text-xs font-bold">{pos.symbol.replace('USDT','')}</p>
                              <p className="text-[9px] text-muted-foreground">
                                {pos.quantity.toFixed(6)} @ <span className="font-mono">${entry > 0 ? entry.toFixed(6) : '—'}</span>
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
                            <p className={`text-[9px] font-mono ${netPnlNow >= 0 ? 'text-gain' : 'text-loss'}`}
                               title={`Net profit after buy+sell fees if force-sold now. Breakeven: $${bep > 0 ? bep.toFixed(6) : '—'}`}>
                              {formatPnL(netPnlNow, 4)} USDT ({pct >= 0 ? '+' : '−'}{Math.abs(pct).toFixed(3)}% vs BEP)
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
                        {/* Three-zone progress bar: Entry → BEP → Target */}
                        <div className="relative w-full h-2 rounded-full" style={{
                          background: `linear-gradient(to right, rgba(239,68,68,0.22) 0%, rgba(239,68,68,0.22) ${bepPct}%, rgba(245,158,11,0.18) ${bepPct}%, rgba(245,158,11,0.18) 100%)`
                        }}>
                          {/* Fill: current price position */}
                          <div
                            className={`absolute top-0 left-0 h-full rounded-full transition-all duration-700 ${
                              readyToSell ? 'bg-gain/80' : profitable ? 'bg-amber-400/80' : 'bg-loss/70'
                            }`}
                            style={{ width: `${Math.min(progressPct, 100).toFixed(1)}%` }}
                          />
                          {/* BEP notch — white tick at breakeven */}
                          <div
                            className="absolute top-1/2 -translate-y-1/2 w-px h-3.5 bg-white/60 rounded-full"
                            style={{ left: `${bepPct}%` }}
                            title={`Breakeven (fees included): $${bep > 0 ? bep.toFixed(6) : '—'}`}
                          />
                        </div>
                        {/* Price labels: Entry | BEP | Target */}
                        <div className="grid grid-cols-3 text-[8px] font-mono mt-0.5">
                          <span className="text-muted-foreground/70 text-left leading-tight">
                            Entry<br/><span className="text-foreground/60">${entry > 0 ? entry.toFixed(5) : '—'}</span>
                          </span>
                          <span className="text-amber-400/80 text-center leading-tight">
                            BEP<br/><span>${bep > 0 ? bep.toFixed(5) : '—'}</span>
                          </span>
                          <span className={`text-right leading-tight ${readyToSell ? 'text-gain' : 'text-accent/80'}`}>
                            Target<br/><span>${exitTarget > 0 ? exitTarget.toFixed(5) : '—'}</span>
                          </span>
                        </div>
                        {/* Status line */}
                        <div className="flex items-center justify-between">
                          <span className="text-[8px] text-muted-foreground/50">
                            {pos.hold_human ? `held ${pos.hold_human}` : ''}
                          </span>
                          <span className={`text-[8px] font-mono font-medium ${readyToSell ? 'text-gain' : profitable ? 'text-amber-400' : 'text-loss'}`}>
                            {readyToSell
                              ? '✓ READY TO SELL'
                              : profitable
                              ? `+${((exitTarget - cur) / exitTarget * 100).toFixed(3)}% to target`
                              : cur > 0
                                ? `${pct.toFixed(3)}% vs BEP`
                                : 'loading…'}
                          </span>
                        </div>
                        {/* Trapped warning — lot-step rounding requires >2% move to break even */}
                        {pos.is_trapped && pos.real_bep_gap_pct !== undefined && (
                          <div className="text-[8px] text-yellow-500 mt-0.5 flex items-center gap-1">
                            <span>⚠</span>
                            <span>Lot-size trap: needs +{pos.real_bep_gap_pct.toFixed(2)}% to break even</span>
                          </div>
                        )}
                      </motion.div>
                    );
                  })}
                  </AnimatePresence>
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
          <div className="flex items-center justify-between mb-2">
            <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Activity Log</p>
            <div className="flex gap-1">
              {(['all', 'orders', 'sells', 'buys', 'errors'] as const).map(f => (
                <button key={f} onClick={() => setActLogFilter(f)}
                  className={`text-[8px] px-1.5 py-0.5 border rounded capitalize ${actLogFilter === f ? 'border-accent text-accent' : 'border-border text-muted-foreground hover:border-accent/50'}`}>
                  {f}
                </button>
              ))}
            </div>
          </div>
          <div className="space-y-0.5 max-h-32 overflow-y-auto scrollbar-thin">
            {actLog
              .filter(line => {
                if (actLogFilter === 'all') return true;
                if (actLogFilter === 'orders') return line.includes('[ORDER_SEND]') || line.includes('[ORDER_REPLY]');
                if (actLogFilter === 'sells') return line.toLowerCase().includes('sell');
                if (actLogFilter === 'buys') return line.toLowerCase().includes('buy');
                if (actLogFilter === 'errors') return line.toLowerCase().includes('error') || line.toLowerCase().includes('failed');
                return true;
              })
              .map((line, i) => (
                <p key={i} className="text-[10px] font-mono text-muted-foreground leading-relaxed">{line}</p>
              ))}
          </div>
        </div>
      )}

      {/* Signal Engine configuration (server mode only) */}
      {isServerMode && (
        <div className="border-t border-border">
          <button
            onClick={() => setShowSignalEngine(!showSignalEngine)}
            className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
            <div className="flex items-center gap-2">
              <FlaskConical className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold">Signal Engine</span>
            </div>
            {showSignalEngine
              ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />
              : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
          </button>
          {showSignalEngine && (
            <div className="px-4 pb-4">
              <SignalEnginePanel baseUrl={botUrl} />
            </div>
          )}
        </div>
      )}

      {/* Diagnostics (server mode only) */}
      {isServerMode && (
        <div className="border-t border-border">
          <button
            onClick={() => setShowDiagnostics(!showDiagnostics)}
            className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
            <div className="flex items-center gap-2">
              <Activity className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold">Diagnostics</span>
            </div>
            {showDiagnostics
              ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />
              : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
          </button>
          {showDiagnostics && (
            <div className="px-4 pb-4">
              <DiagnosticsTab baseUrl={botUrl} />
            </div>
          )}
        </div>
      )}

      {/* Strategy Settings — schema-driven full strategy editor (server mode only, §5.2.1) */}
      {isServerMode && (
        <div className="border-t border-border">
          <button
            onClick={() => setShowStrategySettings(!showStrategySettings)}
            className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
            <div className="flex items-center gap-2">
              <SlidersHorizontal className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold">Strategy Settings</span>
            </div>
            {showStrategySettings
              ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />
              : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
          </button>
          {showStrategySettings && (
            <div className="px-4 pb-4 space-y-3">
              {/* Part O1 — one-click entry-gate opener (independent of the schema
                  editor below so it still shows if the strategy schema fails). */}
              <EntryGatePanel baseUrl={botUrl} />
              <StrategySettingsPanel baseUrl={botUrl} />
            </div>
          )}
        </div>
      )}

      {/* Signals Editor — roles / thresholds / 24h impact (server mode only, §5.2.2) */}
      {isServerMode && (
        <div className="border-t border-border">
          <button
            onClick={() => setShowSignalsEditor(!showSignalsEditor)}
            className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
            <div className="flex items-center gap-2">
              <ListChecks className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold">Signals Editor</span>
            </div>
            {showSignalsEditor
              ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />
              : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
          </button>
          {showSignalsEditor && (
            <div className="px-4 pb-4">
              <SignalsEditorPanel baseUrl={botUrl} />
            </div>
          )}
        </div>
      )}

      {/* Risk breakers — daily stop / loss streak / slots / BNB / correlation (server mode only) */}
      {isServerMode && (
        <div className="border-t border-border">
          <button
            onClick={() => setShowRisk(!showRisk)}
            className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
            <div className="flex items-center gap-2">
              <Shield className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold">Risk</span>
              {(riskSummary?.daily_stopped || riskSummary?.consec_paused) && (
                <span className={`w-1.5 h-1.5 rounded-full ${riskSummary?.daily_stopped ? 'bg-loss' : 'bg-amber-400'} animate-pulse`} />
              )}
            </div>
            {showRisk
              ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />
              : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
          </button>
          {showRisk && (
            <div className="px-4 pb-4">
              <RiskPanel baseUrl={botUrl} />
            </div>
          )}
        </div>
      )}

      {/* Analytics — expectancy / exit labels / attribution (server mode only) */}
      {isServerMode && (
        <div className="border-t border-border">
          <button
            onClick={() => setShowAnalytics(!showAnalytics)}
            className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
            <div className="flex items-center gap-2">
              <BarChart3 className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold">Analytics</span>
            </div>
            {showAnalytics
              ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />
              : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
          </button>
          {showAnalytics && (
            <div className="px-4 pb-4 space-y-4">
              <AnalyticsPanel baseUrl={botUrl} />
              {/* L2.3 — session / hour expectancy tables */}
              <div className="border-t border-border/50 pt-3">
                <SessionStatsPanel baseUrl={botUrl} />
              </div>
            </div>
          )}
        </div>
      )}

      {/* Backtest (server mode only) */}
      {isServerMode && (
        <div className="border-t border-border">
          <button
            onClick={() => setShowBacktest(!showBacktest)}
            className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
            <div className="flex items-center gap-2">
              <History className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold">Backtest</span>
            </div>
            {showBacktest
              ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />
              : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
          </button>
          {showBacktest && (
            <div className="px-4 pb-4 space-y-4">
              <BacktestPanel baseUrl={botUrl} />
              {/* L3 — edge-report / lever matrix ranked variants */}
              <div className="border-t border-border/50 pt-3">
                <LeverMatrixPanel baseUrl={botUrl} />
              </div>
            </div>
          )}
        </div>
      )}

      {/* Config History — version list + rollback (server mode only, §5.2.6) */}
      {isServerMode && (
        <div className="border-t border-border">
          <button
            onClick={() => setShowConfigHistory(!showConfigHistory)}
            className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/20 transition-colors">
            <div className="flex items-center gap-2">
              <FileClock className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold">Config History</span>
            </div>
            {showConfigHistory
              ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />
              : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
          </button>
          {showConfigHistory && (
            <div className="px-4 pb-4">
              <ConfigHistoryPanel baseUrl={botUrl} />
            </div>
          )}
        </div>
      )}

      {/* Signal scanner */}
      <div className="border-t border-border px-4 py-3">
        <div className="flex items-center justify-between mb-2">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Market Signals</p>
          <FreshnessIndicator lastUpdate={signalsUpdated} />
        </div>
        {/* Dynamic registry-driven table (server mode). Renders EVERY selected
            coin: real rows for coins with a signal-cache entry, plus status
            placeholder rows for the rest so the operator always sees all N. */}
        {(isServerMode && signalRegistry.length > 0 && (signalsData.signals.length > 0 || selectedCoins.length > 0)) ? (() => {
          const cols = `4.5rem ${signalRegistry.map(() => '1fr').join(' ')} 2.5rem 3rem`;
          const present = new Set(
            signalsData.signals.map((s: any) => String(s.symbol ?? '').toUpperCase())
          );
          const missing = selectedCoins.filter(c => !present.has(c.toUpperCase()));
          // J1 — only surface the decision-trace legend when the backend actually
          // ships the new per-symbol fields (graceful degradation on older bots).
          const anyTrace = signalsData.signals.some((s: any) =>
            s.cached_green !== undefined || s.engine_ready !== undefined ||
            s.last_evaluated_age_sec !== undefined || s.last_attempt_age_sec !== undefined ||
            s.last_block_reason !== undefined || s.last_block_age_sec !== undefined);
          return (
            <div className="space-y-0 overflow-x-auto">
              {/* Header */}
              <div className="grid pb-1 border-b border-border/60" style={{gridTemplateColumns: cols}}>
                <span className="text-[8px] text-muted-foreground font-semibold">COIN</span>
                {signalRegistry.map(reg => (
                  <span key={reg.id} className="text-[8px] text-muted-foreground text-center" title={`${reg.description} [${reg.role}]`}>
                    {SIGNAL_SHORT_LABELS[reg.id] ?? reg.id.split('_')[0]}
                  </span>
                ))}
                <span className="text-[8px] text-muted-foreground text-center">BUY</span>
                <span className="text-[8px] text-muted-foreground text-right">PRICE</span>
              </div>
              {/* J1 — cached-vs-fresh legend. Explains the two-state badge so the
                  #1 source of operator confusion (green cache, no buy) is labelled. */}
              {anyTrace && (
                <p className="text-[7px] text-muted-foreground/80 leading-tight pt-0.5 pb-1 break-words">
                  <span className="text-gain font-semibold">ready</span> = cached green + fresh re-check passed ·{' '}
                  <span className="text-amber-400 font-semibold">cached green · fresh re-check pending</span> = cache is green but the fresh re-check hasn't confirmed yet (no buy until it does)
                </p>
              )}
              {signalsLoading && signalsData.signals.length === 0 && (
                <div className="space-y-1">
                  {[1,2,3,4,5].map(i => (
                    <div key={i} className="h-5 bg-muted/20 rounded animate-pulse" />
                  ))}
                </div>
              )}
              <AnimatePresence mode="popLayout">
              {signalsData.signals.map((sig: any) => {
                const results = sig.signal_results ?? {};
                const allowed = sig.buy_allowed;
                const reason  = sig.buy_reason ?? '';

                let label: string;
                let labelColor: string;
                if (allowed) {
                  // buy_allowed now reflects the bot's FULL gate chain (signals +
                  // vetoes + cooldowns + macro gate + capacity + paused state):
                  // BUY here means the bot will actually enter given budget.
                  label = 'BUY';  labelColor = 'bg-gain/20 text-gain';
                } else if (sig.signal_engine_allowed && (sig.gate_blockers?.length ?? 0) > 0) {
                  // Signals fired but an execution gate blocks the entry — the
                  // tooltip (reason) names the exact blocker(s).
                  label = 'GATE'; labelColor = 'bg-amber-500/20 text-amber-400';
                } else if (reason.startsWith('veto_')) {
                  label = 'VETO'; labelColor = 'bg-orange-500/20 text-orange-400';
                } else if (reason.startsWith('mandatory_')) {
                  label = 'MAND'; labelColor = 'bg-red-500/20 text-red-400';
                } else if (reason.startsWith('score_')) {
                  label = 'WAIT'; labelColor = 'bg-yellow-500/20 text-yellow-400';
                } else {
                  label = 'HOLD'; labelColor = 'bg-muted/30 text-muted-foreground';
                }

                // J1 — per-symbol decision trace + two-state cached/fresh badge.
                // Guard: all fields may be undefined on older backends.
                const evalAge     = sig.last_evaluated_age_sec ?? null;
                const attemptAge  = sig.last_attempt_age_sec ?? null;
                const blockReason = sig.last_block_reason ?? null;
                const blockAge    = sig.last_block_age_sec ?? null;
                const cachedGreen = sig.cached_green === true;
                const engineReady = sig.engine_ready === true;
                const traceAvailable =
                  sig.cached_green !== undefined || sig.engine_ready !== undefined ||
                  sig.last_evaluated_age_sec !== undefined || sig.last_attempt_age_sec !== undefined ||
                  sig.last_block_reason !== undefined || sig.last_block_age_sec !== undefined;

                // Two-state badge — the key fix. Distinguish solid-green "ready"
                // from the amber "cached green · fresh re-check pending" flicker,
                // and surface the "no decision recorded" gap so it isn't invisible.
                let readyBadge: { text: string; cls: string; title: string } | null = null;
                if (traceAvailable) {
                  if (cachedGreen && engineReady) {
                    readyBadge = { text: 'ready', cls: 'bg-gain/20 text-gain',
                      title: 'Cached green AND fresh re-check passed — the engine will act on this symbol.' };
                  } else if (cachedGreen && attemptAge == null && blockReason == null) {
                    readyBadge = { text: 'no decision recorded', cls: 'bg-muted/30 text-muted-foreground border border-dashed border-border',
                      title: 'Buy-ready but no attempt and no block reason were recorded — a genuine gap, surfaced here so it is visible rather than hidden.' };
                  } else if (cachedGreen && !engineReady) {
                    readyBadge = { text: 'cached green · fresh re-check pending', cls: 'bg-amber-500/20 text-amber-400',
                      title: 'Signal cache is green but the fresh re-check has not confirmed yet (pending or failed). This flicker is expected — no buy fires until the fresh re-check passes.' };
                  }
                }

                // Compact decision trace sub-line (evaluated / attempt / block reason + ages).
                const traceParts: string[] = [];
                if (evalAge != null) traceParts.push(`evaluated ${humanizeAgeSec(evalAge)} ago`);
                traceParts.push(attemptAge != null ? `attempt ${humanizeAgeSec(attemptAge)} ago` : 'no attempt');
                if (blockReason) traceParts.push(blockAge != null ? `${blockReason} · ${humanizeAgeSec(blockAge)} ago` : blockReason);
                const traceText = traceParts.join(' · ');

                return (
                  <motion.div key={sig.symbol} layout layoutId={sig.symbol}
                    initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                    transition={{ layout: { duration: 0.5, ease: 'easeInOut' }, opacity: { duration: 0.2 } }}
                    className="border-b border-border/20 last:border-0">
                    <div className="grid items-center py-0.5" style={{gridTemplateColumns: cols}}>
                    <span className="text-[9px] font-mono font-semibold truncate">{sig.symbol?.replace('USDT','')}</span>
                    {signalRegistry.map(reg => {
                      const r = results[reg.id];
                      const fired = r?.fired ?? false;
                      const isVeto = reg.role === 'veto';
                      const dotColor = isVeto
                        ? (fired ? 'bg-loss' : 'bg-gain/60')
                        : (fired ? 'bg-gain' : 'bg-muted/40');
                      return (
                        <div key={reg.id} className="flex justify-center"
                          title={`${reg.id}: ${r?.raw_value ?? 'n/a'}`}>
                          <div className={`w-2 h-2 rounded-full ${dotColor}`} />
                        </div>
                      );
                    })}
                    <span className={`text-[7px] font-bold text-center px-0.5 rounded ${labelColor}`}
                      title={reason}>
                      {label}
                    </span>
                    <span className="text-[9px] font-mono text-muted-foreground text-right">
                      {sig.price ? (sig.price > 100
                        ? Number(sig.price).toLocaleString('en-US',{maximumFractionDigits:0})
                        : Number(sig.price).toFixed(4)) : ''}
                    </span>
                    </div>
                    {/* J1 — muted decision sub-line + labelled two-state badge. */}
                    {traceAvailable && (readyBadge || traceText) && (
                      <div className="flex items-center gap-1 pb-0.5 pl-0.5 overflow-hidden">
                        {readyBadge && (
                          <span className={`text-[7px] font-bold px-1 rounded shrink-0 ${readyBadge.cls}`}
                            title={readyBadge.title}>
                            {readyBadge.text}
                          </span>
                        )}
                        {traceText && (
                          <span className="text-[7px] text-muted-foreground truncate" title={traceText}>
                            {traceText}
                          </span>
                        )}
                      </div>
                    )}
                  </motion.div>
                );
              })}
              </AnimatePresence>
              {/* Placeholder rows — selected coins without a signal-cache entry.
                  Never silently drop a selected coin from the list. */}
              {missing.map(sym => {
                const st = resolveCoinStatus(sym, universeInputs);
                const badge = placeholderBadge(st.lifecycle, st.successor, st.backfillPct);
                return (
                  <div key={`ph-${sym}`}
                    className="grid items-center py-0.5 border-b border-border/20 last:border-0"
                    style={{gridTemplateColumns: cols}}>
                    <span className="text-[9px] font-mono font-semibold truncate text-muted-foreground">
                      {sym.replace('USDT','')}
                    </span>
                    <span
                      style={{gridColumn: '2 / -1'}}
                      className={`text-[8px] font-semibold px-1 py-px rounded justify-self-start ${badge.cls}`}
                      title={badge.title}>
                      {badge.text}
                    </span>
                  </div>
                );
              })}
              {/* Truthful count so the operator can confirm all N render. */}
              <div className="pt-1 text-[8px] text-muted-foreground">
                {signalsData.signals.length} live · {missing.length} awaiting · {selectedCoins.length} selected
              </div>
            </div>
          );
        })() : (isServerMode && signalsLoading && signalsData.signals.length === 0) ? (
          <div className="space-y-1">
            {[1,2,3,4,5].map(i => (
              <div key={i} className="h-5 bg-muted/20 rounded animate-pulse" />
            ))}
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
