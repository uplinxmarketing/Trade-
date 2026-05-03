import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Play, Square, Brain, TrendingUp, TrendingDown, Zap,
  RotateCcw, ChevronDown, ChevronUp, FlaskConical,
  Pencil, Check, X, BookOpen, Activity, Eye, EyeOff,
  ShoppingCart, Banknote, RefreshCw,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import { checkExits, TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { calcEMA, calcRSI, calcMACD, calcBollingerBands, calcSMA } from '@/lib/indicators';

// ── Simple 4-signal analyser (no API key, Binance public data only) ──────────
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
    const price   = closes[closes.length - 1];

    // Delegate to evaluateSignals — single source of truth for all 4 signals
    const sigs  = evaluateSignals(closes, volumes);
    const score = Object.values(sigs).filter(Boolean).length; // all 4 keys counted
    const rsi   = calcRSI(closes, 14);
    const volSma = calcSMA(volumes, 20);
    const curVol = volumes[volumes.length - 1] ?? 0;
    const volRat = volSma > 0 ? curVol / volSma : 1;

    // BUY only if bullish_count >= 3 — no bypass conditions
    const isBuy  = score >= 3;
    const isHold = rsi >= 72 || rsi < 24;

    const parts: string[] = [];
    parts.push(sigs.trend ? 'EMA↑' : 'EMA↓');
    parts.push(`RSI ${rsi.toFixed(0)}`);
    parts.push(sigs.macd ? 'MACD+' : 'MACD-');
    if (sigs.volume) parts.push(`Vol ${volRat.toFixed(1)}×`);

    return {
      symbol: sym, price, rsi,
      emaBullish: sigs.trend, rsiOk: sigs.rsi, macdPos: sigs.macd, volUp: sigs.volume,
      signal: isHold ? 'HOLD' : isBuy ? 'BUY' : 'HOLD',
      reason: parts.join(' · '),
    };
  } catch (e: any) {
    clearTimeout(timeout);
    return { symbol: sym, price: 0, rsi: 50, emaBullish: false, rsiOk: false, macdPos: false, volUp: false, signal: 'error', reason: e.message ?? 'Fetch failed' };
  }
}

// Sequential fetch with 300 ms gap to stay well inside Binance rate limits.
async function analyseAll(symbols: string[]): Promise<CoinSignal[]> {
  const results: CoinSignal[] = [];
  for (const sym of symbols) {
    results.push(await analyseCoin(sym));
    if (symbols.indexOf(sym) < symbols.length - 1) {
      await new Promise(r => setTimeout(r, 300));
    }
  }
  return results;
}

// ── Types ────────────────────────────────────────────────────────────────────
interface OpenPosition {
  symbol: string;
  quantity: number;
  avg_entry_price: number;
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
}

interface AITradingAgentProps {
  selectedCoins: string[];
  prices: LivePrices;
  binanceConnected?: boolean;
  onConnectBinance?: () => void;
  onCoinsChange?: (coins: string[]) => void;
  onStateChange?: (
    positions: {symbol:string;quantity:number;avg_entry_price:number}[],
    balance: number,
    initialBalance?: number,
    trades?: {side:'BUY'|'SELL';pnl:number|null;quantity:number;price:number}[]
  ) => void;
}

const INSTRUCTIONS_KEY  = 'ai_agent_instructions';
const RAILWAY_URL_KEY   = 'railway_bot_url';
const AGENT_CYCLE_MS    = 30_000;
const BEP_MULT          = 1 / Math.pow(1 - TAKER_FEE, 2);

// ── Component ────────────────────────────────────────────────────────────────
const AITradingAgent = ({ selectedCoins, prices, binanceConnected, onConnectBinance, onCoinsChange, onStateChange }: AITradingAgentProps) => {
  const [mode, setMode]           = useState<'test' | 'live'>('test');
  const [isRunning, setIsRunning] = useState(false);
  const [loading, setLoading]     = useState(false);
  const [balance, setBalance]     = useState(0);
  const [initialBalance, setInitialBalance] = useState(0);
  const [positions, setPositions] = useState<OpenPosition[]>([]);
  const [trades, setTrades]       = useState<TradeRow[]>([]);
  const [coinSignals, setCoinSignals] = useState<CoinSignal[]>([]);
  const [cycleCountdown, setCycleCountdown] = useState(0);
  const [agentStatus, setAgentStatus]       = useState('');
  const [scanning, setScanning]   = useState(false);
  const [showAll, setShowAll]     = useState(false);
  const [forcingBuy, setForcingBuy]   = useState<string | null>(null);
  const [forcingSell, setForcingSell] = useState<string | null>(null);
  const [instructions, setInstructions]   = useState(() => localStorage.getItem(INSTRUCTIONS_KEY) ?? '');
  const [editingInstr, setEditingInstr]   = useState(false);
  const [instrDraft, setInstrDraft]       = useState('');
  const [actLog, setActLog]       = useState<string[]>([]);
  const [showLog, setShowLog]     = useState(false);
  // Unified deployment: frontend and API are served from the same Railway URL.
  // railwayUrl defaults to '' (same origin) so all /api/* calls are relative.
  // Users can override via localStorage if they ever need to point at a different backend.
  const [railwayUrl, setRailwayUrl] = useState(() =>
    localStorage.getItem(RAILWAY_URL_KEY) ??
    (import.meta.env.VITE_API_URL as string | undefined) ??
    ''
  );
  const [showRailwayInput, setShowRailwayInput] = useState(false);
  const [railwayDraft, setRailwayDraft] = useState('');
  const [liveApiKey, setLiveApiKey]         = useState('');
  const [liveApiSecret, setLiveApiSecret]   = useState('');
  const [showLiveSecret, setShowLiveSecret] = useState(false);
  const [liveSetupLoading, setLiveSetupLoading] = useState(false);

  // Always server mode — the Python bot is always running on the same Railway instance.
  const isServerMode    = true;
  const isServerModeRef = useRef(true);
  useEffect(() => { isServerModeRef.current = true; }, []);

  const isRunningRef    = useRef(false);
  const processingRef   = useRef(false);
  const pendingSellsRef = useRef<Set<string>>(new Set());
  const balanceRef     = useRef(balance);
  const positionsRef   = useRef(positions);
  const cycleTimerRef  = useRef<ReturnType<typeof setTimeout> | null>(null);
  const countdownRef   = useRef<ReturnType<typeof setInterval> | null>(null);
  const stopLossRef    = useRef(1.5);
  const onStateChangeRef  = useRef(onStateChange);
  // Refs so the init effect can restart the scheduler after page refresh
  // without creating a circular dependency (scheduler defined later in file)
  const runCycleRef    = useRef<(() => Promise<void>) | null>(null);
  const scheduleNextRef = useRef<(() => void) | null>(null);

  useEffect(() => { isRunningRef.current = isRunning; },   [isRunning]);
  useEffect(() => { balanceRef.current   = balance; },     [balance]);
  useEffect(() => { positionsRef.current = positions; },   [positions]);
  useEffect(() => { localStorage.setItem(INSTRUCTIONS_KEY, instructions); }, [instructions]);
  useEffect(() => { onStateChangeRef.current = onStateChange; }, [onStateChange]);

  const addLog = useCallback((msg: string) => {
    const ts = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    setActLog(prev => [`[${ts}] ${msg}`, ...prev].slice(0, 60));
  }, []);

  // ── Load state from DB ───────────────────────────────────────────────────
  const loadData = useCallback(async () => {
    // Server mode: all data comes from pollRailway (Railway SQLite), not Supabase.
    // Letting loadData run would race with pollRailway and overwrite Railway data with empty Supabase rows.
    if (isServerMode) return;

    const [trdRes, posRes, cfgRes] = await Promise.all([
      supabase.from('bot_trade_history').select('*').eq('user_session', SESSION).order('created_at', { ascending: false }).limit(100),
      supabase.from('paper_portfolio').select('*').eq('user_session', SESSION).gt('quantity', 0),
      supabase.from('bot_config').select('*').eq('user_session', SESSION).maybeSingle(),
    ]);

    if (trdRes.data) {
      const trades = trdRes.data as TradeRow[];
      setTrades(trades);
      // On page load (actLog is empty), reconstruct activity log from persisted trades
      setActLog(prev => {
        if (prev.length > 0) return prev;
        return trades.slice(0, 30).map(t => {
          const ts = new Date(t.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
          if (t.side === 'BUY') return `[${ts}]   BUY  ${t.symbol} @ ${Number(t.price).toFixed(4)} USDT`;
          const pnlStr = t.pnl != null ? ` · P&L: ${t.pnl >= 0 ? '+' : ''}${t.pnl.toFixed(4)} USDT` : '';
          return `[${ts}] SELL ${t.symbol} @ ${Number(t.price).toFixed(4)} USDT${pnlStr}`;
        });
      });
    }

    if (posRes.data)  setPositions(posRes.data as OpenPosition[]);

    // Use same fallback chain as wallet: current_balance → initial_balance → localStorage startingBalance
    const startBal = getPaperCfg().startingBalance ?? 1000;
    let display = startBal;
    if (cfgRes.data) {
      const bal     = Number(cfgRes.data.current_balance);
      const initBal = Number(cfgRes.data.initial_balance ?? 0);
      display = bal > 0 ? bal : initBal > 0 ? initBal : startBal;
      setBalance(display); balanceRef.current = display;
      setInitialBalance(initBal > 0 ? initBal : startBal);
      stopLossRef.current = Number(cfgRes.data.stop_loss_percent ?? 1.5);
      const running = Boolean(cfgRes.data.is_running);
      isRunningRef.current = running;
      setIsRunning(running);
    } else {
      setBalance(startBal); balanceRef.current = startBal;
      setInitialBalance(startBal);
    }

    // Notify parent with fresh state for instant wallet sync
    if (posRes.data) {
      const tradePayload = trdRes.data?.map(t => ({ side: t.side, pnl: t.pnl, quantity: t.quantity, price: t.price }));
      onStateChangeRef.current?.(posRes.data as OpenPosition[], display, initialBalance || display, tradePayload);
    }
  }, []);

  useEffect(() => {
    loadData().then(() => {
      // After refresh: resume the JS scheduler only in local mode.
      // Server mode restart is handled by the polling useEffect above.
      if (isRunningRef.current && !isServerModeRef.current && scheduleNextRef.current) {
        addLog('=== Resumed after page refresh — restarting cycle ===');
        runCycleRef.current?.().then(() => scheduleNextRef.current?.());
      }
    });
    const ch = supabase.channel('ata-rt')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_config' }, loadData)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadData]); // eslint-disable-line

  // ── Exit checker on every WS price tick ─────────────────────────────────
  const pricesRef = useRef(prices);
  useEffect(() => { pricesRef.current = prices; }, [prices]);

  useEffect(() => {
    // Exit checker only runs in local mode — Railway handles exits server-side
    if (isServerMode) return;
    if (!isRunning || processingRef.current) return;
    if (!Object.keys(prices).length) return;
    processingRef.current = true;
    checkExits(prices, supabase, stopLossRef.current, 0, ({ symbol, price, usdtReceived, pnl }) => {
      addLog(`SELL ${symbol} @ ${price.toFixed(4)} USDT · ${usdtReceived.toFixed(4)} USDT · P&L: ${pnl >= 0 ? '+' : ''}${pnl.toFixed(4)} USDT`);
    })
      .then(n => { if (n > 0) { loadData(); toast.success(`Closed ${n} position(s)`, { duration: 2500 }); } })
      .catch(() => {})
      .finally(() => { processingRef.current = false; });
  }, [prices, isServerMode]); // eslint-disable-line

  // ── Core cycle ───────────────────────────────────────────────────────────
  const runCycle = useCallback(async () => {
    if (!isRunningRef.current) return;
    setScanning(true);
    setAgentStatus('Scanning market…');
    addLog('── Cycle started');

    try {
      const [posRes, cfgRes] = await Promise.all([
        supabase.from('paper_portfolio').select('*').eq('user_session', SESSION).gt('quantity', 0),
        supabase.from('bot_config').select('current_balance').eq('user_session', SESSION).maybeSingle(),
      ]);
      const currentPositions: OpenPosition[] = (posRes.data ?? []) as OpenPosition[];
      let runBal = Number(cfgRes.data?.current_balance ?? balanceRef.current);
      const heldSet = new Set(currentPositions.map(p => p.symbol));

      // — Analyse all coins using simple 4-signal method —
      const signals = await analyseAll(selectedCoins);
      setCoinSignals(signals);

      const buySigs  = signals.filter(s => s.signal === 'BUY');
      const holdSigs = signals.filter(s => s.signal === 'HOLD');
      addLog(`Signals: ${buySigs.length} BUY · ${holdSigs.length} HOLD · ${signals.filter(s=>s.signal==='error').length} err`);
      signals.forEach(s => {
        const count = [s.emaBullish, s.rsiOk, s.macdPos, s.volUp].filter(Boolean).length;
        addLog(`[SIGNALS] ${s.symbol}: trend=${s.emaBullish} rsi=${s.rsiOk} macd=${s.macdPos} vol=${s.volUp} count=${count}/4`);
      });

      // — Execute BUYs — dynamic heldSet check so every iteration sees the latest count
      const newlyBought: OpenPosition[] = [];
      for (const sig of buySigs) {
        if (heldSet.size >= MAX_POSITIONS) { addLog(`  Max ${MAX_POSITIONS} positions held — exits handled by price ticker`); break; }
        if (runBal < MIN_USDT) break;
        if (heldSet.has(sig.symbol)) continue;
        const wsPrice = parseFloat(pricesRef.current[sig.symbol]?.price || '0');
        const price   = wsPrice > 0 ? wsPrice : sig.price;
        if (!price) continue;

        const alloc  = getAllocation(runBal, sig.symbol);
        const needed = alloc * 1.002;
        if (runBal < needed) {
          addLog(`[SKIP] ${sig.symbol}: Insufficient USDT — have ${runBal.toFixed(2)}, need ${needed.toFixed(2)}`);
          continue;
        }
        if (alloc < MIN_USDT) { addLog(`  SKIP ${sig.symbol} — alloc ${alloc.toFixed(2)} USDT too low`); continue; }

        const fee = alloc * TAKER_FEE;
        const qty = (alloc - fee) / price;

        const [tradeRes, portRes] = await Promise.all([
          supabase.from('bot_trade_history').insert({
            user_session: SESSION, symbol: sig.symbol,
            side: 'BUY', price, quantity: qty, pnl: null,
            reason: `[AI Paper] ${sig.reason} · ${alloc.toFixed(2)} USDT @ ${price.toFixed(4)} USDT`,
          }),
          supabase.from('paper_portfolio').upsert({
            user_session: SESSION, symbol: sig.symbol,
            quantity: qty, avg_entry_price: price,
            updated_at: new Date().toISOString(),
          }, { onConflict: 'user_session,symbol' }),
        ]);
        if (tradeRes.error) addLog(`[DB ERR] trade_history insert: ${tradeRes.error.message}`);
        if (portRes.error)  addLog(`[DB ERR] paper_portfolio upsert: ${portRes.error.message}`);

        runBal -= alloc;
        newlyBought.push({ symbol: sig.symbol, quantity: qty, avg_entry_price: price });
        heldSet.add(sig.symbol);
        addLog(`  BUY  ${sig.symbol} @ ${price.toFixed(4)} USDT · ${alloc.toFixed(2)} USDT`);
        toast.info(`AI Paper BUY: ${sig.symbol.replace('USDT','')}`, { description: `${alloc.toFixed(2)} USDT @ ${price.toFixed(4)} USDT`, duration: 3000 });
      }

      await supabase.from('bot_config').update({
        current_balance: Math.round(runBal * 10000) / 10000,
        updated_at: new Date().toISOString(),
      }).eq('user_session', SESSION);

      // Optimistic wallet update — no need to wait for loadData
      if (newlyBought.length > 0) {
        onStateChangeRef.current?.([...currentPositions, ...newlyBought], runBal, initialBalance);
      }

      setAgentStatus(`Done · ${new Date().toLocaleTimeString()}`);
      await loadData();
    } catch (e: any) {
      addLog(`ERROR: ${e.message}`);
      setAgentStatus(`Error: ${e.message?.slice(0, 50)}`);
    } finally {
      setScanning(false);
    }
  }, [selectedCoins, loadData, addLog]);

  // ── Scheduler ────────────────────────────────────────────────────────────
  const scheduleNext = useCallback(() => {
    if (cycleTimerRef.current)  clearTimeout(cycleTimerRef.current);
    if (countdownRef.current)   clearInterval(countdownRef.current);
    if (!isRunningRef.current)  return;

    setCycleCountdown(AGENT_CYCLE_MS / 1000);
    countdownRef.current = setInterval(() => {
      setCycleCountdown(p => { if (p <= 1) { clearInterval(countdownRef.current!); return 0; } return p - 1; });
    }, 1000);
    cycleTimerRef.current = setTimeout(() => runCycle().then(scheduleNext), AGENT_CYCLE_MS);
  }, [runCycle]);

  // Keep refs in sync so the init effect can call these after page refresh
  useEffect(() => { runCycleRef.current    = runCycle;    }, [runCycle]);
  useEffect(() => { scheduleNextRef.current = scheduleNext; }, [scheduleNext]);

  // ── Coin signal scanner (server mode) ────────────────────────────────────
  // In server mode runCycle() never runs, so we scan independently for display.
  // Trading decisions are still made server-side by the Python bot.
  useEffect(() => {
    if (!selectedCoins.length) return;
    let cancelled = false;
    const scan = async () => {
      if (cancelled) return;
      setScanning(true);
      // Show loading cards immediately
      const loadingCards: CoinSignal[] = selectedCoins.map(sym => ({
        symbol: sym,
        price: parseFloat(pricesRef.current[sym]?.price || '0'),
        rsi: 50, emaBullish: false, rsiOk: false, macdPos: false, volUp: false,
        signal: 'loading' as const, reason: 'Scanning…',
      }));
      setCoinSignals(loadingCards);
      const results: CoinSignal[] = [...loadingCards];
      for (let i = 0; i < selectedCoins.length; i++) {
        if (cancelled) return;
        results[i] = await analyseCoin(selectedCoins[i]);
        if (!cancelled) setCoinSignals([...results]);
        if (i < selectedCoins.length - 1) await new Promise(r => setTimeout(r, 300));
      }
      if (!cancelled) setScanning(false);
    };
    scan();
    const id = setInterval(scan, AGENT_CYCLE_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, [selectedCoins]); // eslint-disable-line

  // ── Sync selectedCoins to Railway so Python bot watches the right coins ──
  useEffect(() => {
    if (!isServerMode || !selectedCoins.length) return;
    fetch(`${railwayUrl}/api/coins`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ coins: selectedCoins }),
    }).catch(() => {});
  }, [selectedCoins, railwayUrl]); // eslint-disable-line

  // ── Railway server-mode poller ─────────────────────────────────────────────
  // When a Railway URL is configured the JS trading loop is disabled.
  // Instead we poll the Railway bot's REST API every 30 s and mirror its state
  // into the same React state variables so the UI shows live Railway data.
  const serverPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const pollRailway = useCallback(async () => {
    // railwayUrl='' means same-origin — always poll in unified Railway deployment
    try {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 8000);
      const [sRes, pRes, tRes, aRes] = await Promise.all([
        fetch(`${railwayUrl}/api/status`,    { signal: ctrl.signal }),
        fetch(`${railwayUrl}/api/positions`, { signal: ctrl.signal }),
        fetch(`${railwayUrl}/api/trades`,    { signal: ctrl.signal }),
        fetch(`${railwayUrl}/api/activity`,  { signal: ctrl.signal }),
      ]);
      clearTimeout(timer);

      if (sRes.ok) {
        const s = await sRes.json();
        const running = Boolean(s.running);
        isRunningRef.current = running;
        setIsRunning(running);
        const bal = Number(s.balance_usdt ?? 0);
        setBalance(bal); balanceRef.current = bal;
        setInitialBalance(Number(s.initial_balance ?? bal));
        setAgentStatus(`Railway · ${s.mode?.toUpperCase() ?? 'PAPER'} · ${new Date().toLocaleTimeString()}`);
      }

      if (pRes.ok) {
        const p = await pRes.json();
        const mapped: OpenPosition[] = (p.positions ?? []).map((pos: any) => ({
          symbol:          pos.symbol,
          quantity:        Number(pos.quantity),
          avg_entry_price: Number(pos.entry_price ?? pos.avg_entry_price ?? 0),
        }));
        setPositions(mapped);
        positionsRef.current = mapped;
      }

      if (tRes.ok) {
        const t = await tRes.json();
        const railwayTrades: TradeRow[] = [];
        for (const tr of (t.trades ?? [])) {
          if (tr.entry_price && tr.timestamp_buy) {
            railwayTrades.push({
              id: `rw-buy-${tr.id}`, created_at: tr.timestamp_buy,
              symbol: tr.coin, side: 'BUY' as const,
              price: Number(tr.entry_price), quantity: Number(tr.quantity),
              pnl: null, reason: null,
            });
          }
          if (tr.exit_price && tr.timestamp_sell) {
            railwayTrades.push({
              id: `rw-sell-${tr.id}`, created_at: tr.timestamp_sell,
              symbol: tr.coin, side: 'SELL' as const,
              price: Number(tr.exit_price), quantity: Number(tr.quantity),
              pnl: Number(tr.net_profit ?? 0), reason: null,
            });
          }
        }
        const sorted = railwayTrades.sort((a, b) =>
          new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
        if (sorted.length > 0) setTrades(sorted);
        // Keep parent wallet in sync with Railway state
        const tradePayload = sorted.map(t => ({ side: t.side, pnl: t.pnl, quantity: t.quantity, price: t.price }));
        onStateChangeRef.current?.(positionsRef.current, balanceRef.current, initialBalance, tradePayload);
      }

      // Activity log from Python bot's SQLite activity_log table
      if (aRes.ok) {
        const a = await aRes.json();
        const entries: string[] = (a.entries ?? []).map((e: any) => {
          const ts = new Date(e.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
          const icon = e.level === 'warn' ? '⚠ ' : e.level === 'error' ? '✕ ' : '';
          return `[${ts}] ${icon}${e.message}`;
        });
        if (entries.length > 0) setActLog(entries);
      }
    } catch (e: any) {
      if (e.name !== 'AbortError') addLog(`[Railway] poll failed: ${e.message}`);
    }
  }, [railwayUrl, addLog]);

  // Start/stop the polling interval whenever server mode changes
  useEffect(() => {
    if (!isServerMode) {
      if (serverPollRef.current) { clearInterval(serverPollRef.current); serverPollRef.current = null; }
      return;
    }
    // Immediate poll then every 30 s
    pollRailway();
    serverPollRef.current = setInterval(pollRailway, 30_000);
    return () => { if (serverPollRef.current) { clearInterval(serverPollRef.current); serverPollRef.current = null; } };
  }, [isServerMode, pollRailway]);

  // ── Mode change (paper ↔ live) ───────────────────────────────────────────
  const handleModeChange = useCallback(async (newMode: 'test' | 'live') => {
    if (isRunning) { toast.error('Stop the bot first to switch modes'); return; }
    if (isServerMode) {
      if (newMode === 'live') {
        setMode('live'); // reveals the API key form below — no toast
      } else {
        try {
          const res = await fetch(`${railwayUrl}/api/mode`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode: 'paper' }),
          });
          const data = await res.json();
          if (data.ok) { setMode('test'); toast.info('Railway bot set to Paper mode'); }
          else toast.error(data.warning ?? 'Mode switch failed');
        } catch { toast.error('Could not reach Railway to switch mode'); }
      }
    } else {
      if (newMode === 'live' && !binanceConnected) { toast.error('Connect Binance API first'); onConnectBinance?.(); return; }
      setMode(newMode);
    }
  }, [isRunning, isServerMode, railwayUrl, binanceConnected, onConnectBinance]);

  // ── Switch Railway bot to live mode with API keys ────────────────────────
  const submitLiveMode = useCallback(async () => {
    if (!liveApiKey.trim() || !liveApiSecret.trim()) return;
    setLiveSetupLoading(true);
    const toastId = toast.loading('Switching Railway bot to Live mode…');
    try {
      const res = await fetch(`${railwayUrl}/api/mode`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode: 'live', api_key: liveApiKey.trim(), api_secret: liveApiSecret.trim() }),
      });
      const data = await res.json();
      if (!data.ok) throw new Error(data.error ?? 'Mode switch failed');

      toast.loading('Railway restarting with Live mode…', { id: toastId });
      addLog('=== Switching to LIVE mode — Railway restarting ===');

      // Poll /api/ping until the server comes back after restart
      let attempts = 0;
      const poll = setInterval(async () => {
        attempts++;
        if (attempts > 60) {
          clearInterval(poll);
          toast.error('Restart timed out — check Railway logs', { id: toastId });
          setLiveSetupLoading(false);
          return;
        }
        try {
          const ping = await fetch(`${railwayUrl}/api/ping`, { cache: 'no-store' });
          if (ping.ok) {
            clearInterval(poll);
            toast.success('Live mode active — bot restarted!', { id: toastId });
            setLiveApiKey(''); setLiveApiSecret('');
            addLog('=== Railway bot is now in LIVE mode ===');
            await pollRailway();
            setLiveSetupLoading(false);
          }
        } catch { /* still restarting */ }
      }, 1000);
    } catch (e: any) {
      toast.error(`Live mode failed: ${e.message}`, { id: toastId });
      setLiveSetupLoading(false);
    }
  }, [liveApiKey, liveApiSecret, railwayUrl, addLog, pollRailway]);

  // ── Start / Stop ─────────────────────────────────────────────────────────
  const toggleBot = async () => {
    setLoading(true);
    try {
      // ── Server mode: delegate to Railway ──
      if (isServerMode) {
        const endpoint = isRunning ? '/api/agent/stop' : '/api/agent/start';
        let res: Response;
        try {
          res = await fetch(`${railwayUrl}${endpoint}`, { method: 'POST' });
        } catch (networkErr: any) {
          const msg = `Cannot reach Railway (${networkErr.message ?? 'network error'})`;
          toast.error(msg);
          addLog(`[Railway ERROR] ${msg}`);
          return;
        }
        if (!res.ok) {
          const msg = `Railway returned HTTP ${res.status}: ${res.statusText}`;
          toast.error(msg);
          addLog(`[Railway ERROR] ${msg}`);
          return;
        }
        let data: any;
        try {
          data = await res.json();
        } catch {
          const msg = 'Railway response was not valid JSON';
          toast.error(msg);
          addLog(`[Railway ERROR] ${msg}`);
          return;
        }
        if (data.ok === false) {
          const msg = data.error ?? 'Railway call failed';
          toast.error(`Railway: ${msg}`);
          addLog(`[Railway ERROR] ${msg}`);
          return;
        }
        addLog(isRunning ? '=== Railway bot STOPPED ===' : '=== Railway bot STARTED ===');
        toast[isRunning ? 'info' : 'success'](isRunning ? 'Railway bot paused' : 'Railway bot started', {
          description: 'Runs 24/7 on Railway — this browser tab can be closed.',
        });
        await pollRailway();
        return;
      }

      // ── Local paper mode: JS trading loop ──
      if (mode === 'live' && !binanceConnected) { toast.error('Connect Binance API first'); onConnectBinance?.(); return; }
      if (!isRunning) {
        const startBal = getPaperCfg().startingBalance ?? 1000;
        const existingBal  = balanceRef.current  > 0 ? balanceRef.current  : startBal;
        const existingInit = initialBalance       > 0 ? initialBalance      : startBal;
        await supabase.from('bot_config').upsert({
          user_session: SESSION,
          selected_coins: selectedCoins,
          mode, is_running: true,
          current_balance: existingBal,
          initial_balance: existingInit,
          stop_loss_percent: 1.5,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session' });
        isRunningRef.current = true;
        setIsRunning(true);
        setBalance(existingBal); balanceRef.current = existingBal;
        setInitialBalance(existingInit);
        addLog(`=== Agent STARTED · ${existingBal.toFixed(2)} USDT · ${mode.toUpperCase()} ===`);
        toast.success('AI Agent started — PAPER mode', { description: `${existingBal.toLocaleString()} USDT · ${selectedCoins.length} coins · every 30s` });
        runCycle().then(scheduleNext);
      } else {
        if (cycleTimerRef.current) clearTimeout(cycleTimerRef.current);
        if (countdownRef.current)  clearInterval(countdownRef.current);
        await supabase.from('bot_config').update({ is_running: false, updated_at: new Date().toISOString() }).eq('user_session', SESSION);
        isRunningRef.current = false;
        setIsRunning(false); setCycleCountdown(0);
        addLog('=== Agent STOPPED ===');
        toast.info('AI Agent stopped');
      }
    } finally { setLoading(false); }
  };

  // ── Force BUY ────────────────────────────────────────────────────────────
  const forceBuy = useCallback(async (sym: string) => {
    const wsPrice = parseFloat(pricesRef.current[sym]?.price || '0');
    if (!wsPrice) { toast.error('No live price yet'); return; }
    setForcingBuy(sym);
    try {
      // Server mode: delegate to Railway's force-buy endpoint
      if (isServerModeRef.current) {
        const res  = await fetch(`${railwayUrl}/api/force-buy/${sym}`, { method: 'POST' });
        const data = await res.json();
        if (!data.ok) throw new Error(data.error ?? 'Force buy failed');
        addLog(`FORCE BUY ${sym} via Railway @ ${Number(data.price).toFixed(4)} USDT · ${Number(data.budget).toFixed(2)} USDT`);
        toast.success(`Force BUY: ${sym.replace('USDT','')} @ ${Number(data.price).toFixed(4)} USDT`);
        await pollRailway();
        return;
      }
      // Local paper mode: write directly to Supabase
      const bal   = balanceRef.current;
      const alloc = getAllocation(bal, sym);
      if (alloc < MIN_USDT) { toast.error(`Balance too low (${bal.toFixed(2)} USDT)`); return; }
      const fee = alloc * TAKER_FEE;
      const qty = (alloc - fee) / wsPrice;
      const newBal = bal - alloc;
      const { data: existingPos } = await supabase.from('paper_portfolio')
        .select('quantity,avg_entry_price').eq('user_session', SESSION).eq('symbol', sym).maybeSingle();
      const portWrite = existingPos && Number(existingPos.quantity) > 0
        ? supabase.from('paper_portfolio').update({
            quantity: Number(existingPos.quantity) + qty,
            avg_entry_price: (Number(existingPos.quantity) * Number(existingPos.avg_entry_price) + qty * wsPrice)
              / (Number(existingPos.quantity) + qty),
            updated_at: new Date().toISOString(),
          }).eq('user_session', SESSION).eq('symbol', sym)
        : supabase.from('paper_portfolio').upsert({
            user_session: SESSION, symbol: sym, quantity: qty, avg_entry_price: wsPrice,
            updated_at: new Date().toISOString(),
          }, { onConflict: 'user_session,symbol' });
      await Promise.all([
        supabase.from('bot_trade_history').insert({
          user_session: SESSION, symbol: sym, side: 'BUY',
          price: wsPrice, quantity: qty, pnl: null,
          reason: `[Force BUY] manual test · ${alloc.toFixed(2)} USDT @ ${wsPrice.toFixed(4)} USDT`,
        }),
        portWrite,
        supabase.from('bot_config').update({ current_balance: newBal }).eq('user_session', SESSION),
      ]);
      addLog(`FORCE BUY ${sym} @ ${wsPrice.toFixed(4)} USDT · ${alloc.toFixed(2)} USDT`);
      toast.success(`Force BUY: ${sym.replace('USDT','')} @ ${wsPrice.toFixed(4)} USDT`);
      await loadData();
    } finally { setForcingBuy(null); }
  }, [addLog, loadData, railwayUrl, pollRailway]);

  // ── Force SELL ───────────────────────────────────────────────────────────
  const forceSell = useCallback(async (pos: OpenPosition) => {
    if (pendingSellsRef.current.has(pos.symbol)) return;
    pendingSellsRef.current.add(pos.symbol);
    setForcingSell(pos.symbol);

    // Server mode: delegate to Railway's force-sell endpoint
    if (isServerModeRef.current) {
      try {
        const res = await fetch(`${railwayUrl}/api/force-sell/${pos.symbol}`, { method: 'POST' });
        const data = await res.json();
        if (!data.ok) throw new Error(data.error ?? 'Force sell failed');
        addLog(`FORCE SELL ${pos.symbol} via Railway @ ${Number(data.price).toFixed(4)} USDT`);
        toast.success(`Force SELL sent: ${pos.symbol.replace('USDT','')}`);
        await pollRailway();
      } catch (e) {
        toast.error(`Force sell failed: ${e instanceof Error ? e.message : String(e)}`);
      } finally {
        setForcingSell(null);
        pendingSellsRef.current.delete(pos.symbol);
      }
      return;
    }

    const wsPrice = parseFloat(pricesRef.current[pos.symbol]?.price || '0');
    if (!wsPrice) { toast.error('No live price yet'); setForcingSell(null); pendingSellsRef.current.delete(pos.symbol); return; }
    try {
      const proceeds = pos.quantity * wsPrice * (1 - TAKER_FEE);
      const cost     = pos.quantity * pos.avg_entry_price / (1 - TAKER_FEE);
      const pnl      = Math.round((proceeds - cost) * 10000) / 10000;
      const newBal   = balanceRef.current + proceeds;
      await Promise.all([
        supabase.from('bot_trade_history').insert({
          user_session: SESSION, symbol: pos.symbol, side: 'SELL',
          price: wsPrice, quantity: pos.quantity, pnl,
          reason: `[Force SELL] manual · @ ${wsPrice.toFixed(4)} USDT · pnl ${pnl>=0?'+':''}${pnl.toFixed(4)} USDT`,
        }),
        supabase.from('paper_portfolio').delete().eq('user_session', SESSION).eq('symbol', pos.symbol),
        supabase.from('bot_config').update({ current_balance: newBal }).eq('user_session', SESSION),
      ]);
      addLog(`FORCE SELL ${pos.symbol} @ ${wsPrice.toFixed(4)} USDT · P&L ${pnl>=0?'+':''}${pnl.toFixed(4)} USDT`);
      toast[pnl >= 0 ? 'success' : 'error'](`Force SELL: ${pos.symbol.replace('USDT','')} ${pnl>=0?'+':''}${pnl.toFixed(4)} USDT`);
      await loadData();
    } finally { setForcingSell(null); pendingSellsRef.current.delete(pos.symbol); }
  }, [addLog, loadData, railwayUrl]);

  // ── Reset ────────────────────────────────────────────────────────────────
  const resetBot = async () => {
    if (!confirm('Reset all paper trades and restore budget?')) return;
    if (isServerMode) {
      try {
        setLoading(true);
        const res  = await fetch(`${railwayUrl}/api/reset`, { method: 'POST' });
        const data = await res.json();
        if (!data.ok) throw new Error(data.error ?? 'Reset failed');
        setTrades([]); setPositions([]); setActLog([]);
        addLog('=== Railway wallet reset ===');
        toast.success(`Railway reset · ${data.balance_usdt?.toLocaleString()} USDT restored`);
        await pollRailway();
      } catch (e: any) {
        toast.error(`Reset failed: ${e.message}`);
      } finally { setLoading(false); }
      return;
    }
    if (cycleTimerRef.current) clearTimeout(cycleTimerRef.current);
    if (countdownRef.current)  clearInterval(countdownRef.current);
    isRunningRef.current = false;
    const startBal = getPaperCfg().startingBalance ?? 1000;
    await Promise.all([
      supabase.from('bot_trade_history').delete().eq('user_session', SESSION),
      supabase.from('paper_portfolio').delete().eq('user_session', SESSION),
      supabase.from('bot_config').update({
        current_balance: startBal, initial_balance: startBal,
        is_running: false, updated_at: new Date().toISOString(),
      }).eq('user_session', SESSION),
    ]);
    setTrades([]); setPositions([]); setBalance(startBal); setInitialBalance(startBal);
    setIsRunning(false); setCycleCountdown(0); setActLog([]);
    toast.success(`Reset · ${startBal.toLocaleString()} USDT restored`);
  };

  // ── Computed stats ───────────────────────────────────────────────────────
  const sellTrades  = trades.filter(t => t.side === 'SELL' && t.pnl !== null);
  const wins        = sellTrades.filter(t => (t.pnl ?? 0) > 0).length;
  const totalPnl    = sellTrades.reduce((s, t) => s + (t.pnl ?? 0), 0);
  const winRate     = sellTrades.length ? Math.round((wins / sellTrades.length) * 100) : 0;
  const pnlColor    = totalPnl >= 0 ? 'text-gain' : 'text-loss';
  const pnlPct      = initialBalance > 0 ? ((totalPnl / initialBalance) * 100).toFixed(2) : '0.00';
  const displayedTrades = showAll ? trades : trades.slice(0, 12);

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">

      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 flex-wrap">
          <div className={`w-2.5 h-2.5 rounded-full ${isRunning ? 'animate-pulse bg-gain' : 'bg-muted-foreground'}`} />
          <Brain className="w-3.5 h-3.5 text-accent" />
          <h3 className="text-sm font-semibold">AI Trading Agent</h3>
          <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded ${mode === 'live' ? 'bg-loss/20 text-loss' : 'bg-accent/20 text-accent'}`}>
            {mode === 'live' ? 'LIVE' : 'PAPER TEST'}
          </span>
          {isServerMode
            ? isRunning
              ? <span className="text-[9px] text-gain font-mono flex items-center gap-1">
                  <span className="animate-pulse">●</span>Railway 24/7 · {agentStatus ? agentStatus.split('·').slice(-1)[0]?.trim() : 'live'}
                </span>
              : <span className="text-[9px] text-muted-foreground font-mono">Railway · paused</span>
            : scanning
              ? <span className="text-[9px] text-accent font-mono flex items-center gap-1"><RefreshCw className="w-2.5 h-2.5 animate-spin" />Checking signals…</span>
              : isRunning && cycleCountdown > 0
                ? <span className="text-[9px] text-muted-foreground font-mono flex items-center gap-1.5">
                    <span className="text-gain">●</span>exits live
                    <span className="opacity-40">·</span>buy scan in {cycleCountdown}s
                  </span>
                : null
          }
        </div>
        <div className="flex items-center gap-1.5">
          {isRunning && (
            <Button size="sm" variant="outline" className="h-6 text-[10px] px-2"
              onClick={() => isServerMode ? pollRailway() : runCycle()}
              disabled={scanning}
              title={isServerMode ? 'Refresh Railway state' : 'Run cycle now'}>
              <Zap className="w-3 h-3 mr-0.5" />{isServerMode ? 'Sync' : 'Now'}
            </Button>
          )}
          <button onClick={resetBot} disabled={loading} className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground disabled:opacity-40" title="Reset">
            <RotateCcw className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* ── Mode toggle ── */}
      <div className="grid grid-cols-2 gap-1 bg-muted/30 rounded-md p-0.5">
        {(['test', 'live'] as const).map(m => (
          <button key={m} onClick={() => handleModeChange(m)}
            className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold transition-colors disabled:opacity-60
              ${mode === m ? (m === 'live' ? 'bg-loss/80 text-white' : 'bg-accent text-accent-foreground') : 'text-muted-foreground hover:text-foreground'}`}>
            {m === 'test'
              ? <><FlaskConical className="w-3.5 h-3.5" />TEST · Paper</>
              : <><Zap className="w-3.5 h-3.5" />LIVE · Real{!isServerMode && !binanceConnected && <span className="text-[9px] px-1 bg-warn/20 text-warn rounded ml-1">API needed</span>}</>}
          </button>
        ))}
      </div>

      {mode === 'live' && isServerMode && (
        <div className="bg-loss/5 border border-loss/25 rounded-md px-3 py-3 space-y-2.5">
          <div className="flex items-center justify-between">
            <span className="text-xs font-semibold text-loss flex items-center gap-1.5">
              <Zap className="w-3.5 h-3.5" />Live Trading — Real Money
            </span>
            <button onClick={() => handleModeChange('test')} className="text-[10px] text-muted-foreground hover:text-foreground">
              ← Back to Paper
            </button>
          </div>
          <p className="text-[10px] text-muted-foreground leading-relaxed">
            Enter your Binance API credentials. They are sent to the Railway bot and stored in its environment — the bot will restart and connect to your real account.
          </p>
          <div className="space-y-1.5">
            <input
              type="text"
              value={liveApiKey}
              onChange={e => setLiveApiKey(e.target.value)}
              placeholder="Binance API Key"
              disabled={liveSetupLoading}
              className="w-full bg-muted/40 border border-border rounded px-2 py-1.5 text-xs font-mono focus:outline-none focus:border-loss/60 disabled:opacity-50"
            />
            <div className="relative">
              <input
                type={showLiveSecret ? 'text' : 'password'}
                value={liveApiSecret}
                onChange={e => setLiveApiSecret(e.target.value)}
                placeholder="Binance API Secret"
                disabled={liveSetupLoading}
                className="w-full bg-muted/40 border border-border rounded px-2 py-1.5 pr-8 text-xs font-mono focus:outline-none focus:border-loss/60 disabled:opacity-50"
              />
              <button onClick={() => setShowLiveSecret(p => !p)}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground">
                {showLiveSecret ? <EyeOff className="w-3 h-3" /> : <Eye className="w-3 h-3" />}
              </button>
            </div>
          </div>
          <Button
            onClick={submitLiveMode}
            disabled={!liveApiKey.trim() || !liveApiSecret.trim() || liveSetupLoading}
            className="w-full bg-loss/80 hover:bg-loss text-white text-xs py-2 h-auto"
          >
            {liveSetupLoading
              ? <><span className="animate-spin mr-1.5">⟳</span>Restarting Railway in Live mode…</>
              : '⚡ Enable Live Trading on Railway'}
          </Button>
          <p className="text-[9px] text-muted-foreground">Bot will be offline ~30s during restart. Make sure your API key has Spot Trading enabled.</p>
        </div>
      )}

      {mode === 'live' && !isServerMode && (
        <div className={`rounded-md px-3 py-2 text-xs ${binanceConnected ? 'bg-loss/10 border border-loss/30 text-loss' : 'bg-warn/10 border border-warn/30 text-warn'}`}>
          {binanceConnected
            ? '⚠️ LIVE MODE — real USDT will be used.'
            : <span>Binance API not connected. <button onClick={onConnectBinance} className="underline font-semibold">Connect now →</button></span>}
        </div>
      )}

      {/* ── Railway bot URL ── */}
      <div className="bg-muted/20 border border-border rounded-md px-3 py-2.5 space-y-1.5">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Activity className="w-3.5 h-3.5 text-accent" />
            <span className="text-xs font-semibold text-accent">Railway Bot</span>
            {isServerMode && <span className="text-[9px] px-1.5 py-0.5 rounded bg-gain/20 text-gain font-bold">CONNECTED</span>}
          </div>
          {!showRailwayInput ? (
            <button onClick={() => { setRailwayDraft(railwayUrl); setShowRailwayInput(true); }}
              className="text-[10px] text-muted-foreground hover:text-foreground flex items-center gap-1">
              <Pencil className="w-3 h-3" />{isServerMode ? 'Edit URL' : 'Set URL'}
            </button>
          ) : (
            <div className="flex gap-2">
              <button onClick={() => {
                const url = railwayDraft.trim().replace(/\/$/, '');
                setRailwayUrl(url);
                if (url) localStorage.setItem(RAILWAY_URL_KEY, url);
                else localStorage.removeItem(RAILWAY_URL_KEY);
                setShowRailwayInput(false);
              }} className="text-[10px] text-gain flex items-center gap-0.5"><Check className="w-3 h-3" />Save</button>
              <button onClick={() => setShowRailwayInput(false)} className="text-[10px] text-loss flex items-center gap-0.5"><X className="w-3 h-3" />Cancel</button>
            </div>
          )}
        </div>
        {showRailwayInput ? (
          <input value={railwayDraft} onChange={e => setRailwayDraft(e.target.value)}
            placeholder="Leave empty for same-origin (Railway unified deployment)"
            className="w-full text-xs bg-background border border-border rounded px-2 py-1.5 font-mono outline-none focus:border-accent" />
        ) : (
          <p className="text-[10px] text-muted-foreground font-mono break-all">
            {railwayUrl
              ? railwayUrl
              : <span className="font-sans text-gain">Same-origin · API calls go to <code className="bg-muted px-1 rounded text-foreground">/api/*</code></span>}
          </p>
        )}
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
              <button onClick={() => { setInstructions(instrDraft); setEditingInstr(false); }} className="text-[10px] text-gain flex items-center gap-0.5"><Check className="w-3 h-3" />Save</button>
              <button onClick={() => setEditingInstr(false)} className="text-[10px] text-muted-foreground flex items-center gap-0.5"><X className="w-3 h-3" />Cancel</button>
            </div>
          )}
        </div>
        {editingInstr ? (
          <textarea value={instrDraft} onChange={e => setInstrDraft(e.target.value)} rows={3}
            placeholder="Optional notes — e.g. 'focus on BTC/ETH, avoid DOGE'"
            className="w-full bg-muted/40 border border-accent/40 rounded px-2 py-2 text-xs text-foreground placeholder:text-muted-foreground focus:outline-none resize-none" />
        ) : (
          <p className="text-xs text-muted-foreground leading-relaxed">{instructions || <span className="italic opacity-60">No notes — bot uses EMA+RSI+MACD+Volume signals.</span>}</p>
        )}
      </div>

      {/* ── Stats ── */}
      <div className="grid grid-cols-4 gap-1.5">
        {[
          { label: 'Balance', value: `${balance.toFixed(2)} USDT`, color: '' },
          { label: 'Net P&L', value: `${totalPnl>=0?'+':''}${Math.abs(totalPnl).toFixed(2)} USDT`, color: pnlColor },
          { label: 'Return', value: `${totalPnl>=0?'+':''}${pnlPct}%`, color: pnlColor },
          { label: 'Win Rate', value: sellTrades.length ? `${winRate}%` : '—', color: winRate>=50?'text-gain':sellTrades.length?'text-loss':'' },
        ].map(s => (
          <div key={s.label} className="bg-muted/20 rounded-md p-2 text-center">
            <div className="text-[9px] uppercase tracking-widest text-muted-foreground">{s.label}</div>
            <div className={`text-xs font-mono font-semibold tabular-nums ${s.color}`}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* ── Start / Stop ── */}
      <Button onClick={toggleBot} disabled={loading || (!isServerMode && !selectedCoins.length)}
        className={`w-full font-semibold py-5 ${isRunning?'bg-loss/90 hover:bg-loss text-white':'bg-gain/90 hover:bg-gain text-background'}`}>
        {loading ? <span className="animate-spin mr-1.5">⟳</span>
          : isRunning
            ? <><Square className="w-4 h-4 mr-1.5"/>{isServerMode ? 'Pause Railway Bot' : 'Stop Agent'}</>
            : <><Play className="w-4 h-4 mr-1.5"/>{isServerMode ? 'Start Railway Bot (24/7)' : 'Start AI Agent — Paper Test'}</>}
      </Button>
      {isRunning
        ? <p className="text-[10px] text-center text-muted-foreground -mt-2">
            {isServerMode
              ? <>Railway bot running 24/7 · closes browser safe · polling every 30s{agentStatus && <> · <span className="text-gain font-mono">{agentStatus}</span></>}</>
              : <>Every 30s: fetches live candles → checks EMA / RSI / MACD / Volume → buys or holds{agentStatus && <> · <span className="text-accent font-mono">{agentStatus}</span></>}</>}
          </p>
        : <p className="text-[10px] text-center text-muted-foreground -mt-2">
            {isServerMode
              ? 'Railway bot handles all trading 24/7 — no browser required'
              : 'Sells on every price tick · Buys checked every 30s · EMA+RSI+MACD+Volume signals · no API key needed'}
          </p>
      }

      {/* ── Live coin signals ── */}
      {(coinSignals.length > 0 || scanning) && (
        <div>
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 flex items-center gap-1.5">
            <Activity className="w-3 h-3 text-accent" />Live Signals — last scan
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-5 gap-2">
            {coinSignals.map(sig => {
              const held = positions.some(p => p.symbol === sig.symbol);
              const wsP  = parseFloat(pricesRef.current[sig.symbol]?.price || '0') || sig.price;
              return (
                <div key={sig.symbol} className={`rounded-lg p-2.5 space-y-1.5 border ${sig.signal==='loading'?'animate-pulse bg-muted/30 border-border/50':sig.signal==='BUY'?'bg-gain/5 border-gain/30':held?'border-accent/30 bg-accent/5':'bg-secondary/40 border-border'}`}>
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-bold font-mono">{sig.symbol.replace('USDT','')}</span>
                    <span className={`text-[10px] font-bold ${sig.signal==='BUY'?'text-gain':sig.signal==='error'?'text-muted-foreground':'text-warn'}`}>
                      {sig.signal==='loading'?'…':sig.signal}
                    </span>
                  </div>
                  <div className="text-xs font-mono">{wsP > 1 ? `${wsP.toLocaleString('en-US',{maximumFractionDigits:2})} USDT` : `${wsP.toFixed(5)} USDT`}</div>
                  {/* 4 signal dots */}
                  <div className="flex gap-0.5">
                    {([['EMA',sig.emaBullish],['RSI',sig.rsiOk],['MACD',sig.macdPos],['Vol',sig.volUp]] as [string,boolean][]).map(([label,on])=>(
                      <div key={label} className={`flex-1 rounded-full text-center py-0.5 text-[8px] font-bold ${on?'bg-gain/20 text-gain':'bg-loss/10 text-loss/50'}`} title={label}>{label}</div>
                    ))}
                  </div>
                  {/* Force buy/sell */}
                  {!held ? (
                    <button onClick={() => forceBuy(sig.symbol)} disabled={!!forcingBuy||sig.signal==='error'}
                      className="w-full text-[10px] py-1 rounded bg-gain/10 hover:bg-gain/20 text-gain font-semibold disabled:opacity-40 flex items-center justify-center gap-1">
                      <ShoppingCart className="w-2.5 h-2.5" />{forcingBuy===sig.symbol?'…':'Force BUY'}
                    </button>
                  ) : (
                    <div className="text-[10px] text-center text-accent font-semibold">Holding ✓</div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Open positions ── always visible so the user can see status */}
      <div>
        <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 flex items-center justify-between">
          <div className="flex items-center gap-1.5">
            <Zap className="w-3 h-3 text-warn"/>
            Open Positions
            <span className={`text-[9px] font-mono px-1.5 py-0.5 rounded ${positions.length>0?'bg-warn/20 text-warn':'bg-muted/40 text-muted-foreground'}`}>
              {positions.length}
            </span>
          </div>
          {isServerMode && positions.length > 0 && (
            <span className="text-[9px] text-muted-foreground">live from Railway</span>
          )}
        </div>
        {positions.length === 0 ? (
          <div className="text-xs text-muted-foreground text-center py-5 border border-dashed border-border rounded-lg">
            {isRunning ? '⏳ No open positions — bot will buy when signals align' : '▶ Start the agent to begin trading'}
          </div>
        ) : (
          <div className="space-y-1.5">
            {positions.map(pos => {
              const live  = parseFloat(pricesRef.current[pos.symbol]?.price||'0') || pos.avg_entry_price;
              const sells = live * pos.quantity * (1-TAKER_FEE);
              const cost  = pos.avg_entry_price * pos.quantity / (1-TAKER_FEE);
              const uPnl  = sells - cost;
              const pct   = cost > 0 ? (uPnl/cost)*100 : 0;
              const bep   = pos.avg_entry_price * BEP_MULT;
              const prof  = live >= bep;
              const bepProgress = Math.min(100, Math.max(0, ((live-pos.avg_entry_price)/(bep-pos.avg_entry_price))*100));
              const qty   = Number(pos.quantity);
              const budget = pos.avg_entry_price * qty;
              return (
                <div key={pos.symbol} className="bg-muted/20 border border-border/50 rounded-lg px-3 py-2.5 space-y-2">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div className={`w-1.5 h-1.5 rounded-full ${uPnl>=0?'bg-gain animate-pulse':'bg-loss'}`}/>
                      <span className="font-mono font-bold text-sm">{pos.symbol.replace('USDT','')}</span>
                      <span className={`text-xs font-mono font-bold ${pct>=0?'text-gain':'text-loss'}`}>{pct>=0?'+':''}{pct.toFixed(3)}%</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className={`font-mono text-xs font-bold ${uPnl>=0?'text-gain':'text-loss'}`}>{uPnl>=0?'+':''}{uPnl.toFixed(4)} USDT</span>
                      <button onClick={() => forceSell(pos)} disabled={!!forcingSell}
                        className="text-[10px] px-2 py-1 rounded bg-loss/10 hover:bg-loss/20 text-loss font-semibold disabled:opacity-40 flex items-center gap-1">
                        <Banknote className="w-2.5 h-2.5"/>{forcingSell===pos.symbol?'…':'Sell'}
                      </button>
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-x-4 text-[10px] text-muted-foreground font-mono">
                    <span>Entry <span className="text-foreground">{pos.avg_entry_price.toFixed(4)}</span></span>
                    <span>BEP <span className="text-accent">{bep.toFixed(4)}</span></span>
                    <span>Now <span className={prof?'text-gain':'text-foreground'}>{live.toFixed(4)}</span></span>
                    <span>Qty <span className="text-foreground">{qty.toFixed(6)}</span> · <span className="text-foreground">{budget.toFixed(2)} USDT</span></span>
                  </div>
                  <div className="space-y-1">
                    <div className="flex justify-between text-[9px]">
                      <span className="text-muted-foreground">Break-even progress</span>
                      <span className={prof?'text-gain font-bold':'text-warn'}>{prof?'✓ Profitable':bepProgress.toFixed(0)+'% to BEP'}</span>
                    </div>
                    <div className="h-1.5 bg-muted/40 rounded-full overflow-hidden">
                      <div className={`h-full rounded-full transition-all ${prof?'bg-gain':'bg-warn'}`} style={{width:`${Math.max(2, bepProgress)}%`}}/>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* ── Trade history ── */}
      <div>
        <div className="flex items-center justify-between mb-2">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground flex items-center gap-1.5">
            {totalPnl>=0?<TrendingUp className="w-3 h-3 text-gain"/>:<TrendingDown className="w-3 h-3 text-loss"/>}
            Trade History ({trades.length})
          </div>
          {sellTrades.length > 0 && (
            <div className="flex items-center gap-3 text-[10px] font-mono">
              <span className="text-muted-foreground">{wins}W/{sellTrades.length-wins}L</span>
              <span className={pnlColor}>{totalPnl>=0?'+':''}{totalPnl.toFixed(4)} USDT</span>
            </div>
          )}
        </div>
        {!trades.length ? (
          <p className="text-xs text-muted-foreground text-center py-6 border border-dashed border-border rounded-lg">
            {isRunning ? '⏳ First scan running — trades will appear here…' : '▶ Start the agent to begin paper trading'}
          </p>
        ) : (
          <div className="space-y-0.5">
            {displayedTrades.map(t => {
              const isBuy = t.side === 'BUY';
              const win   = (t.pnl ?? 0) > 0;
              const loss  = (t.pnl ?? 0) < 0;
              return (
                <div key={t.id} className={`flex items-center justify-between px-2.5 py-1.5 rounded text-xs border
                  ${win?'bg-gain/5 border-gain/20':loss?'bg-loss/5 border-loss/20':isBuy?'bg-muted/20 border-border/30':'bg-muted/10 border-border/20'}`}>
                  <div className="flex items-center gap-2 min-w-0">
                    {isBuy?<TrendingUp className="w-3 h-3 text-accent shrink-0"/>:win?<TrendingUp className="w-3 h-3 text-gain shrink-0"/>:<TrendingDown className="w-3 h-3 text-loss shrink-0"/>}
                    <span className={`text-[9px] font-bold px-1 py-0.5 rounded shrink-0 ${isBuy?'bg-accent/20 text-accent':win?'bg-gain/20 text-gain':'bg-loss/20 text-loss'}`}>{t.side}</span>
                    <span className="font-mono font-semibold shrink-0">{t.symbol.replace('USDT','')}</span>
                    <span className="text-muted-foreground font-mono text-[10px] truncate">{Number(t.price).toLocaleString('en-US',{maximumFractionDigits:4})} USDT</span>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    {t.pnl!==null && <span className={`font-mono font-bold text-[10px] ${t.pnl>=0?'text-gain':'text-loss'}`}>{t.pnl>=0?'+':''}{t.pnl.toFixed(4)} USDT</span>}
                    <span className="text-muted-foreground text-[9px] font-mono">{new Date(t.created_at).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})}</span>
                  </div>
                </div>
              );
            })}
            {trades.length > 12 && (
              <button onClick={() => setShowAll(p=>!p)} className="w-full text-[10px] text-accent hover:underline py-1 flex items-center justify-center gap-1">
                {showAll?<><ChevronUp className="w-3 h-3"/>Show less</>:<><ChevronDown className="w-3 h-3"/>Show all {trades.length} trades</>}
              </button>
            )}
          </div>
        )}
      </div>

      {/* ── Activity log (collapsible) ── */}
      <div>
        <button onClick={() => setShowLog(p=>!p)} className="flex items-center gap-1.5 text-[10px] text-muted-foreground hover:text-foreground w-full">
          <Activity className="w-3 h-3"/>
          Activity Log ({actLog.length})
          {showLog?<ChevronUp className="w-3 h-3 ml-auto"/>:<ChevronDown className="w-3 h-3 ml-auto"/>}
        </button>
        {showLog && (
          <div className="mt-1.5 bg-secondary/30 rounded-lg p-2 max-h-40 overflow-y-auto scrollbar-thin space-y-0.5">
            {actLog.length===0 && <div className="text-[10px] text-muted-foreground text-center py-2">No activity yet</div>}
            {actLog.map((line,i) => (
              <div key={i} className={`text-[10px] font-mono ${line.includes('BUY')?'text-gain':line.includes('SELL')?'text-loss':line.includes('ERROR')?'text-destructive':'text-muted-foreground'}`}>
                {line}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default AITradingAgent;
