import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Play, Square, Brain, TrendingUp, TrendingDown, Zap,
  RotateCcw, ChevronDown, ChevronUp, FlaskConical,
  Pencil, Check, X, BookOpen, Activity,
  ShoppingCart, Banknote, RefreshCw, Settings2, Plus, Minus,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import { TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { calcEMA, calcRSI, calcMACD, calcBollingerBands, calcSMA } from '@/lib/indicators';
import { API_BASE } from '@/config';

// ── All coins available for the bot to trade ─────────────────────────────────
const ALL_TRADABLE_COINS = [
  'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','DOGEUSDT','XRPUSDT',
  'ADAUSDT','AVAXUSDT','DOTUSDT','LINKUSDT','MATICUSDT','UNIUSDT',
  'LTCUSDT','ATOMUSDT','SHIBUSDT','ARBUSDT','OPUSDT','INJUSDT',
  'FETUSDT','NEARUSDT','TRXUSDT','TONUSDT','APTUSDT','SUIUSDT',
  'PEPEUSDT','WIFUSDT','BONKUSDT','JUPUSDT','RENDERUSDT','TIAUSDT',
];

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
    volume: volRat > 1.05 || recentVol > prevVol,
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

// Fetch raw 60-candle buffer for one coin — called once on agent start to seed the WebSocket buffer.
async function fetchKlineBuffer(sym: string): Promise<{ closes: number[]; volumes: number[] } | null> {
  const ctrl = new AbortController();
  const timeout = setTimeout(() => ctrl.abort(), 8000);
  try {
    const res = await fetch(`${BIN}/klines?symbol=${sym}&interval=1m&limit=60`, { signal: ctrl.signal });
    clearTimeout(timeout);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const klines = await res.json();
    if (!Array.isArray(klines) || klines.length < 30) throw new Error('insufficient data');
    return {
      closes:  klines.map((k: any[]) => parseFloat(k[4])),
      volumes: klines.map((k: any[]) => parseFloat(k[5])),
    };
  } catch {
    clearTimeout(timeout);
    return null;
  }
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
  onStateChange?: (positions: {symbol:string;quantity:number;avg_entry_price:number}[], balance: number) => void;
  onCoinsChange?: (coins: string[]) => void;
}

const INSTRUCTIONS_KEY = 'ai_agent_instructions';
const BEP_MULT         = 1 / Math.pow(1 - TAKER_FEE, 2);

// ── Component ────────────────────────────────────────────────────────────────
const AITradingAgent = ({ selectedCoins, prices, binanceConnected, onConnectBinance, onStateChange, onCoinsChange }: AITradingAgentProps) => {
  const [mode, setMode]           = useState<'test' | 'live'>('test');
  const [isRunning, setIsRunning] = useState(false);
  const [loading, setLoading]     = useState(false);
  const [balance, setBalance]     = useState(0);
  const [initialBalance, setInitialBalance] = useState(0);
  const [positions, setPositions] = useState<OpenPosition[]>([]);
  const [trades, setTrades]       = useState<TradeRow[]>([]);
  const [coinSignals, setCoinSignals] = useState<CoinSignal[]>([]);
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

  // ── Railway server mode ──────────────────────────────────────────────────
  const RAILWAY_URL_KEY = 'railway_bot_url';
  const [railwayUrl, setRailwayUrl] = useState<string>(() => {
    if (API_BASE) return API_BASE;
    return localStorage.getItem(RAILWAY_URL_KEY) ?? '';
  });
  const [railwayConnected, setRailwayConnected] = useState(false);
  const [showUrlInput, setShowUrlInput] = useState(false);
  const [urlDraft, setUrlDraft] = useState('');

  const isRunningRef      = useRef(false);
  const exitProcessingRef = useRef(false);
  const buyProcessingRef  = useRef(false);
  const pendingSellsRef   = useRef<Set<string>>(new Set());
  const balanceRef        = useRef(balance);
  const positionsRef      = useRef(positions);
  const signalCacheRef    = useRef<CoinSignal[]>([]);
  const klineWsRef        = useRef<WebSocket | null>(null);
  const klineBufferRef    = useRef<Map<string, { closes: number[]; volumes: number[] }>>(new Map());
  const selectedCoinsRef  = useRef(selectedCoins);
  const connectKlineWsRef = useRef<() => void>(() => {});
  const stopLossRef       = useRef(1.5);
  const onStateChangeRef  = useRef(onStateChange);

  useEffect(() => { isRunningRef.current     = isRunning; },      [isRunning]);
  useEffect(() => { positionsRef.current     = positions; },      [positions]);
  useEffect(() => { selectedCoinsRef.current = selectedCoins; },  [selectedCoins]);
  useEffect(() => { localStorage.setItem(INSTRUCTIONS_KEY, instructions); }, [instructions]);
  useEffect(() => { onStateChangeRef.current = onStateChange; }, [onStateChange]);

  const isServerMode = railwayUrl.trim().length > 0;

  const saveRailwayUrl = useCallback((url: string) => {
    const trimmed = url.trim().replace(/\/$/, '');
    setRailwayUrl(trimmed);
    localStorage.setItem(RAILWAY_URL_KEY, trimmed);
    setShowUrlInput(false);
    if (trimmed) toast.success('Railway URL saved — bot will run 24/7 on server');
    else toast.info('Railway URL cleared — running in browser');
  }, []);

  const addLog = useCallback((msg: string) => {
    const ts = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    setActLog(prev => [`[${ts}] ${msg}`, ...prev].slice(0, 60));
  }, []);

  // ── Load state from DB ───────────────────────────────────────────────────
  const loadData = useCallback(async () => {
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

    if (posRes.data) {
      const fetched = posRes.data as OpenPosition[];
      setPositions(fetched);
      positionsRef.current = fetched;   // sync ref directly — no render-cycle delay
    }

    // Use same fallback chain as wallet: current_balance → initial_balance → localStorage startingBalance
    const startBal = getPaperCfg().startingBalance ?? 1000;
    let display = startBal;
    if (cfgRes.data) {
      const bal     = Number(cfgRes.data.current_balance);
      const initBal = Number(cfgRes.data.initial_balance ?? 0);
      display = bal > 0 ? bal : initBal > 0 ? initBal : startBal;
      setBalance(display);
      // Only sync balanceRef from DB when no buy/sell cycle is active.
      // During active cycles the ref is authoritative and loadData must not overwrite it.
      if (!buyProcessingRef.current && !exitProcessingRef.current) {
        balanceRef.current = display;
      }
      setInitialBalance(initBal > 0 ? initBal : startBal);
      stopLossRef.current = Number(cfgRes.data.stop_loss_percent ?? 1.5);
      const running = Boolean(cfgRes.data.is_running);
      isRunningRef.current = running;
      setIsRunning(running);
    } else {
      setBalance(startBal);
      if (!buyProcessingRef.current && !exitProcessingRef.current) {
        balanceRef.current = startBal;
      }
      setInitialBalance(startBal);
    }

    // Notify parent with fresh state for instant wallet sync
    if (posRes.data) {
      onStateChangeRef.current?.(posRes.data as OpenPosition[], display);
    }
  }, []);

  useEffect(() => {
    loadData();
    const ch = supabase.channel('ata-rt')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_config' }, loadData)
      .subscribe();
    return () => { supabase.removeChannel(ch); if (klineWsRef.current) { klineWsRef.current.close(); klineWsRef.current = null; } };
  }, [loadData]);

  // ── Price refs ───────────────────────────────────────────────────────────
  const pricesRef = useRef(prices);
  useEffect(() => { pricesRef.current = prices; }, [prices]);

  // ── Poll Railway when in server mode ─────────────────────────────────────
  useEffect(() => {
    if (!isServerMode) return;
    const poll = async () => {
      try {
        const [statusRes, posRes, tradesRes] = await Promise.all([
          fetch(`${railwayUrl}/api/status`),
          fetch(`${railwayUrl}/api/positions`),
          fetch(`${railwayUrl}/api/trades`),
        ]);
        if (!statusRes.ok) throw new Error('unreachable');
        const status     = await statusRes.json();
        const posData    = await posRes.json();
        const tradesData = await tradesRes.json();

        setIsRunning(Boolean(status.running));
        isRunningRef.current = Boolean(status.running);
        const bal = Number(status.balance_usdt ?? 0);
        setBalance(bal);
        balanceRef.current = bal;
        setRailwayConnected(true);

        const mappedPos: OpenPosition[] = (posData.positions ?? []).map((p: any) => ({
          symbol: p.symbol,
          quantity: Number(p.quantity),
          avg_entry_price: Number(p.entry_price),
        }));
        setPositions(mappedPos);
        positionsRef.current = mappedPos;
        onStateChangeRef.current?.(mappedPos, bal);

        setTrades((tradesData.trades ?? []).map((t: any): TradeRow => ({
          id: String(t.id),
          created_at: t.timestamp_sell ?? t.timestamp_buy ?? new Date().toISOString(),
          symbol: t.coin,
          side: t.exit_price != null ? 'SELL' : 'BUY',
          price: Number(t.exit_price ?? t.entry_price),
          quantity: Number(t.quantity),
          pnl: t.net_profit != null ? Number(t.net_profit) : null,
          reason: null,
        })));
      } catch {
        setRailwayConnected(false);
      }
    };
    poll();
    const id = setInterval(poll, 5000);
    return () => clearInterval(id);
  }, [railwayUrl, isServerMode]); // eslint-disable-line

  // ── Pure signal recomputation from in-memory kline buffers — zero network calls ──
  const recomputeSignals = useCallback(() => {
    const coins = selectedCoinsRef.current;
    const results: CoinSignal[] = [];
    for (const sym of coins) {
      const buf = klineBufferRef.current.get(sym);
      if (!buf || buf.closes.length < 30) {
        results.push({ symbol: sym, price: 0, rsi: 50, emaBullish: false, rsiOk: false, macdPos: false, volUp: false, signal: 'loading', reason: 'loading' });
        continue;
      }
      const { closes, volumes } = buf;
      const price  = closes[closes.length - 1];
      const sigs   = evaluateSignals(closes, volumes);
      const score  = Object.values(sigs).filter(Boolean).length;
      const rsi    = calcRSI(closes, 14);
      const volSma = calcSMA(volumes, 20);
      const curVol = volumes[volumes.length - 1] ?? 0;
      const volRat = volSma > 0 ? curVol / volSma : 1;
      const isBuy  = score >= 3;
      const isHold = rsi >= 72 || rsi < 24;
      const parts  = [sigs.trend ? 'EMA↑' : 'EMA↓', `RSI ${rsi.toFixed(0)}`, sigs.macd ? 'MACD+' : 'MACD-'];
      if (sigs.volume) parts.push(`Vol ${volRat.toFixed(1)}×`);
      results.push({
        symbol: sym, price, rsi,
        emaBullish: sigs.trend, rsiOk: sigs.rsi, macdPos: sigs.macd, volUp: sigs.volume,
        signal: isHold ? 'HOLD' : isBuy ? 'BUY' : 'HOLD',
        reason: parts.join(' · '),
      });
    }
    signalCacheRef.current = results;
    setCoinSignals(results);
  }, []);

  // ── Seed kline buffers from REST — one REST call per coin, runs once on start ──
  const seedBuffers = useCallback(async () => {
    if (!isRunningRef.current) return;
    setScanning(true);
    setAgentStatus('Seeding market data…');
    const coins = selectedCoinsRef.current;
    try {
      for (let i = 0; i < coins.length; i++) {
        const sym = coins[i];
        const buf = await fetchKlineBuffer(sym);
        if (buf) {
          klineBufferRef.current.set(sym, buf);
          recomputeSignals();
        } else {
          addLog(`[WARN] Could not seed ${sym}`);
        }
        if (i < coins.length - 1) await new Promise(r => setTimeout(r, 200));
      }
      const buySigs = signalCacheRef.current.filter(s => s.signal === 'BUY');
      addLog(`Seed done: ${buySigs.length} BUY · ${signalCacheRef.current.filter(s => s.signal === 'HOLD').length} HOLD`);
      setAgentStatus('Live');
    } catch (e: any) {
      addLog(`Seed error: ${e.message}`);
    } finally {
      setScanning(false);
    }
  }, [addLog, recomputeSignals]);

  // ── Binance kline WebSocket — keeps buffers live after initial seed ──
  const connectKlineWs = useCallback(() => {
    if (klineWsRef.current) { klineWsRef.current.close(); klineWsRef.current = null; }
    const coins = selectedCoinsRef.current;
    const streams = coins.map(s => `${s.toLowerCase()}@kline_1m`).join('/');
    const ws = new WebSocket(`wss://stream.binance.com:9443/stream?streams=${streams}`);

    ws.onmessage = (event: MessageEvent) => {
      try {
        const msg = JSON.parse(event.data as string);
        const k = msg.data?.k;
        if (!k) return;
        const sym: string = k.s;
        const close = parseFloat(k.c);
        const vol   = parseFloat(k.v);
        const buf   = klineBufferRef.current.get(sym);
        if (!buf) return;
        if (k.x) {
          buf.closes  = [...buf.closes.slice(-59),  close];
          buf.volumes = [...buf.volumes.slice(-59), vol];
        } else {
          buf.closes  = [...buf.closes.slice(0, -1),  close];
          buf.volumes = [...buf.volumes.slice(0, -1), vol];
        }
        klineBufferRef.current.set(sym, buf);
        recomputeSignals();
      } catch { /* malformed frame */ }
    };

    ws.onerror = () => { addLog('[WS] Kline stream error'); };
    ws.onclose = () => {
      klineWsRef.current = null;
      if (isRunningRef.current) {
        addLog('[WS] Kline stream closed — reconnecting in 3s');
        setTimeout(() => connectKlineWsRef.current(), 3000);
      }
    };

    klineWsRef.current = ws;
  }, [addLog, recomputeSignals]);

  useEffect(() => { connectKlineWsRef.current = connectKlineWs; }, [connectKlineWs]);

  // ── Buy executor — runs on every price tick using cached signals ──────────
  // No API calls here — just reads the signal cache and executes if conditions met.
  const executePendingBuys = useCallback(async () => {
    if (!isRunningRef.current) return;
    const signals = signalCacheRef.current;
    const buySigs = signals.filter(s => s.signal === 'BUY');
    if (!buySigs.length) return;

    const currentPositions = positionsRef.current;
    const heldSet = new Set(currentPositions.map(p => p.symbol));
    let runBal = balanceRef.current;

    // Scale max open positions with how many coins the bot is watching
    // so adding more coins actually allows the bot to hold more positions.
    const maxPos = Math.max(MAX_POSITIONS, Math.floor(selectedCoinsRef.current.length / 3));

    const newlyBought: OpenPosition[] = [];
    for (const sig of buySigs) {
      if (heldSet.size >= maxPos) break;
      if (runBal < MIN_USDT) break;
      if (heldSet.has(sig.symbol)) continue;

      const wsPrice = parseFloat(pricesRef.current[sig.symbol]?.price || '0');
      const price   = wsPrice > 0 ? wsPrice : sig.price;
      if (!price) continue;

      const alloc  = getAllocation(runBal, sig.symbol);
      const needed = alloc * 1.002;
      if (runBal < needed) { addLog(`[SKIP] ${sig.symbol}: need ${needed.toFixed(2)} have ${runBal.toFixed(2)}`); continue; }
      if (alloc < MIN_USDT) continue;

      const fee       = alloc * TAKER_FEE;
      const qty       = (alloc - fee) / price;
      const newRunBal = Math.round((runBal - alloc) * 10000) / 10000;

      // Write position AND deduct balance in the same round-trip so realtime
      // subscribers always see a consistent state (no phantom USDT).
      // Update balanceRef BEFORE the await so concurrent loadData calls
      // triggered by the DB write cannot overwrite it with a stale value.
      balanceRef.current = newRunBal;
      setBalance(newRunBal);

      await Promise.all([
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
        supabase.from('bot_config').update({
          current_balance: newRunBal,
          updated_at: new Date().toISOString(),
        }).eq('user_session', SESSION),
      ]);

      runBal = newRunBal;
      newlyBought.push({ symbol: sig.symbol, quantity: qty, avg_entry_price: price });
      heldSet.add(sig.symbol);
      addLog(`  BUY  ${sig.symbol} @ ${price.toFixed(4)} USDT · ${alloc.toFixed(2)} USDT · bal ${runBal.toFixed(2)} USDT`);
      toast.info(`AI Paper BUY: ${sig.symbol.replace('USDT','')}`, { description: `${alloc.toFixed(2)} USDT @ ${price.toFixed(4)} USDT`, duration: 3000 });
    }

    if (newlyBought.length > 0) {
      const updated = [...currentPositions, ...newlyBought];
      setPositions(updated); positionsRef.current = updated;
      // balanceRef.current already set per-buy above; sync UI state + parent
      setBalance(runBal); balanceRef.current = runBal;
      onStateChangeRef.current?.(updated, runBal);
    }
  }, [addLog]);

  // ── Exit executor — pure computation, zero DB reads, fires on every tick ──
  // Reads positionsRef + pricesRef in-memory only; DB writes are fire-and-forget.
  const executeExits = useCallback(async () => {
    if (!isRunningRef.current) return;
    const currentPositions = positionsRef.current;
    if (!currentPositions.length) return;

    type SellItem = { pos: OpenPosition; price: number; pnl: number; proceeds: number };
    const toSell: SellItem[] = [];

    for (const pos of currentPositions) {
      if (pendingSellsRef.current.has(pos.symbol)) continue;
      const wsPrice = parseFloat(pricesRef.current[pos.symbol]?.price || '0');
      if (!wsPrice) continue;

      const qty   = Number(pos.quantity);
      const entry = Number(pos.avg_entry_price);
      const bep   = entry * BEP_MULT;
      const stopPrice = entry * (1 - stopLossRef.current / 100);

      if (wsPrice < bep && wsPrice > stopPrice) continue;

      const proceeds = qty * wsPrice * (1 - TAKER_FEE);
      const cost     = qty * entry / (1 - TAKER_FEE);
      const pnl      = Math.round((proceeds - cost) * 10000) / 10000;

      pendingSellsRef.current.add(pos.symbol);
      toSell.push({ pos, price: wsPrice, pnl, proceeds });
    }

    if (!toSell.length) return;

    // Optimistic state update — happens before any DB round-trip
    const soldSymbols = new Set(toSell.map(s => s.pos.symbol));
    const remaining   = currentPositions.filter(p => !soldSymbols.has(p.symbol));
    const totalProceeds = toSell.reduce((s, t) => s + t.proceeds, 0);
    const newBal = Math.round((balanceRef.current + totalProceeds) * 10000) / 10000;

    setPositions(remaining);  positionsRef.current = remaining;
    setBalance(newBal);        balanceRef.current   = newBal;
    onStateChangeRef.current?.(remaining, newBal);

    for (const { pos, price, pnl } of toSell) {
      addLog(`SELL ${pos.symbol} @ ${price.toFixed(4)} USDT · P&L: ${pnl >= 0 ? '+' : ''}${pnl.toFixed(4)} USDT`);
    }

    // DB writes with error logging
    await Promise.all([
      ...toSell.map(async ({ pos, price, pnl }) => {
        const [histRes, portRes] = await Promise.all([
          supabase.from('bot_trade_history').insert({
            user_session: SESSION, symbol: pos.symbol, side: 'SELL',
            price, quantity: Number(pos.quantity), pnl,
            reason: `[AI Paper] exit · @ ${price.toFixed(4)} USDT · pnl ${pnl >= 0 ? '+' : ''}${pnl.toFixed(4)} USDT`,
          }),
          supabase.from('paper_portfolio').delete().eq('user_session', SESSION).eq('symbol', pos.symbol),
        ]);
        if (histRes.error) addLog(`[ERR] Sell history ${pos.symbol}: ${histRes.error.message}`);
        if (portRes.error) addLog(`[ERR] Portfolio delete ${pos.symbol}: ${portRes.error.message}`);
      }),
      supabase.from('bot_config').update({ current_balance: newBal, updated_at: new Date().toISOString() })
        .eq('user_session', SESSION)
        .then(r => { if (r.error) addLog(`[ERR] Config update: ${r.error.message}`); }),
    ]);

    for (const { pos } of toSell) pendingSellsRef.current.delete(pos.symbol);

    toast.success(`Closed ${toSell.length} position(s)`, { duration: 2500 });
    await loadData();
  }, [addLog, loadData]);

  // ── Price tick: exits (real-time) + buy execution (uses cached signals) ───
  useEffect(() => {
    if (!isRunning || !Object.keys(prices).length) return;

    // Exit check — zero DB reads, pure in-memory computation
    if (!exitProcessingRef.current) {
      exitProcessingRef.current = true;
      executeExits()
        .catch((e: unknown) => { addLog(`[ERR] Exit check: ${e instanceof Error ? e.message : String(e)}`); })
        .finally(() => { exitProcessingRef.current = false; });
    }

    // Buy check — only if signal cache is populated and no concurrent buy running
    if (!buyProcessingRef.current && signalCacheRef.current.length > 0) {
      buyProcessingRef.current = true;
      executePendingBuys()
        .catch((e: unknown) => { addLog(`[ERR] Buy exec: ${e instanceof Error ? e.message : String(e)}`); })
        .finally(() => { buyProcessingRef.current = false; });
    }
  }, [prices]); // eslint-disable-line

  // ── Manual "Now" button — re-seed buffers + reconnect WebSocket ─────────
  const runCycle = useCallback(() => { seedBuffers(); connectKlineWs(); }, [seedBuffers, connectKlineWs]);

  // ── Start / Stop ─────────────────────────────────────────────────────────
  const toggleBot = async () => {
    // Server mode: delegate to Railway, don't run in browser
    if (isServerMode) {
      setLoading(true);
      try {
        const endpoint = isRunning ? 'stop' : 'start';
        const res = await fetch(`${railwayUrl}/api/agent/${endpoint}`, { method: 'POST' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        setIsRunning(Boolean(data.running));
        isRunningRef.current = Boolean(data.running);
        toast[data.running ? 'success' : 'info'](
          data.running ? 'Bot started on Railway server — runs 24/7' : 'Bot stopped on Railway server'
        );
      } catch (e) {
        toast.error(`Railway unreachable: ${e instanceof Error ? e.message : String(e)}`);
      } finally {
        setLoading(false);
      }
      return;
    }

    if (mode === 'live' && !binanceConnected) { toast.error('Connect Binance API first'); onConnectBinance?.(); return; }
    setLoading(true);
    try {
      if (!isRunning) {
        const startBal = getPaperCfg().startingBalance ?? 1000;

        // Read existing config — never reset current_balance on restart,
        // only use startBal on first-ever launch (no existing config row).
        const existing = await supabase.from('bot_config')
          .select('current_balance, initial_balance')
          .eq('user_session', SESSION)
          .maybeSingle();
        const existingBal     = Number(existing.data?.current_balance ?? 0);
        const existingInitBal = Number(existing.data?.initial_balance ?? 0);
        const useBal     = existingBal     > 0 ? existingBal     : startBal;
        const useInitBal = existingInitBal > 0 ? existingInitBal : startBal;

        await supabase.from('bot_config').upsert({
          user_session: SESSION,
          selected_coins: selectedCoins,
          mode, is_running: true,
          current_balance: useBal,
          initial_balance: useInitBal,
          stop_loss_percent: 1.5,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session' });

        isRunningRef.current = true;
        setIsRunning(true);
        setBalance(useBal); balanceRef.current = useBal;
        setInitialBalance(useInitBal);
        addLog(`=== Agent STARTED · ${useBal.toFixed(2)} USDT · ${mode.toUpperCase()} ===`);
        toast.success(`AI Agent started — PAPER mode`, { description: `${useBal.toLocaleString()} USDT · ${selectedCoins.length} coins · live signals` });
        // Seed kline buffers from REST once, then keep live via WebSocket
        seedBuffers();
        connectKlineWs();
      } else {
        if (klineWsRef.current) { klineWsRef.current.close(); klineWsRef.current = null; }
        klineBufferRef.current.clear();
        signalCacheRef.current = [];
        await supabase.from('bot_config').update({ is_running: false, updated_at: new Date().toISOString() }).eq('user_session', SESSION);
        isRunningRef.current = false;
        setIsRunning(false);
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
      const bal   = balanceRef.current;
      const alloc = getAllocation(bal, sym);
      if (alloc < MIN_USDT) { toast.error(`Balance too low (${bal.toFixed(2)} USDT)`); return; }
      const fee    = alloc * TAKER_FEE;
      const qty    = (alloc - fee) / wsPrice;
      const newBal = Math.round((bal - alloc) * 10000) / 10000;

      // Optimistic update before DB writes so wallet reflects instantly
      balanceRef.current = newBal;
      setBalance(newBal);
      const newPos: OpenPosition = { symbol: sym, quantity: qty, avg_entry_price: wsPrice };
      const updatedPositions = [...positionsRef.current.filter(p => p.symbol !== sym), newPos];
      setPositions(updatedPositions); positionsRef.current = updatedPositions;
      onStateChangeRef.current?.(updatedPositions, newBal);

      const [histRes] = await Promise.all([
        supabase.from('bot_trade_history').insert({
          user_session: SESSION, symbol: sym, side: 'BUY',
          price: wsPrice, quantity: qty, pnl: null,
          reason: `[Force BUY] manual · ${alloc.toFixed(2)} USDT @ ${wsPrice.toFixed(4)} USDT`,
        }),
        supabase.from('paper_portfolio').upsert({
          user_session: SESSION, symbol: sym, quantity: qty, avg_entry_price: wsPrice,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session,symbol' }),
        supabase.from('bot_config').update({ current_balance: newBal, updated_at: new Date().toISOString() }).eq('user_session', SESSION),
      ]);
      if (histRes.error) addLog(`[ERR] Force buy DB: ${histRes.error.message}`);
      addLog(`FORCE BUY ${sym} @ ${wsPrice.toFixed(4)} USDT · ${alloc.toFixed(2)} USDT`);
      toast.success(`Force BUY: ${sym.replace('USDT','')} @ ${wsPrice.toFixed(4)} USDT`);
      await loadData();
    } finally { setForcingBuy(null); }
  }, [addLog, loadData]);

  // ── Force SELL ───────────────────────────────────────────────────────────
  const forceSell = useCallback(async (pos: OpenPosition) => {
    const wsPrice = parseFloat(pricesRef.current[pos.symbol]?.price || '0');
    if (!wsPrice) { toast.error('No live price yet'); return; }
    if (pendingSellsRef.current.has(pos.symbol)) return;
    pendingSellsRef.current.add(pos.symbol);
    setForcingSell(pos.symbol);
    try {
      const proceeds = Number(pos.quantity) * wsPrice * (1 - TAKER_FEE);
      const cost     = Number(pos.quantity) * Number(pos.avg_entry_price) / (1 - TAKER_FEE);
      const pnl      = Math.round((proceeds - cost) * 10000) / 10000;
      const newBal   = Math.round((balanceRef.current + proceeds) * 10000) / 10000;

      // Optimistic update before DB writes so wallet reflects instantly
      const remaining = positionsRef.current.filter(p => p.symbol !== pos.symbol);
      setPositions(remaining); positionsRef.current = remaining;
      setBalance(newBal); balanceRef.current = newBal;
      onStateChangeRef.current?.(remaining, newBal);

      const [histRes] = await Promise.all([
        supabase.from('bot_trade_history').insert({
          user_session: SESSION, symbol: pos.symbol, side: 'SELL',
          price: wsPrice, quantity: Number(pos.quantity), pnl,
          reason: `[Force SELL] manual · @ ${wsPrice.toFixed(4)} USDT · pnl ${pnl>=0?'+':''}${pnl.toFixed(4)} USDT`,
        }),
        supabase.from('paper_portfolio').delete().eq('user_session', SESSION).eq('symbol', pos.symbol),
        supabase.from('bot_config').update({ current_balance: newBal, updated_at: new Date().toISOString() }).eq('user_session', SESSION),
      ]);
      if (histRes.error) addLog(`[ERR] Force sell DB: ${histRes.error.message}`);
      addLog(`FORCE SELL ${pos.symbol} @ ${wsPrice.toFixed(4)} USDT · P&L ${pnl>=0?'+':''}${pnl.toFixed(4)} USDT`);
      toast[pnl >= 0 ? 'success' : 'error'](`Force SELL: ${pos.symbol.replace('USDT','')} ${pnl>=0?'+':''}${pnl.toFixed(4)} USDT`);
      await loadData();
    } finally { setForcingSell(null); pendingSellsRef.current.delete(pos.symbol); }
  }, [addLog, loadData]);

  // ── Reset ────────────────────────────────────────────────────────────────
  const resetBot = async () => {
    if (!confirm('Reset all paper trades and restore budget?')) return;
    if (klineWsRef.current) { klineWsRef.current.close(); klineWsRef.current = null; }
    klineBufferRef.current.clear();
    signalCacheRef.current = [];
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
    positionsRef.current = [];
    balanceRef.current = startBal;
    pendingSellsRef.current.clear();
    buyProcessingRef.current = false;
    exitProcessingRef.current = false;
    setTrades([]); setPositions([]); setBalance(startBal); setInitialBalance(startBal);
    setIsRunning(false); setCoinSignals([]); setActLog([]);
    onStateChangeRef.current?.([], startBal);
    toast.success(`Reset · ${startBal.toLocaleString()} USDT restored`);
  };

  // ── Computed stats ───────────────────────────────────────────────────────
  const sellTrades    = trades.filter(t => t.side === 'SELL' && t.pnl !== null);
  const wins          = sellTrades.filter(t => (t.pnl ?? 0) > 0).length;
  const realizedPnl   = sellTrades.reduce((s, t) => s + (t.pnl ?? 0), 0);
  const winRate       = sellTrades.length ? Math.round((wins / sellTrades.length) * 100) : 0;

  // Unrealized P&L from open positions (updates every price tick)
  const unrealizedPnl = positions.reduce((s, pos) => {
    const livePrice = parseFloat(pricesRef.current[pos.symbol]?.price || '0');
    if (!livePrice) return s;
    const qty = Number(pos.quantity);
    const entry = Number(pos.avg_entry_price);
    return s + (qty * livePrice * (1 - TAKER_FEE)) - (qty * entry / (1 - TAKER_FEE));
  }, 0);

  const totalPnl  = realizedPnl + unrealizedPnl;
  const pnlColor  = totalPnl >= 0 ? 'text-gain' : 'text-loss';
  const pnlPct    = initialBalance > 0 ? ((totalPnl / initialBalance) * 100).toFixed(2) : '0.00';
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
            ? <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded ${railwayConnected ? 'bg-gain/20 text-gain' : 'bg-warn/20 text-warn'}`}>
                {railwayConnected ? '⚡ SERVER 24/7' : '⚡ SERVER …'}
              </span>
            : <span className="text-[9px] px-1.5 py-0.5 rounded bg-muted/50 text-muted-foreground">BROWSER</span>
          }
          {!isServerMode && scanning
            ? <span className="text-[9px] text-accent font-mono flex items-center gap-1"><RefreshCw className="w-2.5 h-2.5 animate-spin" />Checking signals…</span>
            : !isServerMode && isRunning
              ? <span className="text-[9px] text-muted-foreground font-mono flex items-center gap-1.5">
                  <span className="text-gain">●</span>live
                </span>
              : null
          }
        </div>
        <div className="flex items-center gap-1.5">
          {isRunning && !isServerMode && (
            <Button size="sm" variant="outline" className="h-6 text-[10px] px-2" onClick={() => runCycle()} disabled={scanning}>
              <Zap className="w-3 h-3 mr-0.5" />Now
            </Button>
          )}
          <button
            onClick={() => { setUrlDraft(railwayUrl); setShowUrlInput(v => !v); }}
            className={`p-1.5 rounded hover:bg-muted/40 ${isServerMode ? 'text-gain' : 'text-muted-foreground hover:text-foreground'}`}
            title="Set Railway server URL for 24/7 trading"
          >
            <Activity className="w-3.5 h-3.5" />
          </button>
          <button onClick={resetBot} className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground" title="Reset">
            <RotateCcw className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* ── Railway URL settings ── */}
      {showUrlInput && (
        <div className="bg-muted/20 border border-gain/30 rounded-md px-3 py-2.5 space-y-2">
          <div className="flex items-center gap-1.5">
            <Activity className="w-3.5 h-3.5 text-gain" />
            <span className="text-xs font-semibold text-gain">Railway Server URL</span>
          </div>
          <p className="text-[10px] text-muted-foreground leading-relaxed">
            Paste your Railway app URL to run the bot 24/7 on a server. Leave empty to run in browser (stops when tab closes).
          </p>
          <div className="flex gap-2">
            <input
              type="text"
              value={urlDraft}
              onChange={e => setUrlDraft(e.target.value)}
              placeholder="https://your-app.railway.app"
              className="flex-1 bg-muted/40 border border-border rounded px-2 py-1.5 text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:border-gain/60"
            />
            <button
              onClick={() => saveRailwayUrl(urlDraft)}
              className="px-3 py-1.5 rounded bg-gain/90 hover:bg-gain text-background text-xs font-semibold"
            >
              Save
            </button>
            {railwayUrl && (
              <button
                onClick={() => saveRailwayUrl('')}
                className="px-2 py-1.5 rounded bg-muted hover:bg-muted/60 text-muted-foreground text-xs"
                title="Clear Railway URL"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>
          {isServerMode && (
            <p className={`text-[10px] font-medium ${railwayConnected ? 'text-gain' : 'text-warn'}`}>
              {railwayConnected ? `✓ Connected — ${railwayUrl}` : `⟳ Connecting to ${railwayUrl}…`}
            </p>
          )}
        </div>
      )}

      {/* ── Mode toggle ── */}
      <div className="grid grid-cols-2 gap-1 bg-muted/30 rounded-md p-0.5">
        {(['test', 'live'] as const).map(m => (
          <button key={m} onClick={() => { if (!isRunning) setMode(m); }} disabled={isRunning}
            className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold transition-colors disabled:opacity-60
              ${mode === m ? (m === 'live' ? 'bg-loss/80 text-white' : 'bg-accent text-accent-foreground') : 'text-muted-foreground hover:text-foreground'}`}>
            {m === 'test' ? <><FlaskConical className="w-3.5 h-3.5" />TEST · Paper</> : <><Zap className="w-3.5 h-3.5" />LIVE · Real{!binanceConnected && <span className="text-[9px] px-1 bg-warn/20 text-warn rounded ml-1">API needed</span>}</>}
          </button>
        ))}
      </div>

      {/* ── Coin picker ── */}
      <div className="bg-muted/20 border border-border rounded-md px-3 py-2.5 space-y-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-1.5">
            <Settings2 className="w-3.5 h-3.5 text-accent" />
            <span className="text-xs font-semibold text-accent">Coins to Trade</span>
            <span className="text-[10px] text-muted-foreground">({selectedCoins.length} active)</span>
          </div>
          {isRunning
            ? <span className="text-[9px] text-muted-foreground">stop bot to edit</span>
            : <span className="text-[9px] text-muted-foreground">tap coin to add/remove</span>
          }
        </div>
        <div className="flex flex-wrap gap-1">
          {ALL_TRADABLE_COINS.map(coin => {
            const ticker = coin.replace('USDT', '');
            const active = selectedCoins.includes(coin);
            return (
              <button
                key={coin}
                disabled={isRunning || (!active && !onCoinsChange)}
                onClick={() => {
                  if (!onCoinsChange || isRunning) return;
                  const next = active
                    ? selectedCoins.filter(c => c !== coin)
                    : [...selectedCoins, coin];
                  if (next.length === 0) { toast.error('Keep at least 1 coin'); return; }
                  onCoinsChange(next);
                }}
                title={active ? `Remove ${ticker}` : `Add ${ticker}`}
                className={`flex items-center gap-0.5 text-[10px] px-2 py-0.5 rounded-full font-semibold transition-colors
                  ${active
                    ? 'bg-accent text-accent-foreground hover:bg-accent/80'
                    : 'bg-muted/50 text-muted-foreground hover:bg-muted hover:text-foreground'
                  } disabled:opacity-50 disabled:cursor-not-allowed`}
              >
                {active
                  ? <Minus className="w-2 h-2" />
                  : <Plus className="w-2 h-2" />
                }
                {ticker}
              </button>
            );
          })}
        </div>
      </div>

      {mode === 'live' && (
        <div className={`rounded-md px-3 py-2 text-xs ${binanceConnected ? 'bg-loss/10 border border-loss/30 text-loss' : 'bg-warn/10 border border-warn/30 text-warn'}`}>
          {binanceConnected ? '⚠️ LIVE MODE — real USDT will be used.' : <span>Binance API not connected. <button onClick={onConnectBinance} className="underline font-semibold">Connect now →</button></span>}
        </div>
      )}

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
          { label: positions.length ? 'Open P&L' : 'Realized P&L', value: `${(positions.length ? unrealizedPnl : realizedPnl) >= 0 ? '+' : ''}${Math.abs(positions.length ? unrealizedPnl : realizedPnl).toFixed(2)} USDT`, color: (positions.length ? unrealizedPnl : realizedPnl) >= 0 ? 'text-gain' : 'text-loss' },
          { label: 'Total Return', value: `${totalPnl>=0?'+':''}${pnlPct}%`, color: pnlColor },
          { label: 'Win Rate', value: sellTrades.length ? `${winRate}% (${wins}W/${sellTrades.length-wins}L)` : '—', color: winRate>=50?'text-gain':sellTrades.length?'text-loss':'' },
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
            ? <><Square className="w-4 h-4 mr-1.5"/>Stop Agent{isServerMode ? ' (Railway)' : ''}</>
            : isServerMode
              ? <><Play className="w-4 h-4 mr-1.5"/>Start Agent on Railway (24/7)</>
              : <><Play className="w-4 h-4 mr-1.5"/>Start AI Agent — Paper Test</>}
      </Button>
      {isServerMode
        ? <p className="text-[10px] text-center text-muted-foreground -mt-2">
            {railwayConnected
              ? `Runs 24/7 on Railway server · stays active when browser is closed · polling every 5s`
              : `Connecting to Railway server… set URL via the ⚡ button above`
            }
          </p>
        : isRunning
          ? <p className="text-[10px] text-center text-muted-foreground -mt-2">
              ⚠️ Runs in browser tab — will stop when tab closes · set Railway URL above for 24/7
              {agentStatus && <> · <span className="text-accent font-mono">{agentStatus}</span></>}
            </p>
          : <p className="text-[10px] text-center text-muted-foreground -mt-2">
              ⚠️ Browser mode — stops when tab closes · set Railway URL above to run 24/7
            </p>
      }

      {/* ── Live coin signals ── */}
      {coinSignals.length > 0 && (
        <div>
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 flex items-center gap-1.5">
            <Activity className="w-3 h-3 text-accent" />Live Signals — last scan
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-5 gap-2">
            {coinSignals.map(sig => {
              const held = positions.some(p => p.symbol === sig.symbol);
              const wsP  = parseFloat(pricesRef.current[sig.symbol]?.price || '0') || sig.price;
              return (
                <div key={sig.symbol} className={`rounded-lg p-2.5 space-y-1.5 border ${sig.signal==='BUY'?'bg-gain/5 border-gain/30':held?'border-accent/30 bg-accent/5':'bg-secondary/40 border-border'}`}>
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

      {/* ── Open positions ── */}
      {positions.length > 0 && (
        <div>
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 flex items-center gap-1.5">
            <Zap className="w-3 h-3 text-warn"/>Open Positions ({positions.length})
          </div>
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
                  <div className="text-[10px] text-muted-foreground font-mono">
                    Entry {pos.avg_entry_price.toFixed(4)} USDT · BEP {bep.toFixed(4)} USDT · Now {live.toFixed(4)} USDT
                  </div>
                  <div className="space-y-1">
                    <div className="flex justify-between text-[9px]">
                      <span className="text-muted-foreground">Break-even progress</span>
                      <span className={prof?'text-gain font-bold':'text-warn'}>{prof?'✓ Profitable':bepProgress.toFixed(0)+'%'}</span>
                    </div>
                    <div className="h-1.5 bg-muted/40 rounded-full overflow-hidden">
                      <div className={`h-full rounded-full transition-all ${prof?'bg-gain':'bg-warn'}`} style={{width:`${bepProgress}%`}}/>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Trade history ── */}
      <div>
        <div className="flex items-center justify-between mb-2">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground flex items-center gap-1.5">
            {totalPnl>=0?<TrendingUp className="w-3 h-3 text-gain"/>:<TrendingDown className="w-3 h-3 text-loss"/>}
            Trade History ({trades.length})
          </div>
          {sellTrades.length > 0 && (
            <span className={`text-[10px] font-mono ${realizedPnl>=0?'text-gain':'text-loss'}`}>
              {realizedPnl>=0?'+':''}{realizedPnl.toFixed(4)} USDT realized
            </span>
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
