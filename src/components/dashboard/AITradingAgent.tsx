import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Play, Square, Brain, TrendingUp, TrendingDown, Zap,
  RotateCcw, ChevronDown, ChevronUp, FlaskConical,
  DollarSign, Pencil, Check, X, BookOpen, Activity,
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
const ALLOC_PCT     = 0.25;   // 25% of balance per trade
const MIN_USDT      = 11;

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

    const ema9   = calcEMA(closes, 9);
    const ema21  = calcEMA(closes, 21);
    const rsi    = calcRSI(closes, 14);
    const macd   = calcMACD(closes);
    const volSma = calcSMA(volumes, 20);
    const curVol = volumes[volumes.length - 1] ?? 0;
    const volRat = volSma > 0 ? curVol / volSma : 1;
    const bb     = calcBollingerBands(closes);

    const recentVol = volumes.slice(-10).reduce((a, b) => a + b, 0);
    const prevVol   = volumes.slice(-20, -10).reduce((a, b) => a + b, 0);
    const volTrend  = recentVol > prevVol;

    const emaBullish = ema9 > ema21;
    const rsiOk      = rsi >= 28 && rsi < 70;
    const macdPos    = macd.histogram > 0;
    const volUp      = volRat > 1.05 || volTrend;
    const nearLow    = bb.position < 0.40;

    let score = 0;
    if (emaBullish) score++;
    if (rsiOk)      score++;
    if (macdPos)    score++;
    if (volUp)      score++;

    // BUY: 3/4 signals, or EMA+RSI+near-BB-low (relaxed entry for learning)
    const isBuy  = score >= 3 || (emaBullish && rsiOk && nearLow);
    const isHold = rsi >= 72 || rsi < 24;   // only block extreme RSI

    const parts: string[] = [];
    parts.push(emaBullish ? 'EMA↑' : 'EMA↓');
    parts.push(`RSI ${rsi.toFixed(0)}`);
    parts.push(macdPos ? 'MACD+' : 'MACD-');
    if (volUp) parts.push(`Vol ${volRat.toFixed(1)}×`);
    if (nearLow) parts.push('BB-low');

    return {
      symbol: sym, price, rsi,
      emaBullish, rsiOk, macdPos, volUp,
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
}

const PRESET_BUDGETS  = [500, 1000, 5000, 10000];
const INSTRUCTIONS_KEY = 'ai_agent_instructions';
const AGENT_CYCLE_MS   = 30_000;
const BEP_MULT         = 1 / Math.pow(1 - TAKER_FEE, 2);

// ── Component ────────────────────────────────────────────────────────────────
const AITradingAgent = ({ selectedCoins, prices, binanceConnected, onConnectBinance }: AITradingAgentProps) => {
  const [mode, setMode]           = useState<'test' | 'live'>('test');
  const [isRunning, setIsRunning] = useState(false);
  const [loading, setLoading]     = useState(false);
  const [totalBudget, setTotalBudget] = useState(1000);
  const [balance, setBalance]     = useState(1000);
  const [positions, setPositions] = useState<OpenPosition[]>([]);
  const [trades, setTrades]       = useState<TradeRow[]>([]);
  const [coinSignals, setCoinSignals] = useState<CoinSignal[]>([]);
  const [cycleCountdown, setCycleCountdown] = useState(0);
  const [agentStatus, setAgentStatus]       = useState('');
  const [scanning, setScanning]   = useState(false);
  const [showAll, setShowAll]     = useState(false);
  const [forcingBuy, setForcingBuy]   = useState<string | null>(null);
  const [forcingSell, setForcingSell] = useState<string | null>(null);
  const [editingBudget, setEditingBudget] = useState(false);
  const [budgetDraft, setBudgetDraft]     = useState('');
  const [instructions, setInstructions]   = useState(() => localStorage.getItem(INSTRUCTIONS_KEY) ?? '');
  const [editingInstr, setEditingInstr]   = useState(false);
  const [instrDraft, setInstrDraft]       = useState('');
  const [actLog, setActLog]       = useState<string[]>([]);
  const [showLog, setShowLog]     = useState(false);

  const isRunningRef   = useRef(false);
  const processingRef  = useRef(false);
  const balanceRef     = useRef(balance);
  const positionsRef   = useRef(positions);
  const cycleTimerRef  = useRef<ReturnType<typeof setTimeout> | null>(null);
  const countdownRef   = useRef<ReturnType<typeof setInterval> | null>(null);
  const stopLossRef    = useRef(1.5);

  useEffect(() => { isRunningRef.current = isRunning; },   [isRunning]);
  useEffect(() => { balanceRef.current   = balance; },     [balance]);
  useEffect(() => { positionsRef.current = positions; },   [positions]);
  useEffect(() => { localStorage.setItem(INSTRUCTIONS_KEY, instructions); }, [instructions]);

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
    if (trdRes.data)  setTrades(trdRes.data as TradeRow[]);
    if (posRes.data)  setPositions(posRes.data as OpenPosition[]);
    if (cfgRes.data) {
      const bal = Number(cfgRes.data.current_balance);
      setBalance(bal); balanceRef.current = bal;
      stopLossRef.current = Number(cfgRes.data.stop_loss_percent ?? 1.5);
      const running = Boolean(cfgRes.data.is_running);
      isRunningRef.current = running;
      setIsRunning(running);
    }
  }, []);

  useEffect(() => {
    loadData();
    const ch = supabase.channel('ata-rt')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_config' }, loadData)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadData]);

  // ── Exit checker on every WS price tick ─────────────────────────────────
  const pricesRef = useRef(prices);
  useEffect(() => { pricesRef.current = prices; }, [prices]);

  useEffect(() => {
    if (!isRunning || processingRef.current) return;
    if (!Object.keys(prices).length) return;
    processingRef.current = true;
    checkExits(prices, supabase, stopLossRef.current)
      .then(n => { if (n > 0) { loadData(); toast.success(`Closed ${n} position(s)`, { duration: 2500 }); } })
      .catch(() => {})
      .finally(() => { processingRef.current = false; });
  }, [prices]); // eslint-disable-line

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
      signals.forEach(s => addLog(`  ${s.symbol.replace('USDT','').padEnd(6)} ${s.signal.padEnd(5)} ${s.reason}`));

      // — Execute BUYs —
      const openSlots = MAX_POSITIONS - heldSet.size;
      if (openSlots > 0 && runBal >= MIN_USDT) {
        for (const sig of buySigs.slice(0, openSlots)) {
          if (heldSet.has(sig.symbol)) continue;
          const wsPrice = parseFloat(pricesRef.current[sig.symbol]?.price || '0');
          const price   = wsPrice > 0 ? wsPrice : sig.price;
          if (!price) continue;

          const alloc = Math.min(runBal * ALLOC_PCT, runBal);
          if (alloc < MIN_USDT) { addLog(`  SKIP ${sig.symbol} — alloc $${alloc.toFixed(2)} too low`); continue; }

          const fee = alloc * TAKER_FEE;
          const qty = (alloc - fee) / price;

          await Promise.all([
            supabase.from('bot_trade_history').insert({
              user_session: SESSION, symbol: sig.symbol,
              side: 'BUY', price, quantity: qty, pnl: null,
              reason: `[AI Paper] ${sig.reason} · $${alloc.toFixed(2)} @ $${price.toFixed(4)}`,
            }),
            supabase.from('paper_portfolio').upsert({
              user_session: SESSION, symbol: sig.symbol,
              quantity: qty, avg_entry_price: price,
              updated_at: new Date().toISOString(),
            }, { onConflict: 'user_session,symbol' }),
          ]);

          runBal -= alloc;
          heldSet.add(sig.symbol);
          addLog(`  BUY  ${sig.symbol} @ $${price.toFixed(4)} · $${alloc.toFixed(2)}`);
          toast.info(`AI Paper BUY: ${sig.symbol.replace('USDT','')}`, { description: `$${alloc.toFixed(2)} @ $${price.toFixed(4)}`, duration: 3000 });
        }
      } else if (openSlots <= 0) {
        addLog(`  Max ${MAX_POSITIONS} positions held — exits handled by price ticker`);
      }

      await supabase.from('bot_config').update({
        current_balance: Math.round(runBal * 10000) / 10000,
        updated_at: new Date().toISOString(),
      }).eq('user_session', SESSION);

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

  // ── Start / Stop ─────────────────────────────────────────────────────────
  const toggleBot = async () => {
    if (mode === 'live' && !binanceConnected) { toast.error('Connect Binance API first'); onConnectBinance?.(); return; }
    setLoading(true);
    try {
      if (!isRunning) {
        await supabase.from('bot_config').upsert({
          user_session: SESSION,
          selected_coins: selectedCoins,
          mode, is_running: true,
          current_balance: totalBudget,
          initial_balance: totalBudget,
          stop_loss_percent: 1.5,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session' });
        isRunningRef.current = true;
        setIsRunning(true);
        setBalance(totalBudget); balanceRef.current = totalBudget;
        addLog(`=== Agent STARTED · $${totalBudget} · ${mode.toUpperCase()} ===`);
        toast.success(`AI Agent started — PAPER mode`, { description: `$${totalBudget.toLocaleString()} · ${selectedCoins.length} coins · every 30s` });
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
      const bal   = balanceRef.current;
      const alloc = Math.min(bal * ALLOC_PCT, bal);
      if (alloc < MIN_USDT) { toast.error(`Balance too low ($${bal.toFixed(2)})`); return; }
      const fee = alloc * TAKER_FEE;
      const qty = (alloc - fee) / wsPrice;
      const newBal = bal - alloc;
      await Promise.all([
        supabase.from('bot_trade_history').insert({
          user_session: SESSION, symbol: sym, side: 'BUY',
          price: wsPrice, quantity: qty, pnl: null,
          reason: `[Force BUY] manual test · $${alloc.toFixed(2)} @ $${wsPrice.toFixed(4)}`,
        }),
        supabase.from('paper_portfolio').upsert({
          user_session: SESSION, symbol: sym, quantity: qty, avg_entry_price: wsPrice,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session,symbol' }),
        supabase.from('bot_config').update({ current_balance: newBal }).eq('user_session', SESSION),
      ]);
      addLog(`FORCE BUY ${sym} @ $${wsPrice.toFixed(4)} · $${alloc.toFixed(2)}`);
      toast.success(`Force BUY: ${sym.replace('USDT','')} @ $${wsPrice.toFixed(4)}`);
      await loadData();
    } finally { setForcingBuy(null); }
  }, [addLog, loadData]);

  // ── Force SELL ───────────────────────────────────────────────────────────
  const forceSell = useCallback(async (pos: OpenPosition) => {
    const wsPrice = parseFloat(pricesRef.current[pos.symbol]?.price || '0');
    if (!wsPrice) { toast.error('No live price yet'); return; }
    setForcingSell(pos.symbol);
    try {
      const proceeds = pos.quantity * wsPrice * (1 - TAKER_FEE);
      const cost     = pos.quantity * pos.avg_entry_price / (1 - TAKER_FEE);
      const pnl      = Math.round((proceeds - cost) * 10000) / 10000;
      const newBal   = balanceRef.current + proceeds;
      await Promise.all([
        supabase.from('bot_trade_history').insert({
          user_session: SESSION, symbol: pos.symbol, side: 'SELL',
          price: wsPrice, quantity: pos.quantity, pnl,
          reason: `[Force SELL] manual · @ $${wsPrice.toFixed(4)} · pnl ${pnl>=0?'+':''}$${pnl.toFixed(4)}`,
        }),
        supabase.from('paper_portfolio').delete().eq('user_session', SESSION).eq('symbol', pos.symbol),
        supabase.from('bot_config').update({ current_balance: newBal }).eq('user_session', SESSION),
      ]);
      addLog(`FORCE SELL ${pos.symbol} @ $${wsPrice.toFixed(4)} · P&L ${pnl>=0?'+':''}$${pnl.toFixed(4)}`);
      toast[pnl >= 0 ? 'success' : 'error'](`Force SELL: ${pos.symbol.replace('USDT','')} ${pnl>=0?'+':''}$${pnl.toFixed(4)}`);
      await loadData();
    } finally { setForcingSell(null); }
  }, [addLog, loadData]);

  // ── Reset ────────────────────────────────────────────────────────────────
  const resetBot = async () => {
    if (!confirm('Reset all paper trades and restore budget?')) return;
    if (cycleTimerRef.current) clearTimeout(cycleTimerRef.current);
    if (countdownRef.current)  clearInterval(countdownRef.current);
    isRunningRef.current = false;
    await Promise.all([
      supabase.from('bot_trade_history').delete().eq('user_session', SESSION),
      supabase.from('paper_portfolio').delete().eq('user_session', SESSION),
      supabase.from('bot_config').update({
        current_balance: totalBudget, initial_balance: totalBudget,
        is_running: false, updated_at: new Date().toISOString(),
      }).eq('user_session', SESSION),
    ]);
    setTrades([]); setPositions([]); setBalance(totalBudget);
    setIsRunning(false); setCycleCountdown(0); setActLog([]);
    toast.success(`Reset · $${totalBudget.toLocaleString()} USDT restored`);
  };

  // ── Computed stats ───────────────────────────────────────────────────────
  const sellTrades  = trades.filter(t => t.side === 'SELL' && t.pnl !== null);
  const wins        = sellTrades.filter(t => (t.pnl ?? 0) > 0).length;
  const totalPnl    = sellTrades.reduce((s, t) => s + (t.pnl ?? 0), 0);
  const winRate     = sellTrades.length ? Math.round((wins / sellTrades.length) * 100) : 0;
  const pnlColor    = totalPnl >= 0 ? 'text-gain' : 'text-loss';
  const pnlPct      = totalBudget > 0 ? ((totalPnl / totalBudget) * 100).toFixed(2) : '0.00';
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
          {scanning
            ? <span className="text-[9px] text-accent font-mono flex items-center gap-1"><RefreshCw className="w-2.5 h-2.5 animate-spin" />Checking signals…</span>
            : isRunning && cycleCountdown > 0
              ? <span className="text-[9px] text-muted-foreground font-mono">next signal check in {cycleCountdown}s</span>
              : null
          }
        </div>
        <div className="flex items-center gap-1.5">
          {isRunning && (
            <Button size="sm" variant="outline" className="h-6 text-[10px] px-2" onClick={() => runCycle()} disabled={scanning}>
              <Zap className="w-3 h-3 mr-0.5" />Now
            </Button>
          )}
          <button onClick={resetBot} className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground" title="Reset">
            <RotateCcw className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

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

      {mode === 'live' && (
        <div className={`rounded-md px-3 py-2 text-xs ${binanceConnected ? 'bg-loss/10 border border-loss/30 text-loss' : 'bg-warn/10 border border-warn/30 text-warn'}`}>
          {binanceConnected ? '⚠️ LIVE MODE — real USDT will be used.' : <span>Binance API not connected. <button onClick={onConnectBinance} className="underline font-semibold">Connect now →</button></span>}
        </div>
      )}

      {/* ── Budget ── */}
      <div className="bg-muted/20 border border-border rounded-md px-3 py-2 space-y-2">
        <div className="flex items-center gap-2">
          <DollarSign className="w-3.5 h-3.5 text-muted-foreground" />
          <span className="text-[10px] uppercase tracking-widest text-muted-foreground">Paper Budget</span>
        </div>
        {editingBudget ? (
          <div className="flex items-center gap-2">
            <input type="number" value={budgetDraft} min={100} step={100} autoFocus
              onChange={e => setBudgetDraft(e.target.value)}
              onKeyDown={e => { if (e.key==='Enter') { setTotalBudget(Math.max(100,Number(budgetDraft)||1000)); setEditingBudget(false); } if (e.key==='Escape') setEditingBudget(false); }}
              className="flex-1 bg-muted/40 border border-accent rounded px-2 py-1 text-xs font-mono focus:outline-none" />
            <button onClick={() => { setTotalBudget(Math.max(100,Number(budgetDraft)||1000)); setEditingBudget(false); }} className="p-1 rounded hover:bg-gain/20 text-gain"><Check className="w-3.5 h-3.5" /></button>
            <button onClick={() => setEditingBudget(false)} className="p-1 rounded hover:bg-loss/20 text-loss"><X className="w-3.5 h-3.5" /></button>
          </div>
        ) : (
          <div className="flex items-center gap-1 flex-wrap">
            {PRESET_BUDGETS.map(p => (
              <button key={p} onClick={() => { if (!isRunning) setTotalBudget(p); }} disabled={isRunning}
                className={`text-[10px] px-2 py-1 rounded font-mono transition-colors disabled:opacity-50 ${totalBudget===p?'bg-accent text-accent-foreground':'bg-muted/50 text-muted-foreground hover:text-foreground'}`}>
                ${p.toLocaleString()}
              </button>
            ))}
            <button onClick={() => { setBudgetDraft(String(totalBudget)); setEditingBudget(true); }} disabled={isRunning}
              className="p-1 rounded hover:bg-muted/40 text-muted-foreground disabled:opacity-50"><Pencil className="w-3 h-3" /></button>
          </div>
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
          { label: 'Balance', value: `$${balance.toFixed(2)}`, color: '' },
          { label: 'Net P&L', value: `${totalPnl>=0?'+':''}$${Math.abs(totalPnl).toFixed(2)}`, color: pnlColor },
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
      <Button onClick={toggleBot} disabled={loading||!selectedCoins.length}
        className={`w-full font-semibold py-5 ${isRunning?'bg-loss/90 hover:bg-loss text-white':'bg-gain/90 hover:bg-gain text-background'}`}>
        {loading ? <span className="animate-spin mr-1.5">⟳</span>
          : isRunning ? <><Square className="w-4 h-4 mr-1.5"/>Stop Agent</>
          : <><Play className="w-4 h-4 mr-1.5"/>Start AI Agent — Paper Test</>}
      </Button>
      {isRunning
        ? <p className="text-[10px] text-center text-muted-foreground -mt-2">
            Every 30s: fetches live candles → checks EMA / RSI / MACD / Volume → buys or holds
            {agentStatus && <> · <span className="text-accent font-mono">{agentStatus}</span></>}
          </p>
        : <p className="text-[10px] text-center text-muted-foreground -mt-2">
            Analyses {selectedCoins.length} coins every 30s · EMA+RSI+MACD+Volume signals · no API key needed
          </p>
      }
      {!isRunning && <p className="text-[10px] text-center text-muted-foreground -mt-2">Scans {selectedCoins.length} coins every 30s · EMA+RSI+MACD+Vol signals · no API key needed</p>}

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
                  <div className="text-xs font-mono">{wsP > 1 ? `$${wsP.toLocaleString('en-US',{maximumFractionDigits:2})}` : `$${wsP.toFixed(5)}`}</div>
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
                      <span className={`font-mono text-xs font-bold ${uPnl>=0?'text-gain':'text-loss'}`}>{uPnl>=0?'+':''}${uPnl.toFixed(4)}</span>
                      <button onClick={() => forceSell(pos)} disabled={!!forcingSell}
                        className="text-[10px] px-2 py-1 rounded bg-loss/10 hover:bg-loss/20 text-loss font-semibold disabled:opacity-40 flex items-center gap-1">
                        <Banknote className="w-2.5 h-2.5"/>{forcingSell===pos.symbol?'…':'Sell'}
                      </button>
                    </div>
                  </div>
                  <div className="text-[10px] text-muted-foreground font-mono">
                    Entry ${pos.avg_entry_price.toFixed(4)} · BEP ${bep.toFixed(4)} · Now ${live.toFixed(4)}
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
            <div className="flex items-center gap-3 text-[10px] font-mono">
              <span className="text-muted-foreground">{wins}W/{sellTrades.length-wins}L</span>
              <span className={pnlColor}>{totalPnl>=0?'+':''}${totalPnl.toFixed(4)}</span>
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
                    <span className="text-muted-foreground font-mono text-[10px] truncate">${Number(t.price).toLocaleString('en-US',{maximumFractionDigits:4})}</span>
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    {t.pnl!==null && <span className={`font-mono font-bold text-[10px] ${t.pnl>=0?'text-gain':'text-loss'}`}>{t.pnl>=0?'+':''}${t.pnl.toFixed(4)}</span>}
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
