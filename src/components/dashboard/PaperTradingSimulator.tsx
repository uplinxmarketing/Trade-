import { useState, useEffect, useRef, useCallback } from 'react';
import {
  FlaskConical, PlayCircle, StopCircle, TrendingUp, TrendingDown,
  Activity, ShoppingCart, Banknote, RefreshCw, Zap,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import { calcEMA, calcRSI, calcMACD, calcBollingerBands, calcSMA } from '@/lib/indicators';
import { TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';

const BIN = 'https://api.binance.com/api/v3';
const SESSION = 'default';
const MAX_POSITIONS = 3;          // max simultaneous holdings
const STOP_LOSS_PCT = 0.015;      // 1.5% stop-loss
const TAKE_PROFIT_PCT = 0.004;    // 0.4% take-profit (covers 0.2% fees + 0.2% gain)
const TRADE_ALLOC_PCT = 0.20;     // use 20% of balance per trade
const MIN_TRADE_USDT = 11;

interface Position {
  symbol: string;
  quantity: number;
  entryPrice: number;
  bep: number;                    // break-even price
}

interface TradeLog {
  id: string;
  ts: string;
  symbol: string;
  side: 'BUY' | 'SELL';
  price: number;
  qty: number;
  pnl: number | null;
  reason: string;
}

interface CoinState {
  symbol: string;
  price: number;
  signal: 'BUY' | 'SELL' | 'HOLD' | 'loading' | 'error';
  rsi: number;
  emaBullish: boolean;
  macdPos: boolean;
  volUp: boolean;
  score: number;      // 0-4 simple signal count
  reason: string;
}

// ── Simple 4-signal scoring ──────────────────────────────────────────────────
async function analyseCoins(symbols: string[]): Promise<CoinState[]> {
  const results = await Promise.allSettled(
    symbols.map(async (sym): Promise<CoinState> => {
      try {
        const [kRes, dRes] = await Promise.all([
          fetch(`${BIN}/klines?symbol=${sym}&interval=5m&limit=60`),
          fetch(`${BIN}/depth?symbol=${sym}&limit=5`),
        ]);
        if (!kRes.ok || !dRes.ok) throw new Error('HTTP error');
        const klines = await kRes.json();
        const depth  = await dRes.json();
        if (!Array.isArray(klines) || klines.length < 30) throw new Error('Not enough data');

        const closes  = klines.map((k: any[]) => parseFloat(k[4]));
        const volumes = klines.map((k: any[]) => parseFloat(k[5]));
        const price   = closes[closes.length - 1];

        const ema9    = calcEMA(closes, 9);
        const ema21   = calcEMA(closes, 21);
        const rsi     = calcRSI(closes, 14);
        const macd    = calcMACD(closes);
        const volSma  = calcSMA(volumes, 20);
        const curVol  = volumes[volumes.length - 1] ?? 0;
        const volRat  = volSma > 0 ? curVol / volSma : 1;
        const bb      = calcBollingerBands(closes);

        const bids    = (depth.bids || []).slice(0, 5);
        const asks    = (depth.asks || []).slice(0, 5);
        const bidVol  = bids.reduce((s: number, b: string[]) => s + parseFloat(b[1]), 0);
        const askVol  = asks.reduce((s: number, a: string[]) => s + parseFloat(a[1]), 0);
        const obBull  = askVol > 0 && bidVol / askVol > 1.1;

        // Four simple boolean signals:
        const emaBullish = ema9 > ema21;                         // trend up
        const rsiOk      = rsi >= 30 && rsi < 68;               // not oversold/overbought
        const macdPos    = macd.histogram > 0;                   // momentum positive
        const volUp      = volRat > 1.1 || obBull;              // buyers present

        // BUY: need emaBullish + rsiOk + at least one of macd/vol
        // This fires in most normal trending conditions
        let score = 0;
        if (emaBullish) score++;
        if (rsiOk)      score++;
        if (macdPos)    score++;
        if (volUp)      score++;

        // BUY signal: 3+ out of 4, or emaBullish + rsiOk + bb near lower band
        const nearBBLow = bb.position < 0.35;
        const isBuy  = (score >= 3) || (emaBullish && rsiOk && nearBBLow);
        // HOLD if RSI > 68 or EMA bearish
        const isHold = !emaBullish || rsi >= 68 || rsi < 28;

        const parts: string[] = [];
        if (emaBullish) parts.push('EMA↑'); else parts.push('EMA↓');
        parts.push(`RSI ${rsi.toFixed(0)}`);
        if (macdPos) parts.push('MACD+'); else parts.push('MACD-');
        if (volUp) parts.push(`Vol${volRat.toFixed(1)}x`);
        if (nearBBLow) parts.push('BB-low');

        return {
          symbol: sym, price, rsi,
          emaBullish, macdPos, volUp,
          score,
          signal: isHold ? 'HOLD' : isBuy ? 'BUY' : 'HOLD',
          reason: parts.join(' · '),
        };
      } catch {
        return { symbol: sym, price: 0, rsi: 50, emaBullish: false, macdPos: false, volUp: false, score: 0, signal: 'error', reason: 'Fetch failed' };
      }
    })
  );
  return results.map((r, i) => r.status === 'fulfilled' ? r.value : {
    symbol: symbols[i], price: 0, rsi: 50, emaBullish: false, macdPos: false, volUp: false, score: 0, signal: 'error' as const, reason: 'Error',
  });
}

// ── Component ────────────────────────────────────────────────────────────────
interface Props {
  selectedCoins: string[];
  prices: LivePrices;
}

const PaperTradingSimulator = ({ selectedCoins, prices }: Props) => {
  const [active, setActive]       = useState(() => localStorage.getItem('pts_active') !== 'false');
  const [balance, setBalance]     = useState(1000);
  const [positions, setPositions] = useState<Position[]>([]);
  const [trades, setTrades]       = useState<TradeLog[]>([]);
  const [coinStates, setCoinStates] = useState<CoinState[]>([]);
  const [checking, setChecking]   = useState(false);
  const [lastRun, setLastRun]     = useState<Date | null>(null);
  const [totalPnl, setTotalPnl]   = useState(0);
  const [wins, setWins]           = useState(0);
  const [totalSells, setTotalSells] = useState(0);
  const [activityLog, setActivityLog] = useState<string[]>([]);

  const activeRef    = useRef(active);
  const balanceRef   = useRef(balance);
  const positionsRef = useRef(positions);
  const timerRef     = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => { activeRef.current = active;    }, [active]);
  useEffect(() => { balanceRef.current = balance;  }, [balance]);
  useEffect(() => { positionsRef.current = positions; }, [positions]);

  const addLog = useCallback((msg: string) => {
    const ts = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    setActivityLog(prev => [`[${ts}] ${msg}`, ...prev].slice(0, 40));
  }, []);

  // ── Load state from Supabase ──────────────────────────────────────────────
  const loadState = useCallback(async () => {
    const [cfgRes, posRes, trdRes] = await Promise.all([
      supabase.from('bot_config').select('current_balance, initial_balance').eq('user_session', SESSION).maybeSingle(),
      supabase.from('paper_portfolio').select('*').eq('user_session', SESSION),
      supabase.from('bot_trade_history').select('*').eq('user_session', SESSION).order('created_at', { ascending: false }).limit(50),
    ]);

    if (cfgRes.data) {
      const bal = cfgRes.data.current_balance ?? 1000;
      setBalance(bal);
    }

    const pos: Position[] = (posRes.data ?? []).map((p: any) => ({
      symbol: p.symbol,
      quantity: Number(p.quantity),
      entryPrice: Number(p.avg_entry_price),
      bep: Number(p.avg_entry_price) * (1 / Math.pow(1 - TAKER_FEE, 2)),
    }));
    setPositions(pos);

    const trdList: TradeLog[] = (trdRes.data ?? []).map((t: any) => ({
      id: t.id, ts: t.created_at, symbol: t.symbol,
      side: t.side, price: Number(t.price), qty: Number(t.quantity),
      pnl: t.pnl !== null ? Number(t.pnl) : null, reason: t.reason ?? '',
    }));
    setTrades(trdList);

    const sellTrades = trdList.filter(t => t.side === 'SELL' && t.pnl !== null);
    setTotalSells(sellTrades.length);
    setWins(sellTrades.filter(t => (t.pnl ?? 0) > 0).length);
    setTotalPnl(sellTrades.reduce((s, t) => s + (t.pnl ?? 0), 0));
  }, []);

  useEffect(() => { loadState(); }, [loadState]);

  // Realtime sync
  useEffect(() => {
    const ch = supabase.channel('pts-rt2')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadState)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadState)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_config' }, loadState)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadState]);

  // ── Execute a BUY ─────────────────────────────────────────────────────────
  const execBuy = useCallback(async (sym: string, price: number, reason: string) => {
    const bal = balanceRef.current;
    const alloc = Math.min(bal * TRADE_ALLOC_PCT, bal);
    if (alloc < MIN_TRADE_USDT) { addLog(`SKIP ${sym} — balance too low ($${bal.toFixed(2)})`); return; }

    const fee      = alloc * TAKER_FEE;
    const qty      = (alloc - fee) / price;
    const newBal   = bal - alloc;
    const bep      = price / Math.pow(1 - TAKER_FEE, 2);

    await Promise.all([
      supabase.from('bot_trade_history').insert({
        user_session: SESSION, symbol: sym, side: 'BUY', price, quantity: qty, pnl: null,
        reason: `[Paper] ${reason} · $${alloc.toFixed(2)} @ $${price.toFixed(4)} · fee $${fee.toFixed(4)}`,
      }),
      supabase.from('paper_portfolio').upsert({
        user_session: SESSION, symbol: sym, quantity: qty, avg_entry_price: price,
        updated_at: new Date().toISOString(),
      }, { onConflict: 'user_session,symbol' }),
      supabase.from('bot_config').update({ current_balance: newBal }).eq('user_session', SESSION),
    ]);

    setBalance(newBal);
    addLog(`BUY  ${sym.padEnd(10)} $${price.toFixed(4)} · ${reason}`);
    toast.success(`Paper BUY: ${sym.replace('USDT', '')}`, { description: `$${alloc.toFixed(2)} @ $${price.toFixed(4)}` });
  }, [addLog]);

  // ── Execute a SELL ────────────────────────────────────────────────────────
  const execSell = useCallback(async (pos: Position, price: number, reason: string) => {
    const proceeds = pos.quantity * price * (1 - TAKER_FEE);
    const cost     = pos.quantity * pos.entryPrice / (1 - TAKER_FEE);
    const pnl      = Math.round((proceeds - cost) * 10000) / 10000;
    const newBal   = balanceRef.current + proceeds;

    await Promise.all([
      supabase.from('bot_trade_history').insert({
        user_session: SESSION, symbol: pos.symbol, side: 'SELL', price, quantity: pos.quantity, pnl,
        reason: `[Paper] ${reason} · @ $${price.toFixed(4)} · pnl $${pnl.toFixed(4)}`,
      }),
      supabase.from('paper_portfolio').delete().eq('user_session', SESSION).eq('symbol', pos.symbol),
      supabase.from('bot_config').update({ current_balance: newBal }).eq('user_session', SESSION),
    ]);

    setBalance(newBal);
    const pctSign = pnl >= 0 ? '+' : '';
    addLog(`SELL ${pos.symbol.padEnd(10)} $${price.toFixed(4)} · ${reason} · P&L ${pctSign}$${pnl.toFixed(4)}`);
    if (pnl >= 0) {
      toast.success(`Paper SELL: ${pos.symbol.replace('USDT', '')} +$${pnl.toFixed(4)}`, { description: reason });
    } else {
      toast.error(`Paper SELL: ${pos.symbol.replace('USDT', '')} $${pnl.toFixed(4)}`, { description: reason });
    }
  }, [addLog]);

  // ── Main tick ─────────────────────────────────────────────────────────────
  const runTick = useCallback(async () => {
    if (!activeRef.current) return;
    setChecking(true);
    addLog('── Scanning coins…');

    try {
      // 1. Check exits on existing positions (use live WS price for speed)
      const currentPositions = positionsRef.current;
      for (const pos of currentPositions) {
        const wsPrice = parseFloat(prices[pos.symbol]?.price || '0');
        const price   = wsPrice > 0 ? wsPrice : pos.entryPrice;
        const pctGain = (price - pos.entryPrice) / pos.entryPrice;

        if (pctGain >= TAKE_PROFIT_PCT) {
          await execSell(pos, price, `Take-profit +${(pctGain * 100).toFixed(2)}%`);
        } else if (pctGain <= -STOP_LOSS_PCT) {
          await execSell(pos, price, `Stop-loss ${(pctGain * 100).toFixed(2)}%`);
        }
      }

      // 2. Check new entries (only if we have room for more positions)
      const heldAfter = positionsRef.current;
      const heldSet   = new Set(heldAfter.map(p => p.symbol));
      const openSlots = MAX_POSITIONS - heldAfter.length;

      if (openSlots > 0 && balanceRef.current >= MIN_TRADE_USDT) {
        const states = await analyseCoins(selectedCoins);
        setCoinStates(states);

        const buyCandidates = states
          .filter(s => s.signal === 'BUY' && !heldSet.has(s.symbol))
          .sort((a, b) => b.score - a.score);

        addLog(`Signals: ${states.map(s => `${s.symbol.replace('USDT','')}=${s.signal}`).join(' ')}`);

        let bought = 0;
        for (const s of buyCandidates) {
          if (bought >= openSlots) break;
          const wsPrice = parseFloat(prices[s.symbol]?.price || '0');
          const price   = wsPrice > 0 ? wsPrice : s.price;
          if (price <= 0) continue;
          await execBuy(s.symbol, price, s.reason);
          bought++;
        }

        if (buyCandidates.length === 0) {
          addLog(`No BUY signals this cycle (${states.filter(s => s.signal === 'HOLD').length} HOLD, ${states.filter(s => s.signal === 'error').length} error)`);
        }
      } else if (openSlots <= 0) {
        addLog(`Max ${MAX_POSITIONS} positions reached — watching for exits only`);
      }

      setLastRun(new Date());
    } catch (e: any) {
      addLog(`ERROR: ${e.message}`);
    } finally {
      setChecking(false);
    }
  }, [selectedCoins, prices, execBuy, execSell, addLog]);

  // ── Timer: run every 30s while active ────────────────────────────────────
  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current);
    if (active) {
      // Run immediately on start
      runTick();
      timerRef.current = setInterval(runTick, 30_000);
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  }, [active, runTick]);

  const toggleActive = () => {
    const next = !active;
    setActive(next);
    localStorage.setItem('pts_active', String(next));
    toast.info(next ? 'Paper bot started — runs every 30s' : 'Paper bot paused');
  };

  // ── Live P&L for open positions ───────────────────────────────────────────
  function getLivePnl(pos: Position) {
    const wsPrice = parseFloat(prices[pos.symbol]?.price || '0');
    const price   = wsPrice > 0 ? wsPrice : pos.entryPrice;
    return { price, pct: (price - pos.entryPrice) / pos.entryPrice * 100 };
  }

  const signalColor = (s: CoinState['signal']) =>
    s === 'BUY' ? 'text-gain' : s === 'SELL' ? 'text-loss' : s === 'error' ? 'text-muted-foreground' : 'text-warn';

  return (
    <div className="trading-card p-4 space-y-4">

      {/* ── Header ── */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <FlaskConical className="w-4 h-4 text-accent" />
          <span className="text-sm font-semibold">Paper Trading Bot</span>
          <span className={`text-[10px] px-2 py-0.5 rounded font-bold ${active ? 'bg-gain/20 text-gain' : 'bg-secondary text-muted-foreground'}`}>
            {active ? 'RUNNING' : 'PAUSED'}
          </span>
          {checking && <RefreshCw className="w-3 h-3 animate-spin text-accent" />}
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">
            {lastRun ? `Last scan ${lastRun.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}` : 'Not run yet'}
          </span>
          <Button size="sm" className={`h-7 text-xs ${active ? 'bg-destructive hover:bg-destructive/80 text-white' : 'bg-gain hover:bg-gain/80 text-black'}`} onClick={toggleActive}>
            {active ? <><StopCircle className="w-3 h-3 mr-1" />Pause</> : <><PlayCircle className="w-3 h-3 mr-1" />Start</>}
          </Button>
          <Button size="sm" variant="outline" className="h-7 text-xs" onClick={runTick} disabled={checking}>
            <Zap className="w-3 h-3 mr-1" />Run Now
          </Button>
        </div>
      </div>

      {/* ── Stats row ── */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {[
          { label: 'Balance', value: `$${balance.toFixed(2)}`, sub: '' },
          { label: 'Open Positions', value: String(positions.length), sub: `/ ${MAX_POSITIONS} max` },
          { label: 'Total P&L', value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(4)}`, sub: `${totalSells} closed`, color: totalPnl >= 0 ? 'text-gain' : 'text-loss' },
          { label: 'Win Rate', value: totalSells > 0 ? `${Math.round((wins / totalSells) * 100)}%` : '—', sub: `${wins}/${totalSells} wins` },
        ].map(s => (
          <div key={s.label} className="bg-secondary/50 rounded-lg p-3">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wide">{s.label}</div>
            <div className={`text-base font-mono font-bold mt-0.5 ${s.color || ''}`}>{s.value}</div>
            {s.sub && <div className="text-[10px] text-muted-foreground">{s.sub}</div>}
          </div>
        ))}
      </div>

      {/* ── Coin signal cards ── */}
      {coinStates.length > 0 && (
        <div>
          <div className="text-xs font-semibold text-muted-foreground mb-2 flex items-center gap-1.5">
            <Activity className="w-3.5 h-3.5" />
            Live Signals
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-5 gap-2">
            {coinStates.map(s => (
              <div key={s.symbol} className="bg-secondary/40 rounded-lg p-2.5 space-y-1">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-bold font-mono">{s.symbol.replace('USDT', '')}</span>
                  <span className={`text-[10px] font-bold ${signalColor(s.signal)}`}>{s.signal === 'loading' ? '…' : s.signal}</span>
                </div>
                <div className="text-xs font-mono text-foreground">
                  {s.price > 1 ? `$${s.price.toLocaleString('en-US', { maximumFractionDigits: 2 })}` : `$${s.price.toFixed(5)}`}
                </div>
                {/* Signal dots */}
                <div className="flex gap-1">
                  {[
                    { label: 'EMA', on: s.emaBullish },
                    { label: 'RSI', on: s.rsi >= 30 && s.rsi < 68 },
                    { label: 'MACD', on: s.macdPos },
                    { label: 'Vol', on: s.volUp },
                  ].map(dot => (
                    <div key={dot.label} className={`flex-1 h-1 rounded-full ${dot.on ? 'bg-gain' : 'bg-loss/40'}`} title={dot.label} />
                  ))}
                </div>
                <div className="text-[9px] text-muted-foreground leading-tight truncate">{s.reason}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Open positions ── */}
      {positions.length > 0 && (
        <div>
          <div className="text-xs font-semibold text-muted-foreground mb-2 flex items-center gap-1.5">
            <ShoppingCart className="w-3.5 h-3.5" />
            Open Positions
          </div>
          <div className="space-y-1.5">
            {positions.map(pos => {
              const { price, pct } = getLivePnl(pos);
              const bepProgress = Math.min(100, Math.max(0, ((price - pos.entryPrice) / (pos.bep - pos.entryPrice)) * 100));
              const profitable = price >= pos.bep;
              return (
                <div key={pos.symbol} className="bg-secondary/40 rounded-lg p-3 flex flex-wrap items-center gap-3">
                  <div className="flex-1 min-w-32">
                    <div className="flex items-center gap-2">
                      <span className="font-mono font-bold text-sm">{pos.symbol.replace('USDT', '')}</span>
                      <span className={`text-xs font-mono font-bold ${pct >= 0 ? 'text-gain' : 'text-loss'}`}>
                        {pct >= 0 ? '+' : ''}{pct.toFixed(3)}%
                      </span>
                    </div>
                    <div className="text-[10px] text-muted-foreground mt-0.5">
                      Entry ${pos.entryPrice.toFixed(4)} · BEP ${pos.bep.toFixed(4)} · Now ${price.toFixed(4)}
                    </div>
                    <div className="mt-1.5 space-y-0.5">
                      <div className="flex justify-between text-[9px] text-muted-foreground">
                        <span>BEP progress</span>
                        <span className={profitable ? 'text-gain font-bold' : ''}>{profitable ? '✓ Profitable' : `${bepProgress.toFixed(0)}%`}</span>
                      </div>
                      <div className="h-1.5 bg-secondary rounded-full overflow-hidden">
                        <div className={`h-full rounded-full transition-all ${profitable ? 'bg-gain' : 'bg-warn'}`} style={{ width: `${bepProgress}%` }} />
                      </div>
                    </div>
                  </div>
                  <Button
                    size="sm"
                    className="h-7 text-xs bg-loss hover:bg-loss/80 text-white shrink-0"
                    onClick={async () => {
                      await execSell(pos, price, 'Manual force-sell');
                      loadState();
                    }}
                  >
                    <Banknote className="w-3 h-3 mr-1" />Force Sell
                  </Button>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Trade history ── */}
      {trades.length > 0 && (
        <div>
          <div className="text-xs font-semibold text-muted-foreground mb-2 flex items-center gap-1.5">
            {totalPnl >= 0 ? <TrendingUp className="w-3.5 h-3.5 text-gain" /> : <TrendingDown className="w-3.5 h-3.5 text-loss" />}
            Trade History ({trades.length})
          </div>
          <div className="space-y-1 max-h-48 overflow-y-auto scrollbar-thin">
            {trades.slice(0, 20).map(t => (
              <div key={t.id} className="flex items-center justify-between text-xs bg-secondary/30 rounded px-3 py-1.5">
                <div className="flex items-center gap-2 min-w-0">
                  <span className={`shrink-0 w-8 text-center text-[10px] font-bold rounded px-0.5 ${t.side === 'BUY' ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'}`}>{t.side}</span>
                  <span className="font-mono font-semibold">{t.symbol.replace('USDT', '')}</span>
                  <span className="text-muted-foreground font-mono">${t.price > 1 ? t.price.toLocaleString('en-US', { maximumFractionDigits: 2 }) : t.price.toFixed(5)}</span>
                </div>
                <div className="flex items-center gap-3 shrink-0">
                  {t.pnl !== null && (
                    <span className={`font-mono font-bold ${t.pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                      {t.pnl >= 0 ? '+' : ''}${t.pnl.toFixed(4)}
                    </span>
                  )}
                  <span className="text-muted-foreground text-[10px]">
                    {new Date(t.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Activity log ── */}
      <div>
        <div className="text-xs font-semibold text-muted-foreground mb-1 flex items-center gap-1.5">
          <Activity className="w-3.5 h-3.5" />
          Activity Log
        </div>
        <div className="bg-secondary/30 rounded-lg p-2 max-h-36 overflow-y-auto scrollbar-thin space-y-0.5">
          {activityLog.length === 0 && (
            <div className="text-[10px] text-muted-foreground text-center py-2">
              {active ? 'Waiting for first scan…' : 'Start the bot to see activity'}
            </div>
          )}
          {activityLog.map((line, i) => (
            <div key={i} className={`text-[10px] font-mono ${line.includes('BUY') ? 'text-gain' : line.includes('SELL') ? 'text-loss' : line.includes('ERROR') ? 'text-destructive' : 'text-muted-foreground'}`}>
              {line}
            </div>
          ))}
        </div>
      </div>

      <div className="text-[10px] text-muted-foreground text-center">
        Paper mode — no real money. Scans every 30s · BUY: EMA↑ + RSI OK + 1 more signal · SELL: +0.4% profit or −1.5% stop-loss
      </div>
    </div>
  );
};

export default PaperTradingSimulator;
