import { useState, useEffect, useCallback, useRef } from 'react';
import {
  FlaskConical, RefreshCw, TrendingUp, TrendingDown,
  Zap, PlayCircle, StopCircle, ShoppingCart, Banknote,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import { TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import {
  fetchIndicators,
  executeBuyDecision,
  executeSellDecision,
  type OpenPosition,
  type RecentTrade,
} from '@/lib/ai-agent';

interface Props {
  selectedCoins: string[];
  prices: LivePrices;
}

interface CoinRow {
  symbol: string;
  price: number;
  score: number;
  rsi: number;
  ema9: number;
  ema21: number;
  macdHist: number;
  volumeRatio: number;
  bbPosition: number;
  change15m: number;
  loading: boolean;
  error: boolean;
}

const BEP_MULT = 1 / Math.pow(1 - TAKER_FEE, 2);

function scoreColor(score: number) {
  if (score >= 60) return '#0ecb81';
  if (score >= 45) return '#f0b90b';
  return '#f6465d';
}

function fmtPct(n: number) {
  return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
}

const PaperTradingSimulator = ({ selectedCoins, prices }: Props) => {
  const [rows, setRows]             = useState<CoinRow[]>([]);
  const [positions, setPositions]   = useState<OpenPosition[]>([]);
  const [trades, setTrades]         = useState<RecentTrade[]>([]);
  const [balance, setBalance]       = useState(10000);
  const [analysing, setAnalysing]   = useState(false);
  const [autoActive, setAutoActive] = useState(false);
  const [threshold, setThreshold]   = useState(40);
  const [forcingBuy, setForcingBuy] = useState<string | null>(null);
  const [forcingSell, setForcingSell] = useState<string | null>(null);
  const autoRef = useRef(false);
  const autoTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // ── Load initial data ───────────────────────────────────────────────────────
  const loadDB = useCallback(async () => {
    const [cfg, pos, trds] = await Promise.all([
      supabase.from('bot_config').select('current_balance').eq('user_session', 'default').maybeSingle(),
      supabase.from('paper_portfolio').select('*').eq('user_session', 'default'),
      supabase.from('bot_trade_history').select('*').eq('user_session', 'default').order('created_at', { ascending: false }).limit(20),
    ]);
    if (cfg.data) setBalance(cfg.data.current_balance ?? 10000);
    setPositions((pos.data ?? []) as OpenPosition[]);
    setTrades((trds.data ?? []) as RecentTrade[]);
  }, []);

  useEffect(() => { loadDB(); }, [loadDB]);

  // Realtime: positions + trades
  useEffect(() => {
    const ch = supabase.channel('pts-rt')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadDB)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadDB)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_config' }, loadDB)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadDB]);

  // ── Fetch indicators for all coins ─────────────────────────────────────────
  const runAnalysis = useCallback(async (quiet = false) => {
    if (!quiet) setAnalysing(true);
    const placeholders: CoinRow[] = selectedCoins.map(sym => ({
      symbol: sym, price: 0, score: 0, rsi: 0, ema9: 0, ema21: 0,
      macdHist: 0, volumeRatio: 1, bbPosition: 0.5, change15m: 0,
      loading: true, error: false,
    }));
    if (!quiet) setRows(placeholders);

    const results = await Promise.all(selectedCoins.map(sym => fetchIndicators(sym)));
    const updated: CoinRow[] = selectedCoins.map((sym, i) => {
      const r = results[i];
      if (!r) return { ...placeholders[i], loading: false, error: true };
      return {
        symbol: sym, loading: false, error: false,
        price: r.ind.price, score: r.score,
        rsi: r.ind.rsi, ema9: r.ind.ema9, ema21: r.ind.ema21,
        macdHist: r.ind.macd.histogram, volumeRatio: r.ind.volumeRatio,
        bbPosition: r.ind.bb.position, change15m: r.ind.change15m,
      };
    });
    setRows(updated);
    if (!quiet) setAnalysing(false);

    // Auto-trade: place buys on coins above threshold with no open position
    if (autoRef.current) {
      const heldSet = new Set(positions.map(p => p.symbol));
      let bal = balance;
      for (const row of updated) {
        if (row.error || row.loading) continue;
        if (heldSet.has(row.symbol)) continue;
        if (row.score < threshold) continue;
        const decision = { symbol: row.symbol, action: 'BUY' as const, confidence: row.score, reasoning: `Auto-test: score ${row.score}/${threshold}` };
        bal = await executeBuyDecision(decision, bal, prices, selectedCoins, supabase);
        await supabase.from('bot_config').update({ current_balance: bal }).eq('user_session', 'default');
        toast.success(`Auto-test BUY: ${row.symbol.replace('USDT', '')} @ $${row.price.toFixed(4)}`);
        heldSet.add(row.symbol);
      }
    }
  }, [selectedCoins, positions, balance, prices, threshold]);

  // Analyse on mount and every 60s
  useEffect(() => {
    runAnalysis();
    const t = setInterval(() => runAnalysis(true), 60_000);
    return () => clearInterval(t);
  }, [runAnalysis]);

  // Auto-trade timer (30s when active)
  useEffect(() => {
    autoRef.current = autoActive;
    if (autoActive) {
      autoTimerRef.current = setInterval(() => runAnalysis(true), 30_000);
    } else {
      if (autoTimerRef.current) clearInterval(autoTimerRef.current);
    }
    return () => { if (autoTimerRef.current) clearInterval(autoTimerRef.current); };
  }, [autoActive, runAnalysis]);

  // ── Force BUY ───────────────────────────────────────────────────────────────
  const forceBuy = useCallback(async (row: CoinRow) => {
    if (!prices[row.symbol]) {
      toast.error('No live price yet — wait a moment');
      return;
    }
    setForcingBuy(row.symbol);
    try {
      const decision = {
        symbol: row.symbol, action: 'BUY' as const,
        confidence: row.score,
        reasoning: `Force-buy test (score ${row.score}/100)`,
      };
      const newBal = await executeBuyDecision(decision, balance, prices, selectedCoins, supabase);
      await supabase.from('bot_config').update({ current_balance: newBal }).eq('user_session', 'default');
      toast.success(`Test BUY placed: ${row.symbol.replace('USDT', '')} @ $${parseFloat(prices[row.symbol]!.price).toFixed(4)}`);
      await loadDB();
    } catch (e: any) {
      toast.error('Force buy failed: ' + e.message);
    } finally {
      setForcingBuy(null);
    }
  }, [balance, prices, selectedCoins, loadDB]);

  // ── Force SELL ──────────────────────────────────────────────────────────────
  const forceSell = useCallback(async (pos: OpenPosition) => {
    if (!prices[pos.symbol]) {
      toast.error('No live price yet');
      return;
    }
    setForcingSell(pos.symbol);
    try {
      const decision = {
        symbol: pos.symbol, action: 'SELL' as const,
        confidence: 90,
        reasoning: 'Force-sell test',
      };
      const proceeds = await executeSellDecision(decision, pos, prices, supabase);
      const newBal = balance + proceeds;
      await supabase.from('bot_config').update({ current_balance: newBal }).eq('user_session', 'default');
      const livePrice = parseFloat(prices[pos.symbol]!.price);
      const pnlPct = ((livePrice - pos.avg_entry_price) / pos.avg_entry_price) * 100;
      toast.success(`Test SELL: ${pos.symbol.replace('USDT', '')} ${fmtPct(pnlPct)}`);
      await loadDB();
    } catch (e: any) {
      toast.error('Force sell failed: ' + e.message);
    } finally {
      setForcingSell(null);
    }
  }, [balance, prices, loadDB]);

  const posSet = new Set(positions.map(p => p.symbol));

  return (
    <div className="trading-card p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <FlaskConical className="w-4 h-4 text-accent" />
          <span className="text-sm font-semibold">Paper Trading Lab</span>
          <span className="text-xs text-muted-foreground bg-secondary px-2 py-0.5 rounded">
            No Binance API required
          </span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">Balance: ${balance.toFixed(2)}</span>
          <Button
            variant="outline" size="sm"
            className="h-7 text-xs"
            onClick={() => runAnalysis()}
            disabled={analysing}
          >
            <RefreshCw className={`w-3 h-3 mr-1 ${analysing ? 'animate-spin' : ''}`} />
            {analysing ? 'Analysing…' : 'Run Analysis'}
          </Button>
          <Button
            size="sm"
            className={`h-7 text-xs ${autoActive ? 'bg-destructive hover:bg-destructive/80' : 'bg-accent hover:bg-accent/80'} text-black`}
            onClick={() => {
              setAutoActive(a => !a);
              toast.info(autoActive ? 'Auto-test stopped' : `Auto-test ON — buys when score ≥ ${threshold}`);
            }}
          >
            {autoActive ? <StopCircle className="w-3 h-3 mr-1" /> : <PlayCircle className="w-3 h-3 mr-1" />}
            {autoActive ? 'Stop Auto' : 'Auto-Trade'}
          </Button>
        </div>
      </div>

      {/* Threshold slider */}
      <div className="flex items-center gap-3 text-xs">
        <span className="text-muted-foreground w-28 shrink-0">Entry threshold: <span className="font-mono font-bold text-foreground">{threshold}/100</span></span>
        <input
          type="range" min={20} max={80} step={5} value={threshold}
          onChange={e => setThreshold(Number(e.target.value))}
          className="flex-1 accent-accent"
        />
        <span className="text-muted-foreground w-20 text-right">
          {threshold <= 35 ? 'Very aggressive' : threshold <= 45 ? 'Aggressive' : threshold <= 55 ? 'Normal' : 'Conservative'}
        </span>
      </div>

      {/* Coin score cards */}
      {rows.length === 0 && !analysing && (
        <div className="text-center text-xs text-muted-foreground py-4">Click "Run Analysis" to fetch live indicator scores</div>
      )}
      {analysing && rows.length === 0 && (
        <div className="text-center text-xs text-muted-foreground py-4 animate-pulse">Fetching indicators from Binance…</div>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-3">
        {rows.map(row => {
          const holding = posSet.has(row.symbol);
          const pos = positions.find(p => p.symbol === row.symbol);
          const livePrice = parseFloat(prices[row.symbol]?.price || '0') || row.price;
          const pnlPct = pos && livePrice > 0
            ? ((livePrice - pos.avg_entry_price) / pos.avg_entry_price) * 100 : null;
          const bep = pos ? pos.avg_entry_price * BEP_MULT : null;
          const bepReached = bep && livePrice >= bep;
          const meetsThreshold = row.score >= threshold;

          return (
            <div key={row.symbol} className={`bg-secondary/50 border rounded-lg p-3 space-y-2 ${holding ? 'border-accent/50' : 'border-border'}`}>
              {/* Coin header */}
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-1.5">
                  <span className="font-mono font-bold text-sm">{row.symbol.replace('USDT', '')}</span>
                  {holding && <span className="text-[10px] bg-accent/20 text-accent px-1.5 py-0.5 rounded font-semibold">HOLDING</span>}
                </div>
                <span className={`text-xs font-mono ${row.change15m >= 0 ? 'text-gain' : 'text-loss'}`}>
                  {row.loading ? '…' : fmtPct(row.change15m)}
                </span>
              </div>

              {/* Price */}
              <div className="text-base font-mono font-semibold">
                {row.loading ? '—' : `$${livePrice > 1 ? livePrice.toLocaleString('en-US', { maximumFractionDigits: 2 }) : livePrice.toFixed(6)}`}
              </div>

              {/* Score bar */}
              {!row.loading && !row.error && (
                <div className="space-y-1">
                  <div className="flex justify-between text-[11px]">
                    <span className="text-muted-foreground">Entry score</span>
                    <span className="font-bold font-mono" style={{ color: scoreColor(row.score) }}>
                      {row.score}/100
                    </span>
                  </div>
                  <div className="h-1.5 bg-secondary rounded-full overflow-hidden">
                    <div
                      className="h-full rounded-full transition-all duration-500"
                      style={{ width: `${row.score}%`, backgroundColor: scoreColor(row.score) }}
                    />
                  </div>
                  {/* Threshold marker */}
                  <div className="relative h-1.5 -mt-1">
                    <div
                      className="absolute top-0 w-0.5 h-full bg-foreground/30"
                      style={{ left: `${threshold}%` }}
                    />
                  </div>
                  <div className="text-[10px] text-muted-foreground">
                    {meetsThreshold
                      ? `✓ Above threshold — eligible for auto-trade`
                      : `${threshold - row.score} pts below threshold`}
                  </div>
                </div>
              )}

              {/* Key indicators */}
              {!row.loading && !row.error && (
                <div className="grid grid-cols-2 gap-x-4 gap-y-0.5 text-[10px] text-muted-foreground">
                  <span>RSI <span className={`font-mono font-bold ${row.rsi < 30 ? 'text-gain' : row.rsi > 70 ? 'text-loss' : 'text-foreground'}`}>{row.rsi.toFixed(1)}</span></span>
                  <span>Vol <span className="font-mono font-bold text-foreground">{row.volumeRatio.toFixed(1)}×</span></span>
                  <span>EMA9 {row.ema9 > row.ema21 ? <span className="text-gain">↑</span> : <span className="text-loss">↓</span>}</span>
                  <span>MACD <span className={`font-mono ${row.macdHist > 0 ? 'text-gain' : 'text-loss'}`}>{row.macdHist > 0 ? '+' : ''}{row.macdHist.toFixed(5)}</span></span>
                  <span>BB pos <span className="font-mono text-foreground">{(row.bbPosition * 100).toFixed(0)}%</span></span>
                </div>
              )}
              {row.error && <div className="text-[10px] text-loss">Failed to fetch — Binance may be rate-limiting</div>}
              {row.loading && <div className="text-[10px] text-muted-foreground animate-pulse">Loading indicators…</div>}

              {/* Position info if holding */}
              {pos && pnlPct !== null && (
                <div className={`text-[11px] rounded px-2 py-1 ${bepReached ? 'bg-gain/10 text-gain' : 'bg-warn/10 text-warn'}`}>
                  {bepReached ? '✓ Profitable' : '⏳ Below breakeven'} · P&L {fmtPct(pnlPct)}
                  {bep && <> · BEP ${bep.toFixed(4)}</>}
                </div>
              )}

              {/* Action buttons */}
              <div className="flex gap-2">
                {!holding ? (
                  <Button
                    size="sm"
                    className="flex-1 h-7 text-xs bg-gain hover:bg-gain/80 text-black"
                    onClick={() => forceBuy(row)}
                    disabled={!!forcingBuy || row.loading || row.error}
                  >
                    <ShoppingCart className={`w-3 h-3 mr-1 ${forcingBuy === row.symbol ? 'animate-spin' : ''}`} />
                    {forcingBuy === row.symbol ? 'Placing…' : 'Force BUY'}
                  </Button>
                ) : (
                  <Button
                    size="sm"
                    className="flex-1 h-7 text-xs bg-loss hover:bg-loss/80 text-white"
                    onClick={() => pos && forceSell(pos)}
                    disabled={!!forcingSell}
                  >
                    <Banknote className={`w-3 h-3 mr-1 ${forcingSell === row.symbol ? 'animate-spin' : ''}`} />
                    {forcingSell === row.symbol ? 'Selling…' : 'Force SELL'}
                  </Button>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Recent trades */}
      {trades.length > 0 && (
        <div className="space-y-2">
          <div className="text-xs font-semibold text-muted-foreground flex items-center gap-1.5">
            <Zap className="w-3.5 h-3.5" />
            Recent Test Trades
          </div>
          <div className="space-y-1">
            {trades.slice(0, 8).map((t, i) => {
              const isWin = (t.pnl ?? 0) > 0;
              return (
                <div key={i} className="flex items-center justify-between text-xs bg-secondary/40 rounded px-3 py-1.5">
                  <div className="flex items-center gap-2">
                    <span className={`w-8 text-center text-[10px] font-bold rounded px-1 ${t.side === 'BUY' ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'}`}>
                      {t.side}
                    </span>
                    <span className="font-mono font-medium">{t.symbol.replace('USDT', '')}</span>
                    <span className="text-muted-foreground font-mono">${t.price.toFixed(4)}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    {t.pnl !== null && (
                      <span className={`font-mono font-bold ${isWin ? 'text-gain' : 'text-loss'}`}>
                        {t.pnl >= 0 ? '+' : ''}${t.pnl.toFixed(4)}
                        {isWin ? <TrendingUp className="inline w-3 h-3 ml-1" /> : <TrendingDown className="inline w-3 h-3 ml-1" />}
                      </span>
                    )}
                    <span className="text-muted-foreground text-[10px]">
                      {new Date(t.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

export default PaperTradingSimulator;
