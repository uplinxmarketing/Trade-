import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Play, Square, Brain, TrendingUp, TrendingDown, Zap,
  RotateCcw, ChevronDown, ChevronUp, FlaskConical,
  DollarSign, Settings2,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import {
  updateSignals, checkExits, checkEntries, runAnalysisCycle,
  type SignalData, type LivePrices,
} from '@/lib/trading-engine';

interface Trade {
  id: string; symbol: string; side: 'BUY' | 'SELL';
  price: number; quantity: number; pnl: number | null;
  reason: string | null; created_at: string;
}
interface Position {
  symbol: string; quantity: number; avg_entry_price: number;
}

interface AIBotPanelProps {
  selectedCoins: string[];
  prices: LivePrices;
  binanceConnected?: boolean;
  onConnectBinance?: () => void;
}

const PRESET_BUDGETS = [1000, 5000, 10000, 25000, 50000];

const AIBotPanel = ({ selectedCoins, prices, binanceConnected, onConnectBinance }: AIBotPanelProps) => {
  const [mode, setMode] = useState<'test' | 'live'>('test');
  const [isRunning, setIsRunning] = useState(false);
  const [loading, setLoading] = useState(false);
  const [showBudget, setShowBudget] = useState(false);
  const [showAllTrades, setShowAllTrades] = useState(false);

  // Budget
  const [totalBudget, setTotalBudget] = useState(10000);
  const [equalSplit, setEqualSplit] = useState(true);
  const [coinBudgets, setCoinBudgets] = useState<Record<string, number>>({});

  // Bot state
  const [trades, setTrades]       = useState<Trade[]>([]);
  const [positions, setPositions] = useState<Position[]>([]);
  const [balance, setBalance]     = useState(10000);
  const [totalPnl, setTotalPnl]   = useState(0);
  const [winRate, setWinRate]     = useState(0);
  const [analysisStatus, setAnalysisStatus] = useState('Initializing analysis...');

  // Refs — avoid stale closures in the price-tick effect
  const signalsRef      = useRef<Record<string, SignalData>>({});
  const isRunningRef    = useRef(false);
  const processingRef   = useRef(false);
  const coinBudgetsRef  = useRef<Record<string, number>>({});
  const selectedRef     = useRef(selectedCoins);

  useEffect(() => { isRunningRef.current = isRunning; }, [isRunning]);
  useEffect(() => { selectedRef.current = selectedCoins; }, [selectedCoins]);
  useEffect(() => {
    const perCoin = totalBudget / Math.max(selectedCoins.length, 1);
    const computed = equalSplit
      ? Object.fromEntries(selectedCoins.map(s => [s, perCoin]))
      : coinBudgets;
    coinBudgetsRef.current = computed;
    if (equalSplit) setCoinBudgets(computed);
  }, [equalSplit, totalBudget, selectedCoins]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadData = useCallback(async () => {
    const [tradesRes, posRes, cfgRes] = await Promise.all([
      supabase.from('bot_trade_history').select('*').eq('user_session', 'default').order('created_at', { ascending: false }).limit(100),
      supabase.from('paper_portfolio').select('*').eq('user_session', 'default').gt('quantity', 0),
      supabase.from('bot_config').select('*').eq('user_session', 'default').maybeSingle(),
    ]);
    if (tradesRes.data) {
      const t = tradesRes.data as Trade[];
      setTrades(t);
      const sells = t.filter(x => x.side === 'SELL' && x.pnl !== null);
      setTotalPnl(Math.round(sells.reduce((s, x) => s + (x.pnl || 0), 0) * 100) / 100);
      const wins = sells.filter(x => (x.pnl || 0) > 0).length;
      setWinRate(sells.length ? Math.round((wins / sells.length) * 100) : 0);
    }
    if (posRes.data)  setPositions(posRes.data as Position[]);
    if (cfgRes.data) {
      isRunningRef.current = cfgRes.data.is_running as boolean;
      setIsRunning(cfgRes.data.is_running as boolean);
      setBalance(Number(cfgRes.data.current_balance));
    }
  }, []);

  // Initial load + realtime updates
  useEffect(() => {
    loadData();
    const ch = supabase.channel('aibotpanel')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadData)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadData]);

  // Always-on: refresh indicator signals every 2 min
  useEffect(() => {
    if (!selectedCoins.length) return;
    const refresh = async () => {
      const sigs = await updateSignals(selectedCoins, supabase);
      signalsRef.current = sigs;
    };
    refresh();
    const id = setInterval(refresh, 2 * 60 * 1000);
    return () => clearInterval(id);
  }, [selectedCoins]);

  // Always-on: run AI analysis cycle every 5 min (stores results to DB)
  useEffect(() => {
    if (!selectedCoins.length) return;
    runAnalysisCycle(selectedCoins, supabase, setAnalysisStatus);
    const id = setInterval(() => runAnalysisCycle(selectedCoins, supabase, setAnalysisStatus), 5 * 60 * 1000);
    return () => clearInterval(id);
  }, [selectedCoins]);

  // ── REAL-TIME LOOP — fires on every WebSocket price tick (~1 s) ─────────────
  useEffect(() => {
    if (!isRunningRef.current || processingRef.current) return;
    if (!Object.keys(prices).length) return;

    processingRef.current = true;
    (async () => {
      try {
        const cfgRes = await supabase.from('bot_config')
          .select('stop_loss_percent, min_profit_percent')
          .eq('user_session', 'default').maybeSingle();
        const stopLoss  = Number(cfgRes.data?.stop_loss_percent  ?? 1.5);
        const minProfit = Number(cfgRes.data?.min_profit_percent ?? 0.25);

        const [exits, entries] = await Promise.all([
          checkExits(prices, supabase, stopLoss, minProfit),
          checkEntries(selectedRef.current, signalsRef.current, prices, coinBudgetsRef.current, supabase),
        ]);
        if (exits > 0 || entries > 0) {
          await loadData();
          if (exits > 0)   toast.success(`Sold ${exits} position(s)`, { duration: 2500 });
          if (entries > 0) toast.info(`Opened ${entries} trade(s)`, { duration: 2500 });
        }
      } finally {
        processingRef.current = false;
      }
    })();
  }, [prices]); // eslint-disable-line react-hooks/exhaustive-deps

  const toggleBot = async () => {
    if (mode === 'live' && !binanceConnected) {
      toast.error('Connect Binance API first', { description: 'Live trading requires your API keys' });
      onConnectBinance?.();
      return;
    }
    setLoading(true);
    try {
      if (!isRunning) {
        await supabase.from('bot_config').upsert({
          user_session: 'default',
          selected_coins: selectedCoins,
          mode,
          is_running: true,
          current_balance: totalBudget,
          initial_balance: totalBudget,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session' });
        isRunningRef.current = true;
        setIsRunning(true);
        setBalance(totalBudget);
        toast.success(`Bot started · ${mode.toUpperCase()} mode`, {
          description: `$${totalBudget.toLocaleString()} · ${selectedCoins.length} coins · instant trade execution`,
        });
      } else {
        await supabase.from('bot_config').update({ is_running: false, updated_at: new Date().toISOString() })
          .eq('user_session', 'default');
        isRunningRef.current = false;
        setIsRunning(false);
        toast.info('Bot paused · analysis continues in background');
      }
    } catch (err: any) {
      toast.error('Error', { description: err.message });
    } finally {
      setLoading(false);
    }
  };

  const resetBot = async () => {
    if (!confirm('Reset all trades and restore the full budget?')) return;
    isRunningRef.current = false;
    await Promise.all([
      supabase.from('bot_trade_history').delete().eq('user_session', 'default'),
      supabase.from('paper_portfolio').delete().eq('user_session', 'default'),
      supabase.from('bot_config').update({
        current_balance: totalBudget, initial_balance: totalBudget,
        is_running: false, updated_at: new Date().toISOString(),
      }).eq('user_session', 'default'),
    ]);
    setTrades([]); setPositions([]); setBalance(totalBudget);
    setTotalPnl(0); setIsRunning(false);
    toast.success(`Reset · $${totalBudget.toLocaleString()} restored`);
  };

  const pnlColor  = totalPnl >= 0 ? 'text-gain' : 'text-loss';
  const pnlPct    = totalBudget > 0 ? ((totalPnl / totalBudget) * 100).toFixed(2) : '0.00';
  const displayed = showAllTrades ? trades : trades.slice(0, 15);

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">

      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className={`w-2.5 h-2.5 rounded-full animate-pulse ${isRunning ? (mode === 'live' ? 'bg-loss' : 'bg-gain') : 'bg-accent'}`} />
          <h3 className="text-sm font-semibold">DeepTrade AI Bot</h3>
          <span className={`text-[9px] font-mono px-1.5 py-0.5 rounded ${mode === 'live' ? 'bg-loss/20 text-loss' : 'bg-accent/20 text-accent'}`}>
            {mode === 'live' ? 'LIVE' : 'PAPER'}
          </span>
        </div>
        <div className="flex gap-1">
          <button onClick={() => setShowBudget(!showBudget)}
            className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground transition-colors" title="Budget">
            <Settings2 className="w-3.5 h-3.5" />
          </button>
          <button onClick={resetBot}
            className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground transition-colors" title="Reset">
            <RotateCcw className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* ── Mode toggle ── */}
      <div className="grid grid-cols-2 gap-1 bg-muted/30 rounded-md p-0.5">
        <button onClick={() => { if (!isRunning) setMode('test'); }} disabled={isRunning}
          className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold transition-colors disabled:opacity-60 ${mode === 'test' ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground'}`}>
          <FlaskConical className="w-3.5 h-3.5" /> TEST — Paper
        </button>
        <button onClick={() => { if (!isRunning) setMode('live'); }} disabled={isRunning}
          className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold transition-colors disabled:opacity-60 ${mode === 'live' ? 'bg-loss/80 text-white' : 'text-muted-foreground hover:text-foreground'}`}>
          <Zap className="w-3.5 h-3.5" /> LIVE — Real
          {!binanceConnected && <span className="text-[9px] px-1 bg-warn/20 text-warn rounded">API needed</span>}
        </button>
      </div>

      {/* Live mode notice */}
      {mode === 'live' && (
        <div className={`rounded-md px-3 py-2 text-xs ${binanceConnected ? 'bg-loss/10 border border-loss/30 text-loss' : 'bg-warn/10 border border-warn/30 text-warn'}`}>
          {binanceConnected
            ? '⚠️ LIVE MODE — real funds will be used. Start only when ready.'
            : <span>Binance API not connected. <button onClick={onConnectBinance} className="underline font-semibold">Connect now →</button></span>}
        </div>
      )}

      {/* ── Budget panel ── */}
      {showBudget && (
        <div className="bg-muted/20 border border-border rounded-md p-3 space-y-3">
          <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-widest text-muted-foreground">
            <DollarSign className="w-3 h-3" />
            {mode === 'test' ? 'Paper Budget Allocation' : 'Trade Budget'}
          </div>

          {/* Preset buttons + custom input */}
          <div className="space-y-1">
            <div className="text-[10px] text-muted-foreground">Total budget</div>
            <div className="flex items-center gap-1 flex-wrap">
              {PRESET_BUDGETS.map(p => (
                <button key={p} onClick={() => setTotalBudget(p)} disabled={isRunning}
                  className={`text-[10px] px-2 py-1 rounded font-mono transition-colors disabled:opacity-50 ${totalBudget === p ? 'bg-accent text-accent-foreground' : 'bg-muted/50 text-muted-foreground hover:text-foreground'}`}>
                  ${p.toLocaleString()}
                </button>
              ))}
              <input type="number" value={totalBudget}
                onChange={e => setTotalBudget(Math.max(100, Number(e.target.value)))}
                disabled={isRunning} min={100} step={500}
                className="w-24 bg-muted/40 border border-border rounded px-2 py-1 text-[10px] font-mono text-foreground disabled:opacity-50 focus:outline-none focus:ring-1 focus:ring-accent" />
            </div>
          </div>

          {/* Per-coin split */}
          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <span className="text-[10px] text-muted-foreground">Per-coin allocation</span>
              <button onClick={() => setEqualSplit(!equalSplit)} disabled={isRunning}
                className={`text-[10px] px-2 py-0.5 rounded border transition-colors disabled:opacity-50 ${equalSplit ? 'border-accent text-accent' : 'border-border text-muted-foreground hover:text-foreground'}`}>
                {equalSplit ? '= Equal split' : '⚙ Custom'}
              </button>
            </div>
            <div className="grid grid-cols-2 gap-1">
              {selectedCoins.map(coin => {
                const val = equalSplit
                  ? totalBudget / Math.max(selectedCoins.length, 1)
                  : (coinBudgets[coin] ?? Math.floor(totalBudget / selectedCoins.length));
                return (
                  <div key={coin} className="flex items-center justify-between bg-muted/30 rounded px-2 py-1.5">
                    <span className="text-[10px] font-mono font-semibold">{coin.replace('USDT', '')}</span>
                    {equalSplit ? (
                      <span className="text-[10px] font-mono text-muted-foreground">${Math.floor(val).toLocaleString()}</span>
                    ) : (
                      <input type="number" value={val}
                        onChange={e => {
                          const next = { ...coinBudgets, [coin]: Number(e.target.value) };
                          setCoinBudgets(next);
                          coinBudgetsRef.current = next;
                        }}
                        disabled={isRunning} min={10} step={50}
                        className="w-20 bg-transparent text-[10px] font-mono text-right text-foreground border-b border-border focus:outline-none disabled:opacity-50" />
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {/* ── Always-on analysis ── */}
      <div className="flex items-center gap-2 bg-accent/10 border border-accent/20 rounded-md px-3 py-2">
        <Brain className="w-3.5 h-3.5 text-accent animate-pulse flex-shrink-0" />
        <div className="flex-1 min-w-0">
          <span className="text-xs text-accent font-semibold">AI Analysis Always Active</span>
          <p className="text-[10px] text-muted-foreground truncate">{analysisStatus}</p>
        </div>
        {isRunning && (
          <span className="flex items-center gap-1 text-[9px] font-mono text-gain flex-shrink-0">
            <span className="w-1.5 h-1.5 rounded-full bg-gain animate-ping" />LIVE
          </span>
        )}
      </div>

      {/* ── Stats ── */}
      <div className="grid grid-cols-4 gap-1.5">
        {[
          { label: 'Balance',   value: `$${balance.toLocaleString('en-US', { maximumFractionDigits: 0 })}`, color: '' },
          { label: 'P&L',       value: `${totalPnl >= 0 ? '+' : ''}$${Math.abs(totalPnl).toFixed(2)}`,     color: pnlColor },
          { label: 'Return',    value: `${totalPnl >= 0 ? '+' : ''}${pnlPct}%`,                             color: pnlColor },
          { label: 'Win Rate',  value: `${winRate}%`,  color: winRate >= 50 ? 'text-gain' : trades.filter(t => t.side === 'SELL').length > 0 ? 'text-loss' : '' },
        ].map(s => (
          <div key={s.label} className="bg-muted/20 rounded-md p-2 text-center">
            <div className="text-[9px] uppercase tracking-widest text-muted-foreground">{s.label}</div>
            <div className={`text-xs font-mono font-semibold tabular-nums ${s.color}`}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* ── Start / Stop ── */}
      <Button onClick={toggleBot} disabled={loading || !selectedCoins.length}
        className={`w-full font-semibold py-5 ${isRunning
          ? 'bg-loss/90 hover:bg-loss text-white'
          : mode === 'live'
            ? 'bg-orange-600 hover:bg-orange-500 text-white'
            : 'bg-gain/90 hover:bg-gain text-background'}`}>
        {loading ? <span className="animate-spin mr-1.5">⟳</span>
          : isRunning
            ? <><Square className="w-4 h-4 mr-1.5" />Stop Bot</>
            : <><Play className="w-4 h-4 mr-1.5" />Start {mode === 'live' ? 'Live' : 'Paper'} Trading</>}
      </Button>
      {!isRunning && (
        <p className="text-[10px] text-center text-muted-foreground -mt-2">
          ${totalBudget.toLocaleString()} · {selectedCoins.length} coins · trades instantly on signal
        </p>
      )}

      {/* ── Open positions ── */}
      {positions.length > 0 && (
        <div>
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 flex items-center gap-1">
            <Zap className="w-3 h-3 text-warn" /> Open Positions ({positions.length})
          </div>
          <div className="space-y-1">
            {positions.map(pos => {
              const live = parseFloat(prices[pos.symbol]?.price || '0') || pos.avg_entry_price;
              const uPnl = (live - pos.avg_entry_price) * pos.quantity;
              const pct  = ((live - pos.avg_entry_price) / pos.avg_entry_price) * 100;
              return (
                <div key={pos.symbol} className="flex items-center justify-between bg-muted/20 rounded px-2.5 py-1.5 text-xs">
                  <div className="flex items-center gap-2">
                    <span className={`w-1.5 h-1.5 rounded-full animate-pulse ${uPnl >= 0 ? 'bg-gain' : 'bg-loss'}`} />
                    <span className="font-mono font-semibold">{pos.symbol.replace('USDT', '')}</span>
                    <span className="text-muted-foreground font-mono text-[10px]">×{Number(pos.quantity).toFixed(6)}</span>
                  </div>
                  <span className={`font-mono font-semibold ${uPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                    {uPnl >= 0 ? '+' : ''}{uPnl.toFixed(2)}{' '}
                    <span className="font-normal opacity-70">({pct.toFixed(2)}%)</span>
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Trade history ── */}
      <div>
        <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5">
          Trade History ({trades.length})
        </div>
        {!trades.length ? (
          <p className="text-xs text-muted-foreground text-center py-4">
            {isRunning ? 'Watching every tick · waiting for score ≥65/100...' : 'Start the bot to see trades here'}
          </p>
        ) : (
          <div className="space-y-0.5">
            {displayed.map(t => {
              const isBuy  = t.side === 'BUY';
              const profit = t.pnl !== null && t.pnl > 0;
              const loss   = t.pnl !== null && t.pnl < 0;
              const time   = new Date(t.created_at).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
              return (
                <div key={t.id} className={`flex items-start justify-between py-1.5 px-2.5 rounded text-xs ${profit ? 'bg-gain/10 border border-gain/20' : loss ? 'bg-loss/10 border border-loss/20' : isBuy ? 'bg-muted/20 border border-border/30' : 'bg-muted/10 border border-border/20'}`}>
                  <div className="flex items-start gap-2 min-w-0">
                    {isBuy ? <TrendingUp className="w-3 h-3 text-accent mt-0.5 flex-shrink-0" />
                      : profit ? <TrendingUp className="w-3 h-3 text-gain mt-0.5 flex-shrink-0" />
                      : <TrendingDown className="w-3 h-3 text-loss mt-0.5 flex-shrink-0" />}
                    <div className="min-w-0">
                      <div className="flex items-center gap-1.5 flex-wrap">
                        <span className="font-mono font-semibold">{t.symbol.replace('USDT', '')}</span>
                        <span className={`text-[9px] font-bold px-1 py-0.5 rounded ${isBuy ? 'bg-accent/20 text-accent' : profit ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'}`}>{t.side}</span>
                        <span className="text-muted-foreground font-mono">${Number(t.price).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 })}</span>
                      </div>
                      {t.reason && <p className="text-[9px] text-muted-foreground truncate max-w-[200px]">{t.reason}</p>}
                    </div>
                  </div>
                  <div className="text-right flex-shrink-0 ml-2">
                    {t.pnl !== null && (
                      <div className={`font-mono font-bold ${t.pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                        {t.pnl >= 0 ? '+' : ''}{t.pnl.toFixed(2)}
                      </div>
                    )}
                    <div className="text-[9px] text-muted-foreground font-mono">{time}</div>
                  </div>
                </div>
              );
            })}
            {trades.length > 15 && (
              <button onClick={() => setShowAllTrades(!showAllTrades)}
                className="w-full text-[10px] text-accent hover:underline py-1 flex items-center justify-center gap-1">
                {showAllTrades ? <><ChevronUp className="w-3 h-3" />Show less</> : <><ChevronDown className="w-3 h-3" />Show all {trades.length} trades</>}
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default AIBotPanel;
