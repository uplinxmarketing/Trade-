import { useState, useEffect, useCallback, useRef } from 'react';
import { Play, Square, Brain, TrendingUp, TrendingDown, Zap, RotateCcw, Timer, ChevronDown, ChevronUp } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import { runTradeCycle, runAnalysisCycle } from '@/lib/trading-engine';

interface Trade {
  id: string;
  symbol: string;
  side: 'BUY' | 'SELL';
  price: number;
  quantity: number;
  pnl: number | null;
  reason: string | null;
  created_at: string;
}

interface Position {
  symbol: string;
  quantity: number;
  avg_entry_price: number;
}

interface AIBotPanelProps {
  selectedCoins: string[];
  prices: Record<string, { price: string; priceChangePercent: string }>;
}

const INTERVALS = [
  { value: 30, label: '30s' },
  { value: 60, label: '1m' },
  { value: 300, label: '5m' },
  { value: 600, label: '10m' },
];

const AIBotPanel = ({ selectedCoins, prices }: AIBotPanelProps) => {
  const [isTrading, setIsTrading] = useState(false);
  const [loading, setLoading] = useState(false);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [positions, setPositions] = useState<Position[]>([]);
  const [balance, setBalance] = useState(10000);
  const [totalPnl, setTotalPnl] = useState(0);
  const [winRate, setWinRate] = useState(0);
  const [tradeInterval, setTradeInterval] = useState(30);
  const [countdown, setCountdown] = useState(0);
  const [analysisStatus, setAnalysisStatus] = useState<string>('Initializing...');
  const [showAllTrades, setShowAllTrades] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const countdownRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const learnRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const isTradingRef = useRef(false);

  const loadData = useCallback(async () => {
    const [tradesRes, posRes, configRes] = await Promise.all([
      supabase.from('bot_trade_history').select('*').eq('user_session', 'default').order('created_at', { ascending: false }).limit(50),
      supabase.from('paper_portfolio').select('*').eq('user_session', 'default').gt('quantity', 0),
      supabase.from('bot_config').select('*').eq('user_session', 'default').maybeSingle(),
    ]);
    if (tradesRes.data) {
      const t = tradesRes.data as Trade[];
      setTrades(t);
      const sells = t.filter(x => x.side === 'SELL' && x.pnl !== null);
      setTotalPnl(Math.round(sells.reduce((s, x) => s + (x.pnl || 0), 0) * 100) / 100);
      const wins = sells.filter(x => (x.pnl || 0) > 0).length;
      setWinRate(sells.length > 0 ? Math.round((wins / sells.length) * 100) : 0);
    }
    if (posRes.data) setPositions(posRes.data as Position[]);
    if (configRes.data) {
      isTradingRef.current = configRes.data.is_running as boolean;
      setIsTrading(configRes.data.is_running as boolean);
      setBalance(Number(configRes.data.current_balance));
    }
  }, []);

  useEffect(() => {
    loadData();
    const ch = supabase.channel('ai-bot-panel')
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'bot_trade_history' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadData)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadData]);

  // Always-on analysis loop (every 5 min) — runs entirely client-side
  const runLearning = useCallback(async () => {
    if (selectedCoins.length === 0) return;
    await runAnalysisCycle(selectedCoins, supabase, setAnalysisStatus);
  }, [selectedCoins]);

  useEffect(() => {
    runLearning();
    learnRef.current = setInterval(runLearning, 5 * 60 * 1000);
    return () => { if (learnRef.current) clearInterval(learnRef.current); };
  }, [runLearning]);

  // Trading cycle — runs entirely client-side
  const runCycle = useCallback(async () => {
    if (selectedCoins.length === 0 || !isTradingRef.current) return;
    try {
      await runTradeCycle(selectedCoins, supabase, undefined);
      await loadData();
    } catch (e) {
      console.error('Trade cycle error:', e);
    }
  }, [selectedCoins, loadData]);

  useEffect(() => {
    if (intervalRef.current) clearInterval(intervalRef.current);
    if (countdownRef.current) clearInterval(countdownRef.current);
    if (!isTrading) { setCountdown(0); return; }
    setCountdown(tradeInterval);
    intervalRef.current = setInterval(() => { runCycle(); setCountdown(tradeInterval); }, tradeInterval * 1000);
    countdownRef.current = setInterval(() => setCountdown(p => Math.max(0, p - 1)), 1000);
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
      if (countdownRef.current) clearInterval(countdownRef.current);
    };
  }, [isTrading, tradeInterval, runCycle]);

  const toggleTrading = async () => {
    setLoading(true);
    try {
      if (!isTrading) {
        await supabase.from('bot_config').upsert({
          user_session: 'default',
          selected_coins: selectedCoins,
          mode: 'test',
          is_running: true,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session' });
        isTradingRef.current = true;
        setIsTrading(true);
        // Run first cycle immediately
        runTradeCycle(selectedCoins, supabase).then(loadData);
        toast.success('AI Bot started', { description: `Trading ${selectedCoins.length} coins every ${tradeInterval}s` });
      } else {
        await supabase.from('bot_config').update({ is_running: false, updated_at: new Date().toISOString() }).eq('user_session', 'default');
        isTradingRef.current = false;
        setIsTrading(false);
        toast.info('AI Bot paused — still learning in background');
      }
    } catch (err: any) {
      toast.error('Bot error', { description: err.message });
    } finally {
      setLoading(false);
    }
  };

  const resetBot = async () => {
    if (!confirm('Reset all paper trades and balance to $10,000?')) return;
    isTradingRef.current = false;
    await Promise.all([
      supabase.from('bot_trade_history').delete().eq('user_session', 'default'),
      supabase.from('paper_portfolio').delete().eq('user_session', 'default'),
      supabase.from('bot_config').update({ current_balance: 10000, is_running: false, updated_at: new Date().toISOString() }).eq('user_session', 'default'),
    ]);
    setTrades([]); setPositions([]); setBalance(10000); setTotalPnl(0); setIsTrading(false);
    toast.success('Reset to $10,000');
  };

  const displayedTrades = showAllTrades ? trades : trades.slice(0, 15);
  const pnlColor = totalPnl >= 0 ? 'text-gain' : 'text-loss';
  const pnlPercent = ((totalPnl / 10000) * 100).toFixed(2);

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className={`w-2.5 h-2.5 rounded-full ${isTrading ? 'bg-gain animate-pulse' : 'bg-accent animate-pulse'}`} />
          <h3 className="text-sm font-semibold">DeepTrade AI Bot</h3>
          <span className="text-[9px] font-mono bg-accent/20 text-accent px-1.5 py-0.5 rounded">PAPER TRADING</span>
        </div>
        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={resetBot} title="Reset">
          <RotateCcw className="w-3.5 h-3.5" />
        </Button>
      </div>

      <div className="flex items-center gap-2 bg-accent/10 border border-accent/20 rounded-md px-3 py-2">
        <Brain className="w-3.5 h-3.5 text-accent animate-pulse flex-shrink-0" />
        <div className="flex-1 min-w-0">
          <span className="text-xs text-accent font-semibold">AI Analysis Always Active</span>
          <p className="text-[10px] text-muted-foreground truncate">{analysisStatus}</p>
        </div>
      </div>

      <div className="grid grid-cols-4 gap-2">
        {[
          { label: 'Balance', value: `$${balance.toLocaleString('en-US', { minimumFractionDigits: 0 })}`, color: '' },
          { label: 'P&L', value: `${totalPnl >= 0 ? '+' : ''}$${Math.abs(totalPnl).toFixed(2)}`, color: pnlColor },
          { label: '% Return', value: `${totalPnl >= 0 ? '+' : ''}${pnlPercent}%`, color: pnlColor },
          { label: 'Win Rate', value: `${winRate}%`, color: winRate >= 50 ? 'text-gain' : 'text-loss' },
        ].map(s => (
          <div key={s.label} className="bg-muted/20 rounded-md p-2 text-center">
            <div className="text-[9px] uppercase tracking-widest text-muted-foreground">{s.label}</div>
            <div className={`text-xs font-mono font-semibold tabular-nums ${s.color}`}>{s.value}</div>
          </div>
        ))}
      </div>

      <div>
        <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 flex items-center gap-1">
          <Timer className="w-3 h-3" /> Trade Cycle
        </div>
        <div className="grid grid-cols-4 gap-1 bg-muted/30 rounded-md p-0.5">
          {INTERVALS.map(iv => (
            <button key={iv.value} onClick={() => setTradeInterval(iv.value)} disabled={isTrading}
              className={`py-1.5 rounded text-[10px] font-medium transition-colors ${tradeInterval === iv.value ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground'} disabled:opacity-50`}>
              {iv.label}
            </button>
          ))}
        </div>
        {isTrading && (
          <div className="mt-1.5 flex items-center gap-2">
            <div className="flex-1 bg-muted/40 rounded-full h-1">
              <div className="bg-accent rounded-full h-1 transition-all duration-1000" style={{ width: `${(countdown / tradeInterval) * 100}%` }} />
            </div>
            <span className="text-[10px] font-mono text-muted-foreground tabular-nums w-8 text-right">{countdown}s</span>
          </div>
        )}
      </div>

      <Button onClick={toggleTrading} disabled={loading || selectedCoins.length === 0}
        className={`w-full font-semibold ${isTrading ? 'bg-loss/90 hover:bg-loss text-background' : 'bg-gain/90 hover:bg-gain text-background'}`}>
        {loading ? <span className="animate-spin mr-1.5">⟳</span> : isTrading
          ? <><Square className="w-4 h-4 mr-1.5" /> Stop Trading</>
          : <><Play className="w-4 h-4 mr-1.5" /> Start Trading</>}
      </Button>

      {positions.length > 0 && (
        <div>
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 flex items-center gap-1">
            <Zap className="w-3 h-3 text-warn" /> Open Positions ({positions.length})
          </div>
          <div className="space-y-1">
            {positions.map(pos => {
              const livePrice = parseFloat(prices[pos.symbol]?.price || '0') || pos.avg_entry_price;
              const unrealized = (livePrice - pos.avg_entry_price) * pos.quantity;
              const pct = ((livePrice - pos.avg_entry_price) / pos.avg_entry_price) * 100;
              const isUp = unrealized >= 0;
              return (
                <div key={pos.symbol} className="flex items-center justify-between bg-muted/20 rounded px-2.5 py-1.5 text-xs">
                  <div className="flex items-center gap-2">
                    <div className={`w-1.5 h-1.5 rounded-full ${isUp ? 'bg-gain' : 'bg-loss'} animate-pulse`} />
                    <span className="font-mono font-semibold">{pos.symbol.replace('USDT', '')}</span>
                    <span className="text-muted-foreground font-mono">×{pos.quantity.toFixed(6)}</span>
                  </div>
                  <span className={`font-mono font-semibold ${isUp ? 'text-gain' : 'text-loss'}`}>
                    {isUp ? '+' : ''}{unrealized.toFixed(2)} ({pct.toFixed(2)}%)
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div>
        <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5">
          Trade History ({trades.length})
        </div>
        {trades.length === 0 ? (
          <p className="text-xs text-muted-foreground text-center py-4">
            {isTrading ? 'Waiting for first trade signal...' : 'Start trading to see trades here'}
          </p>
        ) : (
          <div className="space-y-0.5">
            {displayedTrades.map(t => {
              const isBuy = t.side === 'BUY';
              const hasProfit = t.pnl !== null && t.pnl > 0;
              const hasLoss = t.pnl !== null && t.pnl < 0;
              const time = new Date(t.created_at).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
              return (
                <div key={t.id} className={`flex items-start justify-between py-1.5 px-2.5 rounded text-xs ${hasProfit ? 'bg-gain/10 border border-gain/20' : hasLoss ? 'bg-loss/10 border border-loss/20' : isBuy ? 'bg-muted/20 border border-border/30' : 'bg-muted/10 border border-border/20'}`}>
                  <div className="flex items-start gap-2 min-w-0">
                    {isBuy ? <TrendingUp className="w-3 h-3 text-accent mt-0.5 flex-shrink-0" />
                      : hasProfit ? <TrendingUp className="w-3 h-3 text-gain mt-0.5 flex-shrink-0" />
                      : <TrendingDown className="w-3 h-3 text-loss mt-0.5 flex-shrink-0" />}
                    <div className="min-w-0">
                      <div className="flex items-center gap-1.5">
                        <span className="font-mono font-semibold">{t.symbol.replace('USDT', '')}</span>
                        <span className={`text-[9px] font-bold px-1 py-0.5 rounded ${isBuy ? 'bg-accent/20 text-accent' : hasProfit ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'}`}>{t.side}</span>
                        <span className="text-muted-foreground font-mono">${Number(t.price).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 })}</span>
                      </div>
                      {t.reason && <p className="text-[9px] text-muted-foreground truncate max-w-[200px]">{t.reason}</p>}
                    </div>
                  </div>
                  <div className="text-right flex-shrink-0 ml-2">
                    {t.pnl !== null && (
                      <div className={`font-mono font-bold text-xs ${t.pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                        {t.pnl >= 0 ? '+' : ''}{t.pnl.toFixed(2)}
                      </div>
                    )}
                    <div className="text-[9px] text-muted-foreground">{time}</div>
                  </div>
                </div>
              );
            })}
            {trades.length > 15 && (
              <button onClick={() => setShowAllTrades(!showAllTrades)}
                className="w-full text-[10px] text-accent hover:underline py-1 flex items-center justify-center gap-1">
                {showAllTrades ? <><ChevronUp className="w-3 h-3" /> Show less</> : <><ChevronDown className="w-3 h-3" /> Show all {trades.length} trades</>}
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default AIBotPanel;
