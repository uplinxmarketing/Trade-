import { useState, useEffect, useCallback, useRef } from 'react';
import { Play, Pause, FlaskConical, Zap, TrendingUp, TrendingDown, RotateCcw, Timer, Brain, History } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

interface BotTrade {
  id: string;
  symbol: string;
  side: string;
  price: number;
  quantity: number;
  pnl: number | null;
  reason: string | null;
  created_at: string;
}

interface BotDashboardProps {
  selectedCoins: string[];
  mode: 'test' | 'live';
  onModeChange: (mode: 'test' | 'live') => void;
  binanceConnected: boolean;
}

const INTERVALS = [
  { value: 30, label: '30s' },
  { value: 60, label: '1m' },
  { value: 300, label: '5m' },
  { value: 600, label: '10m' },
  { value: 900, label: '15m' },
];

const BotDashboard = ({ selectedCoins, mode, onModeChange, binanceConnected }: BotDashboardProps) => {
  const [isRunning, setIsRunning] = useState(false);
  const [isLearning, setIsLearning] = useState(false);
  const [balance, setBalance] = useState(10000);
  const [initialBalance] = useState(10000);
  const [trades, setTrades] = useState<BotTrade[]>([]);
  const [loading, setLoading] = useState(false);
  const [autoInterval, setAutoInterval] = useState(60);
  const [countdown, setCountdown] = useState(0);
  const [backtestRunning, setBacktestRunning] = useState(false);
  const [backtestResults, setBacktestResults] = useState<any>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const countdownRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadTrades = useCallback(async () => {
    const { data } = await supabase
      .from('bot_trade_history')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(20);
    if (data) setTrades(data as BotTrade[]);
  }, []);

  const loadConfig = useCallback(async () => {
    const { data } = await supabase
      .from('bot_config')
      .select('*')
      .eq('user_session', 'default')
      .maybeSingle();
    if (data) {
      setIsRunning(data.is_running as boolean);
      setBalance(Number(data.current_balance));
    }
  }, []);

  useEffect(() => {
    loadTrades();
    loadConfig();
    const channel = supabase
      .channel('bot-trades')
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'bot_trade_history' }, () => {
        loadTrades();
        loadConfig();
      })
      .subscribe();
    return () => { supabase.removeChannel(channel); };
  }, [loadTrades, loadConfig]);

  // Auto-trading loop
  const executeTradeCycle = useCallback(async () => {
    try {
      await supabase.functions.invoke('trading-bot', {
        body: { action: 'start', coins: selectedCoins, mode },
      });
      loadTrades();
      loadConfig();
    } catch (err: any) {
      console.error('Auto-trade cycle error:', err);
    }
  }, [selectedCoins, mode, loadTrades, loadConfig]);

  // Passive learning (runs even when bot is stopped)
  const executeLearnCycle = useCallback(async () => {
    try {
      await supabase.functions.invoke('ai-analysis', {
        body: { action: 'analyze', symbols: selectedCoins },
      });
    } catch (err: any) {
      console.error('Learn cycle error:', err);
    }
  }, [selectedCoins]);

  useEffect(() => {
    if (isRunning) {
      // Start auto-trading loop
      setCountdown(autoInterval);
      intervalRef.current = setInterval(() => {
        executeTradeCycle();
        setCountdown(autoInterval);
      }, autoInterval * 1000);

      countdownRef.current = setInterval(() => {
        setCountdown(prev => Math.max(0, prev - 1));
      }, 1000);
    } else {
      if (intervalRef.current) clearInterval(intervalRef.current);
      if (countdownRef.current) clearInterval(countdownRef.current);
      setCountdown(0);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
      if (countdownRef.current) clearInterval(countdownRef.current);
    };
  }, [isRunning, autoInterval, executeTradeCycle]);

  // Passive learning loop (every 5 minutes when learning is on but bot is off)
  useEffect(() => {
    let learnInterval: ReturnType<typeof setInterval> | null = null;
    if (isLearning && !isRunning) {
      executeLearnCycle(); // Run immediately
      learnInterval = setInterval(executeLearnCycle, 5 * 60 * 1000);
    }
    return () => { if (learnInterval) clearInterval(learnInterval); };
  }, [isLearning, isRunning, executeLearnCycle]);

  const toggleBot = async () => {
    setLoading(true);
    try {
      if (!isRunning) {
        if (mode === 'live' && !binanceConnected) {
          toast.error('Connect your Binance API keys first to use live mode');
          setLoading(false);
          return;
        }
        await supabase.from('bot_config').upsert({
          user_session: 'default',
          selected_coins: selectedCoins,
          mode,
          is_running: true,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session' });

        const { error } = await supabase.functions.invoke('trading-bot', {
          body: { action: 'start', coins: selectedCoins, mode },
        });
        if (error) throw error;
        setIsRunning(true);
        toast.success('Bot started', { description: `${mode} mode · ${autoInterval}s interval · ${selectedCoins.length} coins` });
      } else {
        await supabase.from('bot_config').update({
          is_running: false,
          updated_at: new Date().toISOString(),
        }).eq('user_session', 'default');
        setIsRunning(false);
        toast.info('Bot stopped');
      }
    } catch (err: any) {
      toast.error('Bot error', { description: err.message });
    } finally {
      setLoading(false);
    }
  };

  const runBacktest = async () => {
    setBacktestRunning(true);
    try {
      const { data, error } = await supabase.functions.invoke('backtest', {
        body: { coins: selectedCoins, days: 7 },
      });
      if (error) throw error;
      if (data?.error) throw new Error(data.error);
      setBacktestResults(data);
      toast.success('Backtest complete', { description: `${data.totalTrades} trades simulated` });
    } catch (err: any) {
      toast.error('Backtest failed', { description: err.message });
    } finally {
      setBacktestRunning(false);
    }
  };

  const resetBot = async () => {
    await supabase.from('bot_trade_history').delete().eq('user_session', 'default');
    await supabase.from('paper_portfolio').delete().eq('user_session', 'default');
    await supabase.from('bot_config').update({
      current_balance: 10000,
      is_running: false,
      updated_at: new Date().toISOString(),
    }).eq('user_session', 'default');
    setTrades([]);
    setBalance(10000);
    setIsRunning(false);
    setBacktestResults(null);
    toast.success('Bot reset to $10,000');
  };

  const totalPnl = balance - initialBalance;
  const pnlPercent = ((totalPnl / initialBalance) * 100).toFixed(2);
  const winningTrades = trades.filter(t => t.pnl !== null && t.pnl > 0).length;
  const totalClosedTrades = trades.filter(t => t.pnl !== null).length;
  const winRate = totalClosedTrades > 0 ? ((winningTrades / totalClosedTrades) * 100).toFixed(1) : '0';

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className={`w-2.5 h-2.5 rounded-full ${
            isRunning ? 'bg-gain animate-pulse' : isLearning ? 'bg-accent animate-pulse' : 'bg-muted-foreground'
          }`} />
          <h3 className="text-sm font-medium">Spot Trading Bot</h3>
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="ghost" size="icon" className="h-7 w-7"
            onClick={() => { setIsLearning(!isLearning); toast.info(isLearning ? 'Passive learning off' : 'Passive learning on — bot will analyze charts in background'); }}
            title={isLearning ? 'Stop learning' : 'Enable passive learning'}
          >
            <Brain className={`w-3.5 h-3.5 ${isLearning ? 'text-accent' : ''}`} />
          </Button>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={resetBot} title="Reset">
            <RotateCcw className="w-3.5 h-3.5" />
          </Button>
        </div>
      </div>

      {/* Mode toggle */}
      <div className="grid grid-cols-2 gap-1 bg-muted/30 rounded-md p-0.5">
        <button
          onClick={() => onModeChange('test')}
          className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold transition-colors ${
            mode === 'test' ? 'bg-warn/20 text-warn' : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          <FlaskConical className="w-3.5 h-3.5" /> Test Mode
        </button>
        <button
          onClick={() => onModeChange('live')}
          className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold transition-colors ${
            mode === 'live' ? 'bg-gain/20 text-gain' : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          <Zap className="w-3.5 h-3.5" /> Live Mode
        </button>
      </div>

      {/* Auto-trade interval */}
      <div>
        <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 flex items-center gap-1">
          <Timer className="w-3 h-3" /> Auto-Trade Interval
        </label>
        <div className="grid grid-cols-5 gap-1 bg-muted/30 rounded-md p-0.5">
          {INTERVALS.map(iv => (
            <button
              key={iv.value}
              onClick={() => setAutoInterval(iv.value)}
              className={`py-1.5 rounded text-[10px] font-medium transition-colors ${
                autoInterval === iv.value ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground'
              }`}
            >
              {iv.label}
            </button>
          ))}
        </div>
        {isRunning && countdown > 0 && (
          <div className="mt-1.5 flex items-center gap-2">
            <div className="flex-1 bg-muted/40 rounded-full h-1">
              <div
                className="bg-accent rounded-full h-1 transition-all duration-1000"
                style={{ width: `${(countdown / autoInterval) * 100}%` }}
              />
            </div>
            <span className="text-[10px] font-mono text-muted-foreground tabular-nums">{countdown}s</span>
          </div>
        )}
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 gap-3">
        <div className="bg-muted/20 rounded-md p-2.5">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground">Balance</div>
          <div className="text-sm font-mono font-semibold tabular-nums">${balance.toLocaleString('en-US', { minimumFractionDigits: 2 })}</div>
        </div>
        <div className="bg-muted/20 rounded-md p-2.5">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground">P&L</div>
          <div className={`text-sm font-mono font-semibold tabular-nums ${totalPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
            {totalPnl >= 0 ? '+' : ''}{totalPnl.toFixed(2)} ({pnlPercent}%)
          </div>
        </div>
        <div className="bg-muted/20 rounded-md p-2.5">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground">Trades</div>
          <div className="text-sm font-mono font-semibold tabular-nums">{trades.length}</div>
        </div>
        <div className="bg-muted/20 rounded-md p-2.5">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground">Win Rate</div>
          <div className="text-sm font-mono font-semibold tabular-nums">{winRate}%</div>
        </div>
      </div>

      {/* Action buttons */}
      <div className="flex gap-2">
        <Button
          onClick={toggleBot}
          disabled={loading || selectedCoins.length === 0}
          className={`flex-1 font-semibold ${
            isRunning
              ? 'bg-loss/90 hover:bg-loss text-background'
              : 'bg-gain/90 hover:bg-gain text-background'
          }`}
        >
          {loading ? (
            <span className="animate-spin">⟳</span>
          ) : isRunning ? (
            <><Pause className="w-4 h-4 mr-1.5" /> Stop</>
          ) : (
            <><Play className="w-4 h-4 mr-1.5" /> Start</>
          )}
        </Button>
        <Button
          variant="outline"
          onClick={runBacktest}
          disabled={backtestRunning || selectedCoins.length === 0}
          className="font-medium gap-1.5"
        >
          {backtestRunning ? <span className="animate-spin">⟳</span> : <History className="w-4 h-4" />}
          Backtest
        </Button>
      </div>

      {/* Passive learning indicator */}
      {isLearning && !isRunning && (
        <div className="flex items-center gap-2 bg-accent/10 border border-accent/20 rounded-md px-3 py-2">
          <Brain className="w-3.5 h-3.5 text-accent animate-pulse" />
          <span className="text-xs text-accent font-medium">Learning mode active — analyzing charts every 5m</span>
        </div>
      )}

      {/* Backtest results */}
      {backtestResults && (
        <div className="border border-border/50 rounded-md p-3 space-y-2">
          <h4 className="text-[10px] uppercase tracking-widest text-muted-foreground flex items-center gap-1">
            <History className="w-3 h-3" /> Backtest Results (7 days)
          </h4>
          <div className="grid grid-cols-3 gap-2 text-xs">
            <div>
              <span className="text-muted-foreground block">Trades</span>
              <span className="font-mono font-semibold">{backtestResults.totalTrades}</span>
            </div>
            <div>
              <span className="text-muted-foreground block">P&L</span>
              <span className={`font-mono font-semibold ${backtestResults.totalPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                {backtestResults.totalPnl >= 0 ? '+' : ''}${backtestResults.totalPnl?.toFixed(2)}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground block">Win Rate</span>
              <span className="font-mono font-semibold">{backtestResults.winRate?.toFixed(1)}%</span>
            </div>
          </div>
          {backtestResults.trades && backtestResults.trades.length > 0 && (
            <div className="max-h-24 overflow-y-auto scrollbar-thin space-y-0.5 mt-1">
              {backtestResults.trades.slice(0, 8).map((t: any, i: number) => (
                <div key={i} className="flex items-center justify-between text-[10px] py-0.5">
                  <span className="font-mono text-muted-foreground">{t.symbol?.replace('USDT', '')} {t.side}</span>
                  <span className={`font-mono ${t.pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                    {t.pnl >= 0 ? '+' : ''}{t.pnl?.toFixed(2)}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {/* Recent bot trades */}
      {trades.length > 0 && (
        <div>
          <h4 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2">Recent Bot Trades</h4>
          <div className="space-y-1 max-h-32 overflow-y-auto scrollbar-thin">
            {trades.slice(0, 8).map(t => (
              <div key={t.id} className="flex items-center justify-between text-xs py-1.5 border-b border-border/50 last:border-0">
                <div className="flex items-center gap-2">
                  {t.side === 'BUY' ? (
                    <TrendingUp className="w-3 h-3 text-gain" />
                  ) : (
                    <TrendingDown className="w-3 h-3 text-loss" />
                  )}
                  <span className="font-mono">{t.symbol.replace('USDT', '')}</span>
                </div>
                <div className="flex items-center gap-3">
                  <span className="font-mono text-muted-foreground">${Number(t.price).toFixed(2)}</span>
                  {t.pnl !== null && (
                    <span className={`font-mono font-medium ${t.pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                      {t.pnl >= 0 ? '+' : ''}{t.pnl.toFixed(2)}
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {trades.length === 0 && !isRunning && (
        <p className="text-xs text-muted-foreground text-center py-2">
          Start the bot or run a backtest to see results
        </p>
      )}
    </div>
  );
};

export default BotDashboard;
