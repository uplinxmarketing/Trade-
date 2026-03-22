import { useState, useEffect, useCallback } from 'react';
import { Play, Pause, FlaskConical, Zap, TrendingUp, TrendingDown, RotateCcw } from 'lucide-react';
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
}

const BotDashboard = ({ selectedCoins, mode, onModeChange }: BotDashboardProps) => {
  const [isRunning, setIsRunning] = useState(false);
  const [balance, setBalance] = useState(10000);
  const [initialBalance] = useState(10000);
  const [trades, setTrades] = useState<BotTrade[]>([]);
  const [holdings, setHoldings] = useState<Record<string, { qty: number; avgPrice: number }>>({});
  const [loading, setLoading] = useState(false);

  // Load trade history
  const loadTrades = useCallback(async () => {
    const { data } = await supabase
      .from('bot_trade_history')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(20);
    if (data) setTrades(data as BotTrade[]);
  }, []);

  // Load bot config
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

    // Realtime subscription for new trades
    const channel = supabase
      .channel('bot-trades')
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'bot_trade_history' }, () => {
        loadTrades();
        loadConfig();
      })
      .subscribe();

    return () => { supabase.removeChannel(channel); };
  }, [loadTrades, loadConfig]);

  const toggleBot = async () => {
    setLoading(true);
    try {
      if (!isRunning) {
        // Upsert config and start
        await supabase.from('bot_config').upsert({
          user_session: 'default',
          selected_coins: selectedCoins,
          mode,
          is_running: true,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session' });

        // Trigger the bot edge function
        const { error } = await supabase.functions.invoke('trading-bot', {
          body: { action: 'start', coins: selectedCoins, mode },
        });
        if (error) throw error;

        setIsRunning(true);
        toast.success('Bot started', { description: `${mode} mode · watching ${selectedCoins.length} coins` });
      } else {
        await supabase.from('bot_config').update({
          is_running: false,
          updated_at: new Date().toISOString(),
        }).eq('user_session', 'default');

        setIsRunning(false);
        toast.info('Bot paused');
      }
    } catch (err: any) {
      toast.error('Bot error', { description: err.message });
    } finally {
      setLoading(false);
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
    setHoldings({});
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
          <div className={`w-2.5 h-2.5 rounded-full ${isRunning ? 'bg-gain animate-pulse' : 'bg-muted-foreground'}`} />
          <h3 className="text-sm font-medium">
            Spot Trading Bot
          </h3>
        </div>
        <div className="flex items-center gap-1.5">
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

      {/* Start/Stop */}
      <Button
        onClick={toggleBot}
        disabled={loading || selectedCoins.length === 0}
        className={`w-full font-semibold ${
          isRunning
            ? 'bg-loss/90 hover:bg-loss text-background'
            : 'bg-gain/90 hover:bg-gain text-background'
        }`}
      >
        {loading ? (
          <span className="animate-spin">⟳</span>
        ) : isRunning ? (
          <><Pause className="w-4 h-4 mr-1.5" /> Stop Bot</>
        ) : (
          <><Play className="w-4 h-4 mr-1.5" /> Start Bot</>
        )}
      </Button>

      {/* Recent bot trades */}
      {trades.length > 0 && (
        <div>
          <h4 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2">Recent Bot Trades</h4>
          <div className="space-y-1 max-h-40 overflow-y-auto scrollbar-thin">
            {trades.slice(0, 10).map(t => (
              <div key={t.id} className="flex items-center justify-between text-xs py-1.5 border-b border-border/50 last:border-0">
                <div className="flex items-center gap-2">
                  {t.side === 'BUY' ? (
                    <TrendingUp className="w-3 h-3 text-gain" />
                  ) : (
                    <TrendingDown className="w-3 h-3 text-loss" />
                  )}
                  <span className="font-mono">{t.symbol.replace('USDT', '')}</span>
                  <span className="text-muted-foreground">{t.side}</span>
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
        <p className="text-xs text-muted-foreground text-center py-3">
          Select coins and start the bot to begin {mode === 'test' ? 'paper' : 'live'} trading
        </p>
      )}
    </div>
  );
};

export default BotDashboard;
