import { useState, useEffect, useCallback } from 'react';
import { BarChart3, CheckCircle2, XCircle, DollarSign, Activity, Clock } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';

interface TradeStats {
  totalTrades: number;
  successful: number;
  unsuccessful: number;
  totalProfit: number;
  avgTradeProfit: number;
  bestTrade: number;
  worstTrade: number;
  avgHoldTime: string;
}

const ReportDashboard = () => {
  const [stats, setStats] = useState<TradeStats>({
    totalTrades: 0, successful: 0, unsuccessful: 0,
    totalProfit: 0, avgTradeProfit: 0, bestTrade: 0, worstTrade: 0, avgHoldTime: '-',
  });

  const loadStats = useCallback(async () => {
    const { data: trades } = await supabase
      .from('bot_trade_history')
      .select('*')
      .eq('user_session', 'default')
      .order('created_at', { ascending: true });

    if (!trades || trades.length === 0) return;

    const closedTrades = trades.filter(t => t.pnl !== null);
    const successful = closedTrades.filter(t => Number(t.pnl) > 0);
    const unsuccessful = closedTrades.filter(t => Number(t.pnl) <= 0);
    const totalProfit = closedTrades.reduce((s, t) => s + Number(t.pnl), 0);
    const pnls = closedTrades.map(t => Number(t.pnl));

    // Calculate avg hold time from buy/sell pairs
    const buyTimes: Record<string, string> = {};
    let totalHoldMs = 0;
    let holdCount = 0;
    for (const t of trades) {
      if (t.side === 'BUY') {
        buyTimes[t.symbol] = t.created_at;
      } else if (t.side === 'SELL' && buyTimes[t.symbol]) {
        totalHoldMs += new Date(t.created_at).getTime() - new Date(buyTimes[t.symbol]).getTime();
        holdCount++;
        delete buyTimes[t.symbol];
      }
    }
    const avgMs = holdCount > 0 ? totalHoldMs / holdCount : 0;
    const avgHold = avgMs > 0
      ? avgMs > 3600000 ? `${(avgMs / 3600000).toFixed(1)}h` : `${(avgMs / 60000).toFixed(0)}m`
      : '-';

    setStats({
      totalTrades: trades.length,
      successful: successful.length,
      unsuccessful: unsuccessful.length,
      totalProfit: Math.round(totalProfit * 100) / 100,
      avgTradeProfit: closedTrades.length > 0 ? Math.round((totalProfit / closedTrades.length) * 100) / 100 : 0,
      bestTrade: pnls.length > 0 ? Math.max(...pnls) : 0,
      worstTrade: pnls.length > 0 ? Math.min(...pnls) : 0,
      avgHoldTime: avgHold,
    });
  }, []);

  useEffect(() => {
    loadStats();
    const ch = supabase
      .channel('report-updates')
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'bot_trade_history' }, loadStats)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadStats]);

  const metrics = [
    { label: 'Total Trades', value: stats.totalTrades.toString(), icon: Activity, color: 'text-accent' },
    { label: 'Successful', value: stats.successful.toString(), icon: CheckCircle2, color: 'text-gain' },
    { label: 'Unsuccessful', value: stats.unsuccessful.toString(), icon: XCircle, color: 'text-loss' },
    { label: 'Total Profit', value: `${stats.totalProfit >= 0 ? '+' : ''}$${stats.totalProfit.toFixed(2)}`, icon: DollarSign, color: stats.totalProfit >= 0 ? 'text-gain' : 'text-loss' },
    { label: 'Avg Trade P&L', value: `${stats.avgTradeProfit >= 0 ? '+' : ''}$${stats.avgTradeProfit.toFixed(2)}`, icon: BarChart3, color: stats.avgTradeProfit >= 0 ? 'text-gain' : 'text-loss' },
    { label: 'Best Trade', value: `+$${stats.bestTrade.toFixed(2)}`, icon: CheckCircle2, color: 'text-gain' },
    { label: 'Worst Trade', value: `$${stats.worstTrade.toFixed(2)}`, icon: XCircle, color: 'text-loss' },
    { label: 'Avg Hold Time', value: stats.avgHoldTime, icon: Clock, color: 'text-muted-foreground' },
  ];

  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <div className="flex items-center gap-2 mb-4">
        <BarChart3 className="w-4 h-4 text-accent" />
        <h3 className="text-sm font-medium">Trading Report</h3>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
        {metrics.map(m => {
          const Icon = m.icon;
          return (
            <div key={m.label} className="bg-muted/20 rounded-md p-2.5">
              <div className="flex items-center gap-1.5 mb-1">
                <Icon className={`w-3 h-3 ${m.color}`} />
                <span className="text-[10px] uppercase tracking-widest text-muted-foreground">{m.label}</span>
              </div>
              <span className={`text-sm font-mono font-semibold tabular-nums ${m.color}`}>{m.value}</span>
            </div>
          );
        })}
      </div>

      {stats.totalTrades === 0 && (
        <p className="text-xs text-muted-foreground text-center mt-3">Start the bot to generate trading data</p>
      )}
    </div>
  );
};

export default ReportDashboard;
