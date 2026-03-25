import { useState, useEffect, useCallback } from 'react';
import { BarChart3, CheckCircle2, XCircle, DollarSign, Activity, Clock, AlertTriangle, Loader2 } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';

interface TradeStats {
  totalTrades: number;
  successful: number;
  unsuccessful: number;
  openTrades: number;
  totalProfit: number;
  avgTradeProfit: number;
  bestTrade: number;
  worstTrade: number;
  avgHoldTime: string;
  winRate: string;
}

const ReportDashboard = () => {
  const [stats, setStats] = useState<TradeStats>({
    totalTrades: 0, successful: 0, unsuccessful: 0, openTrades: 0,
    totalProfit: 0, avgTradeProfit: 0, bestTrade: 0, worstTrade: 0, avgHoldTime: '-', winRate: '0',
  });
  const [loading, setLoading] = useState(true);

  const loadStats = useCallback(async () => {
    setLoading(true);
    const { data: trades } = await supabase
      .from('bot_trade_history')
      .select('*')
      .eq('user_session', 'default')
      .order('created_at', { ascending: true });

    if (!trades || trades.length === 0) {
      setLoading(false);
      return;
    }

    const closedTrades = trades.filter(t => t.pnl !== null);
    const openTrades = trades.filter(t => t.pnl === null && t.side === 'BUY');
    const successful = closedTrades.filter(t => Number(t.pnl) > 0);
    const unsuccessful = closedTrades.filter(t => Number(t.pnl) <= 0);
    const totalProfit = closedTrades.reduce((s, t) => s + Number(t.pnl), 0);
    const pnls = closedTrades.map(t => Number(t.pnl));

    // Calculate avg hold time
    const buyTimes: Record<string, string> = {};
    let totalHoldMs = 0, holdCount = 0;
    for (const t of trades) {
      if (t.side === 'BUY') buyTimes[t.symbol] = t.created_at;
      else if (t.side === 'SELL' && buyTimes[t.symbol]) {
        totalHoldMs += new Date(t.created_at).getTime() - new Date(buyTimes[t.symbol]).getTime();
        holdCount++;
        delete buyTimes[t.symbol];
      }
    }
    const avgMs = holdCount > 0 ? totalHoldMs / holdCount : 0;
    const avgHold = avgMs > 0
      ? avgMs > 3600000 ? `${(avgMs / 3600000).toFixed(1)}h` : `${(avgMs / 60000).toFixed(0)}m`
      : '-';

    const winRate = closedTrades.length > 0 ? ((successful.length / closedTrades.length) * 100).toFixed(1) : '0';

    setStats({
      totalTrades: trades.length,
      successful: successful.length,
      unsuccessful: unsuccessful.length,
      openTrades: openTrades.length,
      totalProfit: Math.round(totalProfit * 100) / 100,
      avgTradeProfit: closedTrades.length > 0 ? Math.round((totalProfit / closedTrades.length) * 100) / 100 : 0,
      bestTrade: pnls.length > 0 ? Math.max(...pnls) : 0,
      worstTrade: pnls.length > 0 ? Math.min(...pnls) : 0,
      avgHoldTime: avgHold,
      winRate,
    });
    setLoading(false);
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
    { label: 'Open/Pending', value: stats.openTrades.toString(), icon: Clock, color: 'text-warn' },
    { label: 'Total Profit', value: `${stats.totalProfit >= 0 ? '+' : '-'}$${Math.abs(stats.totalProfit).toFixed(2)}`, icon: DollarSign, color: stats.totalProfit >= 0 ? 'text-gain' : 'text-loss' },
    { label: 'Win Rate', value: `${stats.winRate}%`, icon: BarChart3, color: Number(stats.winRate) >= 50 ? 'text-gain' : 'text-warn' },
    { label: 'Best Trade', value: `+$${stats.bestTrade.toFixed(2)}`, icon: CheckCircle2, color: 'text-gain' },
    { label: 'Worst Trade', value: `${stats.worstTrade < 0 ? '-' : ''}$${Math.abs(stats.worstTrade).toFixed(2)}`, icon: XCircle, color: 'text-loss' },
  ];

  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <BarChart3 className="w-4 h-4 text-accent" />
          <h3 className="text-sm font-medium">Trading Report</h3>
        </div>
        {loading && <Loader2 className="w-3 h-3 animate-spin text-muted-foreground" />}
      </div>

      {/* Fix L-03: Total = Successful + Unsuccessful + Open */}
      {stats.totalTrades > 0 && stats.totalTrades !== stats.successful + stats.unsuccessful + stats.openTrades && (
        <div className="flex items-center gap-1.5 mb-3 text-[10px] text-warn bg-warn/10 rounded px-2 py-1">
          <AlertTriangle className="w-3 h-3" />
          <span>Trade count reconciliation in progress</span>
        </div>
      )}

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

      {/* Avg hold time + avg trade P&L */}
      <div className="grid grid-cols-2 gap-2 mt-2">
        <div className="bg-muted/20 rounded-md p-2.5">
          <div className="flex items-center gap-1.5 mb-1">
            <Clock className="w-3 h-3 text-muted-foreground" />
            <span className="text-[10px] uppercase tracking-widest text-muted-foreground">Avg Hold Time</span>
          </div>
          <span className="text-sm font-mono font-semibold tabular-nums text-muted-foreground">{stats.avgHoldTime}</span>
        </div>
        <div className="bg-muted/20 rounded-md p-2.5">
          <div className="flex items-center gap-1.5 mb-1">
            <DollarSign className={`w-3 h-3 ${stats.avgTradeProfit >= 0 ? 'text-gain' : 'text-loss'}`} />
            <span className="text-[10px] uppercase tracking-widest text-muted-foreground">Avg Trade P&L</span>
          </div>
          <span className={`text-sm font-mono font-semibold tabular-nums ${stats.avgTradeProfit >= 0 ? 'text-gain' : 'text-loss'}`}>
            {stats.avgTradeProfit >= 0 ? '+' : '-'}${Math.abs(stats.avgTradeProfit).toFixed(2)}
          </span>
        </div>
      </div>

      {stats.totalTrades === 0 && !loading && (
        <p className="text-xs text-muted-foreground text-center mt-3">Start the bot to generate trading data</p>
      )}
    </div>
  );
};

export default ReportDashboard;
