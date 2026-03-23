import { useState, useEffect, useCallback } from 'react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts';
import { supabase } from '@/integrations/supabase/client';
import { TrendingUp } from 'lucide-react';

interface PnlPoint {
  time: string;
  pnl: number;
  cumPnl: number;
}

const PnlChart = () => {
  const [data, setData] = useState<PnlPoint[]>([]);

  const loadPnl = useCallback(async () => {
    const { data: trades } = await supabase
      .from('bot_trade_history')
      .select('pnl, created_at')
      .eq('user_session', 'default')
      .not('pnl', 'is', null)
      .order('created_at', { ascending: true });

    if (!trades || trades.length === 0) return;

    let cumPnl = 0;
    const points: PnlPoint[] = trades.map(t => {
      cumPnl += Number(t.pnl);
      const d = new Date(t.created_at);
      return {
        time: d.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false }),
        pnl: Number(t.pnl),
        cumPnl: Math.round(cumPnl * 100) / 100,
      };
    });
    setData(points);
  }, []);

  useEffect(() => {
    loadPnl();
    const ch = supabase
      .channel('pnl-updates')
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'bot_trade_history' }, loadPnl)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadPnl]);

  const lastPnl = data.length > 0 ? data[data.length - 1].cumPnl : 0;
  const isPositive = lastPnl >= 0;

  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <TrendingUp className="w-4 h-4 text-accent" />
          <h3 className="text-sm font-medium">P&L Performance</h3>
        </div>
        <span className={`text-sm font-mono font-semibold ${isPositive ? 'text-gain' : 'text-loss'}`}>
          {isPositive ? '+' : ''}${lastPnl.toFixed(2)}
        </span>
      </div>

      {data.length > 0 ? (
        <div className="h-40">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <defs>
                <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={isPositive ? 'hsl(var(--gain))' : 'hsl(var(--loss))'} stopOpacity={0.3} />
                  <stop offset="95%" stopColor={isPositive ? 'hsl(var(--gain))' : 'hsl(var(--loss))'} stopOpacity={0} />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="time"
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 9, fontFamily: 'JetBrains Mono' }}
                axisLine={false} tickLine={false} interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 9, fontFamily: 'JetBrains Mono' }}
                axisLine={false} tickLine={false} width={50}
                tickFormatter={(v) => `$${v}`}
              />
              <ReferenceLine y={0} stroke="hsl(var(--border))" strokeDasharray="3 3" />
              <Tooltip
                contentStyle={{
                  background: 'hsl(var(--popover))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: '8px', fontSize: 11, fontFamily: 'JetBrains Mono',
                }}
              />
              <Area
                type="monotone" dataKey="cumPnl" name="Cumulative P&L"
                stroke={isPositive ? 'hsl(var(--gain))' : 'hsl(var(--loss))'}
                strokeWidth={2} fill="url(#pnlGrad)"
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <p className="text-xs text-muted-foreground text-center py-8">No closed trades yet</p>
      )}
    </div>
  );
};

export default PnlChart;
