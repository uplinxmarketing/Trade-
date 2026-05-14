import { useState, useEffect, useCallback } from 'react';
import { TrendingUp, TrendingDown, Wallet, BarChart3, Bot, Briefcase, Info } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';

interface PortfolioSummaryProps {
  binanceConnected: boolean;
  botCount: number;
}

const PortfolioSummary = ({ binanceConnected, botCount }: PortfolioSummaryProps) => {
  const [accountBalance, setAccountBalance] = useState<number | null>(null);
  const [botTotalPnl, setBotTotalPnl] = useState(0);
  const [botBalance, setBotBalance] = useState(0);
  const [openPositions, setOpenPositions] = useState(0);
  const [activeBots, setActiveBots] = useState(0);

  const loadData = useCallback(async () => {
    // Bot stats from trading_bots
    const { data: bots } = await supabase.from('trading_bots').select('status, total_pnl, available_balance, used_balance').eq('user_session', 'default');
    if (bots) {
      setActiveBots(bots.filter(b => b.status === 'active').length);
      setBotTotalPnl(bots.reduce((s, b) => s + Number(b.total_pnl), 0));
      setBotBalance(bots.reduce((s, b) => s + Number(b.available_balance) + Number(b.used_balance), 0));
    }

    // Open positions
    const { count } = await supabase.from('paper_portfolio').select('*', { count: 'exact', head: true }).eq('user_session', 'default').gt('quantity', 0);
    setOpenPositions(count || 0);

    // Real account balance if connected
    if (binanceConnected) {
      try {
        const { data } = await supabase.functions.invoke('binance-proxy', { body: { action: 'account', params: {} } });
        if (data?.balances) {
          let total = 0;
          for (const b of data.balances) {
            const free = parseFloat(b.free);
            const locked = parseFloat(b.locked);
            if (free + locked <= 0) continue;
            if (b.asset === 'USDT' || b.asset === 'BUSD' || b.asset === 'USDC' || b.asset === 'USD') {
              total += free + locked;
            } else if (!b.asset.startsWith('LD')) {
              try {
                const pr = await fetch(`/api/proxy/binance/ticker/price?symbol=${b.asset}USDT`);
                const pd = await pr.json();
                if (pd.price) total += (free + locked) * parseFloat(pd.price);
              } catch { /* skip */ }
            }
          }
          setAccountBalance(total);
        }
      } catch { /* skip */ }
    }
  }, [binanceConnected]);

  useEffect(() => {
    loadData();
    const ch = supabase.channel('portfolio-summary')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'trading_bots' }, loadData)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadData]);

  const isPositive = botTotalPnl >= 0;

  const stats = [
    {
      label: 'Account Balance',
      sublabel: binanceConnected ? '[Binance]' : '[Not Connected]',
      value: accountBalance !== null
        ? `$${accountBalance.toLocaleString('en-US', { minimumFractionDigits: 2 })}`
        : binanceConnected ? 'Loading...' : '—',
      icon: Wallet,
      color: 'text-accent',
      large: true,
      tooltip: 'Total value of all assets in your Binance account',
    },
    {
      label: 'Bot P&L',
      sublabel: '[All Bots]',
      value: `${isPositive ? '+' : '-'}$${Math.abs(botTotalPnl).toFixed(2)}`,
      icon: isPositive ? TrendingUp : TrendingDown,
      color: isPositive ? 'text-gain' : 'text-loss',
      large: true,
      tooltip: 'Combined profit/loss from all bots',
    },
    {
      label: 'Open Positions',
      value: openPositions.toString(),
      icon: Briefcase,
      color: 'text-foreground',
      tooltip: 'Number of active open trades across all bots',
    },
    {
      label: 'Active Bots',
      value: `${activeBots}/${botCount}`,
      icon: Bot,
      color: activeBots > 0 ? 'text-gain' : 'text-muted-foreground',
      tooltip: 'Currently running bots out of total created',
    },
  ];

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
      {stats.map((stat, i) => (
        <div
          key={stat.label}
          className={`trading-card p-4 animate-fade-in-up ${stat.large ? 'border-l-2 border-l-accent/30' : ''}`}
          style={{ animationDelay: `${i * 80}ms` }}
          title={stat.tooltip}
        >
          <div className="flex items-center justify-between mb-1">
            <div className="flex items-center gap-1">
              <span className="text-[10px] text-muted-foreground uppercase tracking-wider">{stat.label}</span>
              {stat.sublabel && (
                <span className="text-[9px] text-muted-foreground/60 font-mono">{stat.sublabel}</span>
              )}
            </div>
            <stat.icon className={`w-4 h-4 ${stat.color}`} />
          </div>
          <div className={`text-xl font-semibold font-mono tabular-nums ${stat.color}`}>
            {stat.value}
          </div>
        </div>
      ))}
    </div>
  );
};

export default PortfolioSummary;
