import { TrendingUp, TrendingDown, Wallet, BarChart3 } from 'lucide-react';

interface PortfolioSummaryProps {
  totalBalance: number;
  dailyPnl: number;
  dailyPnlPercent: number;
  openPositions: number;
  activeBots: number;
}

const PortfolioSummary = ({ totalBalance, dailyPnl, dailyPnlPercent, openPositions, activeBots }: PortfolioSummaryProps) => {
  const isPositive = dailyPnl >= 0;

  const stats = [
    {
      label: 'Total Balance',
      value: `$${totalBalance.toLocaleString('en-US', { minimumFractionDigits: 2 })}`,
      icon: Wallet,
      color: 'text-accent',
    },
    {
      label: 'Daily P&L',
      value: `${isPositive ? '+' : ''}$${dailyPnl.toLocaleString('en-US', { minimumFractionDigits: 2 })}`,
      subtitle: `${isPositive ? '+' : ''}${dailyPnlPercent.toFixed(2)}%`,
      icon: isPositive ? TrendingUp : TrendingDown,
      color: isPositive ? 'text-gain' : 'text-loss',
    },
    {
      label: 'Open Positions',
      value: openPositions.toString(),
      icon: BarChart3,
      color: 'text-foreground',
    },
    {
      label: 'Active Bots',
      value: activeBots.toString(),
      icon: () => <div className="pulse-dot" />,
      color: 'text-primary',
    },
  ];

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
      {stats.map((stat, i) => (
        <div
          key={stat.label}
          className="trading-card p-4 animate-fade-in-up"
          style={{ animationDelay: `${i * 80}ms` }}
        >
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs text-muted-foreground uppercase tracking-wider">{stat.label}</span>
            <stat.icon className={`w-4 h-4 ${stat.color}`} />
          </div>
          <div className={`text-xl font-semibold font-mono tabular-nums ${stat.color}`}>
            {stat.value}
          </div>
          {stat.subtitle && (
            <span className={`text-xs font-mono ${stat.color}`}>{stat.subtitle}</span>
          )}
        </div>
      ))}
    </div>
  );
};

export default PortfolioSummary;
