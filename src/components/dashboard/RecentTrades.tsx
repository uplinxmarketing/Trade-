interface Trade {
  id: string;
  symbol: string;
  side: 'BUY' | 'SELL';
  price: string;
  qty: string;
  time: string;
  pnl?: number;
}

interface RecentTradesProps {
  trades: Trade[];
}

const RecentTrades = ({ trades }: RecentTradesProps) => {
  return (
    <div className="trading-card p-4 animate-fade-in-up" style={{ animationDelay: '560ms' }}>
      <h3 className="text-sm font-medium text-muted-foreground mb-3">Recent Trades</h3>
      <div className="space-y-1">
        {trades.map((trade) => (
          <div key={trade.id} className="flex items-center justify-between py-1.5 text-xs font-mono">
            <div className="flex items-center gap-2">
              <span className={trade.side === 'BUY' ? 'text-gain' : 'text-loss'}>{trade.side}</span>
              <span className="text-foreground">{trade.symbol}</span>
            </div>
            <div className="flex items-center gap-4">
              <span className="text-muted-foreground">{trade.qty}</span>
              <span className="text-foreground tabular-nums">${parseFloat(trade.price).toLocaleString()}</span>
              {trade.pnl !== undefined && (
                <span className={`tabular-nums ${trade.pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                  {trade.pnl >= 0 ? '+' : ''}{trade.pnl.toFixed(2)}
                </span>
              )}
              <span className="text-muted-foreground">{trade.time}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default RecentTrades;
