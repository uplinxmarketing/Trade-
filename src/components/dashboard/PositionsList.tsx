import type { Position } from '@/lib/binance-types';

interface PositionsListProps {
  positions: Position[];
}

const PositionsList = ({ positions }: PositionsListProps) => {
  return (
    <div className="trading-card p-4 animate-fade-in-up" style={{ animationDelay: '400ms' }}>
      <h3 className="text-sm font-medium text-muted-foreground mb-3">Open Positions</h3>
      {positions.length === 0 ? (
        <p className="text-sm text-muted-foreground py-6 text-center">No open positions</p>
      ) : (
        <div className="space-y-2">
          {positions.map((pos) => {
            const pnl = parseFloat(pos.unrealizedPnl);
            const isPositive = pnl >= 0;
            return (
              <div key={pos.symbol} className="flex items-center justify-between py-2 border-b border-border last:border-0">
                <div className="flex items-center gap-3">
                  <span className={`text-xs font-mono px-1.5 py-0.5 rounded ${pos.side === 'LONG' ? 'bg-gain/15 text-gain' : 'bg-loss/15 text-loss'}`}>
                    {pos.side}
                  </span>
                  <div>
                    <span className="text-sm font-medium">{pos.symbol}</span>
                    <span className="text-xs text-muted-foreground ml-2">{pos.leverage}x</span>
                  </div>
                </div>
                <div className="text-right">
                  <div className={`text-sm font-mono tabular-nums ${isPositive ? 'text-gain' : 'text-loss'}`}>
                    {isPositive ? '+' : ''}${pnl.toFixed(2)}
                  </div>
                  <div className="text-xs text-muted-foreground font-mono">
                    {pos.quantity} @ ${parseFloat(pos.entryPrice).toLocaleString()}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

export default PositionsList;
