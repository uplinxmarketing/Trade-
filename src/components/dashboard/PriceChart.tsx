import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import type { KlineData } from '@/lib/binance-types';

interface PriceChartProps {
  data: KlineData[];
  symbol: string;
  currentPrice: string;
  priceChange: string;
}

const PriceChart = ({ data, symbol, currentPrice, priceChange }: PriceChartProps) => {
  const isPositive = parseFloat(priceChange) >= 0;

  return (
    <div className="trading-card p-4 animate-fade-in-up" style={{ animationDelay: '320ms' }}>
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-sm font-medium text-muted-foreground">{symbol}</h3>
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-semibold font-mono tabular-nums">
              ${parseFloat(currentPrice).toLocaleString('en-US', { minimumFractionDigits: 2 })}
            </span>
            <span className={`text-sm font-mono ${isPositive ? 'text-gain' : 'text-loss'}`}>
              {isPositive ? '+' : ''}{priceChange}%
            </span>
          </div>
        </div>
        <div className="flex gap-1">
          {['1H', '4H', '1D', '1W'].map((tf) => (
            <button
              key={tf}
              className="text-xs px-2 py-1 rounded text-muted-foreground hover:text-foreground hover:bg-secondary transition-colors active:scale-95 first:bg-secondary first:text-foreground"
            >
              {tf}
            </button>
          ))}
        </div>
      </div>

      <div className="h-52">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={isPositive ? 'hsl(160, 70%, 45%)' : 'hsl(0, 72%, 55%)'} stopOpacity={0.3} />
                <stop offset="95%" stopColor={isPositive ? 'hsl(160, 70%, 45%)' : 'hsl(0, 72%, 55%)'} stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="time"
              tick={{ fill: 'hsl(215, 12%, 50%)', fontSize: 10, fontFamily: 'JetBrains Mono' }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              domain={['auto', 'auto']}
              tick={{ fill: 'hsl(215, 12%, 50%)', fontSize: 10, fontFamily: 'JetBrains Mono' }}
              axisLine={false}
              tickLine={false}
              width={60}
              tickFormatter={(v) => `$${v.toLocaleString()}`}
            />
            <Tooltip
              contentStyle={{
                background: 'hsl(220, 18%, 12%)',
                border: '1px solid hsl(220, 14%, 18%)',
                borderRadius: '8px',
                fontSize: 12,
                fontFamily: 'JetBrains Mono',
              }}
              labelStyle={{ color: 'hsl(215, 12%, 50%)' }}
              itemStyle={{ color: 'hsl(210, 20%, 92%)' }}
            />
            <Area
              type="monotone"
              dataKey="close"
              stroke={isPositive ? 'hsl(160, 70%, 45%)' : 'hsl(0, 72%, 55%)'}
              strokeWidth={2}
              fill="url(#priceGradient)"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default PriceChart;
