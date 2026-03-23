import { useState } from 'react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import { useBinanceKlines } from '@/hooks/useBinanceKlines';
import { Loader2 } from 'lucide-react';

interface PriceChartProps {
  symbol: string;
  currentPrice: string;
  priceChange: string;
}

const TIMEFRAMES = [
  { key: '1m', label: '1m' },
  { key: '5m', label: '5m' },
  { key: '15m', label: '15m' },
  { key: '1h', label: '1H' },
  { key: '4h', label: '4H' },
  { key: '1d', label: '1D' },
  { key: '1w', label: '1W' },
];

const PriceChart = ({ symbol, currentPrice, priceChange }: PriceChartProps) => {
  const [interval, setInterval] = useState('1h');
  const binanceSymbol = symbol.replace(' / ', '').replace('/', '');
  const { klines, loading } = useBinanceKlines(binanceSymbol, interval);
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
        <div className="flex gap-0.5 flex-wrap justify-end">
          {TIMEFRAMES.map((tf) => (
            <button
              key={tf.key}
              onClick={() => setInterval(tf.key)}
              className={`text-[10px] px-1.5 py-1 rounded transition-colors active:scale-95 ${
                interval === tf.key
                  ? 'bg-secondary text-foreground font-semibold'
                  : 'text-muted-foreground hover:text-foreground hover:bg-secondary/50'
              }`}
            >
              {tf.label}
            </button>
          ))}
        </div>
      </div>

      <div className="h-52 relative">
        {loading && klines.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center">
            <Loader2 className="w-5 h-5 text-muted-foreground animate-spin" />
          </div>
        )}
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={klines} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={isPositive ? 'hsl(var(--gain))' : 'hsl(var(--loss))'} stopOpacity={0.3} />
                <stop offset="95%" stopColor={isPositive ? 'hsl(var(--gain))' : 'hsl(var(--loss))'} stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="time"
              tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10, fontFamily: 'JetBrains Mono' }}
              axisLine={false}
              tickLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              domain={['auto', 'auto']}
              tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 10, fontFamily: 'JetBrains Mono' }}
              axisLine={false}
              tickLine={false}
              width={60}
              tickFormatter={(v) => `$${v.toLocaleString()}`}
            />
            <Tooltip
              contentStyle={{
                background: 'hsl(var(--popover))',
                border: '1px solid hsl(var(--border))',
                borderRadius: '8px',
                fontSize: 12,
                fontFamily: 'JetBrains Mono',
              }}
              labelStyle={{ color: 'hsl(var(--muted-foreground))' }}
              itemStyle={{ color: 'hsl(var(--foreground))' }}
            />
            <Area
              type="monotone"
              dataKey="close"
              stroke={isPositive ? 'hsl(var(--gain))' : 'hsl(var(--loss))'}
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
