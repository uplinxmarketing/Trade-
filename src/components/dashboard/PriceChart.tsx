import { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import { useBinanceKlines } from '@/hooks/useBinanceKlines';
import { Loader2 } from 'lucide-react';
import type { KlineData } from '@/lib/binance-types';

interface PriceChartProps {
  symbol: string;       // "BTC / USDT"
  currentPrice: string; // live price from WebSocket ticker
  priceChange: string;
}

const TIMEFRAMES = [
  { key: '1m', label: '1m' },
  { key: '5m', label: '5m' },
  { key: '15m', label: '15m' },
  { key: '1h', label: '1H' },
  { key: '4h', label: '4H' },
  { key: '1d', label: '1D' },
];

const PriceChart = ({ symbol, currentPrice, priceChange }: PriceChartProps) => {
  const [interval, setInterval] = useState('1m');
  const binanceSymbol = symbol.replace(' / ', '').replace('/', '');
  const { klines, loading } = useBinanceKlines(binanceSymbol, interval);
  const [liveKlines, setLiveKlines] = useState<KlineData[]>([]);
  const isPositive = parseFloat(priceChange) >= 0;
  const price = parseFloat(currentPrice) || 0;

  // Merge static klines with live ticker price — update the last candle's close in real time
  useEffect(() => {
    if (klines.length === 0) { setLiveKlines([]); return; }
    if (!price) { setLiveKlines(klines); return; }
    const updated = klines.map((k, i) =>
      i === klines.length - 1 ? { ...k, close: price, high: Math.max(k.high, price), low: Math.min(k.low, price) } : k
    );
    setLiveKlines(updated);
  }, [klines, price]);

  const displayed = liveKlines.length > 0 ? liveKlines : klines;

  const yMin = displayed.length > 0 ? Math.min(...displayed.map(k => k.close)) * 0.999 : 'auto';
  const yMax = displayed.length > 0 ? Math.max(...displayed.map(k => k.close)) * 1.001 : 'auto';

  return (
    <div className="trading-card p-4 animate-fade-in-up" style={{ animationDelay: '320ms' }}>
      <div className="flex items-center justify-between mb-3">
        <div>
          <h3 className="text-xs font-medium text-muted-foreground">{symbol}</h3>
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-semibold font-mono tabular-nums">
              ${price > 0 ? price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: price < 1 ? 6 : 2 }) : '—'}
            </span>
            <span className={`text-sm font-mono ${isPositive ? 'text-gain' : 'text-loss'}`}>
              {isPositive ? '+' : ''}{priceChange}%
            </span>
            {/* Live pulse */}
            <span className="flex items-center gap-1 text-[10px] text-gain font-mono">
              <span className="w-1.5 h-1.5 rounded-full bg-gain animate-pulse inline-block" />
              LIVE
            </span>
          </div>
        </div>
        <div className="flex gap-0.5 flex-wrap justify-end">
          {TIMEFRAMES.map(tf => (
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
        {loading && displayed.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center">
            <Loader2 className="w-5 h-5 text-muted-foreground animate-spin" />
          </div>
        )}
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={displayed} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={isPositive ? 'hsl(var(--gain))' : 'hsl(var(--loss))'} stopOpacity={0.3} />
                <stop offset="95%" stopColor={isPositive ? 'hsl(var(--gain))' : 'hsl(var(--loss))'} stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis dataKey="time" tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 9, fontFamily: 'JetBrains Mono' }} axisLine={false} tickLine={false} interval="preserveStartEnd" />
            <YAxis domain={[yMin, yMax]} tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 9, fontFamily: 'JetBrains Mono' }} axisLine={false} tickLine={false} width={60}
              tickFormatter={v => `$${v >= 1000 ? (v / 1000).toFixed(1) + 'k' : v.toFixed(2)}`}
            />
            <Tooltip
              contentStyle={{ background: 'hsl(var(--popover))', border: '1px solid hsl(var(--border))', borderRadius: '8px', fontSize: 11, fontFamily: 'JetBrains Mono' }}
              labelStyle={{ color: 'hsl(var(--muted-foreground))' }}
              itemStyle={{ color: 'hsl(var(--foreground))' }}
              formatter={(v: number) => [`$${v.toLocaleString('en-US', { minimumFractionDigits: 2 })}`, 'Price']}
            />
            <Area type="monotone" dataKey="close"
              stroke={isPositive ? 'hsl(var(--gain))' : 'hsl(var(--loss))'}
              strokeWidth={1.5} fill="url(#priceGradient)" dot={false} isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default PriceChart;
