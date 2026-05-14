import { useState, useEffect, useRef } from 'react';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import { useBinanceKlines } from '@/hooks/useBinanceKlines';
import { Loader2 } from 'lucide-react';
import type { KlineData } from '@/lib/binance-types';
import { formatTime } from '@/lib/format';

interface PriceChartProps {
  symbol: string;
  currentPrice: string;
  priceChange: string;
}

const TIMEFRAMES = [
  { key: 'live', label: 'LIVE' },
  { key: '1m',   label: '1m'  },
  { key: '5m',   label: '5m'  },
  { key: '15m',  label: '15m' },
  { key: '1h',   label: '1H'  },
  { key: '4h',   label: '4H'  },
  { key: '1d',   label: '1D'  },
];

const MAX_LIVE_TICKS = 120;

const PriceChart = ({ symbol, currentPrice, priceChange }: PriceChartProps) => {
  const [interval, setInterval] = useState('live');
  const binanceSymbol = symbol.replace(' / ', '').replace('/', '');
  const { klines, loading } = useBinanceKlines(binanceSymbol, interval === 'live' ? '1m' : interval);

  // Live tick accumulator — one point per WebSocket price update (~1s)
  const [liveTicks, setLiveTicks] = useState<KlineData[]>([]);
  const lastPrice = useRef<string>('');

  const isPositive = parseFloat(priceChange) >= 0;
  const price = parseFloat(currentPrice) || 0;

  useEffect(() => {
    if (interval !== 'live') return;
    if (!currentPrice || currentPrice === lastPrice.current) return;
    lastPrice.current = currentPrice;
    const now = new Date();
    const label = formatTime(now);
    setLiveTicks(prev => {
      const next = [...prev, { time: label, open: price, high: price, low: price, close: price, volume: 0 }];
      return next.length > MAX_LIVE_TICKS ? next.slice(next.length - MAX_LIVE_TICKS) : next;
    });
  }, [currentPrice, interval, price]);

  // Reset ticks when entering live mode
  useEffect(() => {
    if (interval === 'live') { setLiveTicks([]); lastPrice.current = ''; }
  }, [interval]);

  // For kline modes, merge live price into the last candle
  const [liveKlines, setLiveKlines] = useState<KlineData[]>([]);
  useEffect(() => {
    if (interval === 'live') return;
    if (klines.length === 0) { setLiveKlines([]); return; }
    if (!price) { setLiveKlines(klines); return; }
    const updated = klines.map((k, i) =>
      i === klines.length - 1
        ? { ...k, close: price, high: Math.max(k.high, price), low: Math.min(k.low, price) }
        : k
    );
    setLiveKlines(updated);
  }, [klines, price, interval]);

  const displayed = interval === 'live'
    ? liveTicks
    : (liveKlines.length > 0 ? liveKlines : klines);

  const yMin = displayed.length > 1 ? Math.min(...displayed.map(k => k.close)) * 0.9995 : 'auto';
  const yMax = displayed.length > 1 ? Math.max(...displayed.map(k => k.close)) * 1.0005 : 'auto';

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
                  ? tf.key === 'live'
                    ? 'bg-gain/20 text-gain font-semibold border border-gain/40'
                    : 'bg-secondary text-foreground font-semibold'
                  : 'text-muted-foreground hover:text-foreground hover:bg-secondary/50'
              }`}
            >
              {tf.label}
            </button>
          ))}
        </div>
      </div>

      <div className="h-52 relative">
        {interval === 'live' && liveTicks.length < 3 && (
          <div className="absolute inset-0 flex items-center justify-center gap-2 text-xs text-muted-foreground">
            <span className="w-1.5 h-1.5 rounded-full bg-gain animate-pulse inline-block" />
            Collecting live ticks…
          </div>
        )}
        {interval !== 'live' && loading && displayed.length === 0 && (
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
            <XAxis
              dataKey="time"
              tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 9, fontFamily: 'JetBrains Mono' }}
              axisLine={false} tickLine={false}
              interval={interval === 'live' ? Math.max(1, Math.floor(liveTicks.length / 6)) : 'preserveStartEnd'}
            />
            <YAxis
              domain={[yMin, yMax]}
              tick={{ fill: 'hsl(var(--muted-foreground))', fontSize: 9, fontFamily: 'JetBrains Mono' }}
              axisLine={false} tickLine={false} width={60}
              tickFormatter={v => `$${v >= 1000 ? (v / 1000).toFixed(1) + 'k' : v.toFixed(2)}`}
            />
            <Tooltip
              contentStyle={{ background: 'hsl(var(--popover))', border: '1px solid hsl(var(--border))', borderRadius: '8px', fontSize: 11, fontFamily: 'JetBrains Mono' }}
              labelStyle={{ color: 'hsl(var(--muted-foreground))' }}
              itemStyle={{ color: 'hsl(var(--foreground))' }}
              formatter={(v: number) => [`$${v.toLocaleString('en-US', { minimumFractionDigits: 2 })}`, 'Price']}
            />
            <Area
              type="monotone" dataKey="close"
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
