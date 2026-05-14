import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { useBinanceKlines } from '@/hooks/useBinanceKlines';
import { TAKER_FEE } from '@/lib/trading-engine';
import { supabase } from '@/integrations/supabase/client';
import { Loader2 } from 'lucide-react';
import type { LivePrices } from '@/lib/trading-engine';
import { formatTime } from '@/lib/format';

const BREAK_EVEN_MULT = 1 / Math.pow(1 - TAKER_FEE, 2);

const TF = [
  { key: 'live', label: 'LIVE' }, { key: '30s', label: '30s' },
  { key: '1m', label: '1m' },    { key: '3m', label: '3m' },
  { key: '5m', label: '5m' },    { key: '15m', label: '15m' },
  { key: '1h', label: '1h' },    { key: '4h', label: '4h' },
  { key: '1d', label: '1d' },    { key: '1w', label: '1w' },
];

type ChartType = 'candles' | 'line' | 'area';

interface Overlays { ma: boolean; bb: boolean; entry: boolean; breakeven: boolean; volume: boolean; }

interface Props { activeCoin: string; prices: LivePrices; }

function fmtP(p: number) {
  if (p <= 0) return '';
  if (p >= 1000) return p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (p >= 1) return p.toFixed(4);
  return p.toFixed(6);
}

export default function ChartPanelV2({ activeCoin, prices }: Props) {
  const [interval, setIntervalState] = useState('15m');
  const [chartType, setChartType] = useState<ChartType>('candles');
  const [overlays, setOverlays] = useState<Overlays>({ ma: true, bb: false, entry: true, breakeven: true, volume: true });
  const [entryPrice, setEntryPrice] = useState(0);
  const canvasRef   = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Tick buffer for LIVE and 30s synthetic modes
  const tickBufferRef = useRef<{ ts: number; price: number }[]>([]);
  const [liveTick, setLiveTick] = useState(0); // increment to force re-render in live modes

  const binanceSymbol = activeCoin.replace('/', '').replace(' ', '');
  const { klines: rawKlines, loading, error } = useBinanceKlines(binanceSymbol, interval);
  const livePrice = parseFloat(prices[activeCoin]?.price ?? '0');

  // Reset tick buffer when coin changes
  useEffect(() => { tickBufferRef.current = []; }, [activeCoin]);

  // Push each new price tick into the buffer; only re-render when in live/30s mode
  useEffect(() => {
    if (livePrice <= 0) return;
    const now = Date.now();
    const buf = tickBufferRef.current;
    // Skip duplicate consecutive prices
    if (buf.length > 0 && buf[buf.length - 1].price === livePrice) return;
    tickBufferRef.current = [...buf.filter(t => now - t.ts < 10 * 60 * 1000), { ts: now, price: livePrice }].slice(-1200);
    if (interval === 'live' || interval === '30s') setLiveTick(t => t + 1);
  }, [livePrice, interval]);

  // Synthetic klines derived from tick buffer (LIVE = every tick, 30s = bucketed candles)
  const tickKlines = useMemo(() => {
    const buf = tickBufferRef.current;
    if (interval === 'live') {
      return buf.map(t => ({
        time: formatTime(t.ts),
        open: t.price, high: t.price, low: t.price, close: t.price, volume: 0,
      }));
    }
    if (interval === '30s') {
      if (buf.length === 0) return [];
      const buckets = new Map<number, { open: number; high: number; low: number; close: number }>();
      for (const t of buf) {
        const b30 = Math.floor(t.ts / 30000) * 30000;
        const bk = buckets.get(b30);
        if (!bk) buckets.set(b30, { open: t.price, high: t.price, low: t.price, close: t.price });
        else { bk.high = Math.max(bk.high, t.price); bk.low = Math.min(bk.low, t.price); bk.close = t.price; }
      }
      return Array.from(buckets.entries()).sort((a, b) => a[0] - b[0]).map(([ts, bk]) => ({
        time: formatTime(ts),
        open: bk.open, high: bk.high, low: bk.low, close: bk.close, volume: 0,
      }));
    }
    return [];
  }, [interval, liveTick]); // liveTick forces recompute on each new tick

  const isTickMode = interval === 'live' || interval === '30s';

  // Merge live price into last candle (kline modes only)
  const klines = isTickMode ? tickKlines
    : rawKlines.length > 0 && livePrice > 0
      ? rawKlines.map((k, i) => i === rawKlines.length - 1
          ? { ...k, close: livePrice, high: Math.max(k.high, livePrice), low: Math.min(k.low, livePrice) }
          : k)
      : rawKlines;

  const setInterval = (tf: string) => {
    setIntervalState(tf);
    // Auto-switch chart type for tick modes
    if (tf === 'live') setChartType('area');
    else if (tf === '30s') setChartType('candles');
  };

  const breakEven = entryPrice > 0 ? entryPrice * BREAK_EVEN_MULT : 0;

  useEffect(() => {
    const fetchEntry = () => supabase.from('paper_portfolio').select('avg_entry_price')
      .eq('user_session', 'default').eq('symbol', activeCoin).gt('quantity', 0).maybeSingle()
      .then(({ data }) => setEntryPrice(data ? Number(data.avg_entry_price) : 0));
    fetchEntry();
    const ch = supabase.channel(`chart-entry-${activeCoin}`)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' },
        (p) => { if ((p.new as any)?.symbol === activeCoin || (p.old as any)?.symbol === activeCoin) fetchEntry(); })
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [activeCoin]);

  const toggle = (key: keyof Overlays) =>
    setOverlays(prev => ({ ...prev, [key]: !prev[key] }));

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container || klines.length === 0) return;

    const dpr = window.devicePixelRatio || 1;
    const W = container.clientWidth;
    const H = container.clientHeight;
    canvas.width  = W * dpr;
    canvas.height = H * dpr;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.scale(dpr, dpr);

    const VOL_H  = overlays.volume ? Math.round(H * 0.18) : 0;
    const MAIN_H = H - VOL_H;
    const PAD    = { top: 14, right: 72, bottom: 22, left: 4 };
    const cW     = W - PAD.left - PAD.right;
    const cH     = MAIN_H - PAD.top - PAD.bottom;

    // Price range
    let minP = Math.min(...klines.map(k => k.low));
    let maxP = Math.max(...klines.map(k => k.high));
    // Extend range to include entry/breakeven lines if visible
    if (overlays.entry && entryPrice > 0) { minP = Math.min(minP, entryPrice); maxP = Math.max(maxP, entryPrice); }
    if (overlays.breakeven && breakEven > 0) { minP = Math.min(minP, breakEven); maxP = Math.max(maxP, breakEven); }
    const pRange = (maxP - minP) || maxP * 0.001;
    minP -= pRange * 0.04;
    maxP += pRange * 0.04;
    const pRangeF = maxP - minP;

    const toX = (i: number) => PAD.left + (i + 0.5) * (cW / klines.length);
    const toY = (p: number) => PAD.top + (1 - (p - minP) / pRangeF) * cH;
    const CW  = Math.max(1, Math.floor(cW / klines.length * 0.72));

    // Background
    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = 'hsl(220 13% 9%)';
    ctx.fillRect(0, 0, W, H);

    // Horizontal grid lines
    ctx.strokeStyle = 'rgba(255,255,255,0.05)';
    ctx.lineWidth = 1;
    for (let g = 1; g <= 4; g++) {
      const y = PAD.top + (g / 5) * cH;
      ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(W - PAD.right, y); ctx.stroke();
    }

    // MA20
    if (overlays.ma && klines.length >= 20) {
      ctx.strokeStyle = '#3b82f6';
      ctx.lineWidth = 1.5;
      ctx.setLineDash([]);
      ctx.beginPath();
      let started = false;
      for (let i = 19; i < klines.length; i++) {
        const ma = klines.slice(i - 19, i + 1).reduce((s, k) => s + k.close, 0) / 20;
        const x = toX(i), y = toY(ma);
        if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
      }
      ctx.stroke();
    }

    // Bollinger Bands
    if (overlays.bb && klines.length >= 20) {
      const upper: number[] = [], lower: number[] = [];
      for (let i = 19; i < klines.length; i++) {
        const sl = klines.slice(i - 19, i + 1).map(k => k.close);
        const ma = sl.reduce((s, v) => s + v, 0) / 20;
        const std = Math.sqrt(sl.reduce((s, v) => s + (v - ma) ** 2, 0) / 20);
        upper.push(ma + 2 * std);
        lower.push(ma - 2 * std);
      }
      ctx.strokeStyle = '#f59e0b';
      ctx.lineWidth = 1;
      ctx.setLineDash([4, 3]);
      [upper, lower].forEach(band => {
        ctx.beginPath();
        band.forEach((v, j) => {
          const x = toX(j + 19), y = toY(v);
          j === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
        });
        ctx.stroke();
      });
      ctx.setLineDash([]);
    }

    // Candles / Line / Area
    if (chartType === 'candles') {
      klines.forEach((k, i) => {
        const x = toX(i);
        const isUp = k.close >= k.open;
        const col  = isUp ? '#22c55e' : '#ef4444';
        ctx.strokeStyle = col;
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(x, toY(k.high)); ctx.lineTo(x, toY(k.low)); ctx.stroke();
        const y1 = toY(Math.max(k.open, k.close));
        const y2 = toY(Math.min(k.open, k.close));
        ctx.fillStyle = col;
        ctx.fillRect(x - CW / 2, y1, CW, Math.max(1, y2 - y1));
      });
    } else {
      // Line or Area
      const lineColor = livePrice >= (klines[0]?.close ?? livePrice) ? '#22c55e' : '#ef4444';
      ctx.beginPath();
      klines.forEach((k, i) => {
        const x = toX(i), y = toY(k.close);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      });
      if (chartType === 'area') {
        ctx.lineTo(toX(klines.length - 1), PAD.top + cH);
        ctx.lineTo(toX(0), PAD.top + cH);
        ctx.closePath();
        ctx.fillStyle = lineColor + '22';
        ctx.fill();
        ctx.beginPath();
        klines.forEach((k, i) => { const x = toX(i), y = toY(k.close); i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y); });
      }
      ctx.strokeStyle = lineColor;
      ctx.lineWidth = 1.5;
      ctx.setLineDash([]);
      ctx.stroke();
    }

    // Helper: draw horizontal dashed line with right-edge label box
    const drawHLine = (p: number, color: string, label: string) => {
      if (p <= 0 || p < minP || p > maxP) return;
      const y = toY(p);
      ctx.strokeStyle = color;
      ctx.lineWidth = 1;
      ctx.setLineDash([5, 4]);
      ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(W - PAD.right, y); ctx.stroke();
      ctx.setLineDash([]);
      // Price box on right
      ctx.fillStyle = color;
      ctx.fillRect(W - PAD.right, y - 8, PAD.right - 1, 16);
      ctx.fillStyle = '#fff';
      ctx.font = 'bold 9px monospace';
      ctx.textAlign = 'left';
      ctx.fillText(fmtP(p), W - PAD.right + 3, y + 4);
      // Label text above line
      ctx.fillStyle = color;
      ctx.font = '9px monospace';
      ctx.fillText(label, PAD.left + 4, y - 3);
    };

    if (overlays.entry)    drawHLine(entryPrice, '#22c55e', 'Entry');
    if (overlays.breakeven) drawHLine(breakEven,  '#6366f1', 'Breakeven');

    // Y-axis price labels
    ctx.fillStyle = 'rgba(255,255,255,0.38)';
    ctx.font = '9px monospace';
    ctx.textAlign = 'left';
    for (let i = 0; i <= 5; i++) {
      const p = minP + (i / 5) * pRangeF;
      const y = PAD.top + (1 - i / 5) * cH;
      ctx.fillText(fmtP(p), W - PAD.right + 4, y + 4);
    }

    // X-axis time labels
    ctx.textAlign = 'center';
    const step = Math.max(1, Math.floor(klines.length / 6));
    for (let i = 0; i < klines.length; i += step) {
      ctx.fillText(klines[i].time, toX(i), MAIN_H - 3);
    }

    // Volume sub-panel
    if (overlays.volume && VOL_H > 0) {
      const maxVol = Math.max(...klines.map(k => k.volume), 1);
      const vTop   = MAIN_H + 3;
      const vCH    = VOL_H - 6;
      klines.forEach((k, i) => {
        const x = toX(i);
        const h = (k.volume / maxVol) * vCH;
        ctx.fillStyle = k.close >= k.open ? 'rgba(34,197,94,0.45)' : 'rgba(239,68,68,0.45)';
        ctx.fillRect(x - CW / 2, vTop + vCH - h, CW, Math.max(1, h));
      });
      // Volume divider
      ctx.strokeStyle = 'rgba(255,255,255,0.06)';
      ctx.lineWidth = 1;
      ctx.setLineDash([]);
      ctx.beginPath(); ctx.moveTo(PAD.left, MAIN_H); ctx.lineTo(W, MAIN_H); ctx.stroke();
    }
  }, [klines, overlays, chartType, entryPrice, breakEven, livePrice]);

  // Redraw on data/overlay change
  useEffect(() => { draw(); }, [draw]);

  // Redraw on container resize
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => { draw(); });
    ro.observe(el);
    return () => ro.disconnect();
  }, [draw]);

  const overlayBtns: { key: keyof Overlays; label: string }[] = [
    { key: 'ma', label: 'MA(20)' }, { key: 'bb', label: 'BB' },
    { key: 'entry', label: 'Entry' }, { key: 'breakeven', label: 'Breakeven' },
    { key: 'volume', label: 'Volume' },
  ];

  // Mini sparkline from last 30 closes — shown before timeframe buttons
  const sparkPts = klines.slice(-30).map(k => k.close);
  const sparkPath = (() => {
    if (sparkPts.length < 2) return '';
    const mn = Math.min(...sparkPts), mx = Math.max(...sparkPts);
    const rng = mx - mn || 1;
    return sparkPts.map((p, i) => {
      const x = (i / (sparkPts.length - 1)) * 52;
      const y = 18 - ((p - mn) / rng) * 18;
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
  })();
  const sparkUp = sparkPts.length >= 2 && sparkPts[sparkPts.length - 1] >= sparkPts[0];

  return (
    <div className="trading-card flex flex-col h-full overflow-hidden">
      {/* Toolbar — single scrollable row, never wraps */}
      <div className="flex items-center gap-0.5 px-2 py-1 border-b border-border overflow-x-auto scrollbar-none shrink-0">

        {/* Live sparkline preview */}
        {sparkPath && (
          <>
            <svg width="52" height="20" viewBox="0 0 52 20" className="shrink-0 mr-1.5">
              <path d={sparkPath} fill="none" stroke={sparkUp ? '#0ecb81' : '#f6465d'} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
            <div className="w-px h-3.5 bg-border mx-1 shrink-0" />
          </>
        )}

        {/* Timeframes */}
        {TF.map(tf => (
          <button key={tf.key} onClick={() => setInterval(tf.key)}
            className={`shrink-0 px-2 py-0.5 text-[10px] rounded transition-colors flex items-center gap-1 ${
              interval === tf.key ? 'bg-secondary text-foreground font-semibold' : 'text-muted-foreground hover:text-foreground hover:bg-secondary/50'
            } ${tf.key === 'live' ? 'text-gain' : ''}`}>
            {tf.key === 'live' && <span className={`w-1.5 h-1.5 rounded-full bg-gain shrink-0 ${interval === 'live' ? 'animate-pulse' : 'opacity-50'}`} />}
            {tf.label}
          </button>
        ))}

        <div className="w-px h-3.5 bg-border mx-1 shrink-0" />

        {/* Chart type */}
        {(['Candles', 'Line', 'Area'] as const).map(ct => (
          <button key={ct} onClick={() => setChartType(ct.toLowerCase() as ChartType)}
            className={`shrink-0 px-2 py-0.5 text-[10px] rounded transition-colors ${
              chartType === ct.toLowerCase() ? 'bg-secondary text-foreground font-semibold' : 'text-muted-foreground hover:text-foreground hover:bg-secondary/50'
            }`}>
            {ct}
          </button>
        ))}

        <div className="w-px h-3.5 bg-border mx-1 shrink-0" />

        {/* Overlay toggles */}
        {overlayBtns.map(o => (
          <button key={o.key} onClick={() => toggle(o.key)}
            className={`shrink-0 px-1.5 py-0.5 text-[10px] rounded border transition-colors ${
              overlays[o.key]
                ? o.key === 'ma'        ? 'bg-blue-500/20 border-blue-500/50 text-blue-400'
                : o.key === 'bb'        ? 'bg-amber-500/20 border-amber-500/50 text-amber-400'
                : o.key === 'entry'     ? 'bg-gain/20 border-gain/50 text-gain'
                : o.key === 'breakeven' ? 'bg-accent/20 border-accent/50 text-accent'
                : 'bg-secondary border-border text-foreground'
                : 'border-border/50 text-muted-foreground hover:text-foreground'
            }`}>
            {o.label}
          </button>
        ))}
      </div>

      {/* Canvas */}
      <div ref={containerRef} className="flex-1 relative min-h-0">
        {loading && !isTickMode && klines.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center z-10">
            <Loader2 className="w-5 h-5 animate-spin text-muted-foreground" />
          </div>
        )}
        {error && !loading && klines.length === 0 && (
          <div className="absolute inset-0 flex flex-col items-center justify-center z-10 gap-2">
            <span className="text-loss text-sm font-semibold">Symbol unavailable</span>
            <span className="text-[11px] text-muted-foreground font-mono">{binanceSymbol}</span>
            <span className="text-[10px] text-muted-foreground">{error}</span>
          </div>
        )}
        {isTickMode && klines.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center z-10">
            <span className="text-[11px] text-muted-foreground animate-pulse">Waiting for price ticks…</span>
          </div>
        )}
        <canvas ref={canvasRef} className="absolute inset-0 w-full h-full" />
      </div>

      {/* Legend */}
      <div className="flex gap-3 px-3 py-1 border-t border-border text-[9px] overflow-x-auto scrollbar-none shrink-0">
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-gain inline-block" /> Up candle</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-loss inline-block" /> Down candle</span>
        {overlays.ma && <span className="flex items-center gap-1"><span className="w-4 h-0.5 bg-blue-500 inline-block" /> MA(20)</span>}
        {overlays.bb && <span className="flex items-center gap-1"><span className="w-4 h-0.5 bg-amber-500 inline-block" /> BB</span>}
        {overlays.entry && entryPrice > 0 && <span className="flex items-center gap-1"><span className="w-4 h-0.5 bg-gain inline-block" /> Entry</span>}
        {overlays.breakeven && breakEven > 0 && <span className="flex items-center gap-1"><span className="w-4 h-0.5 bg-accent inline-block" /> Breakeven</span>}
      </div>
    </div>
  );
}
