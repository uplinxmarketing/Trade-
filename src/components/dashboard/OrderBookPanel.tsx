import { useState, useEffect, useRef } from 'react';
import { ChevronUp, ChevronDown } from 'lucide-react';

interface Props {
  activeCoin: string;
  prices: Record<string, { price: string; priceChangePercent: string }>;
}

interface DepthData {
  bids: [string, string][];
  asks: [string, string][];
}

interface Stats24h {
  volume: string;
  quoteVolume: string;
  highPrice: string;
  lowPrice: string;
  priceChangePercent: string;
  count: number;
}

function fmtPrice(p: number) {
  if (p <= 0) return '—';
  if (p >= 1000) return p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (p >= 1) return p.toFixed(4);
  return p.toFixed(6);
}

function fmtAmt(v: number) {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(2)}K`;
  return v.toFixed(4);
}

function fmtUsdt(v: number) {
  if (v >= 1_000_000_000) return `$${(v / 1_000_000_000).toFixed(2)}B`;
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(2)}K`;
  return `$${v.toFixed(2)}`;
}

const OrderBookPanel = ({ activeCoin, prices }: Props) => {
  const [expanded, setExpanded] = useState(true);
  const [depth, setDepth] = useState<DepthData | null>(null);
  const [stats, setStats] = useState<Stats24h | null>(null);
  const depthAbort = useRef<AbortController | null>(null);
  const statsAbort = useRef<AbortController | null>(null);

  useEffect(() => {
    if (!activeCoin) return;

    setDepth(null);
    setStats(null);

    const fetchDepth = () => {
      if (depthAbort.current) depthAbort.current.abort();
      depthAbort.current = new AbortController();
      fetch(`/api/proxy/binance/depth?symbol=${activeCoin}&limit=15`, { signal: depthAbort.current.signal })
        .then(r => r.json())
        .then((d: any) => {
          if (Array.isArray(d?.asks) && Array.isArray(d?.bids)) setDepth(d as DepthData);
        })
        .catch(() => {});
    };

    const fetchStats = () => {
      if (statsAbort.current) statsAbort.current.abort();
      statsAbort.current = new AbortController();
      fetch(`/api/proxy/binance/ticker/24hr?symbol=${activeCoin}`, { signal: statsAbort.current.signal })
        .then(r => (r.ok ? r.json() : null))
        // Skip update on error responses — never store an {error: ...} envelope
        .then((d: any) => { if (d && !d.error && d.highPrice !== undefined) setStats(d as Stats24h); })
        .catch(() => {});
    };

    fetchDepth();
    fetchStats();

    const depthTimer = setInterval(fetchDepth, 2000);
    const statsTimer = setInterval(fetchStats, 10000);

    return () => {
      clearInterval(depthTimer);
      clearInterval(statsTimer);
      depthAbort.current?.abort();
      statsAbort.current?.abort();
    };
  }, [activeCoin]);

  const lp = prices[activeCoin];
  const midPrice = parseFloat(lp?.price ?? '0');
  const changePct = parseFloat(lp?.priceChangePercent ?? '0');
  const isUp = changePct >= 0;
  const ticker = activeCoin.replace('USDT', '');

  const asks = depth ? [...depth.asks].slice(0, 8).reverse() : [];
  const bids = depth ? [...depth.bids].slice(0, 8) : [];

  const topAsk = asks.length ? parseFloat(asks[asks.length - 1][0]) : 0;
  const topBid = bids.length ? parseFloat(bids[0][0]) : 0;
  const spread = topAsk > 0 && topBid > 0 ? topAsk - topBid : 0;
  const spreadPct = topBid > 0 && spread > 0 ? ((spread / topBid) * 100).toFixed(3) : '—';

  const maxAskSize = asks.reduce((m, [, s]) => Math.max(m, parseFloat(s)), 0);
  const maxBidSize = bids.reduce((m, [, s]) => Math.max(m, parseFloat(s)), 0);

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      <div
        className="flex items-center justify-between px-4 py-2 cursor-pointer select-none border-b border-border"
        onClick={() => setExpanded(v => !v)}
      >
        <span className="text-xs font-semibold text-foreground tracking-wide">Order Book & 24h Stats</span>
        {expanded ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" /> : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
      </div>

      {expanded && (
        <div className="flex divide-x divide-border">
          {/* Left — Order Book */}
          <div className="flex-1 min-w-0 p-3 space-y-1">
            <div className="flex items-center gap-2 mb-2">
              <span className="text-[10px] font-semibold text-foreground uppercase tracking-wider">Order Book</span>
              {spread > 0 && (
                <span className="text-[9px] bg-muted text-muted-foreground rounded px-1.5 py-0.5 font-mono">
                  {fmtPrice(spread)} ({spreadPct}%)
                </span>
              )}
            </div>

            {/* Column headers */}
            <div className="grid grid-cols-3 text-[9px] text-muted-foreground mb-1 px-1">
              <span>Price</span>
              <span className="text-right">Amt ({ticker})</span>
              <span className="text-right">Total (USDT)</span>
            </div>

            {/* Asks */}
            <div className="space-y-px">
              {asks.length === 0
                ? Array.from({ length: 8 }).map((_, i) => (
                    <div key={i} className="h-4 bg-muted/30 rounded animate-pulse" />
                  ))
                : asks.map(([p, s], i) => {
                    const price = parseFloat(p);
                    const size = parseFloat(s);
                    const total = price * size;
                    const pct = maxAskSize > 0 ? (size / maxAskSize) * 100 : 0;
                    return (
                      <div key={i} className="relative grid grid-cols-3 text-[10px] font-mono tabular-nums px-1 py-px rounded overflow-hidden">
                        <div className="absolute inset-y-0 right-0 bg-loss/10 rounded" style={{ width: `${pct}%` }} />
                        <span className="relative text-loss">{fmtPrice(price)}</span>
                        <span className="relative text-right text-foreground/80">{fmtAmt(size)}</span>
                        <span className="relative text-right text-muted-foreground">{fmtUsdt(total)}</span>
                      </div>
                    );
                  })}
            </div>

            {/* Mid price row */}
            <div className="flex items-center justify-center py-1.5 my-0.5 border-y border-border/50">
              <span className={`text-sm font-bold font-mono tabular-nums ${isUp ? 'text-gain' : 'text-loss'}`}>
                {midPrice > 0 ? fmtPrice(midPrice) : '—'}
              </span>
              <span className={`ml-1.5 text-[9px] font-mono ${isUp ? 'text-gain' : 'text-loss'}`}>
                {isUp ? '▲' : '▼'} {Math.abs(changePct).toFixed(2)}%
              </span>
            </div>

            {/* Bids */}
            <div className="space-y-px">
              {bids.length === 0
                ? Array.from({ length: 8 }).map((_, i) => (
                    <div key={i} className="h-4 bg-muted/30 rounded animate-pulse" />
                  ))
                : bids.map(([p, s], i) => {
                    const price = parseFloat(p);
                    const size = parseFloat(s);
                    const total = price * size;
                    const pct = maxBidSize > 0 ? (size / maxBidSize) * 100 : 0;
                    return (
                      <div key={i} className="relative grid grid-cols-3 text-[10px] font-mono tabular-nums px-1 py-px rounded overflow-hidden">
                        <div className="absolute inset-y-0 right-0 bg-gain/10 rounded" style={{ width: `${pct}%` }} />
                        <span className="relative text-gain">{fmtPrice(price)}</span>
                        <span className="relative text-right text-foreground/80">{fmtAmt(size)}</span>
                        <span className="relative text-right text-muted-foreground">{fmtUsdt(total)}</span>
                      </div>
                    );
                  })}
            </div>
          </div>

          {/* Right — 24h Stats */}
          <div className="w-44 shrink-0 p-3">
            <div className="text-[10px] font-semibold text-foreground uppercase tracking-wider mb-3">24h Stats</div>

            {stats === null ? (
              <div className="space-y-2">
                {Array.from({ length: 7 }).map((_, i) => (
                  <div key={i} className="h-7 bg-muted/30 rounded animate-pulse" />
                ))}
              </div>
            ) : (
              <div className="space-y-2">
                {[
                  { label: `Vol (${ticker})`, value: fmtAmt(parseFloat(stats.volume)) },
                  { label: 'Vol (USDT)', value: fmtUsdt(parseFloat(stats.quoteVolume)) },
                  { label: '24h High', value: fmtPrice(parseFloat(stats.highPrice)) },
                  { label: '24h Low', value: fmtPrice(parseFloat(stats.lowPrice)) },
                  { label: 'Quote Vol', value: fmtUsdt(parseFloat(stats.quoteVolume)) },
                  { label: 'Trades', value: stats.count?.toLocaleString() ?? '—' },
                ].map(({ label, value }) => (
                  <div key={label} className="flex flex-col gap-0.5">
                    <span className="text-[9px] text-muted-foreground leading-none">{label}</span>
                    <span className="text-[10px] font-mono tabular-nums text-foreground leading-none">{value}</span>
                  </div>
                ))}

                <div className="flex flex-col gap-0.5">
                  <span className="text-[9px] text-muted-foreground leading-none">24h Change</span>
                  <span className={`text-[10px] font-mono tabular-nums leading-none font-semibold ${parseFloat(stats.priceChangePercent) >= 0 ? 'text-gain' : 'text-loss'}`}>
                    {parseFloat(stats.priceChangePercent) >= 0 ? '+' : ''}{parseFloat(stats.priceChangePercent).toFixed(2)}%
                  </span>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default OrderBookPanel;
