import { useState, useEffect } from 'react';
import { TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { supabase } from '@/integrations/supabase/client';
import { CoinIcon } from '@/components/CoinIcon';

const BREAK_EVEN_MULT = 1 / Math.pow(1 - TAKER_FEE, 2);

const COIN_COLORS: Record<string, string> = {
  BTC: '#F7931A', ETH: '#627EEA', SOL: '#9945FF', BNB: '#F3BA2F',
  DOGE: '#C3A634', XRP: '#346AA9', ADA: '#0033AD', AVAX: '#E84142',
  DOT: '#E6007A', LINK: '#2A5ADA', MATIC: '#8247E5', UNI: '#FF007A',
  LTC: '#BFBBBB', ATOM: '#2E3148', SHIB: '#FFA409',
};

interface Ticker24h {
  highPrice: string;
  lowPrice: string;
  volume: string;
  quoteVolume: string;
}

interface Props {
  activeCoin: string;
  prices: LivePrices;
}

function fmtVol(v: number) {
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return v.toFixed(2);
}

function fmtPrice(p: number) {
  if (p <= 0) return '—';
  if (p >= 1000) return p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (p >= 1) return p.toFixed(4);
  return p.toFixed(6);
}

export default function MarketStatsBar({ activeCoin, prices }: Props) {
  const [ticker, setTicker]       = useState<Ticker24h | null>(null);
  const [entryPrice, setEntryPrice] = useState(0);

  const lp = prices[activeCoin];
  const price = parseFloat(lp?.price ?? '0');
  const changePct = parseFloat(lp?.priceChangePercent ?? '0');
  const ticker24h = ticker;
  const high = ticker24h ? parseFloat(ticker24h.highPrice) : 0;
  const low  = ticker24h ? parseFloat(ticker24h.lowPrice)  : 0;
  const volBase  = ticker24h ? parseFloat(ticker24h.volume)      : 0;
  const volQuote = ticker24h ? parseFloat(ticker24h.quoteVolume) : 0;
  const breakEven = entryPrice > 0 ? entryPrice * BREAK_EVEN_MULT : 0;
  const ticker3 = activeCoin.replace('USDT', '');
  const coinColor = COIN_COLORS[ticker3] ?? '#6b7280';

  useEffect(() => {
    if (!activeCoin) return;
    fetch(`/api/proxy/binance/ticker/24hr?symbol=${activeCoin}`)
      .then(r => (r.ok ? r.json() : null))
      // Skip update on error responses — keep the last good ticker instead of
      // poisoning state with an error envelope ({error: ...} → NaN stats).
      .then(d => { if (d && !d.error && d.highPrice !== undefined) setTicker(d); })
      .catch(() => {});
  }, [activeCoin]);

  useEffect(() => {
    supabase.from('paper_portfolio').select('avg_entry_price')
      .eq('user_session', 'default').eq('symbol', activeCoin).gt('quantity', 0).maybeSingle()
      .then(({ data }) => setEntryPrice(data ? Number(data.avg_entry_price) : 0));

    const ch = supabase.channel(`msb-${activeCoin}`)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, () => {
        supabase.from('paper_portfolio').select('avg_entry_price')
          .eq('user_session', 'default').eq('symbol', activeCoin).gt('quantity', 0).maybeSingle()
          .then(({ data }) => setEntryPrice(data ? Number(data.avg_entry_price) : 0));
      }).subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [activeCoin]);

  const stats = [
    { label: '24h high',       value: high > 0 ? `$${fmtPrice(high)}` : '—',            color: 'text-gain' },
    { label: '24h low',        value: low  > 0 ? `$${fmtPrice(low)}`  : '—',            color: 'text-loss' },
    { label: `24h vol (${ticker3})`, value: volBase > 0 ? fmtVol(volBase) : '—',        color: 'text-muted-foreground' },
    { label: '24h vol (USDT)', value: volQuote > 0 ? `$${fmtVol(volQuote)}` : '—',      color: 'text-muted-foreground' },
    { label: 'Entry price',    value: entryPrice > 0 ? `$${fmtPrice(entryPrice)}` : '—', color: 'text-gain' },
    { label: 'Breakeven',      value: breakEven  > 0 ? `$${fmtPrice(breakEven)}`  : '—', color: 'text-accent' },
  ];

  return (
    <div className="flex items-center gap-4 px-4 py-2 border-b border-border bg-card/80 overflow-x-auto scrollbar-thin">
      {/* Coin badge */}
      <div className="flex items-center gap-2 flex-shrink-0">
        <CoinIcon symbol={ticker3} size={28} />
        <div>
          <div className="text-xs font-bold text-foreground leading-none">{ticker3}USDT</div>
          <div className="text-[9px] text-muted-foreground">Spot</div>
        </div>
      </div>

      {/* Live price */}
      <div className="flex items-baseline gap-2 flex-shrink-0">
        <span className={`text-2xl font-mono font-semibold tabular-nums ${changePct >= 0 ? 'text-gain' : 'text-loss'}`}>
          {price > 0 ? fmtPrice(price) : '—'}
        </span>
        <span className={`text-xs font-mono px-1.5 py-0.5 rounded ${changePct >= 0 ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'}`}>
          {changePct >= 0 ? '+' : ''}{changePct.toFixed(2)}%
        </span>
      </div>

      {/* Divider */}
      <div className="h-8 w-px bg-border flex-shrink-0" />

      {/* Stats */}
      {stats.map(s => (
        <div key={s.label} className="flex-shrink-0">
          <div className="text-[9px] text-muted-foreground whitespace-nowrap">{s.label}</div>
          <div className={`text-xs font-mono font-semibold tabular-nums ${s.color}`}>{s.value}</div>
        </div>
      ))}
    </div>
  );
}
