import { useState, useEffect, useCallback } from 'react';
import { TrendingUp, TrendingDown, Info, ChevronDown } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';
import { TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { toast } from 'sonner';

const BREAK_EVEN_MULT = 1 / Math.pow(1 - TAKER_FEE, 2); // ≈1.002005

const ORDER_TYPES = [
  { id: 'market', label: 'Market', desc: 'Execute immediately at current market price.' },
  { id: 'limit',  label: 'Limit',  desc: 'Execute only at your specified price or better.' },
  { id: 'stop',   label: 'Stop-limit', desc: 'Triggers a limit order when stop price is hit.' },
  { id: 'oco',    label: 'OCO',    desc: 'One-Cancels-the-Other: limit + stop-limit pair.' },
] as const;

type OrderType = typeof ORDER_TYPES[number]['id'];

interface Props {
  selectedCoins: string[];
  prices: LivePrices;
  binanceConnected: boolean;
}

const TradingForm = ({ selectedCoins, prices, binanceConnected }: Props) => {
  const [side, setSide]         = useState<'BUY' | 'SELL'>('BUY');
  const [orderType, setOrderType] = useState<OrderType>('market');
  const [activeCoin, setActiveCoin] = useState(selectedCoins[0] ?? 'BTCUSDT');
  const [usdtAmount, setUsdtAmount] = useState('');
  const [limitPrice, setLimitPrice] = useState('');
  const [balance, setBalance]   = useState(10000);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (!selectedCoins.includes(activeCoin)) setActiveCoin(selectedCoins[0] ?? 'BTCUSDT');
  }, [selectedCoins, activeCoin]);

  useEffect(() => {
    supabase.from('bot_config').select('current_balance').eq('user_session', 'default').single()
      .then(({ data }) => { if (data) setBalance(Number(data.current_balance)); });
    const ch = supabase.channel('tf-balance')
      .on('postgres_changes', { event: 'UPDATE', schema: 'public', table: 'bot_config' }, (p) => {
        if (p.new?.current_balance) setBalance(Number(p.new.current_balance));
      }).subscribe();
    return () => { supabase.removeChannel(ch); };
  }, []);

  const currentPrice = parseFloat(prices[activeCoin]?.price ?? '0');
  const execPrice    = orderType === 'market' ? currentPrice : parseFloat(limitPrice) || currentPrice;
  const usdt         = parseFloat(usdtAmount) || 0;
  const ticker       = activeCoin.replace('USDT', '');

  const buyQty       = usdt > 0 && execPrice > 0 ? (usdt / execPrice) * (1 - TAKER_FEE) : 0;
  const buyFee       = usdt * TAKER_FEE;
  const sellRevenue  = buyQty * execPrice * (1 - TAKER_FEE);
  const sellFee      = buyQty * execPrice * TAKER_FEE;
  const roundTrip    = buyFee + sellFee;
  const breakEven    = execPrice * BREAK_EVEN_MULT;

  const pcts = [0, 25, 50, 75, 100];

  const applyPct = useCallback((pct: number) => {
    if (pct === 0) { setUsdtAmount(''); return; }
    setUsdtAmount(((balance * pct) / 100).toFixed(2));
  }, [balance]);

  const submit = async () => {
    if (!usdt || usdt < 1) { toast.error('Enter a valid USDT amount'); return; }
    if (execPrice <= 0) { toast.error('Price not available'); return; }
    if (usdt > balance && side === 'BUY') { toast.error('Insufficient balance'); return; }
    setSubmitting(true);
    try {
      if (side === 'BUY') {
        const { error } = await supabase.from('bot_trade_history').insert({
          user_session: 'default', symbol: activeCoin, side: 'BUY',
          price: execPrice, quantity: buyQty, pnl: null,
          reason: `Manual ${orderType} buy — ${ticker}`,
        });
        if (error) throw error;
        await supabase.from('paper_portfolio').upsert({
          user_session: 'default', symbol: activeCoin,
          quantity: buyQty, avg_entry_price: execPrice,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session,symbol' });
        await supabase.from('bot_config').update({
          current_balance: balance - usdt,
          updated_at: new Date().toISOString(),
        }).eq('user_session', 'default');
        toast.success(`Bought ${buyQty.toFixed(6)} ${ticker}`, { description: `at $${execPrice.toLocaleString()}` });
      } else {
        const { data: pos } = await supabase.from('paper_portfolio').select('quantity,avg_entry_price')
          .eq('user_session', 'default').eq('symbol', activeCoin).single();
        if (!pos || Number(pos.quantity) <= 0) { toast.error('No position to sell'); setSubmitting(false); return; }
        const sellQty      = Math.min(buyQty, Number(pos.quantity));
        const costBasis    = sellQty * Number(pos.avg_entry_price) / (1 - TAKER_FEE);
        const proceeds     = sellQty * execPrice * (1 - TAKER_FEE);
        const pnl          = proceeds - costBasis;
        await supabase.from('bot_trade_history').insert({
          user_session: 'default', symbol: activeCoin, side: 'SELL',
          price: execPrice, quantity: sellQty, pnl,
          reason: `Manual ${orderType} sell — ${ticker}`,
        });
        await supabase.from('bot_config').update({
          current_balance: balance + proceeds,
          updated_at: new Date().toISOString(),
        }).eq('user_session', 'default');
        toast.success(`Sold ${sellQty.toFixed(6)} ${ticker}`, { description: `PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}` });
      }
      setUsdtAmount('');
    } catch (e) {
      toast.error('Trade failed', { description: e instanceof Error ? e.message : 'Unknown error' });
    } finally {
      setSubmitting(false);
    }
  };

  const ordType = ORDER_TYPES.find(o => o.id === orderType)!;

  return (
    <div className="trading-card flex flex-col h-full">
      {/* Header */}
      <div className="p-3 border-b border-border flex items-center gap-2">
        <TrendingUp className="w-4 h-4 text-primary" />
        <span className="text-sm font-medium">Trade</span>
        {!binanceConnected && <span className="ml-auto text-[10px] text-muted-foreground bg-muted/30 px-2 py-0.5 rounded">Paper mode</span>}
      </div>

      <div className="p-3 space-y-3 flex-1 overflow-y-auto scrollbar-thin">
        {/* BUY / SELL tabs */}
        <div className="flex rounded-md overflow-hidden border border-border">
          <button
            onClick={() => setSide('BUY')}
            className={`flex-1 py-1.5 text-xs font-semibold transition-colors ${side === 'BUY' ? 'bg-gain text-white' : 'text-muted-foreground hover:bg-gain/10 hover:text-gain'}`}
          >
            BUY
          </button>
          <button
            onClick={() => setSide('SELL')}
            className={`flex-1 py-1.5 text-xs font-semibold transition-colors border-l border-border ${side === 'SELL' ? 'bg-loss text-white' : 'text-muted-foreground hover:bg-loss/10 hover:text-loss'}`}
          >
            SELL
          </button>
        </div>

        {/* Coin selector */}
        <div className="relative">
          <select
            value={activeCoin}
            onChange={e => setActiveCoin(e.target.value)}
            className="w-full bg-secondary border border-border rounded px-3 py-1.5 text-xs font-mono text-foreground appearance-none focus:outline-none focus:ring-1 focus:ring-ring pr-8"
          >
            {selectedCoins.map(c => (
              <option key={c} value={c}>{c.replace('USDT', '')} / USDT</option>
            ))}
          </select>
          <ChevronDown className="w-3 h-3 absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
        </div>

        {/* Order type buttons */}
        <div className="flex gap-1">
          {ORDER_TYPES.map(o => (
            <button key={o.id} onClick={() => setOrderType(o.id)}
              className={`flex-1 py-1 text-[10px] rounded transition-colors ${orderType === o.id ? 'bg-accent text-accent-foreground font-semibold' : 'bg-muted/20 text-muted-foreground hover:bg-muted/40'}`}>
              {o.label}
            </button>
          ))}
        </div>

        {/* Info bar */}
        <div className="flex items-start gap-1.5 text-[10px] text-muted-foreground bg-muted/20 rounded px-2 py-1.5">
          <Info className="w-3 h-3 flex-shrink-0 mt-px text-accent" />
          <span>{ordType.desc} Fee: {(TAKER_FEE * 100).toFixed(1)}% taker (paper BNB discount not applied).</span>
        </div>

        {/* Limit price (only for limit/stop/oco) */}
        {orderType !== 'market' && (
          <div>
            <label className="text-[10px] text-muted-foreground uppercase tracking-widest block mb-1">
              {orderType === 'stop' ? 'Stop Price (USDT)' : orderType === 'oco' ? 'Limit Price (USDT)' : 'Limit Price (USDT)'}
            </label>
            <input
              type="number" min="0" value={limitPrice}
              onChange={e => setLimitPrice(e.target.value)}
              placeholder={currentPrice > 0 ? currentPrice.toLocaleString() : '0.00'}
              className="w-full bg-secondary border border-border rounded px-2.5 py-1.5 text-xs font-mono text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            />
          </div>
        )}

        {/* Market price display */}
        {orderType === 'market' && (
          <div className="flex items-center justify-between text-[10px]">
            <span className="text-muted-foreground">Market price</span>
            <span className="font-mono text-foreground tabular-nums">
              {currentPrice > 0 ? `$${currentPrice.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 6 })}` : '—'}
            </span>
          </div>
        )}

        {/* USDT spend */}
        <div>
          <label className="text-[10px] text-muted-foreground uppercase tracking-widest block mb-1">
            {side === 'BUY' ? 'Spend (USDT)' : 'Value to sell (USDT equiv.)'}
          </label>
          <div className="relative">
            <input
              type="number" min="0" value={usdtAmount}
              onChange={e => setUsdtAmount(e.target.value)}
              placeholder="0.00"
              className="w-full bg-secondary border border-border rounded px-2.5 py-1.5 text-xs font-mono text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring pr-14"
            />
            <span className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[10px] text-muted-foreground font-mono">USDT</span>
          </div>
          <div className="text-[10px] text-muted-foreground mt-0.5">
            Available: <span className="text-foreground font-mono">${balance.toLocaleString('en-US', { minimumFractionDigits: 2 })}</span>
          </div>
        </div>

        {/* % quick buttons */}
        <div className="flex gap-1">
          {pcts.map(p => (
            <button key={p} onClick={() => applyPct(p)}
              className="flex-1 py-1 text-[10px] bg-muted/20 rounded hover:bg-muted/50 transition-colors text-muted-foreground hover:text-foreground">
              {p === 0 ? 'Reset' : `${p}%`}
            </button>
          ))}
        </div>

        {/* Estimated quantity */}
        <div className="flex items-center justify-between text-[10px]">
          <span className="text-muted-foreground">Est. {side === 'BUY' ? 'receive' : 'sell'} qty</span>
          <span className="font-mono tabular-nums text-foreground">
            {buyQty > 0 ? buyQty.toFixed(6) : '—'} {ticker}
          </span>
        </div>

        {/* Fee breakdown */}
        {usdt > 0 && execPrice > 0 && (
          <div className="bg-muted/10 border border-border rounded p-2.5 space-y-1.5">
            <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2">Fee breakdown</div>
            <div className="flex justify-between text-[10px]">
              <span className="text-muted-foreground">Buy fee</span>
              <span className="font-mono tabular-nums text-foreground">${buyFee.toFixed(4)} USDT</span>
            </div>
            <div className="flex justify-between text-[10px]">
              <span className="text-muted-foreground">Sell fee (est.)</span>
              <span className="font-mono tabular-nums text-foreground">${sellFee.toFixed(4)} USDT</span>
            </div>
            <div className="flex justify-between text-[10px] border-t border-border pt-1.5">
              <span className="text-muted-foreground">Round-trip total</span>
              <span className="font-mono tabular-nums text-foreground">${roundTrip.toFixed(4)} USDT</span>
            </div>
            {/* Breakeven price box */}
            <div className="mt-2 bg-gain/10 border border-gain/30 rounded px-2.5 py-2">
              <div className="text-[10px] text-gain font-semibold uppercase tracking-widest mb-0.5">Break-even price</div>
              <div className="font-mono text-sm font-bold text-gain tabular-nums">
                ${breakEven.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 6 })}
              </div>
              <div className="text-[9px] text-gain/70 mt-0.5">+{((BREAK_EVEN_MULT - 1) * 100).toFixed(4)}% above entry to cover fees</div>
            </div>
          </div>
        )}
      </div>

      {/* Submit */}
      <div className="p-3 border-t border-border">
        <button
          onClick={submit} disabled={submitting || !usdt || execPrice <= 0}
          className={`w-full py-2 rounded text-sm font-semibold transition-colors disabled:opacity-40 ${
            side === 'BUY'
              ? 'bg-gain hover:bg-gain/90 text-white'
              : 'bg-loss hover:bg-loss/90 text-white'
          }`}
        >
          {submitting ? 'Placing…' : `${side} ${ticker}`}
        </button>
        {!binanceConnected && (
          <p className="text-[9px] text-muted-foreground text-center mt-1.5">Paper trade — no real funds used</p>
        )}
      </div>
    </div>
  );
};

export default TradingForm;
