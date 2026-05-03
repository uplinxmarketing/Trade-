import { useState, useEffect, useCallback } from 'react';
import { ChevronDown, Info } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';
import { TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { toast } from 'sonner';

const BREAK_EVEN_MULT = 1 / Math.pow(1 - TAKER_FEE, 2);
const MAKER_FEE = 0.001 * 0.075 / 0.1; // ~0.075% maker

type TradingMode = 'spot' | 'futures';
type Side        = 'buy' | 'sell';
type OrderType   = 'market' | 'limit' | 'stop-limit' | 'oco' | 'stop-market';
type AmtMode     = 'coin' | 'usdt';
type MarginType  = 'isolated' | 'cross';

interface Props {
  activeCoin: string;
  prices: LivePrices;
  binanceConnected: boolean;
}

const ORDER_INFO: Record<OrderType, string> = {
  'market':      'Executes immediately at the current market price. Fee: 0.100% taker.',
  'limit':       'Rests on order book at your price, fills when market reaches it. Fee: 0.075% maker.',
  'stop-limit':  'Triggers a limit order when the stop price is hit. Fee: 0.075% maker.',
  'oco':         'One-Cancels-the-Other: limit order + stop-limit order placed simultaneously.',
  'stop-market': 'Triggers a market order when the stop price is hit. Fee: 0.100% taker.',
};

function fmtPrice(p: number) {
  if (p <= 0) return '';
  if (p >= 1000) return p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (p >= 1) return p.toFixed(4);
  return p.toFixed(6);
}

export default function OrderFormPanel({ activeCoin, prices, binanceConnected }: Props) {
  const [tradingMode, setTradingMode] = useState<TradingMode>('spot');
  const [side, setSide]               = useState<Side>('buy');
  const [orderType, setOrderType]     = useState<OrderType>('limit');
  const [amtMode, setAmtMode]         = useState<AmtMode>('usdt');
  const [marginType, setMarginType]   = useState<MarginType>('isolated');
  const [leverage, setLeverage]       = useState(5);
  const [balance, setBalance]         = useState(0);
  const [amount, setAmount]           = useState('');
  const [limitPrice, setLimitPrice]   = useState('');
  const [stopPrice, setStopPrice]     = useState('');
  const [tpPrice, setTpPrice]         = useState('');
  const [submitting, setSubmitting]   = useState(false);

  const lp       = prices[activeCoin];
  const mktPrice = parseFloat(lp?.price ?? '0');
  const ticker   = activeCoin.replace('USDT', '');

  useEffect(() => {
    const readBal = (val: unknown) => {
      const n = Number(val ?? 0);
      // Fall back to localStorage startingBalance when DB value is 0
      if (n > 0) { setBalance(n); return; }
      try {
        const cfg = JSON.parse(localStorage.getItem('paper_wallet_config') ?? '{}');
        setBalance(cfg.startingBalance ?? 1000);
      } catch { setBalance(1000); }
    };
    supabase.from('bot_config').select('current_balance').eq('user_session', 'default').maybeSingle()
      .then(({ data }) => readBal(data?.current_balance));
    const ch = supabase.channel('ofp-bal')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_config' }, (p) => {
        readBal(p.new?.current_balance);
      }).subscribe();
    return () => { supabase.removeChannel(ch); };
  }, []);

  // Reset form on coin change
  useEffect(() => { setAmount(''); setLimitPrice(''); setStopPrice(''); setTpPrice(''); }, [activeCoin]);

  const execPrice = orderType === 'market' ? mktPrice : (parseFloat(limitPrice) || mktPrice);
  const usdt      = amtMode === 'usdt' ? parseFloat(amount) || 0 : (parseFloat(amount) || 0) * execPrice;
  const coinQty   = usdt > 0 && execPrice > 0 ? (usdt / execPrice) * (1 - TAKER_FEE) : 0;
  const fee       = usdt * (orderType === 'market' || orderType === 'stop-market' ? TAKER_FEE : MAKER_FEE);
  const breakEven = execPrice * BREAK_EVEN_MULT;
  const total     = amtMode === 'usdt' ? usdt : coinQty * execPrice;

  const applyPct = useCallback((pct: number) => {
    if (pct === 0) { setAmount(''); return; }
    const val = (balance * pct) / 100;
    setAmount(amtMode === 'usdt' ? val.toFixed(2) : (execPrice > 0 ? (val / execPrice).toFixed(6) : ''));
  }, [balance, amtMode, execPrice]);

  const submit = async () => {
    if (!amount || parseFloat(amount) <= 0) { toast.error('Enter an amount'); return; }
    if (usdt > balance && side === 'buy') { toast.error('Insufficient balance'); return; }
    if (execPrice <= 0) { toast.error('Price not available'); return; }
    setSubmitting(true);
    try {
      if (side === 'buy') {
        await supabase.from('bot_trade_history').insert({
          user_session: 'default', symbol: activeCoin, side: 'BUY',
          price: execPrice, quantity: coinQty, pnl: null,
          reason: `Manual ${orderType} ${tradingMode} buy`,
        });
        // Accumulate into existing position (weighted avg entry) rather than overwriting
        const { data: existingPos } = await supabase.from('paper_portfolio')
          .select('quantity,avg_entry_price').eq('user_session', 'default').eq('symbol', activeCoin).maybeSingle();
        if (existingPos && Number(existingPos.quantity) > 0) {
          const prevQty = Number(existingPos.quantity);
          const prevAvg = Number(existingPos.avg_entry_price);
          const newQty  = prevQty + coinQty;
          const newAvg  = (prevQty * prevAvg + coinQty * execPrice) / newQty;
          await supabase.from('paper_portfolio').update({
            quantity: newQty, avg_entry_price: newAvg, updated_at: new Date().toISOString(),
          }).eq('user_session', 'default').eq('symbol', activeCoin);
        } else {
          await supabase.from('paper_portfolio').upsert({
            user_session: 'default', symbol: activeCoin,
            quantity: coinQty, avg_entry_price: execPrice,
            updated_at: new Date().toISOString(),
          }, { onConflict: 'user_session,symbol' });
        }
        await supabase.from('bot_config').update({
          current_balance: balance - usdt, updated_at: new Date().toISOString(),
        }).eq('user_session', 'default');
        toast.success(`Bought ${coinQty.toFixed(6)} ${ticker}`, { description: `at $${fmtPrice(execPrice)} · fee $${fee.toFixed(4)}` });
      } else {
        const { data: pos } = await supabase.from('paper_portfolio')
          .select('quantity,avg_entry_price').eq('user_session', 'default').eq('symbol', activeCoin).single();
        if (!pos || Number(pos.quantity) <= 0) { toast.error('No position to sell'); setSubmitting(false); return; }
        const sellQty      = Math.min(coinQty || Number(pos.quantity), Number(pos.quantity));
        const remainingQty = Number(pos.quantity) - sellQty;
        const costBasis    = sellQty * Number(pos.avg_entry_price) / (1 - TAKER_FEE);
        const proceeds     = sellQty * execPrice * (1 - TAKER_FEE);
        const pnl          = proceeds - costBasis;
        await supabase.from('bot_trade_history').insert({
          user_session: 'default', symbol: activeCoin, side: 'SELL',
          price: execPrice, quantity: sellQty, pnl,
          reason: `Manual ${orderType} ${tradingMode} sell`,
        });
        await supabase.from('bot_config').update({
          current_balance: balance + proceeds, updated_at: new Date().toISOString(),
        }).eq('user_session', 'default');
        // Close or reduce position
        if (remainingQty <= 0.000001) {
          await supabase.from('paper_portfolio').delete().eq('user_session', 'default').eq('symbol', activeCoin);
        } else {
          await supabase.from('paper_portfolio').update({
            quantity: remainingQty, updated_at: new Date().toISOString(),
          }).eq('user_session', 'default').eq('symbol', activeCoin);
        }
        toast.success(`Sold ${sellQty.toFixed(6)} ${ticker}`, {
          description: `PnL: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)}`,
        });
      }
      setAmount('');
    } catch (e) {
      toast.error('Order failed', { description: e instanceof Error ? e.message : 'Unknown' });
    } finally {
      setSubmitting(false);
    }
  };

  const isBuy      = side === 'buy';
  const btnLabel   = submitting ? 'Placing…'
    : tradingMode === 'futures' ? (isBuy ? `Open Long ${ticker}` : `Open Short ${ticker}`)
    : (isBuy ? `Buy ${ticker}` : `Sell ${ticker}`);
  const btnColor   = isBuy ? 'bg-[#0ecb81] hover:bg-[#0bb873]' : 'bg-[#f6465d] hover:bg-[#e03350]';

  const showLimit      = orderType === 'limit' || orderType === 'stop-limit' || orderType === 'oco';
  const showStop       = orderType === 'stop-limit' || orderType === 'oco';
  const showTp         = orderType === 'oco';
  const showLeverage   = tradingMode === 'futures';

  return (
    <div className="trading-card flex flex-col h-full overflow-y-auto scrollbar-thin">
      {/* Spot / Futures toggle */}
      <div className="flex border-b border-border text-xs font-medium">
        {(['spot', 'futures'] as TradingMode[]).map(m => (
          <button key={m} onClick={() => setTradingMode(m)}
            className={`flex-1 py-2 capitalize transition-colors ${
              tradingMode === m ? 'text-foreground border-b-2 border-accent' : 'text-muted-foreground hover:text-foreground'
            }`}>
            {m}
          </button>
        ))}
      </div>

      <div className="p-3 space-y-3 flex-1">
        {/* Leverage slider (futures) */}
        {showLeverage && (
          <div className="space-y-1">
            <div className="flex justify-between text-[10px]">
              <span className="text-muted-foreground">Leverage</span>
              <span className="font-bold text-warn">{leverage}×</span>
            </div>
            <input type="range" min={1} max={20} value={leverage} onChange={e => setLeverage(Number(e.target.value))}
              className="w-full h-1.5 accent-warn" />
            <div className="flex gap-1">
              {(['isolated', 'cross'] as MarginType[]).map(mt => (
                <button key={mt} onClick={() => setMarginType(mt)}
                  className={`flex-1 py-1 text-[10px] rounded capitalize border transition-colors ${
                    marginType === mt ? 'border-accent bg-accent/10 text-accent' : 'border-border text-muted-foreground hover:text-foreground'
                  }`}>
                  {mt}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Buy / Sell tabs */}
        <div className="flex rounded overflow-hidden">
          <button onClick={() => setSide('buy')}
            className={`flex-1 py-2 text-xs font-bold transition-colors ${isBuy ? 'bg-[#0ecb81] text-white' : 'bg-muted/20 text-muted-foreground hover:text-[#0ecb81]'}`}>
            Buy
          </button>
          <button onClick={() => setSide('sell')}
            className={`flex-1 py-2 text-xs font-bold transition-colors ${!isBuy ? 'bg-[#f6465d] text-white' : 'bg-muted/20 text-muted-foreground hover:text-[#f6465d]'}`}>
            Sell
          </button>
        </div>

        {/* Order type buttons */}
        <div className="flex gap-0.5 flex-wrap">
          {(['market', 'limit', 'stop-limit', 'oco', 'stop-market'] as OrderType[]).map(ot => (
            <button key={ot} onClick={() => setOrderType(ot)}
              className={`px-2.5 py-1 text-[10px] rounded transition-colors capitalize ${
                orderType === ot ? 'bg-secondary text-foreground font-semibold' : 'text-muted-foreground hover:bg-secondary/60 hover:text-foreground'
              }`}>
              {ot.replace('-', '-')}
            </button>
          ))}
        </div>

        {/* Info bar */}
        <div className="flex items-start gap-1.5 text-[10px] text-muted-foreground bg-muted/15 rounded px-2 py-1.5">
          <Info className="w-3 h-3 flex-shrink-0 mt-px text-accent" />
          <span>{ORDER_INFO[orderType]}</span>
        </div>

        {/* Available */}
        <div className="flex justify-between text-[10px]">
          <span className="text-muted-foreground">Available</span>
          <span className="font-mono text-foreground tabular-nums">
            {balance.toLocaleString('en-US', { minimumFractionDigits: 2 })} USDT
          </span>
        </div>

        {/* Limit price */}
        {showLimit && (
          <div>
            <label className="text-[10px] text-muted-foreground block mb-1">Limit price</label>
            <div className="flex items-center border border-border rounded bg-secondary overflow-hidden">
              <button className="px-2 text-muted-foreground hover:text-foreground text-sm font-bold" onClick={() => setLimitPrice(p => String((parseFloat(p) || mktPrice) - 1))}>−</button>
              <input type="number" value={limitPrice} onChange={e => setLimitPrice(e.target.value)}
                placeholder={fmtPrice(mktPrice)}
                className="flex-1 bg-transparent text-xs font-mono text-foreground text-center focus:outline-none px-1 py-1.5 placeholder:text-muted-foreground" />
              <button className="px-2 text-muted-foreground hover:text-foreground text-sm font-bold" onClick={() => setLimitPrice(p => String((parseFloat(p) || mktPrice) + 1))}>+</button>
              <span className="text-[10px] text-muted-foreground pr-2">USDT</span>
            </div>
          </div>
        )}

        {/* Stop price */}
        {showStop && (
          <div>
            <label className="text-[10px] text-muted-foreground block mb-1">Stop price</label>
            <div className="flex items-center border border-border rounded bg-secondary overflow-hidden">
              <input type="number" value={stopPrice} onChange={e => setStopPrice(e.target.value)}
                placeholder={fmtPrice(mktPrice)}
                className="flex-1 bg-transparent text-xs font-mono text-foreground text-center focus:outline-none px-2 py-1.5 placeholder:text-muted-foreground" />
              <span className="text-[10px] text-muted-foreground pr-2">USDT</span>
            </div>
          </div>
        )}

        {/* Take-profit price (OCO) */}
        {showTp && (
          <div>
            <label className="text-[10px] text-muted-foreground block mb-1">Take-profit limit</label>
            <div className="flex items-center border border-border rounded bg-secondary overflow-hidden">
              <input type="number" value={tpPrice} onChange={e => setTpPrice(e.target.value)}
                placeholder={fmtPrice(mktPrice * 1.01)}
                className="flex-1 bg-transparent text-xs font-mono text-foreground text-center focus:outline-none px-2 py-1.5 placeholder:text-muted-foreground" />
              <span className="text-[10px] text-muted-foreground pr-2">USDT</span>
            </div>
          </div>
        )}

        {/* Amount with coin/USDT toggle */}
        <div>
          <div className="flex justify-between items-center mb-1">
            <label className="text-[10px] text-muted-foreground">Amount</label>
            <div className="flex text-[9px] border border-border rounded overflow-hidden">
              {(['usdt', 'coin'] as AmtMode[]).map(m => (
                <button key={m} onClick={() => { setAmtMode(m); setAmount(''); }}
                  className={`px-2 py-0.5 transition-colors ${amtMode === m ? 'bg-secondary text-foreground font-semibold' : 'text-muted-foreground hover:text-foreground'}`}>
                  {m === 'usdt' ? 'USDT' : ticker}
                </button>
              ))}
            </div>
          </div>
          <div className="flex items-center border border-border rounded bg-secondary overflow-hidden">
            <input type="number" value={amount} onChange={e => setAmount(e.target.value)}
              placeholder="0.00"
              className="flex-1 bg-transparent text-xs font-mono text-foreground text-center focus:outline-none px-2 py-1.5 placeholder:text-muted-foreground" />
            <span className="text-[10px] text-muted-foreground pr-2 font-mono">{amtMode === 'usdt' ? 'USDT' : ticker}</span>
          </div>
        </div>

        {/* Market price shown when market order */}
        {orderType === 'market' && (
          <div className="flex justify-between text-[10px]">
            <span className="text-muted-foreground">Market price</span>
            <span className="font-mono tabular-nums">{mktPrice > 0 ? fmtPrice(mktPrice) : '—'} USDT</span>
          </div>
        )}

        {/* % quick buttons */}
        <div className="flex items-center gap-1">
          <div className="flex-1 relative h-1.5 bg-muted/30 rounded-full">
            <div className="absolute inset-y-0 left-0 bg-accent rounded-full transition-all"
              style={{ width: `${balance > 0 && usdt > 0 ? Math.min(100, (usdt / balance) * 100) : 0}%` }} />
            {[0, 25, 50, 75, 100].map(p => (
              <button key={p} onClick={() => applyPct(p)}
                className="absolute -translate-x-1/2 -translate-y-1/2 top-1/2 w-2.5 h-2.5 rounded-full bg-secondary border border-border hover:border-accent transition-colors"
                style={{ left: `${p}%` }} title={`${p}%`} />
            ))}
          </div>
        </div>
        <div className="flex justify-between text-[9px] text-muted-foreground -mt-1">
          {[0, 25, 50, 75, 100].map(p => (
            <button key={p} onClick={() => applyPct(p)} className="hover:text-foreground transition-colors">{p}%</button>
          ))}
        </div>

        {/* Summary */}
        {(usdt > 0 || coinQty > 0) && (
          <div className="bg-muted/10 border border-border rounded p-2.5 space-y-1.5">
            <div className="flex justify-between text-[10px]">
              <span className="text-muted-foreground">Total (USDT)</span>
              <span className="font-mono tabular-nums">{total.toFixed(2)} USDT</span>
            </div>
            <div className="flex justify-between text-[10px]">
              <span className="text-muted-foreground">Est. fee ({((orderType === 'market' || orderType === 'stop-market' ? TAKER_FEE : MAKER_FEE) * 100).toFixed(3)}%)</span>
              <span className="font-mono tabular-nums text-warn">{fee.toFixed(4)} USDT</span>
            </div>
            <div className="flex justify-between text-[10px] border-t border-border pt-1.5">
              <span className="text-muted-foreground">Breakeven sell price</span>
              <span className="font-mono tabular-nums text-accent">{execPrice > 0 ? fmtPrice(breakEven) : '—'} USDT</span>
            </div>
          </div>
        )}
      </div>

      {/* Submit button */}
      <div className="p-3 border-t border-border">
        <button onClick={submit} disabled={submitting || !amount || parseFloat(amount) <= 0 || execPrice <= 0}
          className={`w-full py-2.5 rounded text-sm font-bold text-white transition-colors disabled:opacity-40 ${btnColor}`}>
          {btnLabel}
        </button>
        {!binanceConnected && (
          <p className="text-[9px] text-muted-foreground text-center mt-1.5">Paper trade — no real funds used</p>
        )}
      </div>
    </div>
  );
}
