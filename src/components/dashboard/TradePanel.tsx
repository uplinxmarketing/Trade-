import { useState } from 'react';
import { ArrowUpRight, ArrowDownRight, Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

type OrderSide = 'BUY' | 'SELL';
type OrderType = 'MARKET' | 'LIMIT' | 'STOP_LIMIT';

const ORDER_TYPES: { value: OrderType; label: string }[] = [
  { value: 'MARKET', label: 'Market' },
  { value: 'LIMIT', label: 'Limit' },
  { value: 'STOP_LIMIT', label: 'Stop-Limit' },
];

const QTY_PRESETS = [25, 50, 75, 100];

interface TradePanelProps {
  selectedCoins?: string[];
  availableBalance?: number;
}

const TradePanel = ({ selectedCoins = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'DOGEUSDT'], availableBalance = 10000 }: TradePanelProps) => {
  const [side, setSide] = useState<OrderSide>('BUY');
  const [orderType, setOrderType] = useState<OrderType>('MARKET');
  const [symbol, setSymbol] = useState(selectedCoins[0] || 'BTCUSDT');
  const [quantity, setQuantity] = useState('');
  const [price, setPrice] = useState('');
  const [stopPrice, setStopPrice] = useState('');
  const [loading, setLoading] = useState(false);

  const needsPrice = orderType === 'LIMIT' || orderType === 'STOP_LIMIT';
  const needsStopPrice = orderType === 'STOP_LIMIT';

  const applyQtyPreset = (percent: number) => {
    // For a rough estimate, use available balance / estimated price
    const estimatedPrice = parseFloat(price) || 67000; // fallback
    const maxQty = availableBalance / estimatedPrice;
    const qty = (maxQty * percent) / 100;
    setQuantity(qty.toFixed(6));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!quantity || parseFloat(quantity) <= 0) {
      toast.error('Enter a valid quantity');
      return;
    }
    if (needsPrice && (!price || parseFloat(price) <= 0)) {
      toast.error('Enter a valid limit price');
      return;
    }
    if (needsStopPrice && (!stopPrice || parseFloat(stopPrice) <= 0)) {
      toast.error('Enter a valid stop price');
      return;
    }

    setLoading(true);
    try {
      const binanceType = orderType === 'STOP_LIMIT' ? 'STOP_LOSS_LIMIT' : orderType;

      const { data, error } = await supabase.functions.invoke('binance-proxy', {
        body: {
          action: 'order',
          params: {
            symbol,
            side,
            type: binanceType,
            quantity,
            ...(needsPrice ? { price } : {}),
            ...(needsStopPrice ? { stopPrice } : {}),
          },
        },
      });

      if (error) throw error;
      if (data?.error) throw new Error(data.error);

      const priceLabel = orderType === 'MARKET' ? 'market' : orderType === 'STOP_LIMIT' ? `stop ${stopPrice} / limit ${price}` : price;
      toast.success(`${side} order placed`, {
        description: `${quantity} ${symbol} @ ${priceLabel}`,
      });
      setQuantity('');
      setPrice('');
      setStopPrice('');
    } catch (err: any) {
      toast.error('Order failed', { description: err.message || 'Check your API keys and balance' });
    } finally {
      setLoading(false);
    }
  };

  const isBuy = side === 'BUY';

  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <h3 className="text-sm font-medium text-muted-foreground mb-3">Place Order</h3>

      {/* Side toggle */}
      <div className="grid grid-cols-2 gap-1 bg-muted/30 rounded-md p-0.5 mb-4">
        <button
          type="button"
          onClick={() => setSide('BUY')}
          className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold tracking-wide transition-colors ${
            isBuy ? 'bg-[hsl(var(--gain))] text-background' : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          <ArrowUpRight className="w-3.5 h-3.5" /> BUY
        </button>
        <button
          type="button"
          onClick={() => setSide('SELL')}
          className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold tracking-wide transition-colors ${
            !isBuy ? 'bg-[hsl(var(--loss))] text-background' : 'text-muted-foreground hover:text-foreground'
          }`}
        >
          <ArrowDownRight className="w-3.5 h-3.5" /> SELL
        </button>
      </div>

      <form onSubmit={handleSubmit} className="space-y-3">
        {/* Symbol select */}
        <div>
          <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">Pair</label>
          <select
            value={symbol}
            onChange={e => setSymbol(e.target.value)}
            className="w-full bg-muted/40 border border-border rounded px-3 py-2 text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-accent"
          >
            {selectedCoins.map(p => (
              <option key={p} value={p}>{p.replace('USDT', ' / USDT')}</option>
            ))}
          </select>
        </div>

        {/* Order type */}
        <div>
          <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">Type</label>
          <div className="grid grid-cols-3 gap-1 bg-muted/30 rounded-md p-0.5">
            {ORDER_TYPES.map(t => (
              <button
                key={t.value}
                type="button"
                onClick={() => setOrderType(t.value)}
                className={`py-1.5 rounded text-[11px] font-medium transition-colors ${
                  orderType === t.value ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {t.label}
              </button>
            ))}
          </div>
        </div>

        {/* Quantity */}
        <div>
          <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">Quantity</label>
          <Input
            type="number"
            step="any"
            min="0"
            placeholder="0.00"
            value={quantity}
            onChange={e => setQuantity(e.target.value)}
            className="bg-muted/40 border-border text-sm font-mono"
          />
          {/* Quantity preset buttons */}
          <div className="grid grid-cols-4 gap-1 mt-1.5">
            {QTY_PRESETS.map(pct => (
              <button
                key={pct}
                type="button"
                onClick={() => applyQtyPreset(pct)}
                className="py-1 rounded bg-muted/50 border border-border text-[10px] font-medium text-muted-foreground hover:text-foreground hover:bg-muted/80 transition-colors active:scale-[0.96]"
              >
                {pct}%
              </button>
            ))}
          </div>
        </div>

        {/* Stop Price */}
        {needsStopPrice && (
          <div>
            <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">Stop Price</label>
            <Input
              type="number"
              step="any"
              min="0"
              placeholder="Trigger price"
              value={stopPrice}
              onChange={e => setStopPrice(e.target.value)}
              className="bg-muted/40 border-border text-sm font-mono"
            />
          </div>
        )}

        {/* Limit Price */}
        {needsPrice && (
          <div>
            <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">Limit Price</label>
            <Input
              type="number"
              step="any"
              min="0"
              placeholder="0.00"
              value={price}
              onChange={e => setPrice(e.target.value)}
              className="bg-muted/40 border-border text-sm font-mono"
            />
          </div>
        )}

        {/* Submit */}
        <Button
          type="submit"
          disabled={loading}
          className={`w-full font-semibold tracking-wide ${
            isBuy
              ? 'bg-[hsl(var(--gain))] hover:bg-[hsl(var(--gain))]/90 text-background'
              : 'bg-[hsl(var(--loss))] hover:bg-[hsl(var(--loss))]/90 text-background'
          }`}
        >
          {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : `${side} ${symbol.replace('USDT', '')}`}
        </Button>
      </form>
    </div>
  );
};

export default TradePanel;
