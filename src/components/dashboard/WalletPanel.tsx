import { useState, useEffect, useCallback } from 'react';
import { Wallet, RefreshCw, Loader2, Lock, AlertTriangle, ChevronUp, ChevronDown } from 'lucide-react';
import { formatTime } from '@/lib/format';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

interface WalletAsset {
  asset: string;
  free: string;
  locked: string;
  usdValue: number;
  symbol: string; // e.g. BTCUSDT
}

interface WalletPanelProps {
  binanceConnected: boolean;
  onSelectCoin?: (symbol: string) => void;
  selectedTradingCoins?: string[];
  onTradingCoinsChange?: (coins: string[]) => void;
}

const WalletPanel = ({ binanceConnected, onSelectCoin, selectedTradingCoins = [], onTradingCoinsChange }: WalletPanelProps) => {
  const [assets, setAssets] = useState<WalletAsset[]>([]);
  const [loading, setLoading] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [hasBnbDiscount, setHasBnbDiscount] = useState(false);
  const [minimized, setMinimized] = useState(false);

  const loadWallet = useCallback(async () => {
    if (!binanceConnected) return;
    setLoading(true);
    try {
      const { data, error } = await supabase.functions.invoke('binance-proxy', {
        body: { action: 'account', params: {} },
      });
      if (error || data?.error) {
        toast.error('Failed to load wallet');
        return;
      }

      const balances: WalletAsset[] = [];
      const nonZero = (data.balances || []).filter(
        (b: any) => parseFloat(b.free) > 0 || parseFloat(b.locked) > 0
      );

      // Check BNB for fee discount
      const bnbBalance = nonZero.find((b: any) => b.asset === 'BNB');
      setHasBnbDiscount(bnbBalance && parseFloat(bnbBalance.free) > 0);

      // Get prices for all assets
      for (const b of nonZero) {
        const asset = b.asset as string;
        const free = b.free as string;
        const locked = b.locked as string;
        let usdValue = 0;
        const symbol = asset === 'USDT' ? 'USDT' : `${asset}USDT`;

        if (asset === 'USDT' || asset === 'BUSD' || asset === 'USDC') {
          usdValue = parseFloat(free) + parseFloat(locked);
        } else {
          try {
            const priceResp = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${asset}USDT`);
            const priceData = await priceResp.json();
            if (priceData.price) {
              usdValue = (parseFloat(free) + parseFloat(locked)) * parseFloat(priceData.price);
            }
          } catch { /* skip */ }
        }

        if (usdValue >= 0.01) {
          balances.push({ asset, free, locked, usdValue, symbol });
        }
      }

      balances.sort((a, b) => b.usdValue - a.usdValue);
      setAssets(balances);
      setLastUpdated(new Date());
    } catch {
      toast.error('Wallet load error');
    } finally {
      setLoading(false);
    }
  }, [binanceConnected]);

  useEffect(() => {
    if (binanceConnected) loadWallet();
  }, [binanceConnected, loadWallet]);

  const toggleTradingCoin = (asset: WalletAsset) => {
    if (asset.asset === 'USDT') return; // USDT is base, not togglable
    const symbol = `${asset.asset}USDT`;
    const newCoins = selectedTradingCoins.includes(symbol)
      ? selectedTradingCoins.filter(c => c !== symbol)
      : [...selectedTradingCoins, symbol];
    onTradingCoinsChange?.(newCoins);
  };

  const totalValue = assets.reduce((s, a) => s + a.usdValue, 0);

  if (!binanceConnected) {
    return (
      <div className="bg-card border border-border rounded-lg p-4">
        <div className="flex items-center gap-2 mb-3">
          <Lock className="w-4 h-4 text-muted-foreground" />
          <h3 className="text-sm font-medium text-muted-foreground">Wallet</h3>
        </div>
        <div className="flex flex-col items-center justify-center py-6 text-center">
          <Wallet className="w-8 h-8 text-muted-foreground/30 mb-2" />
          <p className="text-xs text-muted-foreground">
            Connect your Binance API to view your wallet and enable live trading
          </p>
          <p className="text-[10px] text-muted-foreground/60 mt-1">
            Test mode works without API connection
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-3">
      <div className="flex items-center justify-between">
        <button onClick={() => setMinimized(p=>!p)} className="flex items-center gap-2 text-left flex-1 min-w-0">
          <Wallet className="w-4 h-4 text-accent shrink-0" />
          <h3 className="text-sm font-medium">Wallet</h3>
          {hasBnbDiscount && (
            <span className="text-[9px] font-mono bg-warn/20 text-warn px-1.5 py-0.5 rounded">BNB FEE ✓</span>
          )}
          {minimized ? <ChevronDown className="w-3.5 h-3.5 text-muted-foreground ml-1" /> : <ChevronUp className="w-3.5 h-3.5 text-muted-foreground ml-1" />}
        </button>
        <Button variant="ghost" size="icon" className="h-7 w-7 shrink-0" onClick={loadWallet} disabled={loading}>
          {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <RefreshCw className="w-3 h-3" />}
        </Button>
      </div>

      {!minimized && <>
      {/* Total value */}
      <div className="bg-muted/20 rounded-md p-3">
        <div className="text-[10px] uppercase tracking-widest text-muted-foreground">Total Portfolio Value</div>
        <div className="text-lg font-mono font-semibold tabular-nums text-foreground">
          ${totalValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </div>
        {lastUpdated && (
          <div className="text-[9px] text-muted-foreground/60 mt-0.5">
            Updated {formatTime(lastUpdated)}
          </div>
        )}
      </div>

      {/* Fee info */}
      <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground bg-muted/10 rounded px-2 py-1.5">
        <AlertTriangle className="w-3 h-3 shrink-0" />
        <span>
          Fee: {hasBnbDiscount ? '0.075%' : '0.10%'}/trade · Min sell = buy × {hasBnbDiscount ? '1.002' : '1.0025'}
        </span>
      </div>

      {/* Assets list */}
      <div className="space-y-1 max-h-[280px] overflow-y-auto scrollbar-thin">
        <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1">
          Select coins for bot trading
        </div>
        {assets.map(asset => {
          const isUsdt = asset.asset === 'USDT';
          const symbol = `${asset.asset}USDT`;
          const isSelected = isUsdt || selectedTradingCoins.includes(symbol);
          return (
            <button
              key={asset.asset}
              onClick={() => !isUsdt && toggleTradingCoin(asset)}
              disabled={isUsdt}
              className={`w-full flex items-center justify-between p-2 rounded-md transition-colors text-left ${
                isSelected
                  ? 'bg-accent/10 border border-accent/20'
                  : 'hover:bg-muted/20 border border-transparent'
              } ${isUsdt ? 'opacity-80 cursor-default' : 'cursor-pointer'}`}
            >
              <div className="flex items-center gap-2">
                <div className={`w-6 h-6 rounded-full flex items-center justify-center text-[9px] font-bold ${
                  isSelected ? 'bg-accent/20 text-accent' : 'bg-muted/30 text-muted-foreground'
                }`}>
                  {asset.asset.slice(0, 2)}
                </div>
                <div>
                  <div className="text-xs font-semibold">{asset.asset}</div>
                  <div className="text-[10px] font-mono text-muted-foreground tabular-nums">
                    {parseFloat(asset.free).toFixed(asset.asset === 'USDT' ? 2 : 6)}
                    {parseFloat(asset.locked) > 0 && (
                      <span className="text-warn"> +{parseFloat(asset.locked).toFixed(4)} locked</span>
                    )}
                  </div>
                </div>
              </div>
              <div className="text-right">
                <div className="text-xs font-mono tabular-nums">
                  ${asset.usdValue.toFixed(2)}
                </div>
                {isUsdt && (
                  <span className="text-[9px] text-accent font-medium">BASE</span>
                )}
                {!isUsdt && isSelected && (
                  <span className="text-[9px] text-accent font-medium">TRADING</span>
                )}
              </div>
            </button>
          );
        })}
      </div>

      {assets.length === 0 && !loading && (
        <p className="text-xs text-muted-foreground text-center py-4">
          No assets found in your Binance wallet
        </p>
      )}
      </>}
    </div>
  );
};

export default WalletPanel;
