import { useState, useEffect, useCallback } from 'react';
import { Wallet, TrendingUp, TrendingDown, BarChart2 } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';
import { TAKER_FEE } from '@/lib/trading-engine';

interface Position {
  symbol: string;
  quantity: number;
  avg_entry_price: number;
}

interface PaperWalletPanelProps {
  prices: Record<string, { price: string; priceChangePercent: string }>;
}

const PaperWalletPanel = ({ prices }: PaperWalletPanelProps) => {
  const [usdtFree, setUsdtFree]         = useState(0);
  const [initialBalance, setInitialBalance] = useState(0);
  const [positions, setPositions]       = useState<Position[]>([]);
  const [totalFeesPaid, setTotalFeesPaid] = useState(0);
  const [realizedPnl, setRealizedPnl]   = useState(0);
  const [isRunning, setIsRunning]       = useState(false);

  const loadData = useCallback(async () => {
    const [cfgRes, posRes, tradeRes] = await Promise.all([
      supabase.from('bot_config').select('current_balance, initial_balance, is_running').eq('user_session', 'default').maybeSingle(),
      supabase.from('paper_portfolio').select('*').eq('user_session', 'default').gt('quantity', 0),
      supabase.from('bot_trade_history').select('side, pnl, quantity, price').eq('user_session', 'default'),
    ]);

    if (cfgRes.data) {
      setUsdtFree(Number(cfgRes.data.current_balance));
      setInitialBalance(Number(cfgRes.data.initial_balance));
      setIsRunning(Boolean(cfgRes.data.is_running));
    }
    if (posRes.data) setPositions(posRes.data as Position[]);
    if (tradeRes.data) {
      // Sum realized PnL from SELL trades
      const rpnl = tradeRes.data
        .filter(t => t.side === 'SELL' && t.pnl !== null)
        .reduce((s, t) => s + (t.pnl || 0), 0);
      setRealizedPnl(Math.round(rpnl * 10000) / 10000);

      // Estimate fees: BUY fee ≈ allocation * TAKER_FEE = qty * price / (1-FEE) * FEE
      // SELL fee ≈ qty * sell_price * TAKER_FEE
      const fees = tradeRes.data.reduce((s, t) => {
        const qty   = Number(t.quantity);
        const price = Number(t.price);
        if (t.side === 'BUY')  return s + (qty * price / (1 - TAKER_FEE)) * TAKER_FEE;
        if (t.side === 'SELL') return s + qty * price * TAKER_FEE;
        return s;
      }, 0);
      setTotalFeesPaid(Math.round(fees * 10000) / 10000);
    }
  }, []);

  useEffect(() => {
    loadData();
    const ch = supabase.channel('paperwallet')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_config' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadData)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadData]);

  // Live portfolio value: USDT free + all coin holdings at current price
  const coinsValueUsdt = positions.reduce((sum, pos) => {
    const price = parseFloat(prices[pos.symbol]?.price || '0') || pos.avg_entry_price;
    return sum + pos.quantity * price;
  }, 0);
  const totalValue = usdtFree + coinsValueUsdt;
  const totalPnl   = totalValue - initialBalance;
  const totalPnlPct = initialBalance > 0 ? (totalPnl / initialBalance) * 100 : 0;

  const hasActivity = initialBalance > 0;

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">

      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Wallet className="w-4 h-4 text-accent" />
          <h3 className="text-sm font-semibold">Paper Wallet</h3>
          <span className="text-[9px] font-mono px-1.5 py-0.5 rounded bg-accent/20 text-accent">USDT</span>
        </div>
        {isRunning && (
          <div className="flex items-center gap-1 text-[9px] text-gain font-mono">
            <span className="w-1.5 h-1.5 rounded-full bg-gain animate-pulse inline-block" /> Bot active
          </div>
        )}
      </div>

      {!hasActivity ? (
        <p className="text-xs text-muted-foreground text-center py-6">
          Start the bot to see your paper wallet activity here.
        </p>
      ) : (
        <>
          {/* USDT balance — big prominent number */}
          <div className="bg-muted/20 rounded-lg px-4 py-3 text-center">
            <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-0.5">USDT Available (free)</div>
            <div className="text-3xl font-mono font-bold tabular-nums">
              {usdtFree.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </div>
            <div className="text-xs text-muted-foreground mt-0.5">USDT</div>
          </div>

          {/* Portfolio summary grid */}
          <div className="grid grid-cols-2 gap-2">
            <div className="bg-muted/20 rounded-md p-2.5">
              <div className="text-[9px] uppercase tracking-widest text-muted-foreground">Coins in Positions</div>
              <div className="text-sm font-mono font-semibold tabular-nums mt-0.5">
                {coinsValueUsdt.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                <span className="text-[10px] text-muted-foreground ml-1">USDT</span>
              </div>
            </div>
            <div className="bg-muted/20 rounded-md p-2.5">
              <div className="text-[9px] uppercase tracking-widest text-muted-foreground">Total Portfolio</div>
              <div className="text-sm font-mono font-semibold tabular-nums mt-0.5">
                {totalValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                <span className="text-[10px] text-muted-foreground ml-1">USDT</span>
              </div>
            </div>
            <div className={`rounded-md p-2.5 ${totalPnl >= 0 ? 'bg-gain/10' : 'bg-loss/10'}`}>
              <div className="text-[9px] uppercase tracking-widest text-muted-foreground">Total P&L</div>
              <div className={`text-sm font-mono font-semibold tabular-nums mt-0.5 ${totalPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                {totalPnl >= 0 ? '+' : ''}{totalPnl.toFixed(4)}
                <span className="text-[10px] ml-1">USDT</span>
              </div>
              <div className={`text-[10px] font-mono ${totalPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                {totalPnlPct >= 0 ? '+' : ''}{totalPnlPct.toFixed(2)}%
              </div>
            </div>
            <div className="bg-muted/20 rounded-md p-2.5">
              <div className="text-[9px] uppercase tracking-widest text-muted-foreground">Fees Paid (est.)</div>
              <div className="text-sm font-mono font-semibold tabular-nums mt-0.5 text-loss">
                −{totalFeesPaid.toFixed(4)}
                <span className="text-[10px] text-muted-foreground ml-1">USDT</span>
              </div>
              <div className="text-[10px] text-muted-foreground">0.1% per trade</div>
            </div>
          </div>

          {/* Open positions */}
          {positions.length > 0 && (
            <div>
              <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-widest text-muted-foreground mb-2">
                <BarChart2 className="w-3 h-3" /> Open Positions ({positions.length})
              </div>
              <div className="space-y-1.5">
                {positions.map(pos => {
                  const livePrice  = parseFloat(prices[pos.symbol]?.price || '0') || pos.avg_entry_price;
                  const coinValUsdt = pos.quantity * livePrice;
                  // Fee-correct unrealized PnL
                  const sellUsdt   = pos.quantity * livePrice * (1 - TAKER_FEE);
                  const costUsdt   = pos.quantity * pos.avg_entry_price / (1 - TAKER_FEE);
                  const uPnl       = sellUsdt - costUsdt;
                  const uPnlPct    = costUsdt > 0 ? (uPnl / costUsdt) * 100 : 0;
                  const isGain     = uPnl >= 0;
                  return (
                    <div key={pos.symbol} className={`rounded-md px-3 py-2 border ${isGain ? 'bg-gain/5 border-gain/20' : 'bg-loss/5 border-loss/20'}`}>
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          {isGain
                            ? <TrendingUp className="w-3.5 h-3.5 text-gain" />
                            : <TrendingDown className="w-3.5 h-3.5 text-loss" />}
                          <span className="text-xs font-mono font-bold">{pos.symbol.replace('USDT', '')}</span>
                          <span className="text-[10px] text-muted-foreground font-mono">
                            {pos.quantity.toFixed(6)} coins
                          </span>
                        </div>
                        <div className="text-right">
                          <div className="text-xs font-mono font-semibold">
                            {coinValUsdt.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} USDT
                          </div>
                          <div className={`text-[10px] font-mono font-semibold ${isGain ? 'text-gain' : 'text-loss'}`}>
                            {uPnl >= 0 ? '+' : ''}{uPnl.toFixed(4)} ({uPnlPct >= 0 ? '+' : ''}{uPnlPct.toFixed(2)}%)
                          </div>
                        </div>
                      </div>
                      <div className="flex justify-between mt-1 text-[9px] text-muted-foreground font-mono">
                        <span>Entry ${pos.avg_entry_price.toLocaleString('en-US', { maximumFractionDigits: 4 })}</span>
                        <span>Now ${livePrice.toLocaleString('en-US', { maximumFractionDigits: 4 })}</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {positions.length === 0 && (
            <p className="text-xs text-muted-foreground text-center py-2">
              No open positions — bot is watching for entry signals
            </p>
          )}

          {/* Realized PnL footer */}
          <div className="flex justify-between items-center text-[10px] text-muted-foreground border-t border-border pt-2">
            <span>Realized P&L (closed trades)</span>
            <span className={`font-mono font-semibold ${realizedPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
              {realizedPnl >= 0 ? '+' : ''}{realizedPnl.toFixed(4)} USDT
            </span>
          </div>
        </>
      )}
    </div>
  );
};

export default PaperWalletPanel;
