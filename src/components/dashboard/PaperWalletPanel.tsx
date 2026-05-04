import { useState, useEffect, useCallback } from 'react';
import { Wallet, TrendingUp, TrendingDown, ChevronUp, ChevronDown } from 'lucide-react';
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

const COIN_COLORS: Record<string, string> = {
  BTC: '#F7931A', ETH: '#627EEA', BNB: '#F3BA2F',
  SOL: '#9945FF', XRP: '#346AA9', DOGE: '#C2A633',
};

function CoinIcon({ ticker }: { ticker: string }) {
  const bg = COIN_COLORS[ticker] ?? '#6366f1';
  return (
    <div className="w-7 h-7 rounded-full flex items-center justify-center text-[9px] font-bold text-white shrink-0"
      style={{ background: bg }}>
      {ticker.slice(0, 3)}
    </div>
  );
}

const PaperWalletPanel = ({ prices }: PaperWalletPanelProps) => {
  const [usdtFree, setUsdtFree]           = useState(0);
  const [initialBalance, setInitialBalance] = useState(0);
  const [positions, setPositions]         = useState<Position[]>([]);
  const [realizedPnl, setRealizedPnl]     = useState(0);
  const [isRunning, setIsRunning]         = useState(false);
  const [minimized, setMinimized]         = useState(false);

  const loadData = useCallback(async () => {
    const [cfgRes, posRes, tradeRes] = await Promise.all([
      supabase.from('bot_config').select('current_balance, initial_balance, is_running').eq('user_session', 'default').maybeSingle(),
      supabase.from('paper_portfolio').select('*').eq('user_session', 'default').gt('quantity', 0),
      supabase.from('bot_trade_history').select('side, pnl').eq('user_session', 'default'),
    ]);
    if (cfgRes.data) {
      setUsdtFree(Number(cfgRes.data.current_balance));
      setInitialBalance(Number(cfgRes.data.initial_balance));
      setIsRunning(Boolean(cfgRes.data.is_running));
    }
    if (posRes.data) setPositions(posRes.data as Position[]);
    if (tradeRes.data) {
      const rpnl = tradeRes.data
        .filter(t => t.side === 'SELL' && t.pnl !== null)
        .reduce((s, t) => s + (t.pnl || 0), 0);
      setRealizedPnl(Math.round(rpnl * 10000) / 10000);
    }
  }, []);

  useEffect(() => {
    loadData();
    const ch = supabase.channel('paperwallet-v2')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_config' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadData)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadData]);

  const coinsValueUsdt = positions.reduce((sum, pos) => {
    const price = parseFloat(prices[pos.symbol]?.price || '0') || pos.avg_entry_price;
    return sum + pos.quantity * price;
  }, 0);

  const totalValue   = usdtFree + coinsValueUsdt;
  const sessionPnl   = totalValue - initialBalance;
  const hasActivity  = initialBalance > 0;

  const fmt = (n: number, dp = 2) =>
    n.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp });

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">

      {/* ── Header ── */}
      <button onClick={() => setMinimized(p=>!p)} className="flex items-center justify-between w-full text-left">
        <div className="flex items-center gap-2">
          <Wallet className="w-4 h-4 text-accent" />
          <h3 className="text-sm font-semibold">Paper Wallet</h3>
          <span className="text-[9px] font-mono px-1.5 py-0.5 rounded bg-accent/20 text-accent">paper</span>
        </div>
        <div className="flex items-center gap-2">
          {isRunning && (
            <div className="flex items-center gap-1 text-[9px] text-gain font-mono">
              <span className="w-1.5 h-1.5 rounded-full bg-gain animate-pulse inline-block" /> Bot active
            </div>
          )}
          {minimized ? <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" /> : <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />}
        </div>
      </button>

      {!minimized && (!hasActivity ? (
        <p className="text-xs text-muted-foreground text-center py-6">
          Start the bot to see your paper wallet activity here.
        </p>
      ) : (
        <>
          {/* ── Total balance header row ── */}
          <div className="bg-muted/20 rounded-lg px-4 py-3 flex items-start justify-between gap-4">
            <div>
              <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-0.5">Total balance (paper)</div>
              <div className="text-2xl font-mono font-bold tabular-nums">
                {fmt(totalValue)} <span className="text-sm font-normal text-muted-foreground">USDT</span>
              </div>
              <div className="text-[10px] text-muted-foreground mt-0.5">≈ ${fmt(totalValue)} USD</div>
            </div>
            <div className="text-right shrink-0">
              <div className={`text-xs font-mono font-semibold ${sessionPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                Session P&amp;L: {sessionPnl >= 0 ? '+' : ''}{fmt(sessionPnl, 4)} USDT
              </div>
              <div className="text-[10px] text-muted-foreground mt-0.5">
                Started: {fmt(initialBalance)} USDT (paper)
              </div>
              <div className="text-[10px] text-muted-foreground mt-0.5">
                Available: <span className="text-foreground font-mono">{fmt(usdtFree)} USDT</span>
                {' · '}In positions: <span className="text-foreground font-mono">{fmt(coinsValueUsdt)} USDT</span>
              </div>
            </div>
          </div>

          {/* ── Holdings table ── */}
          <div className="overflow-x-auto overflow-y-auto max-h-[260px] scrollbar-thin">
            <table className="w-full text-xs min-w-[420px]">
              <thead>
                <tr className="border-b border-border">
                  {['Coin', 'Quantity', 'Avg buy price', 'Current value', 'P&L'].map(h => (
                    <th key={h} className="text-left pb-2 pr-3 text-[9px] uppercase tracking-widest text-muted-foreground font-medium last:text-right">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-border/40">
                {/* USDT row — always first */}
                <tr className="hover:bg-muted/10">
                  <td className="py-2.5 pr-3">
                    <div className="flex items-center gap-2">
                      <CoinIcon ticker="USD" />
                      <div>
                        <div className="font-semibold text-xs">Tether USD</div>
                        <div className="text-[9px] text-muted-foreground">USDT <span className="opacity-60">(paper)</span></div>
                      </div>
                    </div>
                  </td>
                  <td className="py-2.5 pr-3 font-mono">{fmt(usdtFree, 2)}</td>
                  <td className="py-2.5 pr-3 text-muted-foreground font-mono">—</td>
                  <td className="py-2.5 pr-3">
                    <div className="font-mono">{fmt(usdtFree)} USDT</div>
                    <div className="text-[9px] text-muted-foreground">≈ ${fmt(usdtFree)}</div>
                  </td>
                  <td className="py-2.5 text-right">
                    <span className="text-[9px] px-2 py-0.5 rounded-full bg-muted/40 text-muted-foreground font-medium">stablecoin</span>
                  </td>
                </tr>

                {/* Coin position rows */}
                {positions.map(pos => {
                  const ticker = pos.symbol.replace('USDT', '');
                  const livePrice   = parseFloat(prices[pos.symbol]?.price || '0') || pos.avg_entry_price;
                  const costBasis   = pos.quantity * pos.avg_entry_price;
                  const currentVal  = pos.quantity * livePrice;
                  const pnlUsdt     = currentVal - costBasis;
                  const pnlPct      = costBasis > 0 ? (pnlUsdt / costBasis) * 100 : 0;
                  const isGain      = pnlUsdt > 0.0001;
                  const isLoss      = pnlUsdt < -0.0001;
                  const pillColor   = isGain
                    ? 'bg-gain/15 text-gain'
                    : isLoss
                    ? 'bg-loss/15 text-loss'
                    : 'bg-muted/40 text-muted-foreground';

                  return (
                    <tr key={pos.symbol} className="hover:bg-muted/10">
                      <td className="py-2.5 pr-3">
                        <div className="flex items-center gap-2">
                          <CoinIcon ticker={ticker} />
                          <div>
                            <div className="font-semibold text-xs">{ticker}</div>
                            <div className="text-[9px] text-muted-foreground">{pos.symbol.replace('USDT','')} <span className="opacity-60">(paper)</span></div>
                          </div>
                        </div>
                      </td>
                      <td className="py-2.5 pr-3 font-mono text-xs">
                        {pos.quantity < 0.01 ? pos.quantity.toFixed(6) : pos.quantity.toFixed(4)}
                      </td>
                      <td className="py-2.5 pr-3 font-mono text-xs">
                        {fmt(pos.avg_entry_price, pos.avg_entry_price >= 1 ? 2 : 6)} USDT
                      </td>
                      <td className="py-2.5 pr-3">
                        <div className="font-mono text-xs">{fmt(currentVal)} USDT</div>
                        <div className="text-[9px] text-muted-foreground">≈ ${fmt(currentVal)}</div>
                      </td>
                      <td className="py-2.5 text-right">
                        <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full ${pillColor}`}>
                          {pnlPct >= 0 ? '+' : ''}{pnlPct.toFixed(2)}%
                        </span>
                        <div className={`text-[9px] font-mono mt-0.5 ${isGain ? 'text-gain' : isLoss ? 'text-loss' : 'text-muted-foreground'}`}>
                          {pnlUsdt >= 0 ? '+' : ''}{fmt(pnlUsdt, 4)} USDT
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {positions.length === 0 && (
            <p className="text-xs text-muted-foreground text-center py-2 border-t border-border pt-3">
              No open positions — bot is watching for entry signals
            </p>
          )}

          {/* ── Realized P&L footer ── */}
          <div className="flex items-center justify-between text-[10px] border-t border-border pt-2">
            <span className="text-muted-foreground">Realized P&amp;L (closed trades)</span>
            <span className={`font-mono font-semibold ${realizedPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
              {realizedPnl >= 0 ? '+' : ''}{fmt(realizedPnl, 4)} USDT
            </span>
          </div>

          {/* ── Footer legend ── */}
          <div className="text-[9px] text-muted-foreground/60 border-t border-border/40 pt-2 leading-relaxed">
            Quantities in coin units · values in USDT · green = profitable · red = at loss · gray = breakeven · all balances marked (paper)
          </div>
        </>
      ))}
    </div>
  );
};

export default PaperWalletPanel;
