import { useState, useEffect, useCallback } from 'react';
import { BarChart3, DollarSign, TrendingUp, Percent, Loader2, RefreshCw } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';
import { TAKER_FEE } from '@/lib/trading-engine';
import { API_BASE } from '@/config';

const BNB_DISCOUNT = 0.25; // 25% BNB fee discount (typical Binance rate)

interface Trade {
  id: string;
  symbol: string;
  side: string;
  price: number;
  quantity: number;
  pnl: number | null;
  reason: string | null;
  created_at: string;
}

function fmt(n: number, d = 2) { return n.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d }); }
function fmtDate(iso: string) {
  const d = new Date(iso);
  return `${d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} ${d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })}`;
}

interface PairTrade {
  id: string;
  pair: string;
  direction: 'LONG' | 'SHORT';
  entryPrice: number;
  exitPrice: number;
  qty: number;
  budget: number;
  buyFee: number;
  sellFee: number;
  netPnl: number;
  duration: string;
  closedAt: string;
}

const ReportDashboard = () => {
  const [trades, setTrades]     = useState<Trade[]>([]);
  const [railwayPairs, setRailwayPairs] = useState<PairTrade[]>([]);
  const [loading, setLoading]   = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async () => {
    const [supabaseRes, railwayRes] = await Promise.all([
      supabase.from('bot_trade_history').select('*')
        .eq('user_session', 'default').order('created_at', { ascending: true }),
      fetch(`${API_BASE}/api/trades`).then(r => r.ok ? r.json() : { trades: [] }).catch(() => ({ trades: [] })),
    ]);
    setTrades((supabaseRes.data as Trade[]) ?? []);

    // Map Railway closed trades (already paired) into PairTrade format
    const rPairs: PairTrade[] = ((railwayRes.trades ?? []) as any[])
      .filter(tr => tr.exit_price != null)
      .map(tr => {
        const qty = Number(tr.quantity);
        const entryPrice = Number(tr.entry_price);
        const exitPrice  = Number(tr.exit_price);
        const budget  = qty * entryPrice;
        const buyFee  = budget * TAKER_FEE;
        const sellFee = qty * exitPrice * TAKER_FEE;
        const closedAt = tr.timestamp_sell ?? tr.timestamp_buy ?? new Date().toISOString();
        const durationMs = tr.timestamp_sell && tr.timestamp_buy
          ? new Date(tr.timestamp_sell).getTime() - new Date(tr.timestamp_buy).getTime() : 0;
        const dur = durationMs > 3600000
          ? `${(durationMs / 3600000).toFixed(1)}h` : `${(durationMs / 60000).toFixed(0)}m`;
        return {
          id: `rw-${tr.id}`, pair: String(tr.coin).replace('USDT', '/USDT'), direction: 'LONG' as const,
          entryPrice, exitPrice, qty, budget, buyFee, sellFee,
          netPnl: Number(tr.net_profit ?? 0), duration: dur, closedAt,
        };
      });
    setRailwayPairs(rPairs);
    setLoading(false);
    setRefreshing(false);
  }, []);

  useEffect(() => {
    load();
    const ch = supabase.channel('rd-updates')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, load)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [load]);

  // Build matched buy→sell pairs from Supabase trade history
  const supabasePairs: PairTrade[] = (() => {
    const buyMap: Record<string, Trade> = {};
    const result: PairTrade[] = [];
    for (const t of trades) {
      if (t.side === 'BUY' && t.pnl === null) { buyMap[t.symbol] = t; }
      else if (t.side === 'SELL' && t.pnl !== null && buyMap[t.symbol]) {
        const b = buyMap[t.symbol];
        const budget   = b.quantity * b.price;
        const buyFee   = budget * TAKER_FEE;
        const sellFee  = t.quantity * t.price * TAKER_FEE;
        const durationMs = new Date(t.created_at).getTime() - new Date(b.created_at).getTime();
        const dur = durationMs > 3600000
          ? `${(durationMs / 3600000).toFixed(1)}h`
          : `${(durationMs / 60000).toFixed(0)}m`;
        result.push({
          id: t.id, pair: t.symbol.replace('USDT', '/USDT'), direction: 'LONG',
          entryPrice: b.price, exitPrice: t.price, qty: t.quantity,
          budget, buyFee, sellFee, netPnl: Number(t.pnl),
          duration: dur, closedAt: t.created_at,
        });
        delete buyMap[t.symbol];
      }
    }
    return result;
  })();

  // Merge Supabase + Railway pairs, newest first
  const pairTrades: PairTrade[] = [...supabasePairs, ...railwayPairs]
    .sort((a, b) => new Date(b.closedAt).getTime() - new Date(a.closedAt).getTime());

  // Metrics
  const closedPnls   = pairTrades.map(p => p.netPnl);
  const totalProfit  = closedPnls.reduce((s, v) => s + v, 0);
  const todayProfit  = pairTrades.filter(p => {
    const d = new Date(p.closedAt);
    const now = new Date();
    return d.getFullYear() === now.getFullYear() && d.getMonth() === now.getMonth() && d.getDate() === now.getDate();
  }).reduce((s, p) => s + p.netPnl, 0);
  const totalFees    = pairTrades.reduce((s, p) => s + p.buyFee + p.sellFee, 0);
  const bnbSavings   = totalFees * BNB_DISCOUNT;
  const wins         = pairTrades.filter(p => p.netPnl > 0).length;
  const total        = pairTrades.length;
  const winRate      = total > 0 ? ((wins / total) * 100).toFixed(1) : '—';

  const cards = [
    {
      label: 'Total Profit',
      value: `${totalProfit >= 0 ? '+' : ''}$${fmt(Math.abs(totalProfit))}`,
      sub: `${total} closed trades`,
      color: totalProfit >= 0 ? 'text-gain' : 'text-loss',
      bg: totalProfit >= 0 ? 'bg-gain/10 border-gain/20' : 'bg-loss/10 border-loss/20',
      Icon: DollarSign,
    },
    {
      label: "Today's Profit",
      value: `${todayProfit >= 0 ? '+' : ''}$${fmt(Math.abs(todayProfit))}`,
      sub: new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      color: todayProfit >= 0 ? 'text-gain' : 'text-loss',
      bg: todayProfit >= 0 ? 'bg-gain/10 border-gain/20' : 'bg-muted/20 border-border',
      Icon: TrendingUp,
    },
    {
      label: 'Total Fees',
      value: `$${fmt(totalFees)}`,
      sub: `BNB saves ~$${fmt(bnbSavings)}`,
      color: 'text-warn',
      bg: 'bg-warn/10 border-warn/20',
      Icon: BarChart3,
    },
    {
      label: 'Win Rate',
      value: total > 0 ? `${winRate}%` : '—',
      sub: `${wins}W / ${total - wins}L of ${total}`,
      color: Number(winRate) >= 50 ? 'text-gain' : 'text-muted-foreground',
      bg: Number(winRate) >= 50 ? 'bg-gain/10 border-gain/20' : 'bg-muted/20 border-border',
      Icon: Percent,
    },
  ];

  return (
    <div className="space-y-4">
      {/* Metric cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        {cards.map(c => (
          <div key={c.label} className={`rounded-lg border p-3 ${c.bg}`}>
            <div className="flex items-center gap-1.5 mb-2">
              <c.Icon className={`w-3.5 h-3.5 ${c.color}`} />
              <span className="text-[10px] uppercase tracking-widest text-muted-foreground">{c.label}</span>
            </div>
            <div className={`text-lg font-mono font-bold tabular-nums ${c.color}`}>{c.value}</div>
            <div className="text-[10px] text-muted-foreground mt-0.5">{c.sub}</div>
          </div>
        ))}
      </div>

      {/* Trade history table */}
      <div className="trading-card">
        <div className="p-3 border-b border-border flex items-center gap-2">
          <BarChart3 className="w-4 h-4 text-primary" />
          <span className="text-sm font-medium">Trade History</span>
          <button onClick={() => { setRefreshing(true); load(); }}
            className="ml-auto text-muted-foreground hover:text-foreground transition-colors" title="Refresh">
            <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
          </button>
          {loading && <Loader2 className="w-3.5 h-3.5 animate-spin text-muted-foreground" />}
        </div>

        {pairTrades.length === 0 ? (
          <div className="p-8 text-center text-xs text-muted-foreground">
            {loading ? 'Loading…' : 'No completed trades yet — start the bot to generate data'}
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-[10px]">
              <thead>
                <tr className="border-b border-border text-muted-foreground uppercase tracking-widest">
                  <th className="text-left px-3 py-2 font-medium">Timestamp</th>
                  <th className="text-left px-3 py-2 font-medium">Pair</th>
                  <th className="text-center px-2 py-2 font-medium">Dir.</th>
                  <th className="text-right px-2 py-2 font-medium">Entry</th>
                  <th className="text-right px-2 py-2 font-medium">Exit</th>
                  <th className="text-right px-2 py-2 font-medium">Qty</th>
                  <th className="text-right px-2 py-2 font-medium">Budget</th>
                  <th className="text-right px-2 py-2 font-medium">Buy fee</th>
                  <th className="text-right px-2 py-2 font-medium">Sell fee</th>
                  <th className="text-right px-2 py-2 font-medium">Net P&L</th>
                  <th className="text-right px-3 py-2 font-medium">Duration</th>
                </tr>
              </thead>
              <tbody>
                {pairTrades.map(p => (
                  <tr key={p.id} className="border-b border-border/50 hover:bg-muted/10 transition-colors">
                    <td className="px-3 py-2 text-muted-foreground whitespace-nowrap">{fmtDate(p.closedAt)}</td>
                    <td className="px-3 py-2 font-semibold text-foreground whitespace-nowrap">{p.pair}</td>
                    <td className="px-2 py-2 text-center">
                      <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold ${p.direction === 'LONG' ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'}`}>
                        {p.direction}
                      </span>
                    </td>
                    <td className="px-2 py-2 text-right font-mono tabular-nums">${fmt(p.entryPrice, 4)}</td>
                    <td className="px-2 py-2 text-right font-mono tabular-nums">${fmt(p.exitPrice, 4)}</td>
                    <td className="px-2 py-2 text-right font-mono tabular-nums">{p.qty.toFixed(5)}</td>
                    <td className="px-2 py-2 text-right font-mono tabular-nums">${fmt(p.budget)}</td>
                    <td className="px-2 py-2 text-right font-mono tabular-nums text-warn">${fmt(p.buyFee, 4)}</td>
                    <td className="px-2 py-2 text-right font-mono tabular-nums text-warn">${fmt(p.sellFee, 4)}</td>
                    <td className={`px-2 py-2 text-right font-mono tabular-nums font-bold ${p.netPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                      {p.netPnl >= 0 ? '+' : ''}{fmt(p.netPnl, 4)}
                    </td>
                    <td className="px-3 py-2 text-right text-muted-foreground">{p.duration}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportDashboard;
