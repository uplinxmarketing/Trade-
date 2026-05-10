import { useState, useEffect, useCallback } from 'react';
import { BarChart3, DollarSign, TrendingUp, Percent, Loader2, RefreshCw, Activity, Lock, Trophy, Hash } from 'lucide-react';
import { API_BASE } from '@/config';

// ── Types ─────────────────────────────────────────────────────────────────────

interface BotStatus {
  running: boolean;
  balance_usdt: number;
  initial_balance: number;
  realized_pnl: number;
  today_realized_pnl: number;
  locked_profit: number;
  total_fees: number;
  win_rate: number;
  wins: number;
  losses: number;
  total_trades: number;
  trades_today: number;
  open_positions: number;
  mode: string;
}

interface SqliteTrade {
  id: number;
  coin: string;
  mode: string;
  entry_price: number;
  exit_price: number | null;
  quantity: number;
  budget_usdt: number;
  buy_fee: number;
  sell_fee: number;
  net_profit: number | null;
  profitable: number;
  duration_seconds: number;
  timestamp_buy: string;
  timestamp_sell: string | null;
}

interface DisplayTrade {
  id: string;
  pair: string;
  direction: 'LONG';
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

interface CoinStats {
  coin: string; trades: number; wins: number; losses: number;
  totalPnl: number; avgPnl: number; bestTrade: number; worstTrade: number;
}

function fmt(n: number, d = 2) {
  return n.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}
function fmtDate(iso: string) {
  const d = new Date(iso);
  return `${d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} ${d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })}`;
}
function fmtDur(sec: number) {
  if (sec < 60)   return `${sec}s`;
  if (sec < 3600) return `${Math.round(sec / 60)}m`;
  return `${(sec / 3600).toFixed(1)}h`;
}

// ── Component ─────────────────────────────────────────────────────────────────

const ReportDashboard = () => {
  const [botStatus,  setBotStatus]  = useState<BotStatus | null>(null);
  const [trades,     setTrades]     = useState<DisplayTrade[]>([]);
  const [coinSort,   setCoinSort]   = useState<'pnl' | 'trades'>('pnl');
  const [loading,    setLoading]    = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async () => {
    try {
      // All data comes from Railway SQLite — single source of truth.
      // Supabase is intentionally NOT used here because it may contain
      // trades from a different session (local paper bot) and causes
      // double-counting when merged with the Railway data.
      const [statusRes, tradesRes] = await Promise.all([
        fetch(`${API_BASE}/api/status`).then(r => r.ok ? r.json() : null).catch(() => null),
        fetch(`${API_BASE}/api/trades`).then(r => r.ok ? r.json() : { trades: [] }).catch(() => ({ trades: [] })),
      ]);

      if (statusRes) setBotStatus(statusRes as BotStatus);

      const raw: SqliteTrade[] = (tradesRes.trades ?? []) as SqliteTrade[];
      const display: DisplayTrade[] = raw
        .filter(t => t.exit_price != null && t.net_profit != null)
        .map(t => ({
          id:         `rw-${t.id}`,
          pair:       String(t.coin).replace('USDT', '/USDT'),
          direction:  'LONG' as const,
          entryPrice: Number(t.entry_price),
          exitPrice:  Number(t.exit_price),
          qty:        Number(t.quantity),
          budget:     Number(t.budget_usdt ?? 0),
          buyFee:     Number(t.buy_fee ?? 0),
          sellFee:    Number(t.sell_fee ?? 0),
          netPnl:     Number(t.net_profit ?? 0),
          duration:   fmtDur(Number(t.duration_seconds ?? 0)),
          closedAt:   t.timestamp_sell ?? t.timestamp_buy ?? '',
        }))
        .sort((a, b) => new Date(b.closedAt).getTime() - new Date(a.closedAt).getTime());

      setTrades(display);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    load();
    const iv = setInterval(load, 10_000);
    return () => clearInterval(iv);
  }, [load]);

  // ── Coin stats — computed from trade history table (display slice only) ───
  // Note: these percentages are from the last 200 trades (display limit),
  // not the full history. Use the backend-provided win_rate for the headline.
  const coinStats: CoinStats[] = (() => {
    const map: Record<string, CoinStats> = {};
    for (const p of trades) {
      const coin = p.pair.split('/')[0];
      if (!map[coin]) map[coin] = { coin, trades: 0, wins: 0, losses: 0, totalPnl: 0, avgPnl: 0, bestTrade: -Infinity, worstTrade: Infinity };
      const s = map[coin];
      s.trades++;
      if (p.netPnl > 0) s.wins++; else s.losses++;
      s.totalPnl  += p.netPnl;
      s.bestTrade  = Math.max(s.bestTrade,  p.netPnl);
      s.worstTrade = Math.min(s.worstTrade, p.netPnl);
    }
    return Object.values(map)
      .map(s => ({ ...s, avgPnl: s.totalPnl / s.trades,
        bestTrade: s.bestTrade  === -Infinity ? 0 : s.bestTrade,
        worstTrade: s.worstTrade === Infinity  ? 0 : s.worstTrade,
      }))
      .sort(coinSort === 'pnl'
        ? (a, b) => b.totalPnl - a.totalPnl
        : (a, b) => b.trades   - a.trades);
  })();

  // ── Authoritative numbers — all from botStatus (SQL-aggregated, full history)
  const isActive       = botStatus?.running      ?? false;
  const totalProfit    = botStatus?.realized_pnl ?? 0;
  const todayProfit    = botStatus?.today_realized_pnl ?? 0;
  const lockedProfit   = botStatus?.locked_profit  ?? 0;
  const totalFees      = botStatus?.total_fees     ?? 0;
  const bnbSavings     = totalFees * 0.25;
  const total          = botStatus?.total_trades   ?? 0;
  const wins           = botStatus?.wins           ?? 0;
  const losses         = botStatus?.losses         ?? 0;
  const winRateN       = total > 0 ? (wins / total) * 100 : 0;
  const winRate        = total > 0 ? winRateN.toFixed(1) : '—';
  const initBal        = Number(botStatus?.initial_balance ?? 0);
  const lockedPct      = initBal > 0 && lockedProfit > 0 ? (lockedProfit / initBal) * 100 : 0;
  const hasData        = botStatus !== null || trades.length > 0;

  const cards = [
    {
      label: 'Total P&L',
      value: `${totalProfit >= 0 ? '+' : ''}$${fmt(Math.abs(totalProfit))}`,
      sub:   `${total} closed trade${total !== 1 ? 's' : ''}`,
      color: totalProfit > 0 ? 'text-gain' : totalProfit < 0 ? 'text-loss' : 'text-muted-foreground',
      bg:    totalProfit > 0 ? 'bg-gain/10 border-gain/20' : totalProfit < 0 ? 'bg-loss/10 border-loss/20' : 'bg-muted/20 border-border',
      Icon: DollarSign,
    },
    {
      label: "Today's Profit",
      value: `${todayProfit >= 0 ? '+' : ''}$${fmt(Math.abs(todayProfit))}`,
      sub:   `${botStatus?.trades_today ?? 0} trade${(botStatus?.trades_today ?? 0) !== 1 ? 's' : ''} today`,
      color: todayProfit > 0 ? 'text-gain' : todayProfit < 0 ? 'text-loss' : 'text-muted-foreground',
      bg:    todayProfit > 0 ? 'bg-gain/10 border-gain/20' : 'bg-muted/20 border-border',
      Icon: TrendingUp,
    },
    {
      label: 'Profit Locked',
      value: lockedProfit > 0 ? `+$${fmt(lockedProfit)}` : total > 0 ? '$0.00' : '—',
      sub:   lockedPct > 0
        ? `+${fmt(lockedPct, 2)}% of starting capital · ${wins} win${wins !== 1 ? 's' : ''}`
        : total > 0 ? `${wins} winning trade${wins !== 1 ? 's' : ''}` : isActive ? 'Awaiting first win…' : 'No wins yet',
      color: lockedProfit > 0 ? 'text-gain' : 'text-muted-foreground',
      bg:    lockedProfit > 0 ? 'bg-gain/10 border-gain/20' : 'bg-muted/20 border-border',
      Icon: Lock,
    },
    {
      label: 'Total Fees',
      value: `$${fmt(totalFees)}`,
      sub:   `BNB saves ~$${fmt(bnbSavings)}`,
      color: totalFees > 0 ? 'text-warn' : 'text-muted-foreground',
      bg:    totalFees > 0 ? 'bg-warn/10 border-warn/20' : 'bg-muted/20 border-border',
      Icon: BarChart3,
    },
    {
      label: 'Win Rate',
      value: total > 0 ? `${winRate}%` : '—',
      sub:   total > 0
        ? `${wins}W / ${losses}L of ${total}`
        : isActive ? 'Waiting for first trade…' : 'No trades yet',
      color: total > 0 && winRateN >= 50 ? 'text-gain' : 'text-muted-foreground',
      bg:    total > 0 && winRateN >= 50 ? 'bg-gain/10 border-gain/20' : 'bg-muted/20 border-border',
      Icon: Percent,
    },
  ];

  return (
    <div className="space-y-4">
      {isActive && total === 0 && !loading && (
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-accent/10 border border-accent/20 text-xs text-accent">
          <Activity className="w-3.5 h-3.5 animate-pulse" />
          Bot is active — trade results will appear here after the first closed position
        </div>
      )}

      {/* Metric cards */}
      {hasData && (
        <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
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
      )}

      {/* Coin leaderboard — from last 200 trades only, noted in header */}
      {hasData && coinStats.length > 0 && (() => {
        const maxPnl    = Math.max(...coinStats.map(s => Math.abs(s.totalPnl)), 0.01);
        const maxTrades = Math.max(...coinStats.map(s => s.trades), 1);
        return (
          <div className="trading-card">
            <div className="p-3 border-b border-border flex items-center gap-2">
              <Trophy className="w-4 h-4 text-warn" />
              <span className="text-sm font-medium">Coin Performance</span>
              <span className="text-[10px] text-muted-foreground ml-1">
                {coinStats.length} coins · last {trades.length} trades shown
              </span>
              <div className="ml-auto flex items-center gap-1 bg-muted/30 rounded-md p-0.5">
                <button
                  onClick={() => setCoinSort('pnl')}
                  className={`flex items-center gap-1 text-[10px] px-2 py-1 rounded transition-colors font-semibold ${coinSort === 'pnl' ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground'}`}
                >
                  <DollarSign className="w-3 h-3" />Most Profit
                </button>
                <button
                  onClick={() => setCoinSort('trades')}
                  className={`flex items-center gap-1 text-[10px] px-2 py-1 rounded transition-colors font-semibold ${coinSort === 'trades' ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground'}`}
                >
                  <Hash className="w-3 h-3" />Most Trades
                </button>
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="border-b border-border text-muted-foreground uppercase tracking-widest text-[9px]">
                    <th className="text-left px-3 py-2 font-medium w-6">#</th>
                    <th className="text-left px-3 py-2 font-medium">Coin</th>
                    <th className="text-center px-2 py-2 font-medium">Trades</th>
                    <th className="text-center px-2 py-2 font-medium">W / L</th>
                    <th className="text-center px-2 py-2 font-medium">Win %</th>
                    <th className="text-right px-2 py-2 font-medium">Avg / trade</th>
                    <th className="text-right px-2 py-2 font-medium">Best</th>
                    <th className="text-right px-2 py-2 font-medium">Worst</th>
                    <th className="text-right px-3 py-2 font-medium w-48">Total P&L</th>
                  </tr>
                </thead>
                <tbody>
                  {coinStats.map((s, i) => {
                    const winPct = s.trades > 0 ? (s.wins / s.trades) * 100 : 0;
                    const barPct = coinSort === 'pnl'
                      ? (Math.abs(s.totalPnl) / maxPnl) * 100
                      : (s.trades / maxTrades) * 100;
                    return (
                      <tr key={s.coin} className={`border-b border-border/40 hover:bg-muted/10 transition-colors ${i === 0 ? 'bg-gain/5' : ''}`}>
                        <td className="px-3 py-2.5 text-muted-foreground font-mono">
                          {i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `${i + 1}`}
                        </td>
                        <td className="px-3 py-2.5 font-bold text-foreground tracking-wide">{s.coin}</td>
                        <td className="px-2 py-2.5 text-center font-mono font-semibold">{s.trades}</td>
                        <td className="px-2 py-2.5 text-center font-mono">
                          <span className="text-gain">{s.wins}W</span>
                          <span className="text-muted-foreground mx-0.5">/</span>
                          <span className="text-loss">{s.losses}L</span>
                        </td>
                        <td className="px-2 py-2.5 text-center">
                          <span className={`font-semibold ${winPct >= 60 ? 'text-gain' : winPct >= 40 ? 'text-warn' : 'text-loss'}`}>
                            {winPct.toFixed(0)}%
                          </span>
                        </td>
                        <td className={`px-2 py-2.5 text-right font-mono tabular-nums ${s.avgPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                          {s.avgPnl >= 0 ? '+' : ''}{fmt(s.avgPnl, 4)}
                        </td>
                        <td className="px-2 py-2.5 text-right font-mono tabular-nums text-gain">
                          +{fmt(s.bestTrade, 4)}
                        </td>
                        <td className="px-2 py-2.5 text-right font-mono tabular-nums text-loss">
                          {fmt(s.worstTrade, 4)}
                        </td>
                        <td className="px-3 py-2.5 text-right">
                          <div className="flex items-center justify-end gap-2">
                            <div className="w-24 h-2 bg-muted/30 rounded-full overflow-hidden">
                              <div
                                className={`h-full rounded-full transition-all ${s.totalPnl >= 0 ? 'bg-gain' : 'bg-loss'}`}
                                style={{ width: `${barPct}%` }}
                              />
                            </div>
                            <span className={`font-mono font-bold tabular-nums w-20 text-right ${s.totalPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                              {s.totalPnl >= 0 ? '+' : ''}{fmt(s.totalPnl, 4)}
                            </span>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        );
      })()}

      {/* Trade history — Railway SQLite only, last 200 */}
      <div className="trading-card">
        <div className="p-3 border-b border-border flex items-center gap-2">
          <BarChart3 className="w-4 h-4 text-primary" />
          <span className="text-sm font-medium">Trade History</span>
          {botStatus && (
            <span className={`ml-1 text-[10px] px-1.5 py-0.5 rounded font-medium ${botStatus.running ? 'bg-gain/20 text-gain' : 'bg-muted/40 text-muted-foreground'}`}>
              {botStatus.running ? `● ${botStatus.mode?.toUpperCase() ?? 'PAPER'}` : '○ Stopped'}
            </span>
          )}
          {total > trades.length && (
            <span className="text-[10px] text-muted-foreground">
              showing last {trades.length} of {total}
            </span>
          )}
          <button onClick={() => { setRefreshing(true); load(); }}
            className="ml-auto text-muted-foreground hover:text-foreground transition-colors" title="Refresh">
            <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
          </button>
          {loading && <Loader2 className="w-3.5 h-3.5 animate-spin text-muted-foreground" />}
        </div>

        {trades.length === 0 ? (
          <div className="p-8 text-center text-xs text-muted-foreground">
            {loading ? 'Loading…' : isActive
              ? '⏳ Bot is running — first trade will appear here after a position closes'
              : 'No completed trades yet — start the bot to begin trading'}
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-[10px]">
              <thead>
                <tr className="border-b border-border text-muted-foreground uppercase tracking-widest">
                  <th className="text-left px-3 py-2 font-medium">Timestamp</th>
                  <th className="text-left px-3 py-2 font-medium">Pair</th>
                  <th className="text-right px-2 py-2 font-medium">Entry</th>
                  <th className="text-right px-2 py-2 font-medium">Exit</th>
                  <th className="text-right px-2 py-2 font-medium">Qty</th>
                  <th className="text-right px-2 py-2 font-medium">Budget</th>
                  <th className="text-right px-2 py-2 font-medium">Buy fee</th>
                  <th className="text-right px-2 py-2 font-medium">Sell fee</th>
                  <th className="text-right px-2 py-2 font-medium">Net P&L</th>
                  <th className="text-right px-3 py-2 font-medium">Hold</th>
                </tr>
              </thead>
              <tbody>
                {trades.map(p => (
                  <tr key={p.id} className="border-b border-border/50 hover:bg-muted/10 transition-colors">
                    <td className="px-3 py-2 text-muted-foreground whitespace-nowrap">{fmtDate(p.closedAt)}</td>
                    <td className="px-3 py-2 font-semibold text-foreground whitespace-nowrap">{p.pair}</td>
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
