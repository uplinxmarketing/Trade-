import { useState, useEffect, useCallback, useMemo } from 'react';
import { BarChart3, DollarSign, TrendingUp, Percent, Loader2, RefreshCw, Activity, Lock, Trophy, Hash } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';
import { TAKER_FEE } from '@/lib/trading-engine';
import { API_BASE } from '@/config';

const BNB_DISCOUNT = 0.25;

interface Trade {
  id: string; symbol: string; side: string;
  price: number; quantity: number; pnl: number | null;
  reason: string | null; created_at: string;
}

interface BotStatus {
  running: boolean;
  balance_usdt: number;
  initial_balance: number;  // starting capital for % locked calculation
  realized_pnl: number;
  win_rate: number;
  wins: number;
  losses: number;
  total_trades: number;
  trades_today: number;
  open_positions: number;
  mode: string;
}

interface PairTrade {
  id: string; pair: string; direction: 'LONG' | 'SHORT';
  entryPrice: number; exitPrice: number; qty: number;
  budget: number; buyFee: number; sellFee: number;
  netPnl: number; duration: string; closedAt: string;
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

const ReportDashboard = () => {
  const [trades, setTrades]             = useState<Trade[]>([]);
  const [railwayPairs, setRailwayPairs] = useState<PairTrade[]>([]);
  const [botStatus, setBotStatus]       = useState<BotStatus | null>(null);
  const [loading, setLoading]           = useState(true);
  const [refreshing, setRefreshing]     = useState(false);
  const [coinSort, setCoinSort]         = useState<'pnl' | 'trades'>('pnl');

  const load = useCallback(async () => {
    const [supabaseRes, railwayRes, statusRes, sbBotRes] = await Promise.all([
      supabase.from('bot_trade_history').select('*')
        .eq('user_session', 'default').order('created_at', { ascending: true }),
      fetch(`${API_BASE}/api/trades`).then(r => r.ok ? r.json() : { trades: [] }).catch(() => ({ trades: [] })),
      fetch(`${API_BASE}/api/status`).then(r => r.ok ? r.json() : null).catch(() => null),
      // Always pull Railway bot history from Supabase — survives Railway redeploys
      supabase.from('bot_trade_history').select('*')
        .eq('user_session', 'railway_bot').order('created_at', { ascending: true }),
    ]);

    setTrades((supabaseRes.data as Trade[]) ?? []);
    if (statusRes) setBotStatus(statusRes as BotStatus);

    // Railway SQLite trades (current deploy)
    const rwFromSqlite: PairTrade[] = ((railwayRes.trades ?? []) as any[])
      .filter(tr => tr.exit_price != null)
      .map(tr => {
        const qty        = Number(tr.quantity);
        const entryPrice = Number(tr.entry_price);
        const exitPrice  = Number(tr.exit_price);
        const budget     = Number(tr.budget_usdt ?? qty * entryPrice);
        const buyFee     = budget * TAKER_FEE;
        const sellFee    = qty * exitPrice * TAKER_FEE;
        const closedAt   = tr.timestamp_sell ?? tr.timestamp_buy ?? new Date().toISOString();
        const durationMs = tr.timestamp_sell && tr.timestamp_buy
          ? new Date(tr.timestamp_sell).getTime() - new Date(tr.timestamp_buy).getTime() : 0;
        const dur = durationMs > 3_600_000
          ? `${(durationMs / 3_600_000).toFixed(1)}h` : `${(durationMs / 60_000).toFixed(0)}m`;
        return {
          id: `rw-${tr.id}`, pair: String(tr.coin).replace('USDT', '/USDT'),
          direction: 'LONG' as const, entryPrice, exitPrice, qty, budget, buyFee, sellFee,
          netPnl: Number(tr.net_profit ?? 0), duration: dur, closedAt,
        };
      });

    // Supabase Railway bot trades (persist across redeploys)
    // Pair up buy→sell rows from bot_trade_history using reason field
    const sbBotTrades = (sbBotRes.data ?? []) as any[];
    const sbBotPairs: PairTrade[] = (() => {
      // bot_trade_history has separate buy and sell rows; match by symbol+created_at proximity
      // The sell row has pnl set; use those directly
      const sellRows = sbBotTrades.filter(t => t.side === 'sell' && t.pnl != null);
      const buyRows  = sbBotTrades.filter(t => t.side === 'buy');
      const buyMap: Record<string, any> = {};
      for (const b of buyRows) buyMap[`${b.symbol}|${b.created_at}`] = b;

      return sellRows.map(s => {
        // find the closest buy row for this symbol at or before this sell
        const matchingBuys = buyRows.filter(b => b.symbol === s.symbol && b.created_at <= s.created_at);
        const buy = matchingBuys.length > 0 ? matchingBuys[matchingBuys.length - 1] : null;
        const qty        = Number(s.quantity ?? 0);
        const entryPrice = Number(buy?.price ?? 0);
        const exitPrice  = Number(s.price ?? 0);
        const budget     = entryPrice > 0 && qty > 0 ? qty * entryPrice : 0;
        const buyFee     = budget * TAKER_FEE;
        const sellFee    = qty * exitPrice * TAKER_FEE;
        const closedAt   = s.created_at;
        const durationMs = buy ? new Date(s.created_at).getTime() - new Date(buy.created_at).getTime() : 0;
        const dur = durationMs > 3_600_000
          ? `${(durationMs / 3_600_000).toFixed(1)}h` : `${Math.round(durationMs / 60_000)}m`;
        return {
          id: `sb-${s.id}`, pair: String(s.symbol).replace('USDT', '/USDT'),
          direction: 'LONG' as const, entryPrice, exitPrice, qty, budget, buyFee, sellFee,
          netPnl: Number(s.pnl ?? 0), duration: dur, closedAt,
        };
      });
    })();

    // Merge SQLite + Supabase Railway trades; deduplicate by closedAt+pair
    const sqliteKeys = new Set(rwFromSqlite.map(p => `${p.closedAt}|${p.pair}`));
    const merged = [
      ...rwFromSqlite,
      ...sbBotPairs.filter(p => !sqliteKeys.has(`${p.closedAt}|${p.pair}`)),
    ];
    setRailwayPairs(merged);
    setLoading(false);
    setRefreshing(false);
  }, []);

  useEffect(() => {
    load();
    // Poll every 10 s so trade results appear quickly
    const interval = setInterval(load, 10_000);
    // Also react to local paper trades via Supabase realtime
    const ch = supabase.channel('rd-updates')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, load)
      .subscribe();
    return () => { clearInterval(interval); supabase.removeChannel(ch); };
  }, [load]);

  // Build matched buy→sell pairs from Supabase (local paper mode)
  const supabasePairs: PairTrade[] = (() => {
    const buyMap: Record<string, Trade> = {};
    const result: PairTrade[] = [];
    for (const t of trades) {
      if (t.side === 'BUY' && t.pnl === null) { buyMap[t.symbol] = t; }
      else if (t.side === 'SELL' && t.pnl !== null && buyMap[t.symbol]) {
        const b = buyMap[t.symbol];
        const budget     = b.quantity * b.price;
        const buyFee     = budget * TAKER_FEE;
        const sellFee    = t.quantity * t.price * TAKER_FEE;
        const durationMs = new Date(t.created_at).getTime() - new Date(b.created_at).getTime();
        const dur = durationMs > 3_600_000
          ? `${(durationMs / 3_600_000).toFixed(1)}h` : `${(durationMs / 60_000).toFixed(0)}m`;
        result.push({
          id: t.id, pair: t.symbol.replace('USDT', '/USDT'), direction: 'LONG',
          entryPrice: b.price, exitPrice: t.price, qty: t.quantity,
          budget, buyFee, sellFee, netPnl: Number(t.pnl), duration: dur, closedAt: t.created_at,
        });
        delete buyMap[t.symbol];
      }
    }
    return result;
  })();

  const pairTrades: PairTrade[] = [...supabasePairs, ...railwayPairs]
    .sort((a, b) => new Date(b.closedAt).getTime() - new Date(a.closedAt).getTime());

  const coinStats: CoinStats[] = useMemo(() => {
    const map: Record<string, CoinStats> = {};
    for (const p of pairTrades) {
      const coin = p.pair.split('/')[0];
      if (!map[coin]) map[coin] = { coin, trades: 0, wins: 0, losses: 0, totalPnl: 0, avgPnl: 0, bestTrade: 0, worstTrade: 0 };
      const s = map[coin];
      s.trades++;
      if (p.netPnl > 0) s.wins++; else s.losses++;
      s.totalPnl += p.netPnl;
      if (s.trades === 1) { s.bestTrade = p.netPnl; s.worstTrade = p.netPnl; }
      else { s.bestTrade = Math.max(s.bestTrade, p.netPnl); s.worstTrade = Math.min(s.worstTrade, p.netPnl); }
    }
    const list = Object.values(map).map(s => ({ ...s, avgPnl: s.totalPnl / s.trades }));
    return coinSort === 'pnl'
      ? list.sort((a, b) => b.totalPnl - a.totalPnl)
      : list.sort((a, b) => b.trades - a.trades);
  }, [pairTrades, coinSort]);

  // Always compute from full pairTrades (includes Supabase history); overlay Railway live stats
  const computedPnl  = pairTrades.reduce((s, p) => s + p.netPnl, 0);
  const totalProfit  = botStatus
    ? Math.max(botStatus.realized_pnl, computedPnl)  // take whichever is larger (more complete)
    : computedPnl;

  const todayProfit = pairTrades.filter(p => {
    const d = new Date(p.closedAt), now = new Date();
    return d.getFullYear() === now.getFullYear() && d.getMonth() === now.getMonth() && d.getDate() === now.getDate();
  }).reduce((s, p) => s + p.netPnl, 0);

  const totalFees   = pairTrades.reduce((s, p) => s + p.buyFee + p.sellFee, 0);
  const bnbSavings  = totalFees * BNB_DISCOUNT;

  // Use the larger of Railway SQLite count vs full pairTrades count (Supabase may have more history)
  const computedWins  = pairTrades.filter(p => p.netPnl > 0).length;
  const total         = Math.max(botStatus?.total_trades ?? 0, pairTrades.length);
  const wins          = Math.max(botStatus?.wins ?? 0, computedWins);
  const winRateN      = total > 0 ? (wins / total) * 100 : 0;
  const winRate  = total > 0 ? winRateN.toFixed(1) : '—';

  const isActive = botStatus?.running ?? false;

  // Locked profit = cumulative USDT from winning sells only (ignores losses)
  const lockedProfit = pairTrades.filter(p => p.netPnl > 0).reduce((s, p) => s + p.netPnl, 0);
  const initBal      = Number(botStatus?.initial_balance ?? 0);
  const lockedPct    = initBal > 0 && lockedProfit > 0 ? (lockedProfit / initBal) * 100 : 0;

  const cards = [
    {
      label: 'Total P&L',
      value: `${totalProfit >= 0 ? '+' : ''}$${fmt(Math.abs(totalProfit))}`,
      sub: `${total} closed trade${total !== 1 ? 's' : ''}`,
      color: totalProfit > 0 ? 'text-gain' : totalProfit < 0 ? 'text-loss' : 'text-muted-foreground',
      bg: totalProfit > 0 ? 'bg-gain/10 border-gain/20' : totalProfit < 0 ? 'bg-loss/10 border-loss/20' : 'bg-muted/20 border-border',
      Icon: DollarSign,
    },
    {
      label: "Today's Profit",
      value: `${todayProfit >= 0 ? '+' : ''}$${fmt(Math.abs(todayProfit))}`,
      sub: botStatus ? `${botStatus.trades_today} trade${botStatus.trades_today !== 1 ? 's' : ''} today` : new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      color: todayProfit > 0 ? 'text-gain' : todayProfit < 0 ? 'text-loss' : 'text-muted-foreground',
      bg: todayProfit > 0 ? 'bg-gain/10 border-gain/20' : 'bg-muted/20 border-border',
      Icon: TrendingUp,
    },
    {
      label: 'Profit Locked',
      value: lockedProfit > 0 ? `+$${fmt(lockedProfit)}` : (total > 0 ? '$0.00' : '—'),
      sub: lockedPct > 0 ? `+${fmt(lockedPct, 2)}% of starting capital · ${wins} win${wins !== 1 ? 's' : ''}` : (total > 0 ? `${wins} winning trade${wins !== 1 ? 's' : ''}` : isActive ? 'Awaiting first win…' : 'No wins yet'),
      color: lockedProfit > 0 ? 'text-gain' : 'text-muted-foreground',
      bg: lockedProfit > 0 ? 'bg-gain/10 border-gain/20' : 'bg-muted/20 border-border',
      Icon: Lock,
    },
    {
      label: 'Total Fees',
      value: `$${fmt(totalFees)}`,
      sub: `BNB saves ~$${fmt(bnbSavings)}`,
      color: totalFees > 0 ? 'text-warn' : 'text-muted-foreground',
      bg: totalFees > 0 ? 'bg-warn/10 border-warn/20' : 'bg-muted/20 border-border',
      Icon: BarChart3,
    },
    {
      label: 'Win Rate',
      value: total > 0 ? `${winRate}%` : '—',
      sub: total > 0 ? `${wins}W / ${(botStatus?.losses ?? total - wins)}L of ${total}` : (isActive ? 'Waiting for first trade…' : 'No trades yet'),
      color: total > 0 && winRateN >= 50 ? 'text-gain' : 'text-muted-foreground',
      bg: total > 0 && winRateN >= 50 ? 'bg-gain/10 border-gain/20' : 'bg-muted/20 border-border',
      Icon: Percent,
    },
  ];

  // Hide cards entirely until bot has been active (has at least status data)
  const hasData = botStatus !== null || pairTrades.length > 0;

  return (
    <div className="space-y-4">
      {/* Status banner when bot is running but no trades yet */}
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

      {/* Coin performance leaderboard — always visible once hasData */}
      {hasData && (() => {
        const maxPnl    = Math.max(...coinStats.map(s => Math.abs(s.totalPnl)), 0.01);
        const maxTrades = Math.max(...coinStats.map(s => s.trades), 1);
        return (
          <div className="trading-card">
            <div className="p-3 border-b border-border flex items-center gap-2">
              <Trophy className="w-4 h-4 text-warn" />
              <span className="text-sm font-medium">Coin Performance</span>
              <span className="text-[10px] text-muted-foreground ml-1">{coinStats.length} coins traded</span>
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
            {coinStats.length === 0 ? (
              <div className="p-8 text-center text-xs text-muted-foreground">
                {loading ? 'Loading…' : isActive ? '⏳ Bot is running — coin rankings will appear after the first closed position' : 'No completed trades yet'}
              </div>
            ) : (
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
                    const winPct  = s.trades > 0 ? (s.wins / s.trades) * 100 : 0;
                    const barPct  = coinSort === 'pnl'
                      ? (Math.abs(s.totalPnl) / maxPnl) * 100
                      : (s.trades / maxTrades) * 100;
                    const isTop   = i === 0;
                    return (
                      <tr key={s.coin} className={`border-b border-border/40 hover:bg-muted/10 transition-colors ${isTop ? 'bg-gain/5' : ''}`}>
                        <td className="px-3 py-2.5 text-muted-foreground font-mono">
                          {i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `${i + 1}`}
                        </td>
                        <td className="px-3 py-2.5">
                          <span className="font-bold text-foreground tracking-wide">{s.coin}</span>
                        </td>
                        <td className="px-2 py-2.5 text-center font-mono font-semibold text-foreground">
                          {s.trades}
                        </td>
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
                            {/* bar */}
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
            )}
          </div>
        );
      })()}

      {/* Trade history table */}
      <div className="trading-card">
        <div className="p-3 border-b border-border flex items-center gap-2">
          <BarChart3 className="w-4 h-4 text-primary" />
          <span className="text-sm font-medium">Trade History</span>
          {botStatus && (
            <span className={`ml-1 text-[10px] px-1.5 py-0.5 rounded font-medium ${botStatus.running ? 'bg-gain/20 text-gain' : 'bg-muted/40 text-muted-foreground'}`}>
              {botStatus.running ? `● ${botStatus.mode?.toUpperCase() ?? 'PAPER'}` : '○ Stopped'}
            </span>
          )}
          <button onClick={() => { setRefreshing(true); load(); }}
            className="ml-auto text-muted-foreground hover:text-foreground transition-colors" title="Refresh">
            <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
          </button>
          {loading && <Loader2 className="w-3.5 h-3.5 animate-spin text-muted-foreground" />}
        </div>

        {pairTrades.length === 0 ? (
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
