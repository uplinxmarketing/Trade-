import { useState, useEffect, useCallback } from 'react';
import { TrendingUp, Lock, BarChart3, Calendar, Loader2 } from 'lucide-react';
import { API_BASE } from '@/config';

// Compact P&L strip surfaced at the top of the AI Trading Agent card.
// Mirrors ReportDashboard's status/stats fetch + range semantics, but only
// renders the non-duplicate stats (Today's Profit / Period P&L, Profit Locked,
// Total Fees) plus a Today · 7D · All range selector. Total P&L and Win Rate
// are intentionally omitted (the card already shows them).

interface BotStatus {
  running: boolean;
  today_realized_pnl: number;
  locked_profit: number;
  total_fees: number;
  total_trades: number;
  trades_today: number;
}

interface RangeStats {
  total: number;
  realized_pnl: number;
  locked_profit: number;
  total_fees: number;
}

type RangePreset = 'today' | '7d' | 'all';

function fmt(n: number, d = 2) {
  return n.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

function toDateStr(d: Date): string {
  return d.toISOString().split('T')[0];
}

function getPresetDates(preset: RangePreset): { from: string; to: string } | null {
  const now = new Date();
  const today = toDateStr(now);
  if (preset === 'today') return { from: today, to: today };
  if (preset === '7d') {
    const d = new Date(now); d.setDate(d.getDate() - 6);
    return { from: toDateStr(d), to: today };
  }
  return null;
}

const PnlStatsStrip = () => {
  const [botStatus, setBotStatus] = useState<BotStatus | null>(null);
  const [rangePreset, setRangePreset] = useState<RangePreset>('all');
  const [rangeStats, setRangeStats] = useState<RangeStats | null>(null);
  const [rangeLoading, setRangeLoading] = useState(false);

  // Base status poll
  const loadStatus = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/status`).then(r => r.ok ? r.json() : null).catch(() => null);
      if (res) setBotStatus(res as BotStatus);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    loadStatus();
    const iv = setInterval(loadStatus, 10_000);
    return () => clearInterval(iv);
  }, [loadStatus]);

  // Range stats fetch
  const fetchRange = useCallback(async (range: { from: string; to: string }) => {
    setRangeLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/stats?from=${range.from}&to=${range.to}`);
      if (res.ok) setRangeStats(await res.json() as RangeStats);
    } catch {
      setRangeStats(null);
    } finally {
      setRangeLoading(false);
    }
  }, []);

  const applyPreset = useCallback((preset: RangePreset) => {
    setRangePreset(preset);
    if (preset === 'all') { setRangeStats(null); return; }
    const dates = getPresetDates(preset);
    if (dates) fetchRange(dates);
  }, [fetchRange]);

  // Re-fetch range every 10s while a range is active
  useEffect(() => {
    if (rangePreset === 'all') return;
    const range = getPresetDates(rangePreset);
    if (!range) return;
    const iv = setInterval(() => fetchRange(range), 10_000);
    return () => clearInterval(iv);
  }, [rangePreset, fetchRange]);

  const isRangeActive = rangePreset !== 'all';

  const todayProfit  = isRangeActive ? (rangeStats?.realized_pnl  ?? 0) : (botStatus?.today_realized_pnl ?? 0);
  const lockedProfit = isRangeActive ? (rangeStats?.locked_profit ?? 0) : (botStatus?.locked_profit   ?? 0);
  const totalFees    = isRangeActive ? (rangeStats?.total_fees    ?? 0) : (botStatus?.total_fees      ?? 0);
  const total        = isRangeActive ? (rangeStats?.total ?? 0) : (botStatus?.total_trades ?? 0);
  // BNB saves ~33.3% vs standard 0.1% rate (BNB discount brings it to 0.075%)
  const bnbSavings   = totalFees * (0.001 / 0.00075 - 1);

  const todayLabel = isRangeActive ? 'Period P&L' : "Today's Profit";

  const cards = [
    {
      label: todayLabel,
      value: `${todayProfit >= 0 ? '+' : ''}$${fmt(Math.abs(todayProfit))}`,
      color: todayProfit > 0 ? 'text-gain' : todayProfit < 0 ? 'text-loss' : 'text-muted-foreground',
      Icon: TrendingUp,
    },
    {
      label: 'Profit Locked',
      value: lockedProfit > 0 ? `+$${fmt(lockedProfit)}` : total > 0 ? '$0.00' : '—',
      color: lockedProfit > 0 ? 'text-gain' : 'text-muted-foreground',
      Icon: Lock,
    },
    {
      label: 'Total Fees',
      value: `$${fmt(totalFees)}`,
      sub: totalFees > 0 ? `BNB saves ~$${fmt(bnbSavings)}` : undefined,
      color: totalFees > 0 ? 'text-warn' : 'text-muted-foreground',
      Icon: BarChart3,
    },
  ];

  const presets: { id: RangePreset; label: string }[] = [
    { id: 'today', label: 'Today' },
    { id: '7d',    label: '7D'    },
    { id: 'all',   label: 'All'   },
  ];

  return (
    <div className="space-y-2">
      {/* Range selector */}
      <div className="flex items-center gap-1.5">
        <Calendar className="w-3 h-3 text-muted-foreground shrink-0" />
        {presets.map(p => (
          <button
            key={p.id}
            onClick={() => applyPreset(p.id)}
            className={`text-[8px] px-1.5 py-0.5 border rounded font-semibold transition-colors ${
              rangePreset === p.id
                ? 'border-accent text-accent'
                : 'border-border text-muted-foreground hover:border-accent/50'}`}
          >
            {p.label}
          </button>
        ))}
        {rangeLoading && <Loader2 className="w-2.5 h-2.5 animate-spin text-muted-foreground" />}
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-3 gap-2">
        {cards.map(c => (
          <div key={c.label} className="rounded-lg border border-border bg-muted/20 px-2.5 py-1.5">
            <div className="flex items-center gap-1 mb-0.5">
              <c.Icon className={`w-3 h-3 ${c.color}`} />
              <span className="text-[8px] uppercase tracking-wider text-muted-foreground truncate">{c.label}</span>
            </div>
            <div className={`text-sm font-mono font-bold tabular-nums ${c.color}`}>{c.value}</div>
            {c.sub && <div className="text-[8px] text-muted-foreground mt-0.5 truncate">{c.sub}</div>}
          </div>
        ))}
      </div>
    </div>
  );
};

export default PnlStatsStrip;
