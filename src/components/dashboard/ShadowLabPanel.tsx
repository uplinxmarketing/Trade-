import { useState } from 'react';
import { toast } from 'sonner';
import { FlaskConical, Copy, Loader2 } from 'lucide-react';
import {
  useEvShadow, isUnavailable,
  type EvShadowBucket, type EvShadowTopSymbol,
} from '@/hooks/useEv';

// ── WolfBot Shadow-Lab panel ───────────────────────────────────────────────
// Surfaces the paper-shadow EV-harvesting flywheel: how fast the bot is
// generating labeled trade outcomes (trades/hour) and what those virtual
// trades say about edge (win rate, expectancy R, profit factor, breakdowns).
// The primary ask is the "Copy shadow results" button — it fetches the plain
// text report from /api/ev/shadow/text and drops it on the clipboard so the
// operator can paste it into chat. Everything degrades to a muted
// "Shadow-Lab not available" state on older backends (404 / available:false).

const SCORE_BUCKET_ORDER = ['0-40', '40-55', '55-70', '70-100'];
const REGIME_ORDER: Array<{ key: 'up' | 'down' | 'side'; label: string }> = [
  { key: 'up', label: 'Up' },
  { key: 'down', label: 'Down' },
  { key: 'side', label: 'Side' },
];

function fmtNum(v: number | null | undefined): string {
  return v == null || !isFinite(v) ? '—' : v.toLocaleString();
}

function fmtPct(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return '—';
  // Accept either 0-1 or 0-100 win-rate scales.
  const pct = v <= 1 ? v * 100 : v;
  return `${pct.toFixed(0)}%`;
}

function fmtR(v: number | null | undefined, digits = 2): string {
  if (v == null || !isFinite(v)) return '—';
  const s = v > 0 ? '+' : v < 0 ? '−' : '';
  return `${s}${Math.abs(v).toFixed(digits)}R`;
}

function fmtSigned(v: number | null | undefined, digits = 2): string {
  if (v == null || !isFinite(v)) return '—';
  const s = v > 0 ? '+' : v < 0 ? '−' : '';
  return `${s}${Math.abs(v).toFixed(digits)}`;
}

function fmtUsd(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return '—';
  const s = v < 0 ? '−$' : '$';
  return `${s}${Math.abs(v).toFixed(2)}`;
}

function fmtHold(sec: number | null | undefined): string {
  if (sec == null || !isFinite(sec)) return '—';
  if (sec < 90) return `${sec.toFixed(0)}s`;
  if (sec < 3600) return `${(sec / 60).toFixed(1)}m`;
  return `${(sec / 3600).toFixed(1)}h`;
}

function ticker(sym: string): string {
  return sym.replace(/USDT$/, '');
}

function rClass(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return 'text-muted-foreground';
  return v >= 0 ? 'text-gain' : 'text-loss';
}

// ── Small metric tile ──────────────────────────────────────────────────────

function Tile({ label, value, valueClass = 'text-foreground', sub }: {
  label: string; value: string; valueClass?: string; sub?: string;
}) {
  return (
    <div className="bg-muted/20 rounded px-2 py-1 min-w-0">
      <p className="text-[8px] text-muted-foreground truncate">{label}</p>
      <p className={`text-[11px] font-mono font-bold leading-tight ${valueClass}`}>{value}</p>
      {sub != null && <p className="text-[7px] text-muted-foreground truncate">{sub}</p>}
    </div>
  );
}

// ── Breakdown table (n / win% / avg R) ─────────────────────────────────────

function BreakdownTable({ title, rows }: {
  title: string;
  rows: Array<{ key: string; label: string; bucket: EvShadowBucket }>;
}) {
  const present = rows.filter(r => (r.bucket?.n ?? 0) > 0);
  if (present.length === 0) return null;
  return (
    <div className="space-y-1">
      <p className="text-[8px] font-semibold uppercase tracking-wider text-muted-foreground">{title}</p>
      <div className="rounded border border-border/50 overflow-hidden">
        <div className="flex items-center px-2 py-0.5 bg-muted/20 text-[8px] uppercase tracking-wider text-muted-foreground">
          <span className="flex-1">Bucket</span>
          <span className="w-10 text-right">n</span>
          <span className="w-12 text-right">win%</span>
          <span className="w-12 text-right">avg R</span>
        </div>
        <div className="divide-y divide-border/30">
          {present.map(({ key, label, bucket }) => (
            <div key={key} className="flex items-center px-2 py-0.5 text-[9px]">
              <span className="flex-1 truncate text-foreground">{label}</span>
              <span className="w-10 text-right font-mono text-muted-foreground">{fmtNum(bucket.n)}</span>
              <span className="w-12 text-right font-mono text-foreground">{fmtPct(bucket.win_rate)}</span>
              <span className={`w-12 text-right font-mono font-semibold ${rClass(bucket.avg_r)}`}>{fmtR(bucket.avg_r)}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ── Top symbols table (adds pnl) ───────────────────────────────────────────

function TopSymbolsTable({ rows }: { rows: EvShadowTopSymbol[] }) {
  const present = rows.filter(r => r && r.symbol);
  if (present.length === 0) return null;
  return (
    <div className="space-y-1">
      <p className="text-[8px] font-semibold uppercase tracking-wider text-muted-foreground">Top symbols</p>
      <div className="rounded border border-border/50 overflow-hidden">
        <div className="flex items-center px-2 py-0.5 bg-muted/20 text-[8px] uppercase tracking-wider text-muted-foreground">
          <span className="flex-1">Coin</span>
          <span className="w-8 text-right">n</span>
          <span className="w-10 text-right">win%</span>
          <span className="w-12 text-right">avg R</span>
          <span className="w-14 text-right">pnl</span>
        </div>
        <div className="divide-y divide-border/30 max-h-40 overflow-y-auto scrollbar-thin">
          {present.slice(0, 12).map(s => (
            <div key={s.symbol} className="flex items-center px-2 py-0.5 text-[9px]">
              <span className="flex-1 truncate font-semibold text-foreground">{ticker(s.symbol)}</span>
              <span className="w-8 text-right font-mono text-muted-foreground">{fmtNum(s.n)}</span>
              <span className="w-10 text-right font-mono text-foreground">{fmtPct(s.win_rate)}</span>
              <span className={`w-12 text-right font-mono font-semibold ${rClass(s.avg_r)}`}>{fmtR(s.avg_r)}</span>
              <span className={`w-14 text-right font-mono ${rClass(s.pnl)}`}>{fmtUsd(s.pnl)}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ── Main panel ─────────────────────────────────────────────────────────────

export function ShadowLabPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const { data, loading, error, notFound } = useEvShadow(baseUrl);
  const unavailable = isUnavailable(data ?? null, notFound);
  const [copying, setCopying] = useState(false);

  const stats = data?.stats ?? {};
  const summary = data?.summary ?? {};

  const deployed = stats.deployed_budget;
  const budget = stats.budget;
  const budgetSub = budget != null && isFinite(budget)
    ? `of ${fmtUsd(budget)} virtual`
    : undefined;

  async function handleCopy() {
    setCopying(true);
    const toastId = toast.loading('Collecting shadow results…');
    try {
      const res = await fetch(`${baseUrl}/api/ev/shadow/text`, { cache: 'no-store' });
      if (!res.ok) {
        toast.error('Shadow results fetch failed', { id: toastId, description: `HTTP ${res.status}` });
        return;
      }
      const text = await res.text();
      try {
        await navigator.clipboard.writeText(text);
        toast.success(`Shadow results copied (${(text.length / 1024).toFixed(1)} KB)`, {
          id: toastId,
          description: 'Paste it into the chat for analysis.',
          duration: 6000,
        });
      } catch {
        // Clipboard blocked (non-HTTPS / permissions) — download instead.
        const blob = new Blob([text], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `wolfbot-shadow-${new Date().toISOString().slice(0, 19)}.txt`;
        a.click();
        URL.revokeObjectURL(url);
        toast.success('Clipboard blocked — shadow results downloaded as a file instead', { id: toastId });
      }
    } catch {
      toast.error('Shadow results request failed', { id: toastId });
    } finally {
      setCopying(false);
    }
  }

  const scoreRows = SCORE_BUCKET_ORDER.map(b => ({
    key: b,
    label: b,
    bucket: summary.by_score_bucket?.[b] ?? {},
  }));
  const regimeRows = REGIME_ORDER.map(({ key, label }) => ({
    key,
    label,
    bucket: summary.by_regime?.[key] ?? {},
  }));
  const exitRows = Object.entries(summary.by_exit_type ?? {}).map(([key, bucket]) => ({
    key,
    label: key,
    bucket: bucket ?? {},
  }));

  return (
    <div className="trading-card p-3 space-y-3">
      {/* Header + copy button */}
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <div className="flex items-center gap-1.5">
          <FlaskConical className="w-3.5 h-3.5 text-primary" />
          <p className="text-xs font-medium text-muted-foreground">Shadow-Lab</p>
        </div>
        <button
          type="button"
          onClick={handleCopy}
          disabled={copying || unavailable}
          title="Copy the plain-text shadow-lab report to the clipboard (falls back to a file download if blocked)"
          className="inline-flex items-center gap-1 px-2 py-1 rounded border border-border bg-muted/20 text-[10px] font-semibold text-foreground hover:brightness-125 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {copying
            ? <Loader2 className="w-3 h-3 animate-spin" />
            : <Copy className="w-3 h-3" />}
          <span className="hidden sm:inline">Copy shadow results</span>
          <span className="sm:hidden">Copy</span>
        </button>
      </div>

      {loading && !data ? (
        <p className="text-[9px] text-muted-foreground py-1">Loading Shadow-Lab…</p>
      ) : unavailable ? (
        <p className="text-[9px] text-muted-foreground/70 italic py-1">
          Shadow-Lab not available{error ? ` — ${error}` : ' — endpoint not deployed yet'}
        </p>
      ) : (
        <>
          {/* Headline row — flywheel state */}
          <div className="grid grid-cols-3 gap-1">
            <Tile
              label="Open / cap"
              value={`${fmtNum(stats.open_positions)} / ${fmtNum(stats.effective_cap)}`}
            />
            <Tile
              label="Budget deployed"
              value={fmtUsd(deployed)}
              sub={budgetSub}
            />
            <Tile
              label="Labeled outcomes"
              value={fmtNum(stats.n_total)}
              valueClass="text-primary"
              sub={summary.trades_per_hour != null && isFinite(summary.trades_per_hour)
                ? `${summary.trades_per_hour.toFixed(1)} trades/hr`
                : undefined}
            />
          </div>

          {/* Summary metrics */}
          <div className="grid grid-cols-3 gap-1">
            <Tile label="Win rate" value={fmtPct(summary.win_rate)} />
            <Tile
              label="Expectancy"
              value={fmtR(summary.expectancy_r)}
              valueClass={rClass(summary.expectancy_r)}
            />
            <Tile
              label="Profit factor"
              value={summary.profit_factor != null && isFinite(summary.profit_factor)
                ? summary.profit_factor.toFixed(2) : '—'}
              valueClass={rClass((summary.profit_factor ?? 1) - 1)}
            />
            <Tile label="Avg win" value={fmtR(summary.avg_win_r)} valueClass="text-gain" />
            <Tile label="Avg loss" value={fmtR(summary.avg_loss_r)} valueClass="text-loss" />
            <Tile
              label="Total pnl"
              value={fmtUsd(summary.total_pnl)}
              valueClass={rClass(summary.total_pnl)}
            />
            <Tile label="Avg hold" value={fmtHold(summary.avg_hold_sec)} />
            <Tile label="Wins" value={fmtNum(summary.wins)} sub={`of ${fmtNum(summary.n)}`} />
            <Tile label="Sample n" value={fmtNum(summary.n)} />
          </div>

          {/* Breakdown tables */}
          <BreakdownTable title="By exit type" rows={exitRows} />
          <BreakdownTable title="By regime" rows={regimeRows} />
          <BreakdownTable title="By WolfScore bucket" rows={scoreRows} />
          <TopSymbolsTable rows={summary.top_symbols ?? []} />

          {/* Settings hint */}
          <p className="text-[8px] text-muted-foreground/80 italic border-t border-border/40 pt-2">
            Tune Shadow-Lab scale in Settings → Data (paper_shadow_budget_usdt,
            paper_shadow_position_usdt, paper_shadow_max_open, paper_shadow_max_per_symbol).
          </p>
        </>
      )}
    </div>
  );
}

export default ShadowLabPanel;
