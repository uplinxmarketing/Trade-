import { useEvExpectancy, isUnavailable, type EvExpectancyLeg } from '@/hooks/useEv';

// ── WolfBot S5 — live-vs-paper expectancy card ─────────────────────────────
// Side-by-side comparison of the LIVE (slot-limited) book vs the PAPER-SHADOW
// (unconstrained) book, so the data-flywheel's value is visible: paper-shadow
// trades every ready signal, live only what slots allow. Polls ~30s. Degrades
// to a muted note on older backends (available:false / 404 / missing).

function fmtR(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return '—';
  const s = v > 0 ? '+' : v < 0 ? '−' : '';
  return `${s}${Math.abs(v).toFixed(2)}R`;
}

function fmtPct(v: number | null | undefined): string {
  if (v == null || !isFinite(v)) return '—';
  return `${(v <= 1 ? v * 100 : v).toFixed(0)}%`;
}

function fmtNum(v: number | null | undefined): string {
  return v == null || !isFinite(v) ? '—' : v.toLocaleString();
}

function Leg({ title, sub, leg }: { title: string; sub: string; leg: EvExpectancyLeg | undefined }) {
  const exp = leg?.expectancy;
  const expClass = exp == null ? 'text-muted-foreground' : exp > 0 ? 'text-gain' : exp < 0 ? 'text-loss' : 'text-foreground';
  const n = leg?.n ?? leg?.n_total;
  return (
    <div className="flex-1 min-w-0 bg-muted/20 rounded px-2.5 py-2 space-y-1.5">
      <div className="flex items-baseline justify-between gap-2">
        <p className="text-[10px] font-semibold text-foreground">{title}</p>
        <p className="text-[8px] text-muted-foreground truncate">{sub}</p>
      </div>
      <div>
        <p className="text-[8px] text-muted-foreground">Expectancy</p>
        <p className={`text-lg font-mono font-bold leading-none ${expClass}`}>{fmtR(exp)}</p>
      </div>
      <div className="grid grid-cols-3 gap-1 pt-0.5">
        <div>
          <p className="text-[7px] text-muted-foreground">Win</p>
          <p className="text-[10px] font-mono font-semibold text-foreground">{fmtPct(leg?.win_rate)}</p>
        </div>
        <div>
          <p className="text-[7px] text-muted-foreground">Avg win</p>
          <p className="text-[10px] font-mono font-semibold text-gain">{fmtR(leg?.avg_win_r)}</p>
        </div>
        <div>
          <p className="text-[7px] text-muted-foreground">Avg loss</p>
          <p className="text-[10px] font-mono font-semibold text-loss">{fmtR(leg?.avg_loss_r)}</p>
        </div>
      </div>
      <p className="text-[7px] text-muted-foreground pt-0.5">
        n={fmtNum(n)}
        {leg?.open_positions != null ? ` · open ${fmtNum(leg.open_positions)}` : ''}
        {leg?.trades_today != null ? ` · today ${fmtNum(leg.trades_today)}` : ''}
      </p>
    </div>
  );
}

export function EvExpectancyCard({ baseUrl = '' }: { baseUrl?: string }) {
  const { data, error, loading, notFound } = useEvExpectancy(baseUrl);
  const unavailable = isUnavailable(data, notFound);

  return (
    <div className="space-y-1.5">
      <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
        Expectancy — live
      </p>
      {loading && !data ? (
        <p className="text-[9px] text-muted-foreground py-1">Loading expectancy…</p>
      ) : unavailable ? (
        <p className="text-[9px] text-muted-foreground/70 italic py-1">
          Expectancy unavailable{error ? ` — ${error}` : ' — endpoint not deployed yet'}
        </p>
      ) : (
        <div className="flex gap-2">
          <Leg title="Live" sub="realized, net of fees" leg={data?.live} />
        </div>
      )}
    </div>
  );
}

export default EvExpectancyCard;
