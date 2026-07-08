import { useEffect, useState } from 'react';

// ── L1.2 — entry funnel panel ───────────────────────────────────────────────
// "Coins staying on buy always decomposes into a stage." Renders today's entry
// funnel as a horizontal chain: ready → fresh recheck → budget → order posted →
// filled, with per-stage counts and the drop-off between stages. prev_day is
// shown small/muted for comparison. Polls ~10s. Coded defensively — older
// backends return {available:false} (or nothing), which renders a muted state.

interface FunnelDay {
  ready?: number;
  fresh_recheck_pass?: number;
  budget_pass?: number;
  order_posted?: number;
  filled?: number;
}

interface FunnelData extends FunnelDay {
  available?: boolean;
  day?: string;
  prev_day?: FunnelDay | null;
}

const POLL_MS = 10_000;

const num = (v: unknown): number => (typeof v === 'number' && isFinite(v) ? v : 0);

// Stage order + labels. Keys map onto both today's payload and prev_day.
const STAGES: Array<{ key: keyof FunnelDay; label: string }> = [
  { key: 'ready',              label: 'ready' },
  { key: 'fresh_recheck_pass', label: 'fresh recheck' },
  { key: 'budget_pass',        label: 'budget' },
  { key: 'order_posted',       label: 'order posted' },
  { key: 'filled',             label: 'filled' },
];

export function useFunnel(baseUrl = '') {
  const [funnel, setFunnel] = useState<FunnelData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function poll() {
      try {
        const r = await fetch(`${baseUrl}/api/funnel`, { cache: 'no-store' });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = await r.json();
        if (!cancelled) { setFunnel(d as FunnelData); setError(null); }
      } catch (e: unknown) {
        if (!cancelled) setError((e as Error).message ?? 'fetch failed');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    poll();
    const id = setInterval(poll, POLL_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, [baseUrl]);

  return { funnel, error, loading };
}

function StageCounts({ data }: { data: FunnelDay }) {
  const counts = STAGES.map(s => ({ ...s, count: num(data[s.key]) }));
  const maxCount = Math.max(1, ...counts.map(c => c.count));

  return (
    <div className="flex items-stretch gap-1 overflow-x-auto pb-1">
      {counts.map((c, i) => {
        const prev = i > 0 ? counts[i - 1].count : null;
        const drop = prev != null ? c.count - prev : null;
        return (
          <div key={String(c.key)} className="flex items-center gap-1 shrink-0">
            <div className="bg-muted/20 rounded px-2 py-1 min-w-[3.5rem]">
              <p className="text-[8px] text-muted-foreground truncate">{c.label}</p>
              <p className="text-sm font-mono font-bold leading-tight text-foreground">{c.count}</p>
              <div className="w-full h-1 bg-muted/40 rounded-full overflow-hidden mt-0.5">
                <div
                  className="h-full bg-accent/70 transition-all duration-500"
                  style={{ width: `${Math.max(4, (c.count / maxCount) * 100)}%` }}
                />
              </div>
            </div>
            {drop != null && (
              <div className="flex flex-col items-center shrink-0 px-0.5">
                <span className="text-muted-foreground text-[10px] leading-none">→</span>
                <span className={`text-[8px] font-mono leading-tight ${drop < 0 ? 'text-loss' : 'text-muted-foreground'}`}>
                  {drop <= 0 ? drop : `+${drop}`}
                </span>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export function FunnelPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const { funnel, error, loading } = useFunnel(baseUrl);

  const unavailable = !funnel || funnel.available === false;

  return (
    <div className="trading-card p-3 space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
          Entry Funnel
        </p>
        {funnel?.day && !unavailable && (
          <span className="text-[8px] text-muted-foreground font-mono">{funnel.day}</span>
        )}
      </div>

      {loading && !funnel ? (
        <p className="text-[9px] text-muted-foreground py-1">Loading funnel…</p>
      ) : unavailable ? (
        <p className="text-[9px] text-muted-foreground/70 italic py-1">
          Entry funnel unavailable{error ? ` — ${error}` : ' — endpoint not deployed yet'}
        </p>
      ) : (
        <>
          <StageCounts data={funnel} />

          {/* prev_day comparison — small / muted */}
          {funnel.prev_day && (
            <div className="border-t border-border/40 pt-1.5 space-y-0.5 opacity-60">
              <p className="text-[8px] uppercase tracking-wider text-muted-foreground">Previous day</p>
              <p className="text-[8px] font-mono text-muted-foreground">
                {STAGES.map(s => `${s.label} ${num(funnel.prev_day?.[s.key])}`).join(' → ')}
              </p>
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default FunnelPanel;
