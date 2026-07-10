import { useMemo } from 'react';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import {
  scoreColor, toPct100, REGIME_LABELS, FAMILY_LABELS,
  type EvRegime, type EvFloor, type RegimeState,
} from '@/hooks/useEv';

// ── WolfBot S-2 — Regime + adaptive-floor panel ────────────────────────────
// A card for EvScorePanel that shows: the market regime (UP/DOWN/SIDE) with its
// tilt and dominant family, and the LIVE score distribution as a small
// histogram with the adaptive floor drawn as a vertical line — so the operator
// SEES which coins clear the floor and why the bot buys or holds cash.
// Everything is optional/guarded: on older backends the whole card hides.

const REGIME_ICON: Record<RegimeState, typeof TrendingUp> = {
  up: TrendingUp,
  down: TrendingDown,
  side: Minus,
};

const REGIME_CLS: Record<RegimeState, string> = {
  up: 'text-gain border-gain/40 bg-gain/10',
  down: 'text-loss border-loss/40 bg-loss/10',
  side: 'text-warn border-warn/40 bg-warn/10',
};

function familyText(fam: string | undefined): string | null {
  if (!fam) return null;
  const key = fam.toLowerCase();
  return FAMILY_LABELS[key] ?? fam;
}

const N_BINS = 20;

export function RegimePanel({ regime, floor, distribution }: {
  regime?: EvRegime;
  floor?: EvFloor;
  distribution?: number[];
}) {
  // Normalise the distribution values onto the 0-100 axis.
  const values = useMemo<number[]>(() => {
    if (!Array.isArray(distribution)) return [];
    return distribution
      .map(v => toPct100(v))
      .filter((v): v is number => v != null)
      .map(v => Math.max(0, Math.min(100, v)));
  }, [distribution]);

  const floorPct = toPct100(floor?.threshold);

  const { bins, maxCount, cleared } = useMemo(() => {
    const arr = new Array(N_BINS).fill(0) as number[];
    let clearedN = 0;
    for (const v of values) {
      const idx = Math.min(N_BINS - 1, Math.floor((v / 100) * N_BINS));
      arr[idx] += 1;
      if (floorPct != null && v >= floorPct) clearedN += 1;
    }
    return { bins: arr, maxCount: Math.max(1, ...arr), cleared: clearedN };
  }, [values, floorPct]);

  const hasRegime = regime && (regime.state || regime.tilt != null || regime.dominant_family);
  const hasDist = values.length > 0;
  if (!hasRegime && !hasDist) return null;

  const state = (regime?.state ?? undefined) as RegimeState | undefined;
  const Icon = state ? REGIME_ICON[state] : Minus;
  const regimeLabel = state ? REGIME_LABELS[state] : 'REGIME';
  const famText = familyText(regime?.dominant_family);
  const floorLeftPct = floorPct != null ? Math.max(0, Math.min(100, floorPct)) : null;

  return (
    <div className="space-y-2">
      <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
        Market Regime &amp; Adaptive Floor
      </p>

      {/* Regime headline */}
      {hasRegime && (
        <div className={`flex items-start gap-2 rounded border px-2 py-1.5 ${state ? REGIME_CLS[state] : 'border-border/50 bg-muted/10 text-muted-foreground'}`}>
          <Icon className="w-3.5 h-3.5 mt-px shrink-0" />
          <div className="min-w-0">
            <p className="text-[11px] font-bold leading-none">
              {regimeLabel}
              {regime?.tilt != null && isFinite(regime.tilt) && (
                <span className="ml-1 font-mono text-[9px] font-normal opacity-80">
                  tilt {regime.tilt >= 0 ? '+' : '−'}{Math.abs(regime.tilt).toFixed(2)}
                </span>
              )}
            </p>
            {famText && (
              <p className="text-[9px] text-foreground/80 mt-0.5">
                favoring <span className="font-semibold">{famText}</span>
              </p>
            )}
          </div>
        </div>
      )}

      {/* Live score distribution + adaptive floor line */}
      {hasDist && (
        <div className="space-y-1">
          <div className="flex items-center justify-between text-[8px] text-muted-foreground">
            <span>Live score distribution</span>
            {floorPct != null && (
              <span>
                <span className="font-semibold text-foreground">{cleared}</span>/{values.length} clear floor
                <span className="ml-1 font-mono" style={{ color: scoreColor(floorPct) }}>
                  ≥{floorPct.toFixed(0)}
                </span>
              </span>
            )}
          </div>

          <div className="relative">
            {/* histogram bars */}
            <div className="flex items-end gap-px h-12 border-b border-border/50">
              {bins.map((count, i) => {
                const center = ((i + 0.5) / N_BINS) * 100;
                const above = floorPct == null || center >= floorPct;
                const h = (count / maxCount) * 100;
                return (
                  <div
                    key={i}
                    className="flex-1 rounded-t-sm transition-all"
                    style={{
                      height: `${h}%`,
                      minHeight: count > 0 ? '2px' : '0px',
                      backgroundColor: count > 0 ? scoreColor(center) : 'transparent',
                      opacity: above ? 1 : 0.28,
                    }}
                    title={`${center.toFixed(0)}%: ${count} coin${count === 1 ? '' : 's'}`}
                  />
                );
              })}
            </div>

            {/* adaptive floor line */}
            {floorLeftPct != null && (
              <div
                className="absolute top-0 bottom-0 w-px bg-foreground/70 pointer-events-none"
                style={{ left: `${floorLeftPct}%` }}
              >
                <span className="absolute -top-0.5 left-0.5 text-[7px] font-semibold text-foreground/80 whitespace-nowrap">
                  floor
                </span>
              </div>
            )}
          </div>

          {/* axis + floor meta */}
          <div className="flex items-center justify-between text-[7px] text-muted-foreground">
            <span>0</span>
            <span className="text-center">
              {floor?.mode ? `mode: ${floor.mode}` : ''}
              {floor?.abs_floor != null ? ` · abs ${toPct100(floor.abs_floor)?.toFixed(0)}` : ''}
              {floor?.dist_threshold != null ? ` · dist ${toPct100(floor.dist_threshold)?.toFixed(0)}` : ''}
            </span>
            <span>100</span>
          </div>
        </div>
      )}
    </div>
  );
}

export default RegimePanel;
