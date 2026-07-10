import { useMemo, useState } from 'react';
import { ChevronDown, ChevronRight, ArrowUpDown, Trophy, Target } from 'lucide-react';
import {
  useEvScores, isUnavailable, scoreColor, pctOf,
  type EvScore, type EvReason,
} from '@/hooks/useEv';
import { EvExpectancyCard } from '@/components/dashboard/EvExpectancyCard';
import { EvModelPanel } from '@/components/dashboard/EvModelPanel';

// ── WolfBot S5 — EV win-probability score panel ────────────────────────────
// The main ask: every ready coin's 0-100% win-probability score, colour-coded
// red→green (reuses the app's gain/loss scale), sortable, refreshing each ~5s
// poll of /api/ev/scores. Click a coin to expand its top-reasons contribution
// breakdown. A ranking view (ordered by score, next_buy highlighted) and the
// model badge (trained / untrained N/M) sit alongside. Everything degrades to a
// muted "EV model not available" state on older backends (available:false / 404).

type SortKey = 'score' | 'symbol';

function ticker(sym: string): string {
  return sym.replace(/USDT$/, '');
}

function fmtPct(pct: number | null): string {
  return pct == null ? '—' : `${pct.toFixed(0)}%`;
}

function fmtPoints(p: number): string {
  const s = p > 0 ? '+' : p < 0 ? '−' : '';
  return `${s}${Math.abs(p).toFixed(2)}`;
}

// ── Model badge ────────────────────────────────────────────────────────────

function ModelBadge({ trained, nTrades, minClean, version }: {
  trained?: boolean; nTrades?: number; minClean?: number; version?: string | number;
}) {
  if (trained) {
    return (
      <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded border border-gain/30 bg-gain/10 text-gain text-[9px] font-semibold">
        trained{version != null ? ` · v${version}` : ''}
      </span>
    );
  }
  const have = nTrades ?? 0;
  const need = minClean ?? 0;
  return (
    <span
      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded border border-warn/30 bg-warn/10 text-warn text-[9px] font-semibold"
      title="Model is not trained yet — scores are heuristic until enough clean trades accumulate."
    >
      untrained{need > 0 ? ` (${have}/${need} trades)` : have > 0 ? ` (${have} trades)` : ''}
    </span>
  );
}

// ── Per-coin expandable reasons ────────────────────────────────────────────

function ReasonBreakdown({ score }: { score: EvScore }) {
  const reasons: EvReason[] = (score.top_reasons ?? [])
    .filter(r => r && typeof r.points === 'number')
    .slice()
    .sort((a, b) => Math.abs(b.points) - Math.abs(a.points));
  const pct = pctOf(score);

  return (
    <div className="px-3 py-2 bg-muted/10 border-t border-border/40 space-y-1.5">
      <div className="flex items-center gap-2 text-[9px] text-muted-foreground">
        <span>score <span className="font-mono font-bold" style={{ color: scoreColor(pct) }}>{fmtPct(pct)}</span></span>
        {score.version != null && <span>· model v{score.version}</span>}
        <span>· {score.trained ? 'trained' : 'untrained'}</span>
      </div>
      {reasons.length === 0 ? (
        <p className="text-[9px] text-muted-foreground/70 italic">No contribution breakdown available.</p>
      ) : (
        <div className="space-y-0.5">
          {reasons.map((r, i) => (
            <div key={`${r.feature}-${i}`} className="flex items-center gap-2">
              <span className="text-[9px] text-foreground w-24 truncate">{r.feature}</span>
              <div className="flex-1 h-1.5 bg-muted/30 rounded-full overflow-hidden relative">
                <div
                  className={`h-full ${r.points >= 0 ? 'bg-gain/70' : 'bg-loss/70'}`}
                  style={{ width: `${Math.min(100, Math.abs(r.points) * 100)}%` }}
                />
              </div>
              <span className={`text-[9px] font-mono font-semibold w-10 text-right ${r.points >= 0 ? 'text-gain' : 'text-loss'}`}>
                {fmtPoints(r.points)}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Ranking view ───────────────────────────────────────────────────────────

function RankingView({ ranking, scores, nextBuy }: {
  ranking: string[]; scores: Record<string, EvScore>; nextBuy: string | null | undefined;
}) {
  if (ranking.length === 0) return null;
  const nb = nextBuy ?? null;
  const nbPct = nb ? pctOf(scores[nb]) : null;

  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-1.5">
        <Trophy className="w-3 h-3 text-warn" />
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Ranking</p>
      </div>
      {nb && (
        <div className="flex items-center gap-1.5 rounded border border-gain/40 bg-gain/10 px-2 py-1">
          <Target className="w-3 h-3 text-gain shrink-0" />
          <span className="text-[10px] text-foreground">
            bot will buy next:{' '}
            <span className="font-bold text-gain">{ticker(nb)}</span>{' '}
            <span className="font-mono" style={{ color: scoreColor(nbPct) }}>{fmtPct(nbPct)}</span>
          </span>
        </div>
      )}
      <div className="flex flex-wrap gap-1">
        {ranking.slice(0, 12).map((sym, i) => {
          const pct = pctOf(scores[sym]);
          return (
            <div
              key={sym}
              className={`flex items-center gap-1 rounded px-1.5 py-0.5 border text-[9px] ${
                sym === nb ? 'border-gain/40 bg-gain/5' : 'border-border/50 bg-muted/10'
              }`}
            >
              <span className="text-muted-foreground font-mono">{i + 1}.</span>
              <span className="font-semibold text-foreground">{ticker(sym)}</span>
              <span className="font-mono font-bold" style={{ color: scoreColor(pct) }}>{fmtPct(pct)}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Score table ────────────────────────────────────────────────────────────

function ScoreTable({ scores }: { scores: Record<string, EvScore> }) {
  const [sortKey, setSortKey] = useState<SortKey>('score');
  const [asc, setAsc] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);

  const rows = useMemo(() => {
    const list = Object.entries(scores).map(([symbol, score]) => ({
      symbol, score, pct: pctOf(score),
    }));
    list.sort((a, b) => {
      let cmp: number;
      if (sortKey === 'symbol') {
        cmp = a.symbol.localeCompare(b.symbol);
      } else {
        cmp = (a.pct ?? -1) - (b.pct ?? -1);
      }
      return asc ? cmp : -cmp;
    });
    return list;
  }, [scores, sortKey, asc]);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setAsc(a => !a);
    else { setSortKey(key); setAsc(key === 'symbol'); }
  };

  if (rows.length === 0) {
    return <p className="text-[9px] text-muted-foreground/70 italic py-1">No scored coins yet.</p>;
  }

  return (
    <div className="rounded border border-border/50 overflow-hidden">
      {/* header */}
      <div className="flex items-center px-3 py-1 bg-muted/20 text-[9px] uppercase tracking-wider text-muted-foreground">
        <button className="flex items-center gap-0.5 w-20 hover:text-foreground" onClick={() => toggleSort('symbol')}>
          Coin <ArrowUpDown className="w-2.5 h-2.5" />
        </button>
        <button className="flex items-center gap-0.5 flex-1 justify-end hover:text-foreground" onClick={() => toggleSort('score')}>
          Win prob <ArrowUpDown className="w-2.5 h-2.5" />
        </button>
        <span className="w-6" />
      </div>
      <div className="max-h-64 overflow-y-auto scrollbar-thin divide-y divide-border/30">
        {rows.map(({ symbol, score, pct }) => {
          const open = expanded === symbol;
          const width = pct == null ? 0 : Math.max(0, Math.min(100, pct));
          return (
            <div key={symbol}>
              <button
                className="w-full flex items-center px-3 py-1.5 hover:bg-muted/20 transition-colors"
                onClick={() => setExpanded(open ? null : symbol)}
              >
                <span className="w-20 text-left text-[11px] font-semibold text-foreground truncate">{ticker(symbol)}</span>
                <div className="flex-1 flex items-center gap-2 min-w-0">
                  <div className="flex-1 h-1.5 bg-muted/30 rounded-full overflow-hidden">
                    <div className="h-full transition-all duration-500" style={{ width: `${width}%`, backgroundColor: scoreColor(pct) }} />
                  </div>
                  <span className="text-[11px] font-mono font-bold w-10 text-right" style={{ color: scoreColor(pct) }}>
                    {fmtPct(pct)}
                  </span>
                </div>
                <span className="w-6 flex justify-end text-muted-foreground">
                  {open ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                </span>
              </button>
              {open && <ReasonBreakdown score={score} />}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Main panel ─────────────────────────────────────────────────────────────

export function EvScorePanel({ baseUrl = '' }: { baseUrl?: string }) {
  const { data, error, loading, notFound } = useEvScores(baseUrl);
  const unavailable = isUnavailable(data, notFound);

  const model = data?.model;
  const scores = data?.scores ?? {};
  const ranking = data?.ranking ?? [];
  const floorDisplayOnly = model != null && model.floor_active === false;

  return (
    <div className="trading-card p-3 space-y-3">
      {/* Header + model badge */}
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <p className="text-xs font-medium text-muted-foreground">Win-Probability Scores</p>
        {!unavailable && model && (
          <ModelBadge
            trained={model.trained}
            nTrades={model.n_trades}
            minClean={model.min_clean}
            version={model.version}
          />
        )}
      </div>

      {loading && !data ? (
        <p className="text-[9px] text-muted-foreground py-1">Loading EV scores…</p>
      ) : unavailable ? (
        <p className="text-[9px] text-muted-foreground/70 italic py-1">
          EV model not available{error ? ` — ${error}` : ' — endpoint not deployed yet'}
        </p>
      ) : (
        <>
          {/* Floor hint — display-only until the model is trained */}
          {floorDisplayOnly && (
            <p className="text-[8px] text-muted-foreground/80 italic">
              Note: the min-win-probability floor is display-only until the model is trained.
            </p>
          )}

          <ScoreTable scores={scores} />

          <RankingView ranking={ranking} scores={scores} nextBuy={data?.next_buy} />
        </>
      )}

      {/* Live-vs-paper expectancy (own poll ~30s) */}
      <div className="border-t border-border/40 pt-3">
        <EvExpectancyCard baseUrl={baseUrl} />
      </div>

      {/* Model version / retrain / activate (own poll ~30s) */}
      <div className="border-t border-border/40 pt-3">
        <EvModelPanel baseUrl={baseUrl} />
      </div>
    </div>
  );
}

export default EvScorePanel;
