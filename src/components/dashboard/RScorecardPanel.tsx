import { useEffect, useState } from 'react';

// ── WolfScore-R live scorecard (volume doc §4) ──────────────────────────────
// Daily rollup + 7-day summary shown next to the section-1 baseline for the active
// preset, with a divergence column (live − baseline). Plus an open-position /
// freeze snapshot. This is the weekly refine instrument. Polls ~30s; degrades to a
// muted note on older backends. Only meaningful under scoring_engine=wolf-r-*.

interface DailyRow {
  date: string; trades: number; wins: number; breakevens: number;
  net_usd: number; fees_usd: number; valve_exits: number;
}
interface Summary {
  preset?: string; days?: number; tr_per_day?: number; win_pct?: number;
  usd_per_day?: number; net_7d?: number; trades_7d?: number;
  baseline?: { tr_per_day: number; win_pct: number; usd_per_day: number };
  divergence?: { tr_per_day: number; win_pct: number; usd_per_day: number };
}
interface OpenSnap {
  open_positions?: number; open_gt_24h?: number; open_gt_72h?: number;
  deployed_usd?: number; worst_drawdown_pct?: number; trades_per_slot?: number;
  freezes?: Array<{ coin: string; age_bars: number; drawdown_pct: number }>;
}
interface Scorecard {
  engine?: string; daily?: DailyRow[]; summary?: Summary; open?: OpenSnap; error?: string;
}

const POLL_MS = 30_000;
const n = (v: unknown): number => (typeof v === 'number' && isFinite(v) ? v : 0);
const f2 = (v: unknown) => n(v).toFixed(2);
const usd = (v: unknown) => `${n(v) >= 0 ? '+' : '−'}$${Math.abs(n(v)).toFixed(2)}`;

function useScorecard(baseUrl = '') {
  const [data, setData] = useState<Scorecard | null>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    let dead = false;
    async function poll() {
      try {
        const r = await fetch(`${baseUrl}/api/diagnostics/r-scorecard`, { cache: 'no-store' });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = await r.json();
        if (!dead) setData(d as Scorecard);
      } catch {
        /* keep last */
      } finally {
        if (!dead) setLoading(false);
      }
    }
    poll();
    const id = setInterval(poll, POLL_MS);
    return () => { dead = true; clearInterval(id); };
  }, [baseUrl]);
  return { data, loading };
}

function Div({ v, unit = '' }: { v: number; unit?: string }) {
  const cls = v > 0 ? 'text-gain' : v < 0 ? 'text-loss' : 'text-muted-foreground';
  return <span className={`font-mono ${cls}`}>{v >= 0 ? '+' : '−'}{Math.abs(v).toFixed(2)}{unit}</span>;
}

export function RScorecardPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const { data, loading } = useScorecard(baseUrl);
  const isR = (data?.engine || '').startsWith('wolf-r');
  const s = data?.summary || {};
  const o = data?.open || {};
  const daily = data?.daily || [];

  return (
    <div className="trading-card p-3 space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
          WolfScore-R Scorecard
        </p>
        {data?.engine && <span className="text-[8px] font-mono text-muted-foreground">{data.engine}</span>}
      </div>

      {loading && !data ? (
        <p className="text-[9px] text-muted-foreground py-1">Loading scorecard…</p>
      ) : !isR ? (
        <p className="text-[9px] text-muted-foreground/70 italic py-1">
          Scorecard active only under scoring_engine = wolf-r-volume / wolf-r.
        </p>
      ) : (
        <>
          {/* 7-day summary vs baseline + divergence */}
          <div className="grid grid-cols-4 gap-1 text-[9px]">
            <div className="text-[7px] text-muted-foreground">metric ({s.preset})</div>
            <div className="text-[7px] text-muted-foreground text-right">live 7d</div>
            <div className="text-[7px] text-muted-foreground text-right">baseline</div>
            <div className="text-[7px] text-muted-foreground text-right">Δ</div>

            <div className="text-muted-foreground">tr/day</div>
            <div className="text-right font-mono text-foreground">{f2(s.tr_per_day)}</div>
            <div className="text-right font-mono text-muted-foreground">{f2(s.baseline?.tr_per_day)}</div>
            <div className="text-right"><Div v={n(s.divergence?.tr_per_day)} /></div>

            <div className="text-muted-foreground">win %</div>
            <div className="text-right font-mono text-foreground">{f2(s.win_pct)}%</div>
            <div className="text-right font-mono text-muted-foreground">{f2(s.baseline?.win_pct)}%</div>
            <div className="text-right"><Div v={n(s.divergence?.win_pct)} unit="%" /></div>

            <div className="text-muted-foreground">$/day</div>
            <div className="text-right font-mono text-foreground">{usd(s.usd_per_day)}</div>
            <div className="text-right font-mono text-muted-foreground">{usd(s.baseline?.usd_per_day)}</div>
            <div className="text-right"><Div v={n(s.divergence?.usd_per_day)} /></div>
          </div>

          {/* open / freeze snapshot */}
          <div className="border-t border-border/40 pt-1.5 flex flex-wrap gap-x-3 gap-y-0.5 text-[8px] text-muted-foreground">
            <span>open <span className="font-mono text-foreground">{n(o.open_positions)}</span></span>
            <span>&gt;24h <span className="font-mono text-foreground">{n(o.open_gt_24h)}</span></span>
            <span>&gt;72h <span className={`font-mono ${n(o.open_gt_72h) > 0 ? 'text-loss' : 'text-foreground'}`}>{n(o.open_gt_72h)}</span></span>
            <span>deployed <span className="font-mono text-foreground">${f2(o.deployed_usd)}</span></span>
            <span>worst DD <span className={`font-mono ${n(o.worst_drawdown_pct) < 0 ? 'text-loss' : 'text-foreground'}`}>{f2(o.worst_drawdown_pct)}%</span></span>
            <span>tr/slot <span className="font-mono text-foreground">{f2(o.trades_per_slot)}</span></span>
          </div>
          {Array.isArray(o.freezes) && o.freezes.length > 0 && (
            <p className="text-[8px] text-loss">
              freezes: {o.freezes.map(fz => `${fz.coin} ${fz.drawdown_pct}% (${fz.age_bars}b)`).join(', ')}
            </p>
          )}

          {/* daily rollup */}
          <div className="border-t border-border/40 pt-1.5 overflow-x-auto">
            <table className="w-full text-[8px] font-mono">
              <thead>
                <tr className="text-muted-foreground text-left">
                  <th className="pr-2">date</th><th className="pr-2 text-right">tr</th>
                  <th className="pr-2 text-right">win</th><th className="pr-2 text-right">be</th>
                  <th className="pr-2 text-right">net</th><th className="pr-2 text-right">fees</th>
                  <th className="text-right">valve</th>
                </tr>
              </thead>
              <tbody>
                {daily.length === 0 ? (
                  <tr><td colSpan={7} className="text-muted-foreground/60 py-1">no closed trades yet</td></tr>
                ) : daily.map(r => (
                  <tr key={r.date} className="text-foreground">
                    <td className="pr-2 text-muted-foreground">{(r.date || '').slice(5)}</td>
                    <td className="pr-2 text-right">{n(r.trades)}</td>
                    <td className="pr-2 text-right">{n(r.wins)}</td>
                    <td className="pr-2 text-right">{n(r.breakevens)}</td>
                    <td className={`pr-2 text-right ${n(r.net_usd) >= 0 ? 'text-gain' : 'text-loss'}`}>{usd(r.net_usd)}</td>
                    <td className="pr-2 text-right text-muted-foreground">${f2(r.fees_usd)}</td>
                    <td className="text-right text-muted-foreground">{n(r.valve_exits)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

export default RScorecardPanel;
