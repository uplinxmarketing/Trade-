import { useState, useEffect, useCallback, useRef } from 'react';
import { Info } from 'lucide-react';
import { toast } from 'sonner';

// ── Types (tolerant — every section may arrive as {available:false} while the
//    engine-side Phase 4 APIs are still deploying) ────────────────────────────
interface DailySection {
  available?: boolean;
  pnl_today?: number;
  limit_usdt?: number;
  stopped?: boolean;
  stop_until?: number | string | null;
}

interface ConsecutiveSection {
  available?: boolean;
  count?: number;
  limit?: number;
  paused_until?: number | null;
}

interface SlippageVetoEntry {
  avg_bps?: number;
  since_ts?: number;
}

interface CorrelationSection {
  available?: boolean;
  entries_5min?: number;
  limit?: number;
  btc_5m_red?: boolean;
}

interface SlotsSection {
  available?: boolean;
  max_positions?: number;
  min_position_usdt?: number;
  effective_allocation?: number;
  effective_slots?: number;
  degraded?: boolean;
}

interface BnbSection {
  available?: boolean;
  enabled?: boolean;
  bnb_free?: number;
  bnb_usdt_value?: number;
  low?: boolean;
}

interface RiskStatus {
  ts?: string;
  available?: boolean;
  daily?: DailySection;
  consecutive?: ConsecutiveSection;
  slippage_vetoes?: Record<string, SlippageVetoEntry> | { available?: boolean };
  correlation?: CorrelationSection;
  slots?: SlotsSection;
  bnb?: BnbSection;
}

const POLL_MS = 10_000;

const num = (v: unknown): number => (typeof v === 'number' && isFinite(v) ? v : 0);

// A section is usable when it exists and wasn't explicitly marked unavailable.
const sectionOk = (s: { available?: boolean } | undefined | null): boolean =>
  Boolean(s) && s!.available !== false;

const fmtSince = (ts: unknown): string => {
  const t = num(ts);
  if (t <= 0) return '—';
  // Accept both epoch-seconds and epoch-milliseconds.
  const ms = t > 1e12 ? t : t * 1000;
  const ageSec = Math.max(0, (Date.now() - ms) / 1000);
  if (ageSec < 60) return `${ageSec.toFixed(0)}s ago`;
  if (ageSec < 3600) return `${(ageSec / 60).toFixed(0)}m ago`;
  if (ageSec < 86400) return `${(ageSec / 3600).toFixed(1)}h ago`;
  return `${(ageSec / 86400).toFixed(1)}d ago`;
};

const fmtCountdown = (untilTs: unknown): string => {
  const t = num(untilTs);
  if (t <= 0) return '';
  const ms = t > 1e12 ? t : t * 1000;
  const remSec = Math.max(0, (ms - Date.now()) / 1000);
  if (remSec < 60) return `${remSec.toFixed(0)}s`;
  if (remSec < 3600) return `${Math.ceil(remSec / 60)}m`;
  return `${(remSec / 3600).toFixed(1)}h`;
};

type ChipTone = 'green' | 'amber' | 'red' | 'muted';

const CHIP_CLASSES: Record<ChipTone, string> = {
  green: 'bg-gain/15 text-gain border-gain/30',
  amber: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
  red:   'bg-loss/15 text-loss border-loss/30',
  muted: 'bg-muted/20 text-muted-foreground border-border',
};

function Chip({ tone, children, title }: { tone: ChipTone; children: React.ReactNode; title?: string }) {
  return (
    <span
      title={title}
      className={`inline-flex items-center gap-1.5 text-[9px] font-semibold px-2 py-1 rounded border ${CHIP_CLASSES[tone]}`}
    >
      {children}
    </span>
  );
}

export function RiskPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const [status, setStatus] = useState<RiskStatus | null>(null);
  const [loadFailed, setLoadFailed] = useState(false);
  const [resuming, setResuming] = useState(false);
  // daily_loss_stop_pct comes from /api/settings (risk block), not the status
  // payload — fetched once on mount, purely cosmetic for the "armed at −X%" chip.
  const [dailyStopPct, setDailyStopPct] = useState<number | null>(null);

  const disposedRef = useRef(false);
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const poll = useCallback(async () => {
    try {
      const res = await fetch(`${baseUrl}/api/risk/status`, { cache: 'no-store' });
      if (!res.ok) throw new Error(`http ${res.status}`);
      const data = await res.json();
      if (disposedRef.current) return;
      if (data && typeof data === 'object') {
        setStatus(data as RiskStatus);
        setLoadFailed(false);
      }
    } catch {
      if (!disposedRef.current) setLoadFailed(true);
    } finally {
      if (!disposedRef.current) pollRef.current = setTimeout(poll, POLL_MS);
    }
  }, [baseUrl]);

  useEffect(() => {
    disposedRef.current = false;
    poll();
    // One-shot settings fetch for the configured daily-stop percentage.
    fetch(`${baseUrl}/api/settings`, { cache: 'no-store' })
      .then(r => r.ok ? r.json() : null)
      .then(s => {
        if (disposedRef.current) return;
        const pct = s?.risk?.daily_loss_stop_pct;
        if (typeof pct === 'number' && isFinite(pct)) setDailyStopPct(pct);
      })
      .catch(() => { });
    return () => {
      disposedRef.current = true;
      if (pollRef.current) clearTimeout(pollRef.current);
    };
  }, [poll, baseUrl]);

  const resumeDailyStop = useCallback(async () => {
    if (!window.confirm(
      'Resume trading after the daily loss stop?\n\n' +
      'This clears the daily stop and re-enables new buys for the rest of the day. ' +
      'The stop was triggered because today\'s realized losses hit the configured limit.'
    )) return;
    setResuming(true);
    try {
      const res = await fetch(`${baseUrl}/api/risk/resume`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirm: true }),
      });
      const data = await res.json().catch(() => null);
      if (!res.ok || !data?.ok) throw new Error(data?.error ?? `http ${res.status}`);
      if (data.status && typeof data.status === 'object') setStatus(data.status as RiskStatus);
      toast.success('Daily stop cleared — buys re-enabled');
    } catch (e: any) {
      toast.error(`Resume failed: ${e.message}`);
    } finally {
      if (!disposedRef.current) setResuming(false);
    }
  }, [baseUrl]);

  if (!status && loadFailed) {
    return (
      <p className="text-[9px] text-muted-foreground/70 italic py-1">
        Risk status unavailable — endpoint not deployed yet
      </p>
    );
  }
  if (!status) {
    return <p className="text-[9px] text-muted-foreground py-1">Loading risk status…</p>;
  }

  const daily  = status.daily ?? {};
  const consec = status.consecutive ?? {};
  const corr   = status.correlation ?? {};
  const slots  = status.slots ?? {};
  const bnb    = status.bnb ?? {};

  const vetoesRaw = status.slippage_vetoes;
  const vetoRows: Array<[string, SlippageVetoEntry]> =
    vetoesRaw && typeof vetoesRaw === 'object' && (vetoesRaw as { available?: boolean }).available !== false
      ? Object.entries(vetoesRaw as Record<string, SlippageVetoEntry>)
          .filter(([, v]) => v && typeof v === 'object')
      : [];

  const consecPausedUntil = num(consec.paused_until);
  const consecPaused = consecPausedUntil > 0 &&
    (consecPausedUntil > 1e12 ? consecPausedUntil : consecPausedUntil * 1000) > Date.now();

  // K2.4 — resolved per-trade budget vs. tradeable minimum. The engine skips
  // every buy when the per-trade ticket falls under min_position_usdt, so make
  // it loud instead of silent. Resolved per-trade = effective_allocation spread
  // across the configured max_positions (the intended slot count); when that is
  // below the minimum the sizing degrades / no trades execute. Falls back to the
  // 10 USDT default when the backend doesn't expose min_position_usdt.
  const slotAlloc = num(slots.effective_allocation);
  const slotMaxPos = num(slots.max_positions);
  const minTicket = num(slots.min_position_usdt) > 0 ? num(slots.min_position_usdt) : 10;
  const resolvedPerTrade = slotMaxPos > 0 ? slotAlloc / slotMaxPos : slotAlloc;
  const perTradeBelowMin = sectionOk(slots) && slotAlloc > 0 && resolvedPerTrade < minTicket;

  return (
    <div className="space-y-3">
      {/* Status chips row */}
      <div className="flex flex-wrap items-center gap-1.5">
        {/* Daily loss stop */}
        {!sectionOk(daily) ? (
          <Chip tone="muted">Daily guard: n/a</Chip>
        ) : daily.stopped ? (
          <Chip tone="red">
            <span className="w-1.5 h-1.5 rounded-full bg-loss animate-pulse" />
            DAILY STOP ACTIVE — buys paused, exits running
            <button
              onClick={resumeDailyStop}
              disabled={resuming}
              className="ml-1 px-1.5 py-0.5 rounded bg-loss/25 hover:bg-loss/40 border border-loss/40 text-loss text-[9px] font-bold transition-colors disabled:opacity-40"
            >
              {resuming ? 'Resuming…' : 'Resume'}
            </button>
          </Chip>
        ) : (
          <Chip tone="green">
            Daily loss guard armed
            {dailyStopPct !== null
              ? ` at −${dailyStopPct}%`
              : num(daily.limit_usdt) > 0 ? ` at −${num(daily.limit_usdt).toFixed(2)} USDT` : ''}
          </Chip>
        )}

        {/* Consecutive losses */}
        {!sectionOk(consec) ? (
          <Chip tone="muted">Loss streak: n/a</Chip>
        ) : consecPaused ? (
          <Chip tone="amber">
            Loss streak pause — resumes in {fmtCountdown(consec.paused_until)}
          </Chip>
        ) : (
          <Chip tone={num(consec.count) > 0 ? 'amber' : 'green'}>
            Losses {num(consec.count)}/{num(consec.limit) || '—'}
          </Chip>
        )}

        {/* Slots — degraded here is the auto-degrade feature sizing positions to
            the available balance, NOT a fault. Render it neutral/amber-info. */}
        {!sectionOk(slots) ? (
          <Chip tone="muted">Slots: n/a</Chip>
        ) : slots.degraded ? (
          <Chip
            tone="amber"
            title={`Auto-sized to balance: your allocation (${num(slots.effective_allocation).toFixed(2)}) only funds ${num(slots.effective_slots)} of ${num(slots.max_positions)} positions at the minimum ticket. This is the auto-degrade feature working normally, not a fault.`}
          >
            <Info className="w-2.5 h-2.5" />
            slots: {num(slots.effective_slots)}/{num(slots.max_positions)} — sized to balance
          </Chip>
        ) : (
          <Chip tone="green">running {num(slots.effective_slots) || num(slots.max_positions)} slots</Chip>
        )}

        {/* K2.4 — un-tradeable per-trade budget: resolved ticket under the minimum */}
        {perTradeBelowMin && (
          <Chip
            tone="red"
            title={`Resolved per-trade budget $${resolvedPerTrade.toFixed(2)} is under the tradeable minimum of $${minTicket.toFixed(2)} (min_position_usdt). The bot will skip every buy until the budget is raised.`}
          >
            <span className="w-1.5 h-1.5 rounded-full bg-loss animate-pulse" />
            {`per-trade $${resolvedPerTrade.toFixed(2)} — below tradeable minimum — no trades will execute (min $${minTicket.toFixed(2)})`}
          </Chip>
        )}

        {/* BNB fee balance — hidden unless the fee discount is enabled */}
        {sectionOk(bnb) && bnb.enabled && (
          bnb.low ? (
            <Chip tone="amber">
              BNB low (~{num(bnb.bnb_usdt_value).toFixed(2)} USDT) — fee discount at risk
            </Chip>
          ) : (
            <Chip tone="green">BNB fee discount OK ({num(bnb.bnb_usdt_value).toFixed(2)} USDT)</Chip>
          )
        )}

        {/* Correlation guard */}
        {!sectionOk(corr) ? (
          <Chip tone="muted">Correlation guard: n/a</Chip>
        ) : (
          <Chip tone={corr.btc_5m_red ? 'amber' : 'green'}>
            Entries 5min {num(corr.entries_5min)}/{num(corr.limit) || '—'}
            {corr.btc_5m_red && (
              <span className="inline-flex items-center gap-1">
                · <span className="w-1.5 h-1.5 rounded-full bg-loss" /> BTC 5m red
              </span>
            )}
          </Chip>
        )}
      </div>

      {/* Slippage-vetoed symbols */}
      <div>
        <p className="text-[9px] uppercase tracking-wider text-muted-foreground mb-1">
          Slippage-vetoed symbols
        </p>
        {vetoRows.length === 0 ? (
          <p className="text-[9px] text-muted-foreground/70 italic">none</p>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-[10px]">
                <thead>
                  <tr className="text-left text-muted-foreground">
                    <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Symbol</th>
                    <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Avg bps</th>
                    <th className="py-1 font-medium uppercase tracking-wider text-[8px]">Since</th>
                  </tr>
                </thead>
                <tbody>
                  {vetoRows.map(([sym, v]) => (
                    <tr key={sym} className="border-t border-border/50">
                      <td className="py-1 pr-2 font-mono">{sym}</td>
                      <td className="py-1 pr-2 font-mono text-amber-400">{num(v.avg_bps).toFixed(1)}</td>
                      <td className="py-1 text-muted-foreground">{fmtSince(v.since_ts)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="text-[8px] text-muted-foreground/70 mt-1">
              New entries for these symbols are suspended: recent average slippage
              exceeded the configured max_avg_slippage_bps threshold.
            </p>
          </>
        )}
      </div>
    </div>
  );
}
