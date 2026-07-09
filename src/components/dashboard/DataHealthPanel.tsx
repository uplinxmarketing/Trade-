import { useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { AlertTriangle, X } from 'lucide-react';

// ── Types (GET /api/health/data) ───────────────────────────────────────────

export interface WsConnectionInfo {
  id: string | number;
  streams: number;
  connected_since: number;
  msgs_per_sec: number;
  last_ping_age_sec: number;
}

export interface HealthDataSection {
  available?: boolean;
  connections?: WsConnectionInfo[];
  total_msgs_per_sec?: number;
  worst_candle_ages?: Array<[string, number]>;
  buffer_fill_pct_1m?: number;
  buffer_fill_pct_5m?: number;
  backfill_last_duration_sec?: number | null;
  gap_repairs_24h?: number;
  reconnect_attempts_5min?: number;
  resubscribe_count?: number;
  subscribed_coins?: number;
}

export interface HealthLimitsSection {
  available?: boolean;
  used_weight?: number;
  background_spend_60s?: number;
  rest_paused_until?: number;
  half_rate_until?: number;
  banned_until?: number;
  order_count_10s?: number;
}

export interface HealthScannerInfo {
  mode?: string;
  last_duration_ms?: number;
  effective_interval_sec?: number;
  scan_skipped_overlap?: number;
  universe_size?: number;
  // J3 — authoritative universe breakdown so every panel agrees on the total.
  // `universe_valid` is the count actually scanned; suspended/excluded are
  // reported separately rather than folded into a single ambiguous number.
  universe_valid?: number;
  suspended?: number;
  excluded?: number;
  stale_signal_count?: number;
}

export interface HealthEngineSection {
  available?: boolean;
  scanner?: HealthScannerInfo;
  held_max_price_age_sec?: number | null;
  stale_signal_syms?: string[];
  watchdog_fires_24h?: number;
  held_symbols?: number;
}

// R4.1 — memory pressure section from trade_engine.get_memory_stats(). Key
// names may vary across backend revisions, so every field is optional and read
// defensively (see readMemory). Absent entirely on older backends.
export interface HealthMemorySection {
  available?: boolean;
  rss_mb?: number;
  rss_kb?: number;
  soft_cap_mb?: number;
  memory_restart_pending?: boolean;
  growth_flagged?: boolean;
  leak_suspected?: boolean;
}

export interface DataHealth {
  status: 'green' | 'red';
  causes: string[];
  ts: number;
  data?: HealthDataSection;
  limits?: HealthLimitsSection;
  engine?: HealthEngineSection;
  memory?: HealthMemorySection;
}

// R4.1 — normalized view of the memory section. Returns null when the backend
// ships no `memory` block (older backend), so callers render nothing.
export interface MemoryInfo {
  rssMb: number | null;
  softCapMb: number | null;
  restartPending: boolean;
  growthFlagged: boolean;
}

export function readMemory(health: DataHealth | null): MemoryInfo | null {
  if (!health) return null;
  const raw = (health as { memory?: unknown }).memory;
  if (!raw || typeof raw !== 'object') return null;
  const o = raw as Record<string, unknown>;
  const num = (v: unknown): number | null =>
    typeof v === 'number' && isFinite(v) ? v : null;

  // RSS may arrive as MB or KB — prefer explicit MB, else convert KB.
  let rssMb = num(o.rss_mb);
  if (rssMb == null) {
    const kb = num(o.rss_kb);
    if (kb != null) rssMb = kb / 1024;
  }
  let softCapMb = num(o.soft_cap_mb);
  if (softCapMb == null) {
    const capKb = num(o.soft_cap_kb);
    if (capKb != null) softCapMb = capKb / 1024;
  }
  const restartPending = o.memory_restart_pending === true;
  const growthFlagged = o.growth_flagged === true || o.leak_suspected === true;

  return { rssMb, softCapMb, restartPending, growthFlagged };
}

const POLL_MS = 5000;
const WEIGHT_LIMIT = 6000;

// ── Polling hook (same fetch pattern as DiagnosticsPanel) ─────────────────

export function useDataHealth(baseUrl = '') {
  const [health, setHealth] = useState<DataHealth | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Staleness tracking: one successful poll must not freeze the panel on a
  // green chip forever after the bot dies (mirrors DiagnosticsPanel).
  const [lastOkAt, setLastOkAt] = useState(0);
  const [failCount, setFailCount] = useState(0);

  useEffect(() => {
    let cancelled = false;
    async function poll() {
      try {
        const r = await fetch(`${baseUrl}/api/health/data`);
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const d = await r.json();
        if (!cancelled) { setHealth(d); setError(null); setLastOkAt(Date.now()); setFailCount(0); }
      } catch (e: unknown) {
        if (!cancelled) { setError((e as Error).message ?? 'fetch failed'); setFailCount(c => c + 1); }
      }
    }
    poll();
    const id = setInterval(poll, POLL_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, [baseUrl]);

  // Stale = last two polls failed, or no successful poll in ~15 s.
  const stale = failCount >= 2 || (lastOkAt > 0 && Date.now() - lastOkAt > 15_000);
  return { health, error, stale };
}

// ── Helpers ────────────────────────────────────────────────────────────────

function fmtAge(sec: number | null | undefined): string {
  if (sec == null) return '—';
  if (sec < 10) return `${sec.toFixed(1)}s`;
  if (sec < 90) return `${sec.toFixed(0)}s`;
  if (sec < 3600) return `${(sec / 60).toFixed(1)}m`;
  return `${(sec / 3600).toFixed(1)}h`;
}

function fmtPct(pct: number | null | undefined): string {
  return pct == null ? '—' : `${pct.toFixed(0)}%`;
}

function fmtNum(n: number | null | undefined): string {
  return n == null ? '—' : n.toLocaleString();
}

function fmtMb(n: number | null | undefined): string {
  return n == null ? '—' : `${n.toFixed(0)} MB`;
}

function sectionAvailable(s: { available?: boolean } | null | undefined): boolean {
  return s != null && s.available !== false;
}

// ── Small presentational pieces ───────────────────────────────────────────

function Tile({ label, value, sub, valueClass = 'text-foreground', span2 = false }: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  valueClass?: string;
  span2?: boolean;
}) {
  return (
    <div className={`bg-muted/20 rounded px-2 py-1 min-w-0 ${span2 ? 'col-span-2' : ''}`}>
      <p className="text-[8px] text-muted-foreground truncate">{label}</p>
      <p className={`text-[11px] font-bold leading-tight ${valueClass}`}>{value}</p>
      {sub != null && <p className="text-[7px] text-muted-foreground truncate">{sub}</p>}
    </div>
  );
}

function HealthSection({ title, available, children }: {
  title: string;
  available: boolean;
  children: ReactNode;
}) {
  return (
    <div className="space-y-1">
      <p className="text-[8px] font-semibold uppercase tracking-wider text-muted-foreground">{title}</p>
      {available
        ? children
        : <p className="text-[9px] text-muted-foreground italic">unavailable</p>}
    </div>
  );
}

/**
 * Compact status chip — replaces the old standalone red/green scan dot.
 * Presentational: caller supplies health from useDataHealth so one poller
 * can drive both the chip and an anchored full panel.
 */
export function DataHealthChip({ health, stale = false, open = false, onToggle }: {
  health: DataHealth | null;
  stale?: boolean;
  open?: boolean;
  onToggle?: () => void;
}) {
  const known = health != null && !stale;
  const green = known && health.status === 'green';
  const causeCount = health?.causes?.length ?? 0;
  const label = !known
    ? (health == null ? 'Data: …' : 'Data: stale')
    : green ? 'Data healthy' : `Degraded${causeCount > 0 ? ` (${causeCount})` : ''}`;
  const chipCls = !known
    ? 'bg-muted/20 border-border text-muted-foreground'
    : green
      ? 'bg-gain/10 border-gain/30 text-gain'
      : 'bg-loss/10 border-loss/30 text-loss';
  const dotCls = !known ? 'bg-muted-foreground' : green ? 'bg-gain' : 'bg-loss';

  return (
    <button
      type="button"
      onClick={onToggle}
      title={causeCount > 0 ? health!.causes.join('\n') : label}
      className={`inline-flex items-center gap-1.5 px-1.5 py-0.5 rounded border text-[9px] font-semibold whitespace-nowrap ${chipCls} ${onToggle ? 'cursor-pointer hover:brightness-125' : 'cursor-default'}`}
    >
      <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${dotCls} ${green ? 'animate-pulse' : ''}`} />
      {label}
      {onToggle && <span className="opacity-60">{open ? '▴' : '▾'}</span>}
    </button>
  );
}

// ── Full panel (presentational) ───────────────────────────────────────────

export function DataHealthPanelView({ health, error, stale = false }: {
  health: DataHealth | null;
  error: string | null;
  stale?: boolean;
}) {
  if (!health) {
    return (
      <p className="text-[9px] text-muted-foreground">
        {error ? <span className="text-loss">Data health unavailable — {error}</span> : 'Loading data health…'}
      </p>
    );
  }

  const green = health.status === 'green' && !stale;
  const mem = readMemory(health);
  const d = health.data ?? {};
  const l = health.limits ?? {};
  const e = health.engine ?? {};
  const scanner = e.scanner ?? {};

  const usedWeight = l.used_weight ?? 0;
  const weightClass = usedWeight > 4800 ? 'text-loss' : usedWeight > 3000 ? 'text-yellow-400' : 'text-gain';
  const weightBar = usedWeight > 4800 ? 'bg-loss' : usedWeight > 3000 ? 'bg-yellow-400' : 'bg-gain';

  const heldAge = e.held_max_price_age_sec;
  const heldClass = heldAge == null ? 'text-muted-foreground' : heldAge > 5 ? 'text-loss' : 'text-gain';

  const reconnects = d.reconnect_attempts_5min;
  const reconnectClass = (reconnects ?? 0) > 30 ? 'text-loss' : 'text-foreground';

  const worstAges = (d.worst_candle_ages ?? []).slice(0, 5);

  // J3 — universe count consistency. Show the authoritative valid-universe total
  // (falling back to universe_size on older backends) with suspended/excluded as
  // SEPARATE counts, so this never disagrees with another panel's single number.
  const validCount = scanner.universe_valid ?? scanner.universe_size;
  const suspendedCount = scanner.suspended ?? 0;
  const excludedCount = scanner.excluded ?? 0;
  const universeStr = `universe ${fmtNum(validCount)} valid`
    + (suspendedCount ? ` · ${suspendedCount} suspended` : '')
    + (excludedCount ? ` · ${excludedCount} excluded` : '');

  // REST throttle countdowns — compare against the payload's own clock so
  // units always match, and only surface windows still in the future.
  const throttles: Array<{ label: string; until: number | undefined }> = [
    { label: 'REST paused', until: l.rest_paused_until },
    { label: 'Half-rate', until: l.half_rate_until },
    { label: 'BANNED', until: l.banned_until },
  ];
  const activeThrottles = throttles.filter(t => (t.until ?? 0) > health.ts);

  return (
    <div className="space-y-2">
      {/* Header: big status chip + causes FIRST — that's the point */}
      <div className="flex items-center gap-2 flex-wrap">
        <div className={`inline-flex items-center gap-2 rounded px-2.5 py-1 border text-[11px] font-bold ${
          green ? 'bg-gain/10 border-gain/30 text-gain' : 'bg-loss/10 border-loss/30 text-loss'
        }`}>
          <span className={`w-2 h-2 rounded-full shrink-0 ${green ? 'bg-gain animate-pulse' : 'bg-loss'}`} />
          {green ? 'Data healthy' : 'Degraded'}
        </div>
      </div>

      {stale && (
        <p className="text-[9px] text-loss">
          Health feed stale — bot unreachable{error ? ` (${error})` : ''}
        </p>
      )}

      {(health.causes?.length ?? 0) > 0 && (
        <div className="space-y-0.5">
          {health.causes.map((cause, i) => (
            <p key={i} className="text-[9px] text-loss leading-tight break-words">• {cause}</p>
          ))}
        </div>
      )}

      {/* Data */}
      <HealthSection title="Data" available={sectionAvailable(health.data)}>
        <div className="grid grid-cols-2 gap-1">
          <Tile
            label="WS connections"
            value={`${d.connections?.length ?? 0} · ${d.total_msgs_per_sec != null ? d.total_msgs_per_sec.toFixed(0) : '—'} msg/s`}
            sub={`${fmtNum(d.subscribed_coins)} coins subscribed`}
          />
          <Tile
            label="Reconnects (5min)"
            value={fmtNum(reconnects)}
            valueClass={reconnectClass}
            sub={`resubs ${fmtNum(d.resubscribe_count)}`}
          />
          <Tile label="Buffer fill 1m" value={fmtPct(d.buffer_fill_pct_1m)} />
          <Tile label="Buffer fill 5m" value={fmtPct(d.buffer_fill_pct_5m)} />
          <Tile
            label="Gap repairs (24h)"
            value={fmtNum(d.gap_repairs_24h)}
            sub={d.backfill_last_duration_sec != null ? `last backfill ${fmtAge(d.backfill_last_duration_sec)}` : undefined}
          />
          <div className="bg-muted/20 rounded px-2 py-1 min-w-0">
            <p className="text-[8px] text-muted-foreground">Worst candle ages</p>
            {worstAges.length === 0 ? (
              <p className="text-[9px] text-muted-foreground">—</p>
            ) : (
              <div className="space-y-px">
                {worstAges.map(([sym, age]) => (
                  <div key={sym} className="flex justify-between gap-2 text-[8px]">
                    <span className="font-mono truncate">{sym}</span>
                    <span className={`font-bold shrink-0 ${age > 90 ? 'text-loss' : 'text-muted-foreground'}`}>
                      {fmtAge(age)}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </HealthSection>

      {/* Limits */}
      <HealthSection title="Limits" available={sectionAvailable(health.limits)}>
        <div className="grid grid-cols-2 gap-1">
          <div className="bg-muted/20 rounded px-2 py-1 min-w-0">
            <div className="flex justify-between text-[8px] text-muted-foreground">
              <span>Used weight</span>
              <span className={`font-bold ${weightClass}`}>{fmtNum(l.used_weight)}/{WEIGHT_LIMIT}</span>
            </div>
            <div className="w-full h-1.5 bg-muted/40 rounded-full overflow-hidden mt-1">
              <div
                className={`h-full ${weightBar} transition-all duration-500`}
                style={{ width: `${Math.min(100, Math.max(0, (usedWeight / WEIGHT_LIMIT) * 100))}%` }}
              />
            </div>
            <p className="text-[7px] text-muted-foreground mt-0.5">bg spend {fmtNum(l.background_spend_60s)}/60s</p>
          </div>
          <Tile label="Orders (10s)" value={fmtNum(l.order_count_10s)} />
        </div>
        {activeThrottles.length > 0 && (
          <div className="space-y-0.5">
            {activeThrottles.map(t => (
              <p key={t.label} className="text-[9px] text-loss font-semibold">
                {t.label} — {fmtAge((t.until as number) - health.ts)} remaining
              </p>
            ))}
          </div>
        )}
      </HealthSection>

      {/* Engine */}
      <HealthSection title="Engine" available={sectionAvailable(health.engine)}>
        <div className="grid grid-cols-2 gap-1">
          <Tile
            label="Held price age (max)"
            value={fmtAge(heldAge)}
            valueClass={heldClass}
            sub={`${fmtNum(e.held_symbols)} held`}
          />
          <Tile
            label="Watchdog fires (24h)"
            value={fmtNum(e.watchdog_fires_24h)}
            valueClass={(e.watchdog_fires_24h ?? 0) > 0 ? 'text-yellow-400' : 'text-foreground'}
          />
          <Tile
            label="Scan"
            span2
            value={
              <>
                {scanner.mode ?? '—'}
                <span className="text-muted-foreground font-normal"> · skipped {fmtNum(scanner.scan_skipped_overlap)}</span>
              </>
            }
            sub={`${fmtNum(scanner.last_duration_ms)}ms · every ${fmtNum(scanner.effective_interval_sec)}s · ${universeStr}`}
          />
        </div>
        {(scanner.stale_signal_count ?? 0) > 0 && (
          <p className="text-[9px] text-loss break-words">
            {scanner.stale_signal_count} stale signal{scanner.stale_signal_count !== 1 ? 's' : ''}
            {(e.stale_signal_syms?.length ?? 0) > 0 ? `: ${e.stale_signal_syms!.slice(0, 8).join(', ')}` : ''}
          </p>
        )}
      </HealthSection>

      {/* Memory (R4.1) — only when the backend ships a memory section. Surfaces
          current RSS vs soft cap, a red line when a self-restart is imminent,
          and a softer amber note when growth is merely flagged. */}
      {mem && (
        <HealthSection title="Memory" available={true}>
          <div className="grid grid-cols-2 gap-1">
            <Tile
              label="RSS"
              value={fmtMb(mem.rssMb)}
              valueClass={
                mem.restartPending ? 'text-loss'
                  : mem.growthFlagged ? 'text-amber-400'
                    : 'text-foreground'
              }
              sub={mem.softCapMb != null ? `cap ${fmtMb(mem.softCapMb)}` : undefined}
            />
            <Tile label="Soft cap" value={fmtMb(mem.softCapMb)} />
          </div>
          {mem.restartPending && (
            <p className="text-[9px] text-loss font-semibold break-words">
              Memory high{mem.rssMb != null ? ` (RSS ${fmtMb(mem.rssMb)})` : ''} — clean self-restart imminent to reclaim memory
            </p>
          )}
          {!mem.restartPending && mem.growthFlagged && (
            <p className="text-[9px] text-amber-400 break-words">
              memory growth detected ({fmtMb(mem.rssMb)}{mem.softCapMb != null ? ` / cap ${fmtMb(mem.softCapMb)}` : ''})
            </p>
          )}
        </HealthSection>
      )}
    </div>
  );
}

// ── Self-polling panel (mounted in DiagnosticsTab) ────────────────────────

export function DataHealthPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const { health, error, stale } = useDataHealth(baseUrl);
  return (
    <div className="space-y-2">
      <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Data Health</p>
      <DataHealthPanelView health={health} error={error} stale={stale} />
    </div>
  );
}

// ── R4.1 — Memory restart banner (self-polling, dismissible) ───────────────
// A loud red banner shown at the top of the dashboard when RSS has crossed the
// soft cap and a clean self-restart is imminent, so an OOM-style restart is
// never a surprise. Renders nothing when: no `memory` section (older backend),
// feed stale, restart not pending, or the operator dismissed it. Mirrors the
// UniverseNoticesBanner "removed" (loud) styling.
export function MemoryRestartBanner({ baseUrl = '' }: { baseUrl?: string }) {
  const { health, stale } = useDataHealth(baseUrl);
  const [dismissed, setDismissed] = useState(false);
  const mem = readMemory(health);

  if (stale || !mem || !mem.restartPending || dismissed) return null;

  return (
    <div className="px-3 py-2">
      <div className="rounded border border-loss/60 bg-loss/15 text-loss px-3 py-2 flex items-start gap-2">
        <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0 animate-pulse" />
        <div className="flex-1 min-w-0">
          <p className="text-[11px] font-bold leading-tight">
            Memory high{mem.rssMb != null ? ` (RSS ${fmtMb(mem.rssMb)})` : ''} — bot will restart cleanly shortly to reclaim memory.
          </p>
          {mem.softCapMb != null && (
            <p className="text-[9px] opacity-80 mt-0.5">
              soft cap {fmtMb(mem.softCapMb)} · a clean self-restart avoids an abrupt OOM kill
            </p>
          )}
        </div>
        <button
          type="button"
          onClick={() => setDismissed(true)}
          className="opacity-70 hover:opacity-100 shrink-0"
          title="Dismiss"
        >
          <X className="w-3 h-3" />
        </button>
      </div>
    </div>
  );
}
