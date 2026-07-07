import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { ChevronDown, ChevronUp, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { LiveConfirmModal } from './LiveConfirmModal';
import { isLiveRunning, fmtValue } from './strategy-shared';
import { isLowSample, LowSampleNA, readLowSample, readDenominator } from '@/lib/sampleGuard';

// ── Phase 5 §5.2.2 — signals editor ───────────────────────────────────────────
// Table over GET /api/signal-registry with a role dropdown per signal
// (scored / mandatory / veto / off), expandable threshold inputs (rendered
// from whatever thresholds the API provides), and a 24h-impact column built
// from /api/signals/telemetry (fire rate) + /api/stats/attribution (lift).
// Staged changes are PUT to /api/signals/registry (LIVE modal applies).

interface RegistrySignal {
  id: string;
  label?: string;
  description?: string;
  category?: string;
  role?: string;
  thresholds?: Record<string, unknown>;
  [k: string]: unknown;
}

const ROLE_OPTIONS = ['scored', 'mandatory', 'veto', 'off'] as const;

const ROLE_COLORS: Record<string, string> = {
  scored:    'text-accent',
  mandatory: 'text-red-400',
  veto:      'text-orange-400',
  off:       'text-muted-foreground',
  disabled:  'text-muted-foreground',
};

const num = (v: unknown): number | null => {
  const n = Number(v);
  return isFinite(n) ? n : null;
};

/** Tolerant per-signal lookup in a telemetry/attribution payload (map or array). */
function perSignalEntry(payload: unknown, id: string): Record<string, unknown> | null {
  if (!payload || typeof payload !== 'object') return null;
  const p = payload as Record<string, unknown>;
  const container = (p.signals ?? p.per_signal ?? p) as unknown;
  if (Array.isArray(container)) {
    const hit = container.find((e: any) => e && (e.id === id || e.signal === id || e.signal_id === id));
    return hit && typeof hit === 'object' ? hit : null;
  }
  if (container && typeof container === 'object') {
    const hit = (container as Record<string, unknown>)[id];
    return hit && typeof hit === 'object' ? hit as Record<string, unknown> : null;
  }
  return null;
}

export function SignalsEditorPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const [signals, setSignals] = useState<RegistrySignal[]>([]);
  const [registryThresholds, setRegistryThresholds] = useState<Record<string, Record<string, unknown>>>({});
  const [telemetry, setTelemetry] = useState<unknown>(null);
  const [attribution, setAttribution] = useState<unknown>(null);
  const [loadFailed, setLoadFailed] = useState(false);
  const [loaded, setLoaded] = useState(false);
  // Staged edits
  const [roleDraft, setRoleDraft] = useState<Record<string, string>>({});
  const [thresholdDraft, setThresholdDraft] = useState<Record<string, Record<string, unknown>>>({});
  const [rowErrors, setRowErrors] = useState<Record<string, string>>({});
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [saving, setSaving] = useState(false);
  const [liveModalOpen, setLiveModalOpen] = useState(false);
  const pendingBodyRef = useRef<Record<string, unknown> | null>(null);

  const disposedRef = useRef(false);
  useEffect(() => {
    disposedRef.current = false;
    return () => { disposedRef.current = true; };
  }, []);

  const load = useCallback(async () => {
    try {
      const [regRes, telRes, attRes] = await Promise.all([
        fetch(`${baseUrl}/api/signal-registry`, { cache: 'no-store' }),
        fetch(`${baseUrl}/api/signals/telemetry`, { cache: 'no-store' }).catch(() => null),
        fetch(`${baseUrl}/api/stats/attribution`, { cache: 'no-store' }).catch(() => null),
      ]);
      const reg = regRes?.ok ? await regRes.json().catch(() => null) : null;
      const tel = telRes?.ok ? await telRes.json().catch(() => null) : null;
      const att = attRes?.ok ? await attRes.json().catch(() => null) : null;
      if (disposedRef.current) return;
      if (!reg || reg.error || !Array.isArray(reg.signals)) {
        setLoadFailed(true);
        setLoaded(true);
        return;
      }
      setSignals(reg.signals.filter((s: any) => s && typeof s.id === 'string'));
      // Thresholds may live per-signal or as a top-level {id: {...}} map
      setRegistryThresholds(
        reg.thresholds && typeof reg.thresholds === 'object' ? reg.thresholds : {}
      );
      if (tel && !tel.error) setTelemetry(tel);
      if (att && !att.error) setAttribution(att);
      setRoleDraft({});
      setThresholdDraft({});
      setRowErrors({});
      setLoadFailed(false);
      setLoaded(true);
    } catch {
      if (!disposedRef.current) { setLoadFailed(true); setLoaded(true); }
    }
  }, [baseUrl]);

  useEffect(() => { load(); }, [load]);

  const thresholdsFor = useCallback((sig: RegistrySignal): Record<string, unknown> | null => {
    const own = sig.thresholds && typeof sig.thresholds === 'object' ? sig.thresholds : null;
    const reg = registryThresholds[sig.id] && typeof registryThresholds[sig.id] === 'object'
      ? registryThresholds[sig.id] : null;
    const merged = { ...(reg ?? {}), ...(own ?? {}) };
    return Object.keys(merged).length > 0 ? merged : null;
  }, [registryThresholds]);

  const effectiveRole = useCallback((sig: RegistrySignal): string =>
    roleDraft[sig.id] ?? (typeof sig.role === 'string' ? sig.role : 'off'), [roleDraft]);

  // Changed sets
  const changedRoles = useMemo(() => {
    const out: Record<string, string> = {};
    for (const sig of signals) {
      const staged = roleDraft[sig.id];
      if (staged !== undefined && staged !== sig.role) out[sig.id] = staged;
    }
    return out;
  }, [signals, roleDraft]);

  const changedThresholds = useMemo(() => {
    const out: Record<string, Record<string, unknown>> = {};
    for (const sig of signals) {
      const staged = thresholdDraft[sig.id];
      if (!staged) continue;
      const base = thresholdsFor(sig) ?? {};
      const diff: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(staged)) {
        if (JSON.stringify(v) !== JSON.stringify(base[k])) diff[k] = v;
      }
      if (Object.keys(diff).length > 0) out[sig.id] = diff;
    }
    return out;
  }, [signals, thresholdDraft, thresholdsFor]);

  const isDirty = Object.keys(changedRoles).length > 0 || Object.keys(changedThresholds).length > 0;

  const doSave = useCallback(async (confirmWord?: string) => {
    const body = pendingBodyRef.current;
    if (!body) return;
    if (confirmWord) body.confirm = confirmWord;
    setSaving(true);
    try {
      const res = await fetch(`${baseUrl}/api/signals/registry`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json().catch(() => null);
      if (res.status === 422 && data?.errors && typeof data.errors === 'object') {
        // Map dotted error keys ("roles.X1_atr", "thresholds.M1.value" or plain
        // signal ids) onto rows tolerantly.
        const mapped: Record<string, string> = {};
        for (const [key, msg] of Object.entries(data.errors as Record<string, string>)) {
          const parts = key.split('.');
          const id = parts.length > 1 ? parts[1] : parts[0];
          mapped[id] = String(msg);
        }
        setRowErrors(mapped);
        toast.error('Validation failed — see highlighted signals');
        return;
      }
      if (res.status === 409) {
        toast.error(data?.error ?? 'Live guard: confirmation required');
        return;
      }
      if (!res.ok || !data || data.error) throw new Error(data?.error ?? `HTTP ${res.status}`);
      toast.success('Signal roles saved');
      pendingBodyRef.current = null;
      await load();
    } catch (e: any) {
      toast.error(`Save failed: ${e.message}`);
    } finally {
      if (!disposedRef.current) setSaving(false);
    }
  }, [baseUrl, load]);

  const handleSaveClick = useCallback(async () => {
    if (!isDirty || saving) return;
    const body: Record<string, unknown> = { roles: changedRoles };
    if (Object.keys(changedThresholds).length > 0) body.thresholds = changedThresholds;
    pendingBodyRef.current = body;
    if (await isLiveRunning(baseUrl)) {
      setLiveModalOpen(true);
    } else {
      doSave();
    }
  }, [isDirty, saving, changedRoles, changedThresholds, baseUrl, doSave]);

  // ── Render ──────────────────────────────────────────────────────────────────
  if (loaded && loadFailed && signals.length === 0) {
    return (
      <p className="text-[9px] text-muted-foreground/70 italic py-1">
        Signal registry unavailable — endpoint not deployed yet
      </p>
    );
  }
  if (!loaded) {
    return <p className="text-[9px] text-muted-foreground py-1">Loading signal registry…</p>;
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-[9px] text-muted-foreground">
          Change a signal's role, then Save. Roles apply through the strategy config.
        </p>
        <button onClick={load}
          className="text-[10px] text-muted-foreground hover:text-foreground flex items-center gap-1 shrink-0">
          <RefreshCw className="w-3 h-3" />Reload
        </button>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-[10px]">
          <thead>
            <tr className="text-left text-muted-foreground">
              <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Signal</th>
              <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Category</th>
              <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Role</th>
              <th className="py-1 font-medium uppercase tracking-wider text-[8px] text-right">24h impact</th>
            </tr>
          </thead>
          <tbody>
            {signals.map(sig => {
              const role = effectiveRole(sig);
              const changed = sig.id in changedRoles || sig.id in changedThresholds;
              const err = rowErrors[sig.id];
              const thresholds = thresholdsFor(sig);
              const isOpen = expanded[sig.id] ?? false;

              // 24h impact — fire rate (telemetry) + lift (attribution)
              const tel = perSignalEntry(telemetry, sig.id);
              const att = perSignalEntry(attribution, sig.id);
              const fireRate = tel ? num(tel.fire_rate) : null;
              const lift = att
                ? (num(att.lift) ?? num((att as any).lift_pct) ?? num((att as any).value))
                : null;

              // Min-sample guard — a fire-rate / lift from a handful of
              // evaluations is noise. Suppress the confident % when the
              // backend flags low_sample or the denominator is < 20.
              const fireCount = readDenominator(tel, ['evaluated', 'evaluations', 'fired']);
              const fireLowSample = isLowSample(fireCount, readLowSample(tel));
              const liftCount = readDenominator(att, ['trades', 'fired_trades', 'n_fired']);
              const liftLowSample = isLowSample(liftCount, readLowSample(att));

              // Keep the current role selectable even when it isn't a canonical option
              const options: string[] = ROLE_OPTIONS.includes(role as any)
                ? [...ROLE_OPTIONS]
                : [role, ...ROLE_OPTIONS];

              return [
                <tr key={sig.id} className={`border-t border-border/50 ${err ? 'bg-loss/5' : ''}`}>
                  <td className="py-1.5 pr-2 align-top">
                    <button
                      onClick={() => setExpanded(e => ({ ...e, [sig.id]: !isOpen }))}
                      className="flex items-start gap-1 text-left hover:text-accent transition-colors"
                      title={thresholds ? 'Show thresholds' : undefined}>
                      {thresholds
                        ? (isOpen ? <ChevronUp className="w-3 h-3 mt-0.5 shrink-0" /> : <ChevronDown className="w-3 h-3 mt-0.5 shrink-0" />)
                        : <span className="w-3 shrink-0" />}
                      <span>
                        <span className="font-mono font-semibold flex items-center gap-1.5">
                          {sig.id}
                          {changed && <span className="w-1.5 h-1.5 rounded-full bg-accent" title="unsaved change" />}
                        </span>
                        {(sig.description || sig.label) && (
                          <span className="block text-[9px] text-muted-foreground">{sig.description ?? sig.label}</span>
                        )}
                        {err && <span className="block text-[9px] text-loss">{err}</span>}
                      </span>
                    </button>
                  </td>
                  <td className="py-1.5 pr-2 align-top text-muted-foreground capitalize">{sig.category ?? '—'}</td>
                  <td className="py-1.5 pr-2 align-top">
                    <select
                      value={role}
                      onChange={e => setRoleDraft(d => ({ ...d, [sig.id]: e.target.value }))}
                      className={`bg-muted/40 border rounded px-1.5 py-1 text-[10px] font-semibold capitalize focus:outline-none focus:border-accent/60 ${
                        err ? 'border-loss' : 'border-border'
                      } ${ROLE_COLORS[role] ?? 'text-foreground'}`}>
                      {options.map(r => (
                        <option key={r} value={r} className="capitalize">{r}</option>
                      ))}
                    </select>
                  </td>
                  <td className="py-1.5 align-top text-right font-mono whitespace-nowrap">
                    {fireRate === null && lift === null ? (
                      <span className="text-muted-foreground/60">—</span>
                    ) : (
                      <>
                        <span className="text-muted-foreground" title="Fire rate (fired / evaluated)">
                          {fireRate === null ? '—'
                            : fireLowSample ? <LowSampleNA count={fireCount} />
                            : `${(fireRate <= 1 ? fireRate * 100 : fireRate).toFixed(0)}% fire`}
                        </span>
                        {' · '}
                        <span
                          className={lift === null || liftLowSample ? 'text-muted-foreground/60' : lift >= 0 ? 'text-gain' : 'text-loss'}
                          title="Win-rate lift when this signal fired (attribution)">
                          {lift === null ? '—'
                            : liftLowSample ? <LowSampleNA count={liftCount} />
                            : `${lift >= 0 ? '+' : ''}${lift.toFixed(2)} lift`}
                        </span>
                      </>
                    )}
                  </td>
                </tr>,
                isOpen && thresholds ? (
                  <tr key={`${sig.id}-thresholds`} className="bg-muted/10">
                    <td colSpan={4} className="px-4 py-2">
                      <div className="flex flex-wrap gap-3">
                        {Object.entries(thresholds).map(([key, baseVal]) => {
                          const staged = thresholdDraft[sig.id]?.[key];
                          const val = staged !== undefined ? staged : baseVal;
                          const isNum = typeof baseVal === 'number' || (typeof val === 'number');
                          return (
                            <label key={key} className="flex items-center gap-1.5">
                              <span className="text-[9px] text-muted-foreground font-mono">{key}</span>
                              {typeof baseVal === 'boolean' ? (
                                <input type="checkbox" checked={Boolean(val)}
                                  onChange={e => setThresholdDraft(d => ({
                                    ...d, [sig.id]: { ...(d[sig.id] ?? {}), [key]: e.target.checked },
                                  }))} />
                              ) : isNum ? (
                                <input type="number" step="any"
                                  value={val === undefined || val === null ? '' : String(val)}
                                  onChange={e => setThresholdDraft(d => ({
                                    ...d, [sig.id]: { ...(d[sig.id] ?? {}), [key]: e.target.value === '' ? '' : parseFloat(e.target.value) },
                                  }))}
                                  className="w-20 bg-muted/40 border border-border rounded px-1.5 py-0.5 text-[10px] font-mono focus:outline-none focus:border-accent/60" />
                              ) : (
                                <span className="text-[9px] font-mono text-muted-foreground/70">{fmtValue(baseVal)}</span>
                              )}
                            </label>
                          );
                        })}
                      </div>
                    </td>
                  </tr>
                ) : null,
              ];
            })}
          </tbody>
        </table>
      </div>

      <div className="flex items-center gap-2">
        <Button size="sm" className="flex-1 font-bold" disabled={!isDirty || saving} onClick={handleSaveClick}>
          {saving ? 'Saving…' : isDirty ? 'Save signal changes' : 'No changes'}
        </Button>
        {isDirty && (
          <Button size="sm" variant="outline"
            onClick={() => { setRoleDraft({}); setThresholdDraft({}); setRowErrors({}); }}>
            Discard
          </Button>
        )}
      </div>

      <LiveConfirmModal
        open={liveModalOpen}
        description="The bot is RUNNING in LIVE mode. Changing signal roles or thresholds affects real-money entries immediately."
        onConfirm={word => { setLiveModalOpen(false); doSave(word); }}
        onCancel={() => setLiveModalOpen(false)}
      />
    </div>
  );
}

export default SignalsEditorPanel;
