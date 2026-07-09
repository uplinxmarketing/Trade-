import { useState, useEffect, useCallback, useRef } from 'react';
import { Unlock, ArrowRight, ShieldCheck, CheckCircle2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';

// ── WolfBot v0.4 Part O1 — one-click "open the entry gate" ────────────────────
// A single obvious action that loosens the four entry filters at once (replacing
// four separate multi-field Settings edits). It shows exactly what it changes as
// a current→target table, requires a confirm, and is safe because exits/stops are
// unchanged. Data comes from GET /api/gate/preset; POST applies the preset.

interface PresetKey {
  key: string;
  label?: string;
  current?: unknown;
  target?: unknown;
}

interface MinimumViable {
  keys?: PresetKey[];
  already_applied?: boolean;
}

interface GatePresetResponse {
  available?: boolean;
  presets?: unknown[];
  minimum_viable?: MinimumViable;
  error?: string;
}

// Backends still deploying may answer 404 or {available:false}; render nothing /
// a muted note in that case rather than a broken table.
type LoadState = 'loading' | 'ok' | 'unavailable';

const fmt = (v: unknown): string => {
  if (v === null || v === undefined) return '—';
  if (typeof v === 'number') return String(v);
  if (typeof v === 'boolean') return v ? 'on' : 'off';
  return String(v);
};

// Parse the minimum_viable block defensively — the essential data is per-key
// current→target and already_applied; shape may vary slightly.
function parseKeys(mv: MinimumViable | undefined): PresetKey[] {
  const raw = Array.isArray(mv?.keys) ? mv!.keys! : [];
  return raw
    .filter((k): k is PresetKey => Boolean(k) && typeof k === 'object' && typeof (k as PresetKey).key === 'string')
    .map(k => ({
      key: k.key,
      label: typeof k.label === 'string' && k.label ? k.label : k.key,
      current: k.current,
      target: k.target,
    }));
}

export function EntryGatePanel({ baseUrl = '' }: { baseUrl?: string }) {
  const [state, setState] = useState<LoadState>('loading');
  const [keys, setKeys] = useState<PresetKey[]>([]);
  const [alreadyApplied, setAlreadyApplied] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [applying, setApplying] = useState(false);

  const disposedRef = useRef(false);

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${baseUrl}/api/gate/preset`, { cache: 'no-store' });
      if (res.status === 404) {
        if (!disposedRef.current) setState('unavailable');
        return;
      }
      const data: GatePresetResponse | null = res.ok ? await res.json().catch(() => null) : null;
      if (disposedRef.current) return;
      if (!data || data.error || data.available === false) {
        setState('unavailable');
        return;
      }
      const parsed = parseKeys(data.minimum_viable);
      setKeys(parsed);
      setAlreadyApplied(Boolean(data.minimum_viable?.already_applied));
      setState('ok');
    } catch {
      if (!disposedRef.current) setState('unavailable');
    }
  }, [baseUrl]);

  useEffect(() => {
    disposedRef.current = false;
    load();
    return () => { disposedRef.current = true; };
  }, [load]);

  const apply = useCallback(async () => {
    setApplying(true);
    try {
      const res = await fetch(`${baseUrl}/api/gate/preset?name=minimum_viable`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      const data = await res.json().catch(() => null);
      if (res.status === 422) {
        // 422 field-error shape — surface the offending field/message.
        if (data?.errors && typeof data.errors === 'object') {
          const first = Object.entries(data.errors as Record<string, unknown>)[0];
          toast.error(first ? `${first[0]}: ${String(first[1])}` : 'Preset rejected (422)');
        } else {
          const msg = (typeof data?.error === 'string' && data.error)
            || (typeof data?.detail === 'string' && data.detail)
            || (typeof data?.message === 'string' && data.message)
            || 'Preset rejected (422)';
          toast.error(msg);
        }
        return;
      }
      if (!res.ok || !data || data.error || data.ok === false) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }
      setConfirmOpen(false);
      const version = data.version !== undefined && data.version !== null ? ` (v${String(data.version)})` : '';
      toast.success(`Entry gate opened — bot will open more trades${version}`);
      await load();
    } catch (e: any) {
      toast.error(`Failed to open gate: ${e.message}`);
    } finally {
      if (!disposedRef.current) setApplying(false);
    }
  }, [baseUrl, load]);

  if (state === 'loading') {
    return <p className="text-[9px] text-muted-foreground py-1">Loading entry gate…</p>;
  }
  if (state === 'unavailable') {
    return (
      <p className="text-[9px] text-muted-foreground/70 italic py-1">
        gate presets unavailable
      </p>
    );
  }

  const changeTable = keys.length > 0 && (
    <div className="overflow-x-auto">
      <table className="w-full text-[10px]">
        <thead>
          <tr className="text-left text-muted-foreground">
            <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Filter</th>
            <th className="py-1 font-medium uppercase tracking-wider text-[8px]">Change</th>
          </tr>
        </thead>
        <tbody>
          {keys.map(k => (
            <tr key={k.key} className="border-t border-border/50">
              <td className="py-1 pr-2">{k.label}</td>
              <td className="py-1 font-mono">
                <span className="text-muted-foreground">{fmt(k.current)}</span>
                <ArrowRight className="inline-block w-2.5 h-2.5 mx-1 text-accent align-middle" />
                <span className="text-accent font-semibold">{fmt(k.target)}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );

  return (
    <div className="bg-accent/5 border border-accent/30 rounded-md p-3 space-y-2.5">
      <div className="flex items-center gap-2">
        <div className="p-1.5 rounded bg-accent/15">
          <Unlock className="w-3.5 h-3.5 text-accent" />
        </div>
        <div className="min-w-0">
          <p className="text-xs font-bold">Open entry gate (minimum-viable)</p>
          <p className="text-[10px] text-muted-foreground">
            One action loosens the entry filters so the bot actually opens trades.
          </p>
        </div>
      </div>

      {changeTable}

      <p className="text-[9px] text-muted-foreground/80 flex items-start gap-1">
        <ShieldCheck className="w-3 h-3 text-gain shrink-0 mt-[1px]" />
        Safe because stops/TP are unchanged — losers stay capped at ~1R.
      </p>

      {alreadyApplied ? (
        <div className="flex items-center gap-1.5 text-[10px] text-muted-foreground">
          <CheckCircle2 className="w-3.5 h-3.5 text-gain" />
          <span className="font-semibold text-gain">Gate opened</span>
          <span className="text-muted-foreground/70">· gate already loosened</span>
        </div>
      ) : (
        <Button
          size="sm"
          className="w-full font-bold"
          onClick={() => setConfirmOpen(true)}
          disabled={applying}
        >
          <Unlock className="w-3.5 h-3.5 mr-1.5" />
          Open entry gate — make it trade
        </Button>
      )}

      {/* Confirm dialog — mirrors the LiveConfirmModal overlay pattern used
          elsewhere in the dashboard, but a plain confirm (not a typed word). */}
      {confirmOpen && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
          onClick={() => !applying && setConfirmOpen(false)}
        >
          <div
            className="w-full max-w-sm bg-background border border-accent/40 rounded-lg shadow-xl p-4 space-y-3"
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-center gap-2">
              <div className="p-1.5 rounded bg-accent/20">
                <Unlock className="w-4 h-4 text-accent" />
              </div>
              <h3 className="text-sm font-bold">Open the entry gate?</h3>
            </div>
            <p className="text-[11px] text-muted-foreground leading-relaxed">
              This loosens entry filters so the bot opens more trades. Exits/stops are
              unchanged. Audited and reversible in Settings.
            </p>
            {changeTable}
            <p className="text-[9px] text-muted-foreground/80 flex items-start gap-1">
              <ShieldCheck className="w-3 h-3 text-gain shrink-0 mt-[1px]" />
              Safe because stops/TP are unchanged — losers stay capped at ~1R.
            </p>
            <div className="flex gap-2 pt-1">
              <Button
                variant="outline"
                size="sm"
                className="flex-1"
                onClick={() => setConfirmOpen(false)}
                disabled={applying}
              >
                Cancel
              </Button>
              <Button
                size="sm"
                className="flex-1 font-bold"
                onClick={apply}
                disabled={applying}
              >
                {applying ? 'Opening…' : 'Open entry gate'}
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default EntryGatePanel;
