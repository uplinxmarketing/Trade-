import { useState, useEffect, useCallback, useRef } from 'react';
import { Target, Zap, AlertTriangle } from 'lucide-react';
import { scoreColor } from '@/hooks/useEv';

// ── Entry Gate (live) ─────────────────────────────────────────────────────────
// The operator's model: WolfScore ≥ buy_score_threshold is THE buy gate, highest
// first. This panel makes that visible and dynamic — the effective gates actually
// in force, which coins clear the gate, exactly where a coin dies, and how fresh
// the data it acted on is. Data: GET /api/diagnostics/entry-report (read-only).

interface EffectiveGates {
  trading_active?: boolean;
  buy_score_threshold?: number | null;
  ev_floor_GATING_LIVE?: boolean;
  ev_model_trained?: boolean;
  live_up_regime_mode?: string;
  confirm_seconds?: number | null;
  eval_heartbeat_sec?: number | null;
  instant_fire_when_slots_free?: boolean;
  max_positions?: number | null;
  open_positions?: number | null;
  current_regime?: { regime?: string; tilt?: number };
}
interface CoinRow {
  symbol: string;
  wolfscore?: number | null;
  eligible_ge_65?: boolean;
  regime?: string | null;
  blocked_at?: string | null;
  signal_age_sec?: number | null;
  attempt_age_sec?: number | null;
}
interface EntryReport {
  effective_gates?: EffectiveGates;
  buy_ready_count?: number;
  silent_gap_count?: number;
  stale_signal_count_gt10s?: number;
  blockers_summary?: Record<string, number>;
  per_coin?: CoinRow[];
  error?: string;
}

const fmtNum = (v: unknown, d = 0): string =>
  typeof v === 'number' && isFinite(v) ? v.toFixed(d) : '—';

export function EntryGatePanel({ baseUrl = '' }: { baseUrl?: string }) {
  const [rep, setRep] = useState<EntryReport | null>(null);
  const [unavailable, setUnavailable] = useState(false);
  const disposed = useRef(false);

  // Editable buy gate — read/written via the dedicated fast endpoint so the
  // control works even when the (heavier) entry-report is slow to respond.
  const [gateSaved, setGateSaved] = useState<number | null>(null);
  const [gateInput, setGateInput] = useState<string>('');
  const [saving, setSaving] = useState(false);
  const [savedFlash, setSavedFlash] = useState(false);
  const editing = useRef(false);

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${baseUrl}/api/diagnostics/entry-report`, { cache: 'no-store' });
      if (res.status === 404) { if (!disposed.current) setUnavailable(true); return; }
      const data: EntryReport | null = res.ok ? await res.json().catch(() => null) : null;
      if (disposed.current) return;
      if (!data || data.error) { setUnavailable(true); return; }
      setRep(data); setUnavailable(false);
    } catch { if (!disposed.current) setUnavailable(true); }
  }, [baseUrl]);

  const loadGate = useCallback(async () => {
    try {
      const res = await fetch(`${baseUrl}/api/entries/buy-threshold`, { cache: 'no-store' });
      const data = res.ok ? await res.json().catch(() => null) : null;
      if (disposed.current || !data || typeof data.value !== 'number') return;
      setGateSaved(data.value);
      if (!editing.current) setGateInput(String(data.value));
    } catch { /* leave prior value */ }
  }, [baseUrl]);

  const saveGate = useCallback(async () => {
    const v = parseFloat(gateInput);
    if (!isFinite(v) || v < 0 || v > 100) return;
    setSaving(true);
    try {
      const res = await fetch(`${baseUrl}/api/entries/buy-threshold`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value: v }),
      });
      const data = res.ok ? await res.json().catch(() => null) : null;
      if (data?.ok && typeof data.value === 'number') {
        setGateSaved(data.value); setGateInput(String(data.value));
        setSavedFlash(true); setTimeout(() => { if (!disposed.current) setSavedFlash(false); }, 1500);
      }
    } catch { /* ignore */ } finally {
      editing.current = false;
      if (!disposed.current) setSaving(false);
    }
  }, [baseUrl, gateInput]);

  useEffect(() => {
    disposed.current = false;
    load(); loadGate();
    const id = setInterval(load, 6000);
    const gid = setInterval(loadGate, 8000);
    return () => { disposed.current = true; clearInterval(id); clearInterval(gid); };
  }, [load, loadGate]);

  const g = rep?.effective_gates ?? {};
  const gate = gateSaved ?? g.buy_score_threshold ?? 61;
  const eligible = (rep?.per_coin ?? []).filter(c => c.eligible_ge_65);
  const regime = g.current_regime?.regime ?? '—';

  // Reusable inline editor for the buy gate — always available (uses the fast
  // dedicated endpoint), so the operator can retune the gate even if the live
  // report below is slow or momentarily unavailable.
  const gateEditor = (
    <div className="flex items-center gap-1.5">
      <span className="text-[8px] uppercase tracking-wider text-muted-foreground">Gate</span>
      <input
        type="number" min={0} max={100} step={1} inputMode="decimal"
        className="w-12 px-1 py-0.5 text-[11px] font-mono font-bold text-center rounded bg-muted/30 border border-border/60 focus:border-accent outline-none"
        value={gateInput}
        onFocus={() => { editing.current = true; }}
        onChange={e => { editing.current = true; setGateInput(e.target.value); }}
        onKeyDown={e => { if (e.key === 'Enter') saveGate(); }}
      />
      <button
        onClick={saveGate}
        disabled={saving || gateInput === '' || parseFloat(gateInput) === gateSaved}
        className="text-[9px] font-semibold px-1.5 py-0.5 rounded bg-accent/20 text-accent hover:bg-accent/30 disabled:opacity-40 disabled:cursor-not-allowed"
      >{saving ? '…' : savedFlash ? '✓' : 'Set'}</button>
    </div>
  );

  if (unavailable || !rep) {
    return (
      <div className="bg-accent/5 border border-accent/30 rounded-md p-3 space-y-2">
        <div className="flex items-center justify-between gap-2">
          <p className="text-xs font-bold">Entry Gate · WolfScore ≥ {fmtNum(gate)}</p>
          {gateEditor}
        </div>
        <p className="text-[9px] text-muted-foreground/70 italic">
          {unavailable ? 'Live gate report unavailable — the gate value above is still editable.'
                       : 'Loading live entry gate…'}
        </p>
      </div>
    );
  }

  const chip = (label: string, val: string, tone: 'ok' | 'warn' | 'muted' = 'muted') => (
    <div className={`px-2 py-1 rounded text-center ${
      tone === 'ok' ? 'bg-gain/10' : tone === 'warn' ? 'bg-amber-500/10' : 'bg-muted/20'}`}>
      <p className="text-[7px] uppercase tracking-wider text-muted-foreground">{label}</p>
      <p className={`text-[11px] font-mono font-bold ${
        tone === 'ok' ? 'text-gain' : tone === 'warn' ? 'text-amber-400' : 'text-foreground'}`}>{val}</p>
    </div>
  );

  return (
    <div className="bg-accent/5 border border-accent/30 rounded-md p-3 space-y-2.5">
      <div className="flex items-center gap-2">
        <div className="p-1.5 rounded bg-accent/15"><Target className="w-3.5 h-3.5 text-accent" /></div>
        <div className="min-w-0 flex-1">
          <p className="text-xs font-bold">Entry Gate · WolfScore ≥ {fmtNum(gate)}</p>
          <p className="text-[10px] text-muted-foreground">
            Single gate — highest score buys first.
            {!g.ev_model_trained && <span className="text-amber-400/90"> Interim (untrained) weights.</span>}
          </p>
        </div>
        {g.trading_active === false && (
          <span className="text-[8px] font-bold px-1.5 py-0.5 rounded-full bg-loss/20 text-loss">PAUSED</span>
        )}
        {gateEditor}
      </div>

      {/* Effective gates */}
      <div className="grid grid-cols-4 gap-1">
        {chip('Gate', `≥${fmtNum(gate)}`, 'ok')}
        {chip('Eligible', String(eligible.length), eligible.length > 0 ? 'ok' : 'muted')}
        {chip('Slots', `${fmtNum(g.open_positions)}/${fmtNum(g.max_positions)}`)}
        {chip('Regime', regime)}
        {chip('Confirm', `${fmtNum(g.confirm_seconds, 0)}s`)}
        {chip('Heartbeat', `${fmtNum(g.eval_heartbeat_sec, 0)}s`)}
        {chip('Instant', g.instant_fire_when_slots_free ? 'on' : 'off',
              g.instant_fire_when_slots_free ? 'ok' : 'muted')}
        {chip('Floor gating', g.ev_floor_GATING_LIVE ? 'live' : 'off',
              g.ev_floor_GATING_LIVE ? 'ok' : 'warn')}
      </div>

      {/* Freshness / silent-gap flags */}
      {(rep.silent_gap_count || rep.stale_signal_count_gt10s) ? (
        <div className="flex items-center gap-1.5 text-[9px] text-amber-400">
          <AlertTriangle className="w-3 h-3 shrink-0" />
          {rep.silent_gap_count ? <span>{rep.silent_gap_count} silent-drop</span> : null}
          {rep.stale_signal_count_gt10s ? <span>· {rep.stale_signal_count_gt10s} stale (&gt;10s)</span> : null}
        </div>
      ) : null}

      {/* Top eligible / blocked coins */}
      {(rep.per_coin?.length ?? 0) > 0 ? (
        <div className="overflow-x-auto">
          <table className="w-full text-[10px]">
            <thead>
              <tr className="text-left text-muted-foreground">
                <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Coin</th>
                <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Score</th>
                <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Age</th>
                <th className="py-1 font-medium uppercase tracking-wider text-[8px]">Status</th>
              </tr>
            </thead>
            <tbody>
              {(rep.per_coin ?? []).slice(0, 12).map(c => {
                const fired = c.blocked_at == null && c.attempt_age_sec != null;
                return (
                  <tr key={c.symbol} className="border-t border-border/40">
                    <td className="py-1 pr-2 font-mono">{c.symbol.replace('USDT', '')}</td>
                    <td className="py-1 pr-2 font-mono font-semibold"
                        style={{ color: scoreColor(c.wolfscore ?? undefined) }}>
                      {c.wolfscore != null ? c.wolfscore.toFixed(0) : '—'}
                    </td>
                    <td className="py-1 pr-2 font-mono text-muted-foreground">
                      {c.signal_age_sec != null ? `${c.signal_age_sec.toFixed(0)}s` : '—'}
                    </td>
                    <td className="py-1">
                      {fired ? (
                        <span className="text-gain font-semibold inline-flex items-center gap-0.5">
                          <Zap className="w-2.5 h-2.5" /> order
                        </span>
                      ) : c.eligible_ge_65 ? (
                        <span className="text-amber-400">{c.blocked_at ?? 'evaluating'}</span>
                      ) : (
                        <span className="text-muted-foreground/70">below gate</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="text-[9px] text-muted-foreground/70 italic">
          No coins clearing WolfScore {fmtNum(gate)} right now — the gate is selective on interim weights.
          Lower it in Settings (buy_score_threshold) for more flow.
        </p>
      )}
    </div>
  );
}

export default EntryGatePanel;
