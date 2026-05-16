import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';

interface SignalDef {
  id: string;
  category: string;
  description: string;
}

interface SignalEngineConfig {
  enabled: boolean;
  mandatory_signals: string[];
  scored_signals: string[];
  veto_signals: string[];
  min_scored: number;
}

interface SignalThresholds {
  rsi_buy_threshold: number;
  near_low_pct: number;
  reversal_volume_multiplier: number;
  spread_max_pct: number;
  allowed_trading_hours_utc: string;
  stoch_rsi_threshold: number;
}

type SignalRole = 'mandatory' | 'scored' | 'veto' | 'disabled';

const ROLE_LABELS: Record<SignalRole, string> = {
  mandatory: 'Mandatory',
  scored:    'Scored',
  veto:      'Veto',
  disabled:  'Disabled',
};

const ROLE_COLORS: Record<SignalRole, string> = {
  mandatory: 'bg-red-500/15 text-red-400 border-red-500/30',
  scored:    'bg-accent/15 text-accent border-accent/30',
  veto:      'bg-orange-500/15 text-orange-400 border-orange-500/30',
  disabled:  'bg-muted/30 text-muted-foreground border-border',
};

const ROLE_BADGE: Record<SignalRole, string> = {
  mandatory: 'bg-red-500/20 text-red-400',
  scored:    'bg-accent/20 text-accent',
  veto:      'bg-orange-500/20 text-orange-400',
  disabled:  'bg-muted/40 text-muted-foreground',
};

export function SignalEnginePanel({ baseUrl = '' }: { baseUrl?: string }) {
  const [registry,   setRegistry]   = useState<SignalDef[]>([]);
  const [config,     setConfig]     = useState<SignalEngineConfig | null>(null);
  const [thresholds, setThresholds] = useState<SignalThresholds | null>(null);
  const [dirty,      setDirty]      = useState(false);
  const [saving,     setSaving]     = useState(false);
  const [loading,    setLoading]    = useState(true);

  const load = useCallback(() => {
    setLoading(true);
    fetch(`${baseUrl}/api/signal-engine/config`, { cache: 'no-store' })
      .then(r => r.json())
      .then(d => {
        setRegistry(d.registry   ?? []);
        setConfig(d.config       ?? null);
        setThresholds(d.thresholds ?? null);
        setDirty(false);
      })
      .catch(() => toast.error('Failed to load signal engine config'))
      .finally(() => setLoading(false));
  }, [baseUrl]);

  useEffect(() => { load(); }, [load]);

  if (loading) {
    return <p className="text-[10px] text-muted-foreground py-2 px-4">Loading signal engine…</p>;
  }

  if (!config || !thresholds) {
    return <p className="text-[10px] text-muted-foreground py-2 px-4">Signal engine unavailable</p>;
  }

  const getRole = (id: string): SignalRole => {
    if (config.mandatory_signals.includes(id)) return 'mandatory';
    if (config.scored_signals.includes(id))    return 'scored';
    if (config.veto_signals.includes(id))      return 'veto';
    return 'disabled';
  };

  const setRole = (id: string, newRole: SignalRole) => {
    const next = { ...config };
    next.mandatory_signals = next.mandatory_signals.filter(s => s !== id);
    next.scored_signals    = next.scored_signals.filter(s => s !== id);
    next.veto_signals      = next.veto_signals.filter(s => s !== id);
    if (newRole === 'mandatory') next.mandatory_signals.push(id);
    if (newRole === 'scored')    next.scored_signals.push(id);
    if (newRole === 'veto')      next.veto_signals.push(id);
    setConfig(next);
    setDirty(true);
  };

  const toggleEnabled = () => {
    setConfig({ ...config, enabled: !config.enabled });
    setDirty(true);
  };

  const save = async () => {
    setSaving(true);
    try {
      const res = await fetch(`${baseUrl}/api/signal-engine/config`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ signal_engine: config, signal_thresholds: thresholds }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Save failed');
      setDirty(false);
      toast.success('Signal engine config saved');
    } catch (e: any) {
      toast.error(`Save failed: ${e.message}`);
    } finally {
      setSaving(false);
    }
  };

  const groupedByRole = (role: SignalRole) => registry.filter(s => getRole(s.id) === role);

  return (
    <div className="space-y-3">
      {/* Engine toggle + status */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Engine</span>
          <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded ${config.enabled ? 'bg-gain/20 text-gain' : 'bg-muted/40 text-muted-foreground'}`}>
            {config.enabled ? 'ACTIVE' : 'INACTIVE'}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {dirty && <span className="text-[9px] text-orange-400">unsaved</span>}
          <button
            onClick={toggleEnabled}
            className={`text-[9px] font-semibold px-2 py-0.5 rounded border transition-colors ${
              config.enabled
                ? 'border-gain/40 text-gain hover:bg-gain/10'
                : 'border-border text-muted-foreground hover:bg-muted/20'
            }`}
          >
            {config.enabled ? 'Disable' : 'Enable'}
          </button>
          <button
            onClick={save}
            disabled={!dirty || saving}
            className="text-[9px] font-semibold px-2 py-0.5 rounded border border-accent/40 text-accent hover:bg-accent/10 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            {saving ? 'Saving…' : 'Save'}
          </button>
          <button
            onClick={load}
            className="text-[9px] text-muted-foreground hover:text-foreground transition-colors"
          >
            ↺
          </button>
        </div>
      </div>

      {/* Signal lists */}
      {(['mandatory', 'scored', 'veto', 'disabled'] as SignalRole[]).map(role => {
        const sigs = groupedByRole(role);
        const headerLabel =
          role === 'mandatory' ? 'Mandatory — all must fire' :
          role === 'scored'    ? `Scored — need ${config.min_scored} of ${config.scored_signals.length}` :
          role === 'veto'      ? 'Veto — any firing blocks buy' :
                                 'Disabled';
        return (
          <div key={role}>
            <p className={`text-[9px] font-semibold uppercase tracking-wider mb-1 ${
              role === 'mandatory' ? 'text-red-400' :
              role === 'scored'    ? 'text-accent' :
              role === 'veto'      ? 'text-orange-400' :
                                     'text-muted-foreground'
            }`}>
              {headerLabel}
            </p>
            {sigs.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/60 italic pl-1">none</p>
            ) : (
              <div className="space-y-1">
                {sigs.map(s => (
                  <div key={s.id} className={`flex items-center justify-between gap-2 border rounded px-2 py-1.5 ${ROLE_COLORS[role]}`}>
                    <div className="min-w-0 flex-1">
                      <p className="text-[9px] font-mono font-semibold truncate">{s.id}</p>
                      <p className="text-[8px] text-muted-foreground leading-tight truncate">{s.description}</p>
                    </div>
                    <select
                      value={role}
                      onChange={e => setRole(s.id, e.target.value as SignalRole)}
                      className="shrink-0 text-[8px] bg-background border border-border rounded px-1 py-0.5 text-foreground outline-none cursor-pointer"
                    >
                      {(Object.keys(ROLE_LABELS) as SignalRole[]).map(r => (
                        <option key={r} value={r}>{ROLE_LABELS[r]}</option>
                      ))}
                    </select>
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })}

      {/* min_scored slider */}
      <div>
        <div className="flex items-center justify-between mb-1">
          <p className="text-[9px] text-muted-foreground">Min scored to fire</p>
          <span className="text-[9px] font-mono font-semibold text-accent">{config.min_scored} / {config.scored_signals.length}</span>
        </div>
        <input
          type="range"
          min={0}
          max={config.scored_signals.length}
          value={config.min_scored}
          onChange={e => { setConfig({ ...config, min_scored: parseInt(e.target.value) }); setDirty(true); }}
          className="w-full accent-accent"
        />
      </div>

      {/* Thresholds */}
      <div className="border-t border-border/50 pt-2 space-y-2">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">Thresholds</p>

        {[
          { label: 'RSI buy threshold', key: 'rsi_buy_threshold',          min: 10,   max: 90,  step: 1,    fmt: (v: number) => v.toFixed(0) },
          { label: 'Near 24h low %',    key: 'near_low_pct',               min: 0.5,  max: 10,  step: 0.1,  fmt: (v: number) => v.toFixed(1) + '%' },
          { label: 'Spread max %',      key: 'spread_max_pct',             min: 0.01, max: 1,   step: 0.01, fmt: (v: number) => v.toFixed(2) + '%' },
          { label: 'Stoch RSI level',   key: 'stoch_rsi_threshold',        min: 5,    max: 50,  step: 1,    fmt: (v: number) => v.toFixed(0) },
          { label: 'Reversal vol ×',    key: 'reversal_volume_multiplier', min: 1.0,  max: 2.0, step: 0.05, fmt: (v: number) => v.toFixed(2) + '×' },
        ].map(({ label, key, min, max, step, fmt }) => (
          <div key={key}>
            <div className="flex items-center justify-between mb-0.5">
              <p className="text-[9px] text-muted-foreground">{label}</p>
              <span className="text-[9px] font-mono text-foreground">
                {fmt((thresholds as any)[key] ?? 0)}
              </span>
            </div>
            <input
              type="range" min={min} max={max} step={step}
              value={(thresholds as any)[key] ?? min}
              onChange={e => {
                setThresholds({ ...thresholds, [key]: parseFloat(e.target.value) });
                setDirty(true);
              }}
              className="w-full accent-accent"
            />
          </div>
        ))}

        <div>
          <p className="text-[9px] text-muted-foreground mb-0.5">Allowed hours UTC (comma-separated)</p>
          <input
            type="text"
            value={thresholds.allowed_trading_hours_utc}
            onChange={e => { setThresholds({ ...thresholds, allowed_trading_hours_utc: e.target.value }); setDirty(true); }}
            placeholder="13,14,15,16,17,18,19,20,21,22"
            className="w-full text-[9px] bg-background border border-border rounded px-2 py-1 font-mono outline-none focus:border-accent"
          />
        </div>
      </div>
    </div>
  );
}
