import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { ChevronDown, ChevronUp, RefreshCw, Minus, Plus, RotateCcw } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { LiveConfirmModal } from './LiveConfirmModal';
import { StrategyPreviewDrawer } from './StrategyPreviewDrawer';
import { getAtPath, buildNestedPatch, isLiveRunning, fmtValue } from './strategy-shared';

// ── Phase 5 §5.2.1 — schema-driven strategy settings page ─────────────────────
// The renderer iterates GET /api/strategy/schema — it never hardcodes field
// lists, so new backend fields appear with ZERO frontend changes. Values come
// from GET /api/strategy; only changed fields are PUT back as a nested patch.

interface FieldDef {
  type?: 'number' | 'integer' | 'boolean' | 'select' | string;
  default?: unknown;
  min?: number;
  max?: number;
  step?: number;
  unit?: string;
  section?: string;
  label?: string;
  help?: string;
  read_only?: boolean;
  options?: Array<string | number | { value?: unknown; label?: string }>;
}

interface StrategySchema {
  sections?: Array<string | { id?: string; name?: string; key?: string; label?: string; title?: string }>;
  fields?: Record<string, FieldDef>;
  error?: string;
}

interface StrategyDoc {
  config?: Record<string, unknown>;
  schema_version?: number | string;
  last_modified?: string;
  config_hash?: string;
  error?: string;
}

const ACK_POLL_MS = 1_000;
const ACK_TIMEOUT_MS = 10_000;

// Toggle — same visual pattern as AITradingAgent
const Toggle = ({ on, onChange, color = 'bg-accent/80' }: { on: boolean; onChange: () => void; color?: string }) => (
  <button onClick={onChange}
    className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors shrink-0 ${on ? color : 'bg-muted/60'}`}>
    <span className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform ${on ? 'translate-x-4' : 'translate-x-0.5'}`} />
  </button>
);

// Backend field metadata (GET /api/strategy/schema) uses python-flavoured
// keys: the field map lives under `schema` (not `fields`), types are
// float/int/bool/enum/object and enum choices are under `choices`. Normalize
// once at load time into the renderer's vocabulary so every backend field
// actually renders (previously the shape mismatch rendered ZERO fields).
const TYPE_MAP: Record<string, FieldDef['type']> = {
  float: 'number', number: 'number',
  int: 'integer', integer: 'integer',
  bool: 'boolean', boolean: 'boolean',
  enum: 'select', select: 'select',
};

function normalizeSchema(raw: any): StrategySchema | null {
  if (!raw || typeof raw !== 'object' || raw.error) return null;
  const srcFields = (raw.fields && typeof raw.fields === 'object') ? raw.fields
    : (raw.schema && typeof raw.schema === 'object') ? raw.schema : null;
  if (!srcFields) return null;
  const fields: Record<string, FieldDef> = {};
  for (const [path, defRaw] of Object.entries(srcFields as Record<string, any>)) {
    if (!defRaw || typeof defRaw !== 'object') continue;
    const t = String(defRaw.type ?? 'number');
    const def: FieldDef = {
      ...defRaw,
      type: TYPE_MAP[t] ?? t,
      min: defRaw.min ?? undefined,
      max: defRaw.max ?? undefined,
      step: defRaw.step ?? undefined,
      unit: defRaw.unit ?? undefined,
      options: Array.isArray(defRaw.options) ? defRaw.options
        : Array.isArray(defRaw.choices) ? defRaw.choices : undefined,
    };
    // Complex objects (e.g. fees.per_symbol_overrides) have no form control —
    // show them read-only rather than as a broken number input.
    if (def.type === 'object') def.read_only = true;
    fields[path] = def;
  }
  return { sections: Array.isArray(raw.sections) ? raw.sections : [], fields };
}

function normalizeSection(s: string | { id?: string; name?: string; key?: string; label?: string; title?: string }): { id: string; label: string } {
  if (typeof s === 'string') return { id: s, label: s };
  const id = s.id ?? s.key ?? s.name ?? s.label ?? s.title ?? 'other';
  const label = s.label ?? s.title ?? s.name ?? id;
  return { id: String(id), label: String(label) };
}

function optionValue(o: string | number | { value?: unknown; label?: string }): unknown {
  return (o !== null && typeof o === 'object') ? (o.value ?? o.label) : o;
}
function optionLabel(o: string | number | { value?: unknown; label?: string }): string {
  if (o !== null && typeof o === 'object') return String(o.label ?? o.value ?? '');
  return String(o);
}

/** min/max validation mirrored client-side; returns error message or null. */
function validateField(def: FieldDef, value: unknown): string | null {
  if (def.type === 'number' || def.type === 'integer') {
    const n = Number(value);
    if (value === '' || value === null || value === undefined || !isFinite(n)) return 'required number';
    if (def.type === 'integer' && !Number.isInteger(n)) return 'must be an integer';
    if (typeof def.min === 'number' && n < def.min) return `min ${def.min}`;
    if (typeof def.max === 'number' && n > def.max) return `max ${def.max}`;
  }
  return null;
}

export function StrategySettingsPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const [schema, setSchema] = useState<StrategySchema | null>(null);
  const [doc, setDoc] = useState<StrategyDoc | null>(null);
  const [loadFailed, setLoadFailed] = useState(false);
  // draft: dotted path → edited value (only edited fields live here)
  const [draft, setDraft] = useState<Record<string, unknown>>({});
  // serverErrors: dotted path → message from a 422 response
  const [serverErrors, setServerErrors] = useState<Record<string, string>>({});
  const [openSections, setOpenSections] = useState<Record<string, boolean>>({});
  const [saving, setSaving] = useState(false);
  const [ackWaiting, setAckWaiting] = useState(false);
  // Save-flow staging
  const [previewOpen, setPreviewOpen] = useState(false);
  const [liveModalOpen, setLiveModalOpen] = useState(false);
  const [pendingPatch, setPendingPatch] = useState<Record<string, unknown> | null>(null);

  const disposedRef = useRef(false);
  useEffect(() => {
    disposedRef.current = false;
    return () => { disposedRef.current = true; };
  }, []);

  const load = useCallback(async () => {
    try {
      const [schemaRes, cfgRes] = await Promise.all([
        fetch(`${baseUrl}/api/strategy/schema`, { cache: 'no-store' }),
        fetch(`${baseUrl}/api/strategy`, { cache: 'no-store' }),
      ]);
      const schemaData = schemaRes.ok ? await schemaRes.json().catch(() => null) : null;
      const cfgData = cfgRes.ok ? await cfgRes.json().catch(() => null) : null;
      if (disposedRef.current) return;
      const normalized = normalizeSchema(schemaData);
      if (!normalized || !cfgData || cfgData.error) {
        setLoadFailed(true);
        return;
      }
      setSchema(normalized);
      setDoc(cfgData as StrategyDoc);
      setDraft({});
      setServerErrors({});
      setLoadFailed(false);
    } catch {
      if (!disposedRef.current) setLoadFailed(true);
    }
  }, [baseUrl]);

  useEffect(() => { load(); }, [load]);

  const fields: Record<string, FieldDef> =
    (schema?.fields && typeof schema.fields === 'object') ? schema.fields : {};
  const config = (doc?.config && typeof doc.config === 'object') ? doc.config : {};

  // Group fields by section, ordered by schema.sections (unknown sections appended)
  const sectionGroups = useMemo(() => {
    const declared = Array.isArray(schema?.sections) ? schema!.sections!.map(normalizeSection) : [];
    const order: Array<{ id: string; label: string }> = [...declared];
    const byId = new Map(order.map(s => [s.id, s]));
    const groups = new Map<string, string[]>();
    for (const [path, def] of Object.entries(fields)) {
      if (!def || typeof def !== 'object') continue;
      const sec = def.section ?? path.split('.')[0] ?? 'other';
      if (!byId.has(sec)) {
        const entry = { id: sec, label: sec };
        byId.set(sec, entry);
        order.push(entry);
      }
      if (!groups.has(sec)) groups.set(sec, []);
      groups.get(sec)!.push(path);
    }
    return order
      .filter(s => (groups.get(s.id)?.length ?? 0) > 0)
      .map(s => ({ ...s, paths: groups.get(s.id)! }));
  }, [schema, fields]);

  /** Effective value: draft override → current config → schema default. */
  const valueOf = useCallback((path: string): unknown => {
    if (path in draft) return draft[path];
    const cur = getAtPath(config, path);
    if (cur !== undefined) return cur;
    return fields[path]?.default;
  }, [draft, config, fields]);

  // Client-side validation across the whole draft
  const clientErrors = useMemo(() => {
    const errs: Record<string, string> = {};
    for (const path of Object.keys(draft)) {
      const def = fields[path];
      if (!def) continue;
      const msg = validateField(def, draft[path]);
      if (msg) errs[path] = msg;
    }
    return errs;
  }, [draft, fields]);

  // Dirty paths: draft entries that actually differ from the current config
  const dirtyPaths = useMemo(() =>
    Object.keys(draft).filter(p => {
      const cur = getAtPath(config, p);
      return JSON.stringify(draft[p]) !== JSON.stringify(cur);
    }), [draft, config]);

  const hasErrors = Object.keys(clientErrors).length > 0;
  const isDirty = dirtyPaths.length > 0;

  const setField = useCallback((path: string, value: unknown) => {
    setDraft(d => ({ ...d, [path]: value }));
    setServerErrors(e => {
      if (!(path in e)) return e;
      const next = { ...e };
      delete next[path];
      return next;
    });
  }, []);

  const resetSectionToDefaults = useCallback((paths: string[]) => {
    setDraft(d => {
      const next = { ...d };
      for (const p of paths) {
        const def = fields[p];
        if (!def || def.read_only) continue;
        if (def.default !== undefined) next[p] = def.default;
      }
      return next;
    });
  }, [fields]);

  // ── Ack polling: wait until the engine reports our config_hash applied ─────
  const pollAck = useCallback(async (expectedHash: string | undefined) => {
    if (!expectedHash) return;
    setAckWaiting(true);
    const started = Date.now();
    while (Date.now() - started < ACK_TIMEOUT_MS) {
      if (disposedRef.current) return;
      try {
        const res = await fetch(`${baseUrl}/api/strategy/ack`, { cache: 'no-store' });
        if (res.ok) {
          const data = await res.json().catch(() => null);
          if (data && data.config_hash === expectedHash) {
            if (!disposedRef.current) setAckWaiting(false);
            toast.success('Applied by engine ✓');
            return;
          }
        }
      } catch { /* keep polling */ }
      await new Promise(r => setTimeout(r, ACK_POLL_MS));
    }
    if (!disposedRef.current) setAckWaiting(false);
    toast.warning('Saved, but the engine has not acknowledged the new config yet');
  }, [baseUrl]);

  // ── PUT the patch (optionally with LIVE confirm word) ──────────────────────
  const putStrategy = useCallback(async (patch: Record<string, unknown>, confirmWord?: string) => {
    setSaving(true);
    try {
      const body: Record<string, unknown> = { config: patch };
      if (confirmWord) body.confirm = confirmWord;
      const res = await fetch(`${baseUrl}/api/strategy`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json().catch(() => null);
      if (res.status === 422 && data?.errors && typeof data.errors === 'object') {
        setServerErrors(data.errors as Record<string, string>);
        toast.error('Validation failed — fix the highlighted fields');
        return;
      }
      if (res.status === 409) {
        toast.error(data?.error ?? 'Live guard: confirmation required');
        return;
      }
      if (!res.ok || !data || data.error) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }
      const newHash: string | undefined = typeof data.config_hash === 'string' ? data.config_hash : undefined;
      toast.success('Strategy config saved');
      await load();
      pollAck(newHash);
    } catch (e: any) {
      toast.error(`Save failed: ${e.message}`);
    } finally {
      if (!disposedRef.current) setSaving(false);
    }
  }, [baseUrl, load, pollAck]);

  // ── Save flow: build patch → preview drawer → (LIVE modal) → PUT ───────────
  const handleSaveClick = useCallback(() => {
    if (!isDirty || hasErrors) return;
    const dotted: Record<string, unknown> = {};
    for (const p of dirtyPaths) {
      const def = fields[p];
      let v = draft[p];
      if (def && (def.type === 'number' || def.type === 'integer')) v = Number(v);
      dotted[p] = v;
    }
    setPendingPatch(buildNestedPatch(dotted));
    setPreviewOpen(true);
  }, [isDirty, hasErrors, dirtyPaths, draft, fields]);

  const handlePreviewConfirm = useCallback(async () => {
    setPreviewOpen(false);
    if (!pendingPatch) return;
    if (await isLiveRunning(baseUrl)) {
      setLiveModalOpen(true);
    } else {
      putStrategy(pendingPatch);
    }
  }, [pendingPatch, baseUrl, putStrategy]);

  // ── Field renderer — driven entirely by the schema field def ───────────────
  const renderField = (path: string, def: FieldDef) => {
    const value = valueOf(path);
    const err = clientErrors[path] ?? serverErrors[path];
    const label = def.label ?? path.split('.').pop();
    const changed = path in draft && dirtyPaths.includes(path);

    // read_only → muted display
    if (def.read_only) {
      return (
        <div key={path} className="flex items-center justify-between gap-2 py-1.5">
          <div className="min-w-0">
            <p className="text-[11px] text-muted-foreground">{label}</p>
            {def.help && <p className="text-[9px] text-muted-foreground/60">{def.help}</p>}
          </div>
          <span className="text-[11px] font-mono text-muted-foreground/70 shrink-0">
            {fmtValue(value)}{def.unit ? ` ${def.unit}` : ''}
          </span>
        </div>
      );
    }

    let control: React.ReactNode;
    if (def.type === 'boolean') {
      control = <Toggle on={Boolean(value)} onChange={() => setField(path, !value)} />;
    } else if (def.type === 'select' && Array.isArray(def.options)) {
      control = (
        <div className="flex flex-wrap gap-1">
          {def.options.map((o, i) => {
            const val = optionValue(o);
            const active = String(val) === String(value);
            return (
              <button key={i} onClick={() => setField(path, val)}
                className={`px-2 py-1 text-[10px] font-semibold rounded border transition-colors ${
                  active ? 'bg-accent text-accent-foreground border-accent'
                         : 'border-border text-muted-foreground hover:border-accent/50'
                }`}>
                {optionLabel(o)}
              </button>
            );
          })}
        </div>
      );
    } else if (def.type === 'integer') {
      const n = Number(value);
      const step = typeof def.step === 'number' && def.step > 0 ? def.step : 1;
      const bump = (dir: 1 | -1) => {
        let next = (isFinite(n) ? n : (typeof def.min === 'number' ? def.min : 0)) + dir * step;
        if (typeof def.min === 'number') next = Math.max(def.min, next);
        if (typeof def.max === 'number') next = Math.min(def.max, next);
        setField(path, Math.round(next));
      };
      control = (
        <div className="flex items-center gap-1">
          <button onClick={() => bump(-1)}
            className="p-1 rounded border border-border text-muted-foreground hover:border-accent/50 hover:text-accent transition-colors">
            <Minus className="w-3 h-3" />
          </button>
          <input type="number" value={value === undefined || value === null ? '' : String(value)}
            min={def.min} max={def.max} step={step}
            onChange={e => setField(path, e.target.value === '' ? '' : parseInt(e.target.value, 10))}
            className={`w-20 bg-muted/40 border rounded px-2 py-1 text-xs font-mono text-center focus:outline-none ${
              err ? 'border-loss focus:border-loss' : 'border-border focus:border-accent/60'
            }`} />
          <button onClick={() => bump(1)}
            className="p-1 rounded border border-border text-muted-foreground hover:border-accent/50 hover:text-accent transition-colors">
            <Plus className="w-3 h-3" />
          </button>
          {def.unit && <span className="text-[10px] text-muted-foreground ml-1">{def.unit}</span>}
        </div>
      );
    } else {
      // number (default)
      const hasRange = typeof def.min === 'number' && typeof def.max === 'number' && def.max > def.min;
      const unitLc = (def.unit ?? '').toLowerCase();
      const showSlider = hasRange && (def.max! - def.min!) <= 100 &&
        (unitLc.includes('%') || unitLc.includes('ratio') || unitLc.includes('pct'));
      const n = Number(value);
      control = (
        <div className="flex items-center gap-2 flex-1 justify-end min-w-0">
          {showSlider && (
            <input type="range" min={def.min} max={def.max}
              step={typeof def.step === 'number' ? def.step : 'any'}
              value={isFinite(n) ? n : def.min}
              onChange={e => setField(path, parseFloat(e.target.value))}
              className="flex-1 max-w-[140px] accent-[hsl(var(--accent))]" />
          )}
          <input type="number" value={value === undefined || value === null ? '' : String(value)}
            min={def.min} max={def.max}
            step={typeof def.step === 'number' ? def.step : 'any'}
            onChange={e => setField(path, e.target.value === '' ? '' : parseFloat(e.target.value))}
            className={`w-24 bg-muted/40 border rounded px-2 py-1 text-xs font-mono focus:outline-none ${
              err ? 'border-loss focus:border-loss' : 'border-border focus:border-accent/60'
            }`} />
          {def.unit && <span className="text-[10px] text-muted-foreground shrink-0">{def.unit}</span>}
        </div>
      );
    }

    return (
      <div key={path} className="py-1.5 space-y-0.5">
        <div className="flex items-center justify-between gap-3">
          <div className="min-w-0">
            <p className="text-[11px] flex items-center gap-1.5">
              {label}
              {changed && <span className="w-1.5 h-1.5 rounded-full bg-accent shrink-0" title="unsaved change" />}
            </p>
            {def.help && <p className="text-[9px] text-muted-foreground/60">{def.help}</p>}
          </div>
          {control}
        </div>
        {err && <p className="text-[9px] text-loss text-right">{err}</p>}
      </div>
    );
  };

  // ── Render ──────────────────────────────────────────────────────────────────
  if (!schema && loadFailed) {
    return (
      <p className="text-[9px] text-muted-foreground/70 italic py-1">
        Strategy settings unavailable — endpoint not deployed yet
      </p>
    );
  }
  if (!schema || !doc) {
    return <p className="text-[9px] text-muted-foreground py-1">Loading strategy schema…</p>;
  }

  return (
    <div className="space-y-3">
      {/* Meta row */}
      <div className="flex items-center justify-between">
        <p className="text-[9px] text-muted-foreground font-mono">
          schema v{String(doc.schema_version ?? '?')}
          {doc.last_modified ? ` · modified ${doc.last_modified}` : ''}
        </p>
        <button onClick={load}
          className="text-[10px] text-muted-foreground hover:text-foreground flex items-center gap-1">
          <RefreshCw className="w-3 h-3" />Reload
        </button>
      </div>

      {/* Sections (schema-driven) */}
      {sectionGroups.length === 0 && (
        <p className="text-[10px] text-muted-foreground italic">Schema contains no fields yet.</p>
      )}
      {sectionGroups.map(sec => {
        const open = openSections[sec.id] ?? false;
        const secDirty = sec.paths.some(p => dirtyPaths.includes(p));
        const secErr = sec.paths.some(p => clientErrors[p] || serverErrors[p]);
        return (
          <div key={sec.id} className="bg-muted/20 border border-border rounded-md">
            <button
              onClick={() => setOpenSections(s => ({ ...s, [sec.id]: !open }))}
              className="w-full flex items-center justify-between px-3 py-2 hover:bg-muted/20 transition-colors">
              <span className="text-xs font-semibold capitalize flex items-center gap-2">
                {sec.label}
                {secDirty && <span className="w-1.5 h-1.5 rounded-full bg-accent" />}
                {secErr && <span className="text-[8px] text-loss font-bold">invalid</span>}
              </span>
              {open ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" />
                    : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
            </button>
            {open && (
              <div className="px-3 pb-2 divide-y divide-border/40">
                {sec.paths.map(p => renderField(p, fields[p] ?? {}))}
                <div className="pt-2 flex justify-end">
                  <button onClick={() => resetSectionToDefaults(sec.paths)}
                    className="text-[9px] text-muted-foreground hover:text-foreground flex items-center gap-1">
                    <RotateCcw className="w-2.5 h-2.5" />Reset section to defaults
                  </button>
                </div>
              </div>
            )}
          </div>
        );
      })}

      {/* Save row */}
      <div className="flex items-center gap-2">
        <Button size="sm" className="flex-1 font-bold"
          disabled={!isDirty || hasErrors || saving}
          onClick={handleSaveClick}>
          {saving ? 'Saving…' : ackWaiting ? 'Waiting for engine…'
            : isDirty ? `Save ${dirtyPaths.length} change${dirtyPaths.length === 1 ? '' : 's'}` : 'No changes'}
        </Button>
        {isDirty && (
          <Button size="sm" variant="outline" onClick={() => { setDraft({}); setServerErrors({}); }}>
            Discard
          </Button>
        )}
      </div>
      {hasErrors && (
        <p className="text-[9px] text-loss">Fix invalid fields before saving.</p>
      )}

      {/* Preview drawer (§5.2.3) — always shown before confirming a save */}
      <StrategyPreviewDrawer
        open={previewOpen}
        baseUrl={baseUrl}
        patch={pendingPatch}
        onConfirm={handlePreviewConfirm}
        onCancel={() => { setPreviewOpen(false); setPendingPatch(null); }}
      />

      {/* LIVE confirm modal (§5.2.7) */}
      <LiveConfirmModal
        open={liveModalOpen}
        description="The bot is RUNNING in LIVE mode. Saving these strategy settings changes real-money trading behaviour immediately."
        onConfirm={word => { setLiveModalOpen(false); if (pendingPatch) putStrategy(pendingPatch, word); }}
        onCancel={() => setLiveModalOpen(false)}
      />
    </div>
  );
}

export default StrategySettingsPanel;
