import { useState, useEffect, useCallback, useRef } from 'react';
import { ChevronDown, ChevronUp, RefreshCw, RotateCcw } from 'lucide-react';
import { toast } from 'sonner';
import { LiveConfirmModal } from './LiveConfirmModal';
import { isLiveRunning, shortHash, fmtValue } from './strategy-shared';

// ── Phase 5 §5.2.6 — config version history + rollback ────────────────────────
// GET /api/strategy/history?limit=20 → list of versions with per-path diffs.
// Each version expands to a colored old → new diff and offers one-click
// rollback (confirm dialog, plus the LIVE modal when mode==live && running).

interface HistoryVersion {
  version?: number | string;
  ts?: string | number;
  actor?: string;
  diff?: Record<string, [unknown, unknown] | unknown[]>;
  config_hash?: string;
}

const fmtTs = (ts: unknown): string => {
  if (ts === null || ts === undefined) return '—';
  if (typeof ts === 'number') {
    const ms = ts > 1e12 ? ts : ts * 1000;
    return new Date(ms).toLocaleString();
  }
  const d = new Date(String(ts));
  return isNaN(d.getTime()) ? String(ts) : d.toLocaleString();
};

export function ConfigHistoryPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const [versions, setVersions] = useState<HistoryVersion[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [loadFailed, setLoadFailed] = useState(false);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [rollingBack, setRollingBack] = useState<string | null>(null);
  const [liveModalOpen, setLiveModalOpen] = useState(false);
  const pendingVersionRef = useRef<number | string | null>(null);

  const disposedRef = useRef(false);
  useEffect(() => {
    disposedRef.current = false;
    return () => { disposedRef.current = true; };
  }, []);

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${baseUrl}/api/strategy/history?limit=20`, { cache: 'no-store' });
      const data = res.ok ? await res.json().catch(() => null) : null;
      if (disposedRef.current) return;
      if (!data || data.error || !Array.isArray(data.versions)) {
        setLoadFailed(true);
        setLoaded(true);
        return;
      }
      setVersions(data.versions.filter((v: any) => v && typeof v === 'object'));
      setLoadFailed(false);
      setLoaded(true);
    } catch {
      if (!disposedRef.current) { setLoadFailed(true); setLoaded(true); }
    }
  }, [baseUrl]);

  useEffect(() => { load(); }, [load]);

  const doRollback = useCallback(async (version: number | string, confirmWord?: string) => {
    setRollingBack(String(version));
    try {
      const body: Record<string, unknown> = { version };
      if (confirmWord) body.confirm = confirmWord;
      const res = await fetch(`${baseUrl}/api/strategy/rollback`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await res.json().catch(() => null);
      if (res.status === 409) {
        toast.error(data?.error ?? 'Live guard: confirmation required');
        return;
      }
      if (!res.ok || !data || data.error || data.ok === false) {
        throw new Error(data?.error ?? `HTTP ${res.status}`);
      }
      toast.success(`Rolled back to version ${version}`);
      await load();
    } catch (e: any) {
      toast.error(`Rollback failed: ${e.message}`);
    } finally {
      if (!disposedRef.current) setRollingBack(null);
    }
  }, [baseUrl, load]);

  const handleRollbackClick = useCallback(async (version: number | string | undefined) => {
    if (version === undefined || version === null) return;
    if (!window.confirm(
      `Roll the strategy config back to version ${version}?\n\n` +
      'All fields will revert to that version\'s values. A new history entry is recorded, so this is itself reversible.'
    )) return;
    pendingVersionRef.current = version;
    if (await isLiveRunning(baseUrl)) {
      setLiveModalOpen(true);
    } else {
      doRollback(version);
    }
  }, [baseUrl, doRollback]);

  if (loaded && loadFailed) {
    return (
      <p className="text-[9px] text-muted-foreground/70 italic py-1">
        Config history unavailable — endpoint not deployed yet
      </p>
    );
  }
  if (!loaded) {
    return <p className="text-[9px] text-muted-foreground py-1">Loading config history…</p>;
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-[9px] text-muted-foreground">Last {versions.length} config versions</p>
        <button onClick={load}
          className="text-[10px] text-muted-foreground hover:text-foreground flex items-center gap-1">
          <RefreshCw className="w-3 h-3" />Reload
        </button>
      </div>

      {versions.length === 0 ? (
        <p className="text-[10px] text-muted-foreground italic py-1">No config changes recorded yet</p>
      ) : (
        <div className="space-y-1">
          {versions.map((v, i) => {
            const key = String(v.version ?? i);
            const isOpen = expanded[key] ?? false;
            const diffEntries: Array<[string, unknown[]]> =
              v.diff && typeof v.diff === 'object'
                ? Object.entries(v.diff).filter(([, d]) => Array.isArray(d))
                : [];
            return (
              <div key={key} className="bg-muted/20 border border-border rounded-md">
                <div className="flex items-center gap-2 px-3 py-2">
                  <button
                    onClick={() => setExpanded(e => ({ ...e, [key]: !isOpen }))}
                    className="flex-1 flex items-center gap-2 text-left min-w-0 hover:text-accent transition-colors">
                    {isOpen ? <ChevronUp className="w-3 h-3 shrink-0 text-muted-foreground" />
                            : <ChevronDown className="w-3 h-3 shrink-0 text-muted-foreground" />}
                    <span className="text-[10px] font-mono font-bold shrink-0">v{String(v.version ?? '?')}</span>
                    <span className="text-[9px] text-muted-foreground truncate">{fmtTs(v.ts)}</span>
                    <span className="text-[9px] text-accent/80 truncate">{v.actor ?? 'unknown'}</span>
                    <span className="text-[8px] font-mono text-muted-foreground/60 shrink-0">{shortHash(v.config_hash)}</span>
                    {diffEntries.length > 0 && (
                      <span className="text-[8px] text-muted-foreground shrink-0">
                        {diffEntries.length} change{diffEntries.length === 1 ? '' : 's'}
                      </span>
                    )}
                  </button>
                  <button
                    onClick={() => handleRollbackClick(v.version)}
                    disabled={rollingBack !== null}
                    className="flex items-center gap-1 text-[9px] font-semibold px-2 py-1 rounded border border-border text-muted-foreground hover:border-accent/50 hover:text-accent transition-colors disabled:opacity-40 shrink-0"
                    title="Roll back to this version">
                    {rollingBack === key
                      ? <RefreshCw className="w-3 h-3 animate-spin" />
                      : <RotateCcw className="w-3 h-3" />}
                    Rollback
                  </button>
                </div>
                {isOpen && (
                  <div className="px-3 pb-2 border-t border-border/40 pt-1.5">
                    {diffEntries.length === 0 ? (
                      <p className="text-[9px] text-muted-foreground/70 italic">No diff recorded</p>
                    ) : (
                      <div className="space-y-0.5">
                        {diffEntries.map(([path, d]) => (
                          <div key={path} className="flex items-center gap-2 text-[10px] font-mono">
                            <span className="text-muted-foreground min-w-0 truncate">{path}</span>
                            <span className="text-loss line-through">{fmtValue(d[0])}</span>
                            <span className="text-muted-foreground/60">→</span>
                            <span className="text-gain">{fmtValue(d[1])}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      <LiveConfirmModal
        open={liveModalOpen}
        description="The bot is RUNNING in LIVE mode. Rolling back the strategy config changes real-money trading behaviour immediately."
        onConfirm={word => {
          setLiveModalOpen(false);
          if (pendingVersionRef.current !== null) doRollback(pendingVersionRef.current, word);
        }}
        onCancel={() => setLiveModalOpen(false)}
      />
    </div>
  );
}

export default ConfigHistoryPanel;
