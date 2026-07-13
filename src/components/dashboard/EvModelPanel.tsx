import { useEffect, useMemo, useState } from 'react';
import { toast } from 'sonner';
import { Play, Loader2, CheckCircle2, ShieldAlert } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { LiveConfirmModal } from '@/components/dashboard/LiveConfirmModal';
import {
  useEvModel, useEvTrainStatus, isUnavailable,
  type EvReport, type EvVersion,
} from '@/hooks/useEv';

// ── WolfBot S5 — model panel ───────────────────────────────────────────────
// Version list + "Run retrain" (POST /api/ev/train, disabled while running,
// shows the held-out report) + activate a version (POST /api/ev/activate with a
// LIVE confirm dialog, surfacing 409/422 responses). Counts row (live / paper /
// total / wins). Coded defensively for older backends.

function fmtNum(v: number | null | undefined): string {
  return v == null || !isFinite(v) ? '—' : v.toLocaleString();
}

function fmtMetric(v: number | null | undefined, digits = 3): string {
  return v == null || !isFinite(v) ? '—' : v.toFixed(digits);
}

function fmtVersion(v: string | number | undefined): string {
  if (v == null) return '—';
  return typeof v === 'number' ? `v${v}` : String(v);
}

function ReportView({ report }: { report: EvReport | null | undefined }) {
  if (!report) return null;
  const metrics: Array<{ label: string; value: string }> = [
    { label: 'AUC', value: fmtMetric(report.auc) },
    { label: 'Accuracy', value: fmtMetric(report.accuracy) },
    { label: 'Brier', value: fmtMetric(report.brier) },
    { label: 'Log-loss', value: fmtMetric(report.log_loss) },
    { label: 'Train n', value: fmtNum(report.n_train) },
    { label: 'Test n', value: fmtNum(report.n_test) },
  ].filter(m => m.value !== '—');

  if (metrics.length === 0 && !report.note) return null;

  return (
    <div className="bg-muted/20 rounded px-2 py-1.5 space-y-1">
      <p className="text-[8px] uppercase tracking-wider text-muted-foreground">
        Held-out report{report.version != null ? ` · ${fmtVersion(report.version)}` : ''}
      </p>
      {metrics.length > 0 && (
        <div className="grid grid-cols-3 gap-1">
          {metrics.map(m => (
            <div key={m.label}>
              <p className="text-[7px] text-muted-foreground">{m.label}</p>
              <p className="text-[10px] font-mono font-semibold text-foreground">{m.value}</p>
            </div>
          ))}
        </div>
      )}
      {report.note && <p className="text-[8px] text-muted-foreground break-words">{report.note}</p>}
    </div>
  );
}

export function EvModelPanel({ baseUrl = '' }: { baseUrl?: string }) {
  const { data, error, loading, notFound, refetch } = useEvModel(baseUrl);
  const unavailable = isUnavailable(data, notFound);

  const backendRunning = data?.status?.running === true;
  const [training, setTraining] = useState(false);
  const active = training || backendRunning;
  const { data: trainStatus, refetch: refetchTrain } = useEvTrainStatus(baseUrl, active);
  const running = active || trainStatus?.running === true;

  // Activation confirm modal state.
  const [confirmVersion, setConfirmVersion] = useState<string | number | null>(null);
  const [activating, setActivating] = useState(false);

  const versions = useMemo<EvVersion[]>(() => data?.versions ?? [], [data]);
  const counts = data?.counts ?? {};
  const report = trainStatus?.report ?? null;

  const activeVersion = useMemo(() => {
    const v = versions.find(x => x.active);
    return v?.version ?? data?.status?.version;
  }, [versions, data]);

  async function runRetrain() {
    setTraining(true);
    try {
      const r = await fetch(`${baseUrl}/api/ev/train`, { method: 'POST' });
      if (r.status === 409) {
        toast.info('Retrain already running');
      } else if (!r.ok) {
        throw new Error(`HTTP ${r.status}`);
      } else {
        toast.success('Retrain started');
      }
      refetchTrain();
    } catch (e: unknown) {
      toast.error(`Retrain failed — ${(e as Error).message ?? 'error'}`);
      setTraining(false);
    }
    // `training` clears once trainStatus reports running:false (below).
  }

  // Once the backend confirms the run finished, drop the optimistic flag.
  useEffect(() => {
    if (training && trainStatus && trainStatus.running === false) setTraining(false);
  }, [training, trainStatus]);

  async function doActivate(confirmWord: string) {
    const version = confirmVersion;
    setConfirmVersion(null);
    if (version == null) return;
    setActivating(true);
    try {
      const r = await fetch(`${baseUrl}/api/ev/activate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version, confirm: confirmWord }),
      });
      if (r.status === 422 || r.status === 400) {
        const body = await r.json().catch(() => ({}));
        toast.error(`Activation rejected — ${body?.error ?? body?.detail ?? 'confirmation required'}`);
      } else if (r.status === 409) {
        toast.info('Version already active or a change is in progress');
      } else if (!r.ok) {
        throw new Error(`HTTP ${r.status}`);
      } else {
        toast.success(`Activated ${fmtVersion(version)}`);
        refetch();
      }
    } catch (e: unknown) {
      toast.error(`Activation failed — ${(e as Error).message ?? 'error'}`);
    } finally {
      setActivating(false);
    }
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <p className="text-[9px] font-semibold uppercase tracking-wider text-muted-foreground">
          EV Model
        </p>
        <Button
          size="sm"
          variant="outline"
          className="h-6 px-2 text-[10px]"
          disabled={running || unavailable}
          onClick={runRetrain}
        >
          {running
            ? <><Loader2 className="w-3 h-3 mr-1 animate-spin" />Training…</>
            : <><Play className="w-3 h-3 mr-1" />Run retrain</>}
        </Button>
      </div>

      {/* Retrain status — always shows rows-loaded / status / error so a click is
          never silent. */}
      {(trainStatus?.status || trainStatus?.rows_loaded != null || trainStatus?.error) && (
        <div className={`rounded px-2 py-1 text-[9px] ${
          trainStatus?.status === 'failed'
            ? 'bg-loss/15 text-loss border border-loss/30'
            : trainStatus?.running || trainStatus?.status === 'running'
            ? 'bg-amber-500/15 text-amber-400 border border-amber-500/30'
            : 'bg-muted/25 text-muted-foreground'
        }`}>
          {trainStatus?.running || trainStatus?.status === 'running' ? (
            <span>Training… loading {trainStatus?.rows_loaded ?? '…'} rows</span>
          ) : trainStatus?.status === 'failed' ? (
            <span>Retrain failed{trainStatus?.error ? ` — ${trainStatus.error}` : ''}</span>
          ) : trainStatus?.status === 'done' ? (
            <span>
              Trained on {trainStatus?.rows_loaded ?? '?'} rows
              {trainStatus?.rows_usable != null ? ` (${trainStatus.rows_usable} with submetrics)` : ''}
              {trainStatus?.saved_version ? ` → ${fmtVersion(trainStatus.saved_version)}` : ''}. Review the held-out report, then Activate.
            </span>
          ) : null}
        </div>
      )}

      {loading && !data ? (
        <p className="text-[9px] text-muted-foreground py-1">Loading model…</p>
      ) : unavailable ? (
        <p className="text-[9px] text-muted-foreground/70 italic py-1">
          EV model not available{error ? ` — ${error}` : ' — endpoint not deployed yet'}
        </p>
      ) : (
        <>
          {/* Counts (live-only; paper-shadow retired under P5) */}
          <div className="grid grid-cols-3 gap-1">
            {([
              ['Live', counts.live],
              ['Total', counts.total],
              ['Wins', counts.wins],
            ] as Array<[string, number | undefined]>).map(([label, v]) => (
              <div key={label} className="bg-muted/20 rounded px-2 py-1">
                <p className="text-[7px] text-muted-foreground">{label}</p>
                <p className="text-[11px] font-mono font-bold text-foreground">{fmtNum(v)}</p>
              </div>
            ))}
          </div>

          {/* Live train report */}
          <ReportView report={report} />

          {/* S4.4 — what the active model values (per-signal learned weights) */}
          {Array.isArray(data?.status?.weights) && data!.status!.weights!.length > 0 && (
            <div className="bg-muted/20 rounded px-2 py-1.5 space-y-1">
              <p className="text-[8px] uppercase tracking-wider text-muted-foreground">
                What the model values (per-signal weights)
              </p>
              <div className="space-y-0.5">
                {data!.status!.weights!.slice(0, 8).map(w => {
                  const mag = Math.min(1, Math.abs(w.weight) / 0.5);
                  const pos = w.weight >= 0;
                  return (
                    <div key={w.feature} className="flex items-center gap-1.5">
                      <span className="text-[9px] font-mono text-muted-foreground w-28 truncate">{w.feature}</span>
                      <div className="flex-1 h-1.5 bg-muted/40 rounded overflow-hidden">
                        <div className={`h-full ${pos ? 'bg-gain' : 'bg-loss'}`}
                             style={{ width: `${mag * 100}%` }} />
                      </div>
                      <span className={`text-[9px] font-mono w-10 text-right ${pos ? 'text-gain' : 'text-loss'}`}>
                        {w.weight >= 0 ? '+' : ''}{w.weight.toFixed(2)}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Version list */}
          <div className="space-y-1">
            <p className="text-[8px] uppercase tracking-wider text-muted-foreground">Versions</p>
            {versions.length === 0 ? (
              <p className="text-[9px] text-muted-foreground/70 italic">No versions yet — run a retrain to create one.</p>
            ) : (
              <div className="space-y-1">
                {versions.map(v => {
                  const isActive = v.active || v.version === activeVersion;
                  return (
                    <div
                      key={String(v.version)}
                      className={`flex items-center gap-2 rounded border px-2 py-1 ${
                        isActive ? 'border-gain/40 bg-gain/5' : 'border-border/50 bg-muted/10'
                      }`}
                    >
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-1.5">
                          <span className="text-[11px] font-mono font-semibold text-foreground">{fmtVersion(v.version)}</span>
                          {isActive && (
                            <span className="inline-flex items-center gap-0.5 text-[8px] font-semibold text-gain">
                              <CheckCircle2 className="w-2.5 h-2.5" />active
                            </span>
                          )}
                          {v.trained === false && (
                            <span className="text-[8px] text-warn">untrained</span>
                          )}
                        </div>
                        <p className="text-[8px] text-muted-foreground truncate">
                          {v.n_trades != null ? `${fmtNum(v.n_trades)} trades` : ''}
                          {v.created != null ? ` · ${String(v.created)}` : ''}
                        </p>
                      </div>
                      {!isActive && (
                        <Button
                          size="sm"
                          variant="outline"
                          className="h-5 px-1.5 text-[9px] shrink-0"
                          disabled={activating}
                          onClick={() => setConfirmVersion(v.version)}
                        >
                          Activate
                        </Button>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {data?.status?.note && (
            <p className="text-[8px] text-muted-foreground flex items-start gap-1">
              <ShieldAlert className="w-2.5 h-2.5 mt-px shrink-0" />
              <span className="break-words">{data.status.note}</span>
            </p>
          )}
        </>
      )}

      <LiveConfirmModal
        open={confirmVersion != null}
        title="Activate EV model version"
        description={`Activate ${fmtVersion(confirmVersion ?? undefined)} for LIVE win-probability scoring. This changes which model gates real-money entries. Type LIVE to confirm.`}
        onConfirm={doActivate}
        onCancel={() => setConfirmVersion(null)}
      />
    </div>
  );
}

export default EvModelPanel;
