import { useState, useEffect, useRef } from 'react';
import { X } from 'lucide-react';
import { Button } from '@/components/ui/button';

// ── Phase 5 §5.2.3 — pre-save preview drawer ──────────────────────────────────
// Before a strategy-config save is confirmed, POST the pending patch to
// /api/strategy/preview and show how the candidate config would have changed
// entry decisions over the recent snapshot window. Renders tolerantly — the
// endpoint may not be deployed yet, shapes may drift.

interface PreviewTotals {
  would_allow?: number;
  would_block?: number;
}

interface PreviewReasonRow {
  current?: number;
  candidate?: number;
}

interface PreviewResult {
  totals?: PreviewTotals;
  per_reason?: Record<string, PreviewReasonRow>;
  insufficient_data?: boolean;
  error?: string;
}

interface StrategyPreviewDrawerProps {
  open: boolean;
  baseUrl?: string;
  /** The pending config patch (nested block dict) to preview. */
  patch: Record<string, unknown> | null;
  confirmLabel?: string;
  onConfirm: () => void;
  onCancel: () => void;
}

const num = (v: unknown): number => (typeof v === 'number' && isFinite(v) ? v : 0);

export function StrategyPreviewDrawer({
  open, baseUrl = '', patch, confirmLabel = 'Confirm save', onConfirm, onCancel,
}: StrategyPreviewDrawerProps) {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<PreviewResult | null>(null);
  const [failed, setFailed] = useState(false);
  const disposedRef = useRef(false);

  useEffect(() => {
    disposedRef.current = false;
    if (!open || !patch) { setResult(null); setFailed(false); return; }
    setLoading(true);
    setResult(null);
    setFailed(false);
    fetch(`${baseUrl}/api/strategy/preview`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ config: patch }),
    })
      .then(async r => {
        const data = await r.json().catch(() => null);
        if (disposedRef.current) return;
        if (!r.ok || !data || typeof data !== 'object' || data.error) {
          setFailed(true);
        } else {
          setResult(data as PreviewResult);
        }
      })
      .catch(() => { if (!disposedRef.current) setFailed(true); })
      .finally(() => { if (!disposedRef.current) setLoading(false); });
    return () => { disposedRef.current = true; };
  }, [open, patch, baseUrl]);

  if (!open) return null;

  const totals = result?.totals ?? {};
  const allow = num(totals.would_allow);
  const block = num(totals.would_block);
  const perReason: Array<[string, PreviewReasonRow]> =
    result?.per_reason && typeof result.per_reason === 'object'
      ? Object.entries(result.per_reason).filter(([, v]) => v && typeof v === 'object')
      : [];

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/50" onClick={onCancel}>
      <div
        className="w-full max-w-md h-full bg-background border-l border-border shadow-xl flex flex-col"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-border">
          <h3 className="text-sm font-bold">Preview config change</h3>
          <button onClick={onCancel} className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground">
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {loading && (
            <p className="text-[11px] text-muted-foreground py-2">Simulating candidate config…</p>
          )}
          {!loading && failed && (
            <p className="text-[11px] text-muted-foreground/70 italic py-2">
              Preview unavailable — you can still confirm the save.
            </p>
          )}
          {!loading && !failed && result?.insufficient_data && (
            <p className="text-[11px] text-muted-foreground/70 italic py-2">
              Not enough snapshot data yet for a meaningful preview.
            </p>
          )}
          {!loading && !failed && result && !result.insufficient_data && (
            <>
              <p className="text-xs leading-relaxed">
                Over the recent snapshot window, this config would have allowed{' '}
                <span className="font-mono font-bold text-gain">+{allow}</span> and blocked{' '}
                <span className="font-mono font-bold text-loss">−{block}</span> entries.
              </p>

              {perReason.length > 0 && (
                <div className="overflow-x-auto">
                  <table className="w-full text-[10px]">
                    <thead>
                      <tr className="text-left text-muted-foreground">
                        <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px]">Reason</th>
                        <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px] text-right">Current</th>
                        <th className="py-1 pr-2 font-medium uppercase tracking-wider text-[8px] text-right">Candidate</th>
                        <th className="py-1 font-medium uppercase tracking-wider text-[8px] text-right">Δ</th>
                      </tr>
                    </thead>
                    <tbody>
                      {perReason.map(([reason, row]) => {
                        const cur = num(row.current);
                        const cand = num(row.candidate);
                        const delta = cand - cur;
                        return (
                          <tr key={reason} className="border-t border-border/50">
                            <td className="py-1 pr-2 font-mono">{reason}</td>
                            <td className="py-1 pr-2 font-mono text-right text-muted-foreground">{cur}</td>
                            <td className="py-1 pr-2 font-mono text-right">{cand}</td>
                            <td className={`py-1 font-mono text-right font-bold ${
                              delta > 0 ? 'text-gain' : delta < 0 ? 'text-loss' : 'text-muted-foreground'
                            }`}>
                              {delta > 0 ? `+${delta}` : delta}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="flex gap-2 p-4 border-t border-border">
          <Button variant="outline" size="sm" className="flex-1" onClick={onCancel}>
            Cancel
          </Button>
          <Button size="sm" className="flex-1 font-bold" onClick={onConfirm} disabled={loading}>
            {confirmLabel}
          </Button>
        </div>
      </div>
    </div>
  );
}

export default StrategyPreviewDrawer;
