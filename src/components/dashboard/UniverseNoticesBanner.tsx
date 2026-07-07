import { useMemo, useState } from 'react';
import { AlertTriangle, X, PlusCircle, PauseCircle, RotateCcw } from 'lucide-react';
import {
  useUniverseNotices, noticeKey,
  type UniverseNotice,
} from '@/hooks/useUniverseNotices';

// I4 — auto-remove notices UI. Unobtrusive but visible: a dismissible banner
// for 'removed' (delisted) notices with a one-click "Add successor", plus
// quieter info lines for 'suspended'/'restored'. The operator asked to know
// when their universe changed under them.

interface Props {
  /** current watchlist (upper-cased comparison done internally) */
  selectedCoins: string[];
  /** add the successor pair to the watchlist (triggers the /api/coins sync) */
  onAddSuccessor: (successor: string) => void | Promise<void>;
}

export default function UniverseNoticesBanner({ selectedCoins, onAddSuccessor }: Props) {
  const { notices, dismissed, dismiss, dismissAll } = useUniverseNotices();
  const [adding, setAdding] = useState<string | null>(null);

  const held = useMemo(
    () => new Set(selectedCoins.map(c => c.toUpperCase())),
    [selectedCoins],
  );

  const visible = notices.filter(n => !dismissed.has(noticeKey(n)));
  const removed = visible.filter(n => n.action === 'removed');
  const quiet = visible.filter(n => n.action === 'suspended' || n.action === 'restored');

  if (removed.length === 0 && quiet.length === 0) return null;

  const addSuccessor = async (n: UniverseNotice) => {
    if (!n.successor) return;
    setAdding(n.successor);
    try {
      await onAddSuccessor(n.successor);
      dismiss(noticeKey(n));
    } finally {
      setAdding(null);
    }
  };

  return (
    <div className="px-3 py-2 space-y-1.5">
      {/* Removed (delisted) — the loud one */}
      {removed.length > 0 && (
        <div className="rounded border border-loss/40 bg-loss/10 text-loss px-3 py-2">
          <div className="flex items-start gap-2">
            <AlertTriangle className="w-3.5 h-3.5 mt-0.5 shrink-0" />
            <div className="flex-1 min-w-0">
              <p className="text-[11px] font-semibold leading-tight">
                Auto-removed {removed.length} delisted coin{removed.length !== 1 ? 's' : ''}
              </p>
              <div className="mt-1 flex flex-wrap gap-1.5">
                {removed.map(n => {
                  const base = n.symbol.replace('USDT', '');
                  const succBase = n.successor?.replace('USDT', '');
                  const alreadyHeld = n.successor ? held.has(n.successor.toUpperCase()) : false;
                  return (
                    <span
                      key={noticeKey(n)}
                      className="inline-flex items-center gap-1 rounded bg-loss/15 px-1.5 py-0.5 text-[10px]"
                      title={n.reason ?? 'Delisted — removed from the trading universe'}
                    >
                      <span className="font-mono font-semibold">{base}</span>
                      {succBase && <span className="opacity-70">→ {succBase}</span>}
                      {n.successor && !alreadyHeld && (
                        <button
                          type="button"
                          onClick={() => addSuccessor(n)}
                          disabled={adding === n.successor}
                          className="inline-flex items-center gap-0.5 rounded border border-loss/40 px-1 text-[9px] font-semibold hover:bg-loss/20 disabled:opacity-50"
                          title={`Add ${succBase} to your watchlist`}
                        >
                          <PlusCircle className="w-2.5 h-2.5" />
                          {adding === n.successor ? 'adding…' : 'Add successor'}
                        </button>
                      )}
                      {n.successor && alreadyHeld && (
                        <span className="text-[9px] opacity-70">added ✓</span>
                      )}
                      <button
                        type="button"
                        onClick={() => dismiss(noticeKey(n))}
                        className="opacity-60 hover:opacity-100"
                        title="Dismiss"
                      >
                        <X className="w-2.5 h-2.5" />
                      </button>
                    </span>
                  );
                })}
              </div>
            </div>
            <button
              type="button"
              onClick={dismissAll}
              className="text-[9px] opacity-70 hover:opacity-100 shrink-0"
              title="Dismiss all"
            >
              dismiss all
            </button>
          </div>
        </div>
      )}

      {/* Suspended / restored — quiet info lines */}
      {quiet.map(n => {
        const base = n.symbol.replace('USDT', '');
        const isRestore = n.action === 'restored';
        return (
          <div
            key={noticeKey(n)}
            className="flex items-center gap-1.5 rounded border border-border/60 bg-muted/10 px-2 py-1 text-[10px] text-muted-foreground"
          >
            {isRestore
              ? <RotateCcw className="w-3 h-3 text-gain shrink-0" />
              : <PauseCircle className="w-3 h-3 text-warn shrink-0" />}
            <span className="flex-1 min-w-0 truncate">
              <span className="font-mono font-semibold text-foreground">{base}</span>{' '}
              {isRestore ? 'restored to the universe' : 'suspended (trading break) — auto-restores'}
              {n.reason ? ` · ${n.reason}` : ''}
            </span>
            <button
              type="button"
              onClick={() => dismiss(noticeKey(n))}
              className="opacity-60 hover:opacity-100 shrink-0"
              title="Dismiss"
            >
              <X className="w-2.5 h-2.5" />
            </button>
          </div>
        );
      })}
    </div>
  );
}
