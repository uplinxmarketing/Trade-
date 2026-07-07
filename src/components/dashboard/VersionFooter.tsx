import { RefreshCw, AlertTriangle } from 'lucide-react';
import { useBackendVersion } from '@/hooks/useBackendVersion';

// ── I2 — self-diagnosing version footer ─────────────────────────────────────
// Third recurrence of the stale-UI bug. A compact, always-visible bottom bar
// that prints `UI <app commit> / API <backend commit>` so every future
// mismatch is diagnosable at a glance. When the commits differ — or the server
// provably serves a different bundle than the one this page loaded — it turns
// amber and exposes a one-click cache-busting reload.

const short = (c: string | null | undefined) => (c ? c.slice(0, 7) : '—');

export default function VersionFooter() {
  const {
    appVersion, appCommit, backendVersion, backendCommit,
    commitMismatch, assetMismatch, servedAsset, currentAsset, reloadNow,
  } = useBackendVersion();

  const stale = commitMismatch || assetMismatch;

  // Definitive "browser is stale" phrasing when the served asset differs.
  const staleReason = assetMismatch
    ? `server serves ${servedAsset}, you loaded ${currentAsset} — reload`
    : commitMismatch
      ? `UI built from ${short(appCommit)} but backend is running ${short(backendCommit)} — reload to sync`
      : 'UI and backend commits match';

  return (
    <footer
      className={`flex items-center justify-between gap-3 px-3 py-1 border-t text-[10px] font-mono select-none ${
        stale
          ? 'border-amber-500/40 bg-amber-500/10 text-amber-400'
          : 'border-border bg-card/60 text-muted-foreground'
      }`}
      title={staleReason}
    >
      <div className="flex items-center gap-2 min-w-0 truncate">
        {stale && <AlertTriangle className="w-3 h-3 shrink-0" />}
        <span className="truncate">
          UI {short(appCommit)}
          <span className="opacity-50"> (v{appVersion})</span>
          {' / '}
          API {short(backendCommit)}
          {backendVersion && <span className="opacity-50"> (v{backendVersion})</span>}
        </span>
      </div>

      {stale && (
        <button
          onClick={reloadNow}
          className="flex items-center gap-1 px-2 py-0.5 rounded bg-amber-500/20 hover:bg-amber-500/30 border border-amber-500/40 text-amber-300 font-semibold shrink-0 transition-colors"
          title={staleReason}
        >
          <RefreshCw className="w-3 h-3" />
          reload
        </button>
      )}
    </footer>
  );
}
