import { RefreshCw, AlertTriangle } from 'lucide-react';
import { useBackendVersion } from '@/hooks/useBackendVersion';

// ── I2 — self-diagnosing version footer ─────────────────────────────────────
// Third recurrence of the stale-UI bug. A compact, always-visible bottom bar
// that prints `UI <app commit> / API <backend commit>` so every future
// mismatch is diagnosable at a glance. When the commits differ — or the server
// provably serves a different bundle than the one this page loaded — it turns
// amber and exposes a one-click cache-busting reload.

const short = (c: string | null | undefined) => (c ? c.slice(0, 7) : '—');

// Format an optional timestamp that may arrive as an ISO string or an epoch
// (seconds or millis). Falls back to the raw value when it isn't parseable.
const fmtTime = (v: string | null): string | null => {
  if (!v) return null;
  const t = v.trim();
  let d: Date;
  if (/^\d+$/.test(t)) {
    const n = Number(t);
    d = new Date(n > 1e12 ? n : n * 1000);
  } else {
    d = new Date(t);
  }
  return isNaN(d.getTime()) ? t : d.toLocaleString();
};

// A restart is "unclean" when the reason points at a crash/kill rather than a
// deliberate deploy. Anything else (incl. a plain "deploy") is treated as clean.
const UNCLEAN_RESTART = /crash|unclean|oom|kill|unexpected|abnormal|sig(segv|kill|term|abrt)|panic|fatal/i;

export default function VersionFooter() {
  const {
    appVersion, appCommit, backendVersion, backendCommit,
    commitMismatch, assetMismatch, servedAsset, currentAsset, reloadNow,
    deployTime, processStart, restartReason,
  } = useBackendVersion();

  const stale = commitMismatch || assetMismatch;

  // K3 — deploy vs. restart labeling. deploy_time is the authoritative
  // "deployed" timestamp; an unclean restart_reason surfaces a muted note using
  // the process start time. All three fields are absent on older backends, so
  // guard with ?? null and render nothing when missing.
  const deployedFmt = fmtTime(deployTime ?? null);
  const restartUnclean = restartReason != null && UNCLEAN_RESTART.test(restartReason);
  const restartedFmt = fmtTime(processStart ?? null);

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
          {deployedFmt && (
            <span className="opacity-60 hidden sm:inline">{' · '}deployed {deployedFmt}</span>
          )}
          {restartUnclean && (
            <span
              className="text-amber-400/80"
              title={`restart_reason: ${restartReason}`}
            >
              {' · '}restarted {restartedFmt ?? '—'} · unclean
            </span>
          )}
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
