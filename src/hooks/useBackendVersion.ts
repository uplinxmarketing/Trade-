import { useState, useEffect, useRef, useCallback } from 'react';
import { toast } from 'sonner';

// The version + git commit this browser bundle was BUILT with.
const APP_VERSION = __APP_VERSION__;
const APP_COMMIT  = __APP_COMMIT__;

// One-shot auto-heal guard, keyed per backend version so we reload at most
// once per distinct mismatch (no infinite reload loop if the reload doesn't
// actually swap the stale bundle).
const HEAL_FLAG_PREFIX = 'tradebot_version_heal_';

// Bulletproof cache-busting hard reload. A stale index.html or a lingering
// service worker can otherwise keep serving the same old bundle no matter how
// many times we reload. So before navigating we (a) unregister every service
// worker and (b) wipe the Cache API, THEN navigate with location.replace() so
// the stale history entry is dropped (href/reload would keep it around).
async function bulletproofReload(): Promise<void> {
  try {
    if ('serviceWorker' in navigator) {
      const rs = await navigator.serviceWorker.getRegistrations();
      await Promise.all(rs.map(r => r.unregister()));
    }
  } catch { /* ignore — keep tearing down */ }
  try {
    if (window.caches) {
      const ks = await caches.keys();
      await Promise.all(ks.map(k => caches.delete(k)));
    }
  } catch { /* ignore — keep going to the navigation */ }
  window.location.replace(`${window.location.pathname}?v=${Date.now()}`);
}

// Fire-and-forget wrapper for callers that don't await.
function hardReload() {
  void bulletproofReload();
}

// Read the filename of the main module script this page was served from, so we
// can compare it against what GET /api/version/served says the server intends
// to serve. A difference proves the browser is on a cached/stale bundle.
function currentAssetFilename(): string | null {
  try {
    const scripts = Array.from(document.querySelectorAll('script[src]')) as HTMLScriptElement[];
    // Prefer the ES module entry (Vite emits <script type="module" src=...>).
    const mod = scripts.find(s => s.type === 'module' && s.src) ?? scripts.find(s => s.src);
    if (!mod) return null;
    const path = new URL(mod.src, window.location.href).pathname;
    return path.split('/').pop() || null;
  } catch {
    return null;
  }
}

interface ServedInfo { version: string | null; commit: string | null; indexAsset: string | null; }

// Best-effort GET /api/version/served -> {version, commit, index_asset, mtime, dist_path}.
async function fetchServed(): Promise<ServedInfo | null> {
  try {
    const res = await fetch(`/api/version/served?t=${Date.now()}`, { cache: 'no-store' });
    if (!res.ok) return null;
    const data = await res.json().catch(() => null);
    if (!data || typeof data !== 'object' || (data as Record<string, unknown>).error) return null;
    const o = data as Record<string, unknown>;
    const idx = o.index_asset;
    return {
      version: parseVersion(data),
      commit: parseCommit(data),
      indexAsset: typeof idx === 'string' && idx.trim()
        ? (idx.split('/').pop() || idx).trim()
        : null,
    };
  } catch {
    return null;
  }
}

// Tolerantly extract a git commit (short or full sha) from a version payload.
function parseCommit(data: unknown): string | null {
  if (data && typeof data === 'object') {
    const o = data as Record<string, unknown>;
    const c = o.commit ?? o.git_commit ?? o.sha ?? o.commit_sha ?? o.rev;
    if (typeof c === 'string' && c.trim()) return c.trim();
  }
  return null;
}

// Tolerantly extract an optional string/timestamp field from a version/health
// payload. These diagnostics fields (deploy_time, process_start, restart_reason)
// are added by a parallel backend change and are ABSENT on older backends — the
// caller must treat every returned value as possibly null and render nothing
// when missing. Accepts strings and numeric epochs (returned as-is / stringified).
function parseOptField(data: unknown, keys: string[]): string | null {
  if (data && typeof data === 'object') {
    const o = data as Record<string, unknown>;
    for (const k of keys) {
      const v = o[k];
      if (typeof v === 'string' && v.trim()) return v.trim();
      if (typeof v === 'number' && isFinite(v)) return String(v);
    }
  }
  return null;
}

// Tolerantly extract a version string from whatever /api/version returns:
// a bare string, {version}, {app_version}, {backend_version}, etc.
function parseVersion(data: unknown): string | null {
  if (typeof data === 'string') return data.trim() || null;
  if (data && typeof data === 'object') {
    const o = data as Record<string, unknown>;
    const v = o.version ?? o.app_version ?? o.backend_version ?? o.running_version;
    if (typeof v === 'string') return v.trim() || null;
  }
  return null;
}

export interface BackendVersionState {
  appVersion: string;
  /** Git commit this browser bundle was compiled from (compile-time __APP_COMMIT__). */
  appCommit: string;
  backendVersion: string | null;
  /** Git commit the running backend reports (from /api/version, else /api/version/served). */
  backendCommit: string | null;
  mismatch: boolean;
  /** True when both commits are known and differ. */
  commitMismatch: boolean;
  /** Authoritative "deployed" timestamp from /api/version, when the backend exposes it (null on older backends). */
  deployTime: string | null;
  /** When the current process last started/restarted, from /api/version (null when absent). */
  processStart: string | null;
  /** Why the process last restarted, e.g. "deploy", "crash", "oom" (null when absent). */
  restartReason: string | null;
  /** Asset filename the running server intends to serve (from /api/version/served). */
  servedAsset: string | null;
  /** Asset filename THIS browser bundle was actually served from. */
  currentAsset: string | null;
  /** True when servedAsset is known and differs from currentAsset — proof the browser is on a stale bundle. */
  assetMismatch: boolean;
  reloadNow: () => void;
}

/**
 * Polls GET /api/version and compares the RUNNING backend version against the
 * version this browser bundle was built with (__APP_VERSION__). This catches
 * "backend redeployed but my browser is still serving a stale bundle" — which
 * the git-based useUpdateChecker does not, because that compares the server's
 * checkout against origin/main, not the browser bundle against the server.
 *
 * When a mismatch persists for 2 consecutive polls (to skip a transient
 * mid-deploy flicker), it auto-triggers ONE cache-busting reload per backend
 * version. All fetches are tolerant: non-ok, {error}, and unreachable states
 * simply keep the last known value with no badge churn.
 */
export function useBackendVersion(pollIntervalMs = 60_000): BackendVersionState {
  const [backendVersion, setBackendVersion] = useState<string | null>(null);
  const [backendCommit, setBackendCommit] = useState<string | null>(null);
  const [servedAsset, setServedAsset] = useState<string | null>(null);
  const [deployTime, setDeployTime] = useState<string | null>(null);
  const [processStart, setProcessStart] = useState<string | null>(null);
  const [restartReason, setRestartReason] = useState<string | null>(null);
  const currentAsset = useRef<string | null>(currentAssetFilename()).current;
  const mismatchStreak = useRef(0);

  const check = useCallback(async () => {
    try {
      const res = await fetch(`/api/version?t=${Date.now()}`, { cache: 'no-store' });
      if (!res.ok) return;
      const ct = res.headers.get('content-type') ?? '';
      const data = ct.includes('json')
        ? await res.json().catch(() => null)
        : await res.text().catch(() => null);
      if (data && typeof data === 'object' && (data as Record<string, unknown>).error) return;
      const ver = parseVersion(data);
      if (!ver) return;

      setBackendVersion(ver);
      // /api/version now includes the backend git commit — surface it for the footer.
      const verCommit = parseCommit(data);
      if (verCommit) setBackendCommit(verCommit);

      // Optional deploy/restart diagnostics (parallel backend change). Only
      // update when present so older backends that omit them don't clobber a
      // previously-seen value with null.
      const dt = parseOptField(data, ['deploy_time', 'deployed_at', 'deploy_ts']);
      if (dt) setDeployTime(dt);
      const ps = parseOptField(data, ['process_start', 'started_at', 'process_start_ts']);
      if (ps) setProcessStart(ps);
      const rr = parseOptField(data, ['restart_reason', 'restartReason']);
      if (rr) setRestartReason(rr);

      // Definitive stale-bundle check: ask the server which index asset it
      // serves and compare against the script this page actually loaded. A
      // difference proves the browser is running a cached bundle (vs. the
      // server merely being mid-deploy on a different version string).
      const served = await fetchServed();
      if (served) {
        setServedAsset(served.indexAsset);
        // Fall back to the served-endpoint commit if /api/version lacked one.
        if (!verCommit && served.commit) setBackendCommit(served.commit);
      }
      const assetStale = Boolean(
        served?.indexAsset && currentAsset && served.indexAsset !== currentAsset,
      );

      if (ver !== APP_VERSION || assetStale) {
        mismatchStreak.current += 1;
        // Auto-heal after the mismatch is confirmed on 2 consecutive polls,
        // OR immediately when the served asset provably differs from ours.
        if (mismatchStreak.current >= 2 || assetStale) {
          const flag = HEAL_FLAG_PREFIX + (served?.indexAsset ?? ver);
          if (!sessionStorage.getItem(flag)) {
            sessionStorage.setItem(flag, '1');
            toast.loading('Reloading to new version…', { duration: 4000 });
            setTimeout(hardReload, 800);
          }
        }
      } else {
        mismatchStreak.current = 0;
      }
    } catch {
      /* backend unreachable — keep last known version, no churn */
    }
  }, [currentAsset]);

  useEffect(() => {
    // Small initial delay so it doesn't race the first paint / other pollers.
    const first = setTimeout(check, 3_000);
    const id = setInterval(check, pollIntervalMs);
    return () => { clearTimeout(first); clearInterval(id); };
  }, [check, pollIntervalMs]);

  const reloadNow = useCallback(() => { void bulletproofReload(); }, []);

  const assetMismatch = Boolean(
    servedAsset && currentAsset && servedAsset !== currentAsset,
  );

  const commitMismatch = Boolean(
    backendCommit && APP_COMMIT && backendCommit !== APP_COMMIT
    // Tolerate short-vs-full sha: treat as match when one is a prefix of the other.
    && !backendCommit.startsWith(APP_COMMIT) && !APP_COMMIT.startsWith(backendCommit),
  );

  return {
    appVersion: APP_VERSION,
    appCommit: APP_COMMIT,
    backendVersion,
    backendCommit,
    mismatch: (backendVersion != null && backendVersion !== APP_VERSION) || assetMismatch,
    commitMismatch,
    deployTime,
    processStart,
    restartReason,
    servedAsset,
    currentAsset,
    assetMismatch,
    reloadNow,
  };
}
