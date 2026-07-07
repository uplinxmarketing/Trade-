import { useState, useEffect, useRef, useCallback } from 'react';
import { toast } from 'sonner';

// The version this browser bundle was BUILT with.
const APP_VERSION = __APP_VERSION__;

// One-shot auto-heal guard, keyed per backend version so we reload at most
// once per distinct mismatch (no infinite reload loop if the reload doesn't
// actually swap the stale bundle).
const HEAL_FLAG_PREFIX = 'tradebot_version_heal_';

// Cache-busting hard reload — mirrors the pattern in useUpdateChecker.
function hardReload() {
  window.location.href = `${window.location.pathname}?_cb=${Date.now()}`;
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
  backendVersion: string | null;
  mismatch: boolean;
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

      if (ver !== APP_VERSION) {
        mismatchStreak.current += 1;
        // Auto-heal only after the mismatch is confirmed on 2 consecutive polls.
        if (mismatchStreak.current >= 2) {
          const flag = HEAL_FLAG_PREFIX + ver;
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
  }, []);

  useEffect(() => {
    // Small initial delay so it doesn't race the first paint / other pollers.
    const first = setTimeout(check, 3_000);
    const id = setInterval(check, pollIntervalMs);
    return () => { clearTimeout(first); clearInterval(id); };
  }, [check, pollIntervalMs]);

  const reloadNow = useCallback(() => hardReload(), []);

  return {
    appVersion: APP_VERSION,
    backendVersion,
    mismatch: backendVersion != null && backendVersion !== APP_VERSION,
    reloadNow,
  };
}
