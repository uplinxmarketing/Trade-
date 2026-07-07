import { useState, useEffect, useRef, useCallback } from 'react';
import { toast } from 'sonner';

// The version this browser bundle was BUILT with.
const APP_VERSION = __APP_VERSION__;

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

interface ServedInfo { version: string | null; indexAsset: string | null; }

// Best-effort GET /api/version/served -> {version, index_asset, dist_path}.
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
      indexAsset: typeof idx === 'string' && idx.trim()
        ? (idx.split('/').pop() || idx).trim()
        : null,
    };
  } catch {
    return null;
  }
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
  const [servedAsset, setServedAsset] = useState<string | null>(null);
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

      // Definitive stale-bundle check: ask the server which index asset it
      // serves and compare against the script this page actually loaded. A
      // difference proves the browser is running a cached bundle (vs. the
      // server merely being mid-deploy on a different version string).
      const served = await fetchServed();
      if (served) setServedAsset(served.indexAsset);
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

  return {
    appVersion: APP_VERSION,
    backendVersion,
    mismatch: (backendVersion != null && backendVersion !== APP_VERSION) || assetMismatch,
    servedAsset,
    currentAsset,
    assetMismatch,
    reloadNow,
  };
}
