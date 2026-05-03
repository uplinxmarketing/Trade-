import { useState, useEffect, useCallback, useRef } from 'react';
import { toast } from 'sonner';
import { API_BASE } from '@/config';

// Baked into the JS bundle at build time.
const LOCAL_VERSION     = __APP_VERSION__;
const LOCAL_FINGERPRINT = `${__APP_COMMIT__}:${__APP_BUILD_TIME__}`;

// Two separate localStorage keys:
// SEEN_VERSION  — for the "Updated to vX" toast on first load after a rebuild
// SEEN_DEPLOY   — for the "Update available" polling banner (UUID-based)
const SEEN_VERSION = 'tradebot_seen_version';
const SEEN_DEPLOY  = 'tradebot_deploy_id';

interface VersionInfo { version: string; buildTime: string; commit: string; deployId?: string; }

// Hard cache-busting redirect — forces browser to re-download index.html and
// all assets, bypassing bfcache and HTTP cache for the entry point.
function hardReload() {
  window.location.href = `${window.location.pathname}?_cb=${Date.now()}`;
}

export function useUpdateChecker(pollIntervalMs = 60_000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking]               = useState(false);
  const [updating, setUpdating]               = useState(false);

  // Holds the deployId from the most recent successful poll so applyUpdate
  // can store it before the hard-reload (prevents false "update available"
  // immediately after the page comes back up).
  const latestDeployIdRef = useRef<string>('');

  // On first load: if the bundle fingerprint changed, a new build is running.
  // Show a toast so the user knows new frontend code is active.
  useEffect(() => {
    const lastSeen = localStorage.getItem(SEEN_VERSION) ?? '';
    if (lastSeen !== LOCAL_FINGERPRINT) {
      localStorage.setItem(SEEN_VERSION, LOCAL_FINGERPRINT);
      if (lastSeen) {
        toast.success(`Updated to v${LOCAL_VERSION}`, {
          description: 'New fixes and features are now active.',
          duration: 7000,
        });
      }
    }
  }, []);

  const checkForUpdates = useCallback(async (): Promise<boolean> => {
    setChecking(true);
    try {
      const resp = await fetch(`/version.json?t=${Date.now()}`, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data: VersionInfo = await resp.json();

      // ── Primary: deployment UUID comparison ───────────────────────────────
      // _DEPLOY_ID on the server is a UUID generated at process start.
      // Railway restarts the process on every deploy, so this value changes
      // reliably regardless of Docker layer caching or build artifacts.
      if (data.deployId) {
        latestDeployIdRef.current = data.deployId;
        const seenDeploy = localStorage.getItem(SEEN_DEPLOY) ?? '';
        if (!seenDeploy) {
          // First visit — establish baseline, no notification
          localStorage.setItem(SEEN_DEPLOY, data.deployId);
          setUpdateAvailable(false);
          return false;
        }
        if (data.deployId !== seenDeploy) {
          setUpdateAvailable(true);
          return true;
        }
        setUpdateAvailable(false);
        return false;
      }

      // ── Fallback: bundle fingerprint comparison (older server builds) ─────
      const remote = `${data.commit}:${data.buildTime}`;
      if (remote !== LOCAL_FINGERPRINT) {
        setUpdateAvailable(true);
        return true;
      }
      setUpdateAvailable(false);
      return false;
    } catch {
      throw new Error('Could not reach server to check for updates');
    } finally {
      setChecking(false);
    }
  }, []);

  // Check on mount, then retry with backoff until first success,
  // then settle into the normal poll interval.
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;
    let settled = false;

    const tryCheck = async (retryDelay: number) => {
      try {
        await checkForUpdates();
        settled = true;
        timer = setInterval(() => checkForUpdates().catch(() => {}), pollIntervalMs) as unknown as ReturnType<typeof setTimeout>;
      } catch {
        if (!settled) {
          const next = Math.min(retryDelay * 2, 60_000);
          timer = setTimeout(() => tryCheck(next), retryDelay);
        }
      }
    };

    timer = setTimeout(() => tryCheck(10_000), 500);
    return () => { clearTimeout(timer); clearInterval(timer as unknown as ReturnType<typeof setInterval>); };
  }, [checkForUpdates, pollIntervalMs]);

  const applyUpdate = useCallback(async () => {
    setUpdating(true);
    const toastId = toast.loading('Applying update…');
    // Store the new deployId BEFORE reloading so the page doesn't immediately
    // show "Update available" again right after it comes back up.
    if (latestDeployIdRef.current) {
      localStorage.setItem(SEEN_DEPLOY, latestDeployIdRef.current);
    }
    try {
      await fetch(`${API_BASE}/api/update`, { method: 'POST' }).catch(() => {});
    } finally {
      toast.loading('Reloading with fresh build…', { id: toastId });
      setTimeout(hardReload, 600);
    }
  }, []);

  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, updating, checkForUpdates, applyUpdate, dismiss };
}
