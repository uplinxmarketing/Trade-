import { useState, useEffect, useCallback, useRef } from 'react';
import { toast } from 'sonner';
import { API_BASE } from '@/config';

const LOCAL_VERSION     = __APP_VERSION__;
const LOCAL_FINGERPRINT = `${__APP_COMMIT__}:${__APP_BUILD_TIME__}`;

const SEEN_VERSION = 'tradebot_seen_version';
const SEEN_DEPLOY  = 'tradebot_deploy_id';

interface VersionInfo { version: string; buildTime: string; commit: string; deployId?: string; }

function hardReload() {
  window.location.href = `${window.location.pathname}?_cb=${Date.now()}`;
}

export function useUpdateChecker(pollIntervalMs = 30_000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking]               = useState(false);
  const [updating, setUpdating]               = useState(false);
  const latestDeployIdRef = useRef<string>('');

  // On first load: if bundle fingerprint changed, clear stored deployId so the
  // next poll re-establishes baseline (prevents "no update" when new code is running).
  useEffect(() => {
    const lastSeen = localStorage.getItem(SEEN_VERSION) ?? '';
    if (lastSeen !== LOCAL_FINGERPRINT) {
      localStorage.setItem(SEEN_VERSION, LOCAL_FINGERPRINT);
      localStorage.removeItem(SEEN_DEPLOY); // force baseline re-establish on next poll
      if (lastSeen) {
        toast.success(`Updated to v${LOCAL_VERSION}`, {
          description: 'New fixes are now active.',
          duration: 5000,
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

      if (data.deployId) {
        latestDeployIdRef.current = data.deployId;
        const seenDeploy = localStorage.getItem(SEEN_DEPLOY) ?? '';
        if (!seenDeploy) {
          localStorage.setItem(SEEN_DEPLOY, data.deployId);
          return false;
        }
        if (data.deployId !== seenDeploy) {
          // Auto-apply: store new id then reload immediately — no user click needed.
          localStorage.setItem(SEEN_DEPLOY, data.deployId);
          setUpdating(true);
          toast.loading(`New version deployed — reloading…`, { duration: 3000 });
          setTimeout(hardReload, 2000);
          return true;
        }
        return false;
      }

      // Fallback: fingerprint comparison — auto-reload on mismatch so the user
      // never has to click anything to receive updates.
      const remote = `${data.commit}:${data.buildTime}`;
      if (remote !== LOCAL_FINGERPRINT) {
        setUpdateAvailable(true);
        setUpdating(true);
        toast.loading(`New version v${data.version} deployed — reloading…`, { duration: 3000 });
        setTimeout(hardReload, 2000);
        return true;
      }
      return false;
    } catch {
      throw new Error('Could not reach server');
    } finally {
      setChecking(false);
    }
  }, []);

  // Check 1 s after mount, then every 30 s.
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;
    let interval: ReturnType<typeof setInterval>;
    let settled = false;

    const tryCheck = async (retryDelay: number) => {
      try {
        await checkForUpdates();
        settled = true;
        interval = setInterval(() => checkForUpdates().catch(() => {}), pollIntervalMs);
      } catch {
        if (!settled) {
          const next = Math.min(retryDelay * 2, 30_000);
          timer = setTimeout(() => tryCheck(next), retryDelay);
        }
      }
    };

    timer = setTimeout(() => tryCheck(5_000), 1_000);
    return () => { clearTimeout(timer); clearInterval(interval); };
  }, [checkForUpdates, pollIntervalMs]);

  const applyUpdate = useCallback(async () => {
    setUpdating(true);
    if (latestDeployIdRef.current) localStorage.setItem(SEEN_DEPLOY, latestDeployIdRef.current);
    toast.loading('Reloading…');
    setTimeout(hardReload, 400);
  }, []);

  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, updating, checkForUpdates, applyUpdate, dismiss };
}
