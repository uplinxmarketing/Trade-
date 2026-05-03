import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';
import { API_BASE } from '@/config';

// Baked into the JS bundle at build time — frozen for the life of this bundle.
const LOCAL_VERSION     = __APP_VERSION__;
const LOCAL_FINGERPRINT = `${__APP_COMMIT__}:${__APP_BUILD_TIME__}`;
const SEEN_KEY          = 'tradebot_seen_version';

interface VersionInfo { version: string; buildTime: string; commit: string; }

// Hard cache-busting redirect — forces browser to re-download index.html and
// all assets, bypassing bfcache and HTTP cache for the entry point.
function hardReload() {
  window.location.href = `${window.location.pathname}?_cb=${Date.now()}`;
}

export function useUpdateChecker(pollIntervalMs = 5 * 60 * 1000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking]               = useState(false);
  const [updating, setUpdating]               = useState(false);

  // On first load: if the version baked into this bundle differs from the last
  // seen version stored in localStorage, a new deployment just happened.
  // Show a toast so the user knows new code is active.
  useEffect(() => {
    const lastSeen = localStorage.getItem(SEEN_KEY) ?? '';
    if (lastSeen !== LOCAL_FINGERPRINT) {
      localStorage.setItem(SEEN_KEY, LOCAL_FINGERPRINT);
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

  // Check once on mount (after a short delay so the app fully renders first)
  // and then every 5 minutes in the background.
  useEffect(() => {
    const initial = setTimeout(() => { checkForUpdates().catch(() => {}); }, 3000);
    const interval = setInterval(() => { checkForUpdates().catch(() => {}); }, pollIntervalMs);
    return () => { clearTimeout(initial); clearInterval(interval); };
  }, [checkForUpdates, pollIntervalMs]);

  const applyUpdate = useCallback(async () => {
    setUpdating(true);
    const toastId = toast.loading('Applying update…');
    try {
      // Try the server-side update endpoint first; it returns {success: false}
      // on Railway (auto-deploy handles rebuilds), which triggers a hard reload.
      await fetch(`${API_BASE}/api/update`, { method: 'POST' }).catch(() => {});
    } finally {
      toast.loading('Reloading with fresh build…', { id: toastId });
      // Small delay so the toast is visible, then hard-reload to bypass cache.
      setTimeout(hardReload, 600);
    }
  }, []);

  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, updating, checkForUpdates, applyUpdate, dismiss };
}
