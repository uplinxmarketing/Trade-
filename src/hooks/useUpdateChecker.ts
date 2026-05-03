import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';
import { API_BASE } from '@/config';

// Baked into the JS bundle at build time — frozen for the life of this bundle.
const LOCAL_VERSION     = __APP_VERSION__;
const LOCAL_FINGERPRINT = `${__APP_COMMIT__}:${__APP_BUILD_TIME__}`;
const SEEN_KEY          = 'tradebot_seen_version';

interface VersionInfo { version: string; buildTime: string; commit: string; }

export function useUpdateChecker(pollIntervalMs = 5 * 60 * 1000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking]               = useState(false);
  const [updating, setUpdating]               = useState(false);

  // On first load: if the version baked into this bundle is newer than what the
  // browser last saw, show a "just updated" toast so the user knows new code arrived.
  useEffect(() => {
    const lastSeen = localStorage.getItem(SEEN_KEY) ?? '';
    if (lastSeen !== LOCAL_FINGERPRINT) {
      localStorage.setItem(SEEN_KEY, LOCAL_FINGERPRINT);
      if (lastSeen) {
        // Not the very first load — a real version change happened
        toast.success(`Updated to v${LOCAL_VERSION}`, {
          description: 'New fixes are now active.',
          duration: 6000,
        });
      }
    }
  }, []);

  // Check whether the server is now serving a NEWER bundle than what this
  // browser session has loaded.  After a Railway redeploy the server's
  // /version.json changes; users who haven't refreshed yet will see the banner.
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
      return false;
    } catch {
      throw new Error('Could not reach server to check for updates');
    } finally {
      setChecking(false);
    }
  }, []);

  // Background polling every 5 min — catches a live Railway redeploy
  // while the user has the app open without refreshing.
  useEffect(() => {
    const id = setInterval(() => { checkForUpdates().catch(() => {}); }, pollIntervalMs);
    return () => clearInterval(id);
  }, [checkForUpdates, pollIntervalMs]);

  const applyUpdate = useCallback(async () => {
    setUpdating(true);
    const toastId = toast.loading('Applying update…');
    try {
      const resp = await fetch(`${API_BASE}/api/update`, { method: 'POST' });
      const data = await resp.json();
      if (!data.success) {
        toast.loading('Reloading with latest version…', { id: toastId });
        setTimeout(() => window.location.reload(), 800);
        return;
      }
      // Server restarting — poll /api/ping then reload.
      toast.loading('Restarting with new code…', { id: toastId });
      let attempts = 0;
      const poll = setInterval(async () => {
        attempts++;
        if (attempts > 30) { clearInterval(poll); window.location.reload(); return; }
        try {
          const ping = await fetch(`${API_BASE}/api/ping`, { cache: 'no-store' });
          if (ping.ok) {
            clearInterval(poll);
            toast.success('Updated! Reloading…', { id: toastId });
            setTimeout(() => window.location.reload(), 300);
          }
        } catch { /* server still restarting */ }
      }, 1000);
    } catch {
      toast.loading('Reloading with latest version…', { id: toastId });
      setTimeout(() => window.location.reload(), 800);
    }
  }, []);

  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, updating, checkForUpdates, applyUpdate, dismiss };
}
