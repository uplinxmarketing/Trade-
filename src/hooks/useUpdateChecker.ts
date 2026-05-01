import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';

const REMOTE_VERSION_URL =
  'https://raw.githubusercontent.com/uplinxmarketing/Trade-/main/public/version.json';

// Baked into the JS bundle at Vite startup — frozen for the life of this run.
// When new code is pushed to GitHub and start.bat is restarted, the new
// version.json is baked in. Mismatch = update available.
const LOCAL_FINGERPRINT = `${__APP_COMMIT__}:${__APP_BUILD_TIME__}`;

interface VersionInfo { version: string; buildTime: string; commit: string; }

export function useUpdateChecker(pollIntervalMs = 5 * 60 * 1000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking] = useState(false);
  const [updating, setUpdating] = useState(false);

  const checkForUpdates = useCallback(async (): Promise<boolean> => {
    setChecking(true);
    try {
      const resp = await fetch(`${REMOTE_VERSION_URL}?t=${Date.now()}`, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data: VersionInfo = await resp.json();
      const remote = `${data.commit}:${data.buildTime}`;
      if (remote !== LOCAL_FINGERPRINT) {
        setUpdateAvailable(true);
        return true;
      }
      return false;
    } catch {
      throw new Error('Could not reach GitHub to check for updates');
    } finally {
      setChecking(false);
    }
  }, []);

  // Background polling every 5 min
  useEffect(() => {
    const id = setInterval(() => { checkForUpdates().catch(() => {}); }, pollIntervalMs);
    return () => clearInterval(id);
  }, [checkForUpdates, pollIntervalMs]);

  const applyUpdate = useCallback(async () => {
    setUpdating(true);
    const toastId = toast.loading('Pulling latest code from GitHub…');
    try {
      const resp = await fetch('/api/update', { method: 'POST' });
      const data = await resp.json();

      if (!data.success) {
        // git not available or pull failed — tell user to restart start.bat
        toast.error('Auto-update failed', {
          id: toastId,
          description: 'Close start.bat, then reopen it — it will download the latest code automatically.',
          duration: 8000,
        });
        setUpdating(false);
        return;
      }

      // Pull succeeded — Vite is restarting with the new bundle.
      // Poll /api/ping until the server comes back, then reload.
      toast.loading('Restarting with new code…', { id: toastId });
      let attempts = 0;
      const poll = setInterval(async () => {
        attempts++;
        if (attempts > 30) {
          clearInterval(poll);
          window.location.reload();
          return;
        }
        try {
          const ping = await fetch('/api/ping', { cache: 'no-store' });
          if (ping.ok) {
            clearInterval(poll);
            toast.success('Updated! Reloading…', { id: toastId });
            setTimeout(() => window.location.reload(), 300);
          }
        } catch { /* server still restarting */ }
      }, 1000);
    } catch {
      // /api/update not reachable at all — app not running via start.bat
      toast.error('Update server not available', {
        id: toastId,
        description: 'Close and reopen start.bat to get the latest version.',
        duration: 8000,
      });
      setUpdating(false);
    }
  }, []);

  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, updating, checkForUpdates, applyUpdate, dismiss };
}
