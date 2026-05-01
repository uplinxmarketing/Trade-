import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';

const REMOTE_VERSION_URL =
  'https://raw.githubusercontent.com/uplinxmarketing/Trade-/main/public/version.json';

// Baked into the JS bundle at startup by Vite — frozen for the life of this run.
// When new code is pushed with an updated version.json, running bundles keep the
// old fingerprint; the remote URL returns the new one → mismatch → update detected.
const LOCAL_FINGERPRINT = `${__APP_COMMIT__}:${__APP_BUILD_TIME__}`;

interface VersionInfo { version: string; buildTime: string; commit: string; }

export function useUpdateChecker(pollIntervalMs = 5 * 60 * 1000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking]   = useState(false);
  const [updating, setUpdating]   = useState(false);

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

  // Background polling
  useEffect(() => {
    const id = setInterval(() => { checkForUpdates().catch(() => {}); }, pollIntervalMs);
    return () => clearInterval(id);
  }, [checkForUpdates, pollIntervalMs]);

  // Pull latest code from GitHub via local Vite server, then reload
  const applyUpdate = useCallback(async () => {
    setUpdating(true);
    const toastId = toast.loading('Pulling latest code from GitHub…');
    try {
      const resp = await fetch('/api/update', { method: 'POST' });
      const data = await resp.json();
      if (data.success) {
        toast.success('Update applied! Reloading…', { id: toastId, description: data.output || 'Code is up to date' });
        setTimeout(() => window.location.reload(), 1500);
      } else {
        toast.error('Pull failed — restart the app manually', { id: toastId, description: data.error });
        setUpdating(false);
        // Still reload in case files were partially updated
        setTimeout(() => window.location.reload(), 2000);
      }
    } catch {
      toast.error('Could not reach update server', { id: toastId, description: 'Close and rerun start.bat / start.sh to update' });
      setUpdating(false);
    }
  }, []);

  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, updating, checkForUpdates, applyUpdate, dismiss };
}
