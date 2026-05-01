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

  // Pull latest code, then wait for Vite server to restart before reloading
  const applyUpdate = useCallback(async () => {
    setUpdating(true);
    const toastId = toast.loading('Pulling latest code from GitHub…');
    try {
      const resp = await fetch('/api/update', { method: 'POST' });
      const data = await resp.json();
      if (!data.success) {
        toast.error('Pull failed', { id: toastId, description: data.error || 'Restart start.bat / start.sh manually' });
        setUpdating(false);
        return;
      }

      // Git pull succeeded. Vite will restart ~1.5 s from now.
      // Poll /api/ping every second: while it errors = server restarting;
      // once it responds = server is back with new bundle → reload.
      toast.loading('Server restarting with new code…', { id: toastId, description: data.output || 'Waiting for Vite to restart…' });

      let attempts = 0;
      const poll = setInterval(async () => {
        attempts++;
        if (attempts > 30) { // bail after 30 s
          clearInterval(poll);
          window.location.reload();
          return;
        }
        try {
          const ping = await fetch('/api/ping', { cache: 'no-store' });
          if (ping.ok) {
            clearInterval(poll);
            toast.success('Update complete! Reloading…', { id: toastId });
            setTimeout(() => window.location.reload(), 300);
          }
        } catch { /* server still restarting — keep polling */ }
      }, 1000);
    } catch {
      toast.error('Could not reach update server', { id: toastId, description: 'Close and rerun start.bat / start.sh to update' });
      setUpdating(false);
    }
  }, []);

  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, updating, checkForUpdates, applyUpdate, dismiss };
}
