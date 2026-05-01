import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';

const REMOTE_VERSION_URL =
  'https://raw.githubusercontent.com/uplinxmarketing/Trade-/main/public/version.json';

// Baked into the JS bundle at deploy time by Vite.
// When new code is deployed, the remote version.json changes → mismatch → update banner.
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

  // Browser-hosted app: updates deploy automatically when code is pushed.
  // Reloading the page fetches the latest deployed bundle — no git pull needed.
  const applyUpdate = useCallback(() => {
    setUpdating(true);
    toast.success('Reloading to apply update…', { duration: 1500 });
    setTimeout(() => window.location.reload(), 800);
  }, []);

  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, updating, checkForUpdates, applyUpdate, dismiss };
}
