import { useState, useEffect, useCallback } from 'react';

const REMOTE_VERSION_URL =
  'https://raw.githubusercontent.com/uplinxmarketing/Trade-/main/public/version.json';

// These constants are injected by Vite at startup from public/version.json.
// They are frozen in the running bundle — if we push new code with an updated
// version.json, users still running the old bundle will see a different
// fingerprint vs the remote, triggering the update banner.
const LOCAL_FINGERPRINT = `${__APP_COMMIT__}:${__APP_BUILD_TIME__}`;

interface VersionInfo {
  version: string;
  buildTime: string;
  commit: string;
}

export function useUpdateChecker(pollIntervalMs = 5 * 60 * 1000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking] = useState(false);

  const checkForUpdates = useCallback(async (): Promise<boolean> => {
    setChecking(true);
    try {
      const resp = await fetch(`${REMOTE_VERSION_URL}?t=${Date.now()}`, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data: VersionInfo = await resp.json();
      const remoteFingerprint = `${data.commit}:${data.buildTime}`;

      if (remoteFingerprint !== LOCAL_FINGERPRINT) {
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

  useEffect(() => {
    const interval = setInterval(() => {
      checkForUpdates().catch(() => {});
    }, pollIntervalMs);
    return () => clearInterval(interval);
  }, [checkForUpdates, pollIntervalMs]);

  const reload = () => window.location.reload();
  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, checkForUpdates, reload, dismiss };
}
