import { useState, useEffect, useRef, useCallback } from 'react';

interface VersionInfo {
  version: string;
  buildTime: string;
  commit: string;
}

export function useUpdateChecker(pollIntervalMs = 5 * 60 * 1000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking] = useState(false);
  const baseline = useRef<string | null>(null);

  const checkForUpdates = useCallback(async (): Promise<boolean> => {
    setChecking(true);
    try {
      const resp = await fetch(`/version.json?t=${Date.now()}`, { cache: 'no-store' });
      if (!resp.ok) return false;
      const data: VersionInfo = await resp.json();
      // Use commit + buildTime as a unique fingerprint
      const fingerprint = `${data.commit}:${data.buildTime}`;

      if (baseline.current === null) {
        baseline.current = fingerprint;
        return false;
      }

      if (fingerprint !== baseline.current) {
        setUpdateAvailable(true);
        return true;
      }
    } catch {
      // silently ignore network errors
    } finally {
      setChecking(false);
    }
    return false;
  }, []);

  useEffect(() => {
    checkForUpdates();
    const interval = setInterval(checkForUpdates, pollIntervalMs);
    return () => clearInterval(interval);
  }, [checkForUpdates, pollIntervalMs]);

  const reload = () => window.location.reload();
  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, checkForUpdates, reload, dismiss };
}
