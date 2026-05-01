import { useState, useEffect, useRef, useCallback } from 'react';

const REMOTE_VERSION_URL =
  'https://raw.githubusercontent.com/uplinxmarketing/Trade-/main/public/version.json';

interface VersionInfo {
  version: string;
  buildTime: string;
  commit: string;
}

export function useUpdateChecker(pollIntervalMs = 5 * 60 * 1000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking] = useState(false);
  const localFingerprint = useRef<string | null>(null);

  // On mount, record what version is currently running locally
  useEffect(() => {
    fetch(`/version.json?t=${Date.now()}`, { cache: 'no-store' })
      .then(r => r.json())
      .then((data: VersionInfo) => {
        localFingerprint.current = `${data.commit}:${data.buildTime}`;
      })
      .catch(() => {
        // fallback fingerprint so checks still work
        localFingerprint.current = 'unknown';
      });
  }, []);

  const checkForUpdates = useCallback(async (): Promise<boolean> => {
    setChecking(true);
    try {
      const resp = await fetch(`${REMOTE_VERSION_URL}?t=${Date.now()}`, { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data: VersionInfo = await resp.json();
      const remoteFingerprint = `${data.commit}:${data.buildTime}`;

      // If we don't know our local version yet, treat as up to date
      if (!localFingerprint.current || localFingerprint.current === 'unknown') return false;

      if (remoteFingerprint !== localFingerprint.current) {
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
    // Poll periodically in background
    const interval = setInterval(() => {
      checkForUpdates().catch(() => {});
    }, pollIntervalMs);
    return () => clearInterval(interval);
  }, [checkForUpdates, pollIntervalMs]);

  const reload = () => window.location.reload();
  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, checkForUpdates, reload, dismiss };
}
