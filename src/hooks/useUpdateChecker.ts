import { useState, useEffect, useRef, useCallback } from 'react';

export function useUpdateChecker(pollIntervalMs = 5 * 60 * 1000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking] = useState(false);
  const baselineHash = useRef<string | null>(null);

  const extractScriptHash = (html: string): string | null => {
    const match = html.match(/\/assets\/index-([^.]+)\.js/);
    return match ? match[1] : null;
  };

  const checkForUpdates = useCallback(async (): Promise<boolean> => {
    setChecking(true);
    try {
      const resp = await fetch(`/?nocache=${Date.now()}`, { cache: 'no-store' });
      if (!resp.ok) return false;
      const html = await resp.text();
      const hash = extractScriptHash(html);
      if (!hash) return false;

      if (baselineHash.current === null) {
        baselineHash.current = hash;
        return false;
      }

      if (hash !== baselineHash.current) {
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
    // Capture baseline hash on mount
    checkForUpdates();

    // Then poll periodically
    const interval = setInterval(checkForUpdates, pollIntervalMs);
    return () => clearInterval(interval);
  }, [checkForUpdates, pollIntervalMs]);

  const reload = () => window.location.reload();
  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, checkForUpdates, reload, dismiss };
}
