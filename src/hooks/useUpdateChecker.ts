import { useState, useEffect, useCallback, useRef } from 'react';
import { toast } from 'sonner';

const LOCAL_VERSION     = __APP_VERSION__;
const LOCAL_FINGERPRINT = `${__APP_COMMIT__}:${__APP_BUILD_TIME__}`;

// Fetch the latest published version directly from GitHub so the check works
// even before the bot has been restarted with new code.
const GITHUB_VERSION_URL =
  'https://raw.githubusercontent.com/uplinxmarketing/Trade-/main/public/version.json';

const SEEN_VERSION = 'tradebot_seen_version';
const SEEN_DEPLOY  = 'tradebot_deploy_id';

interface VersionInfo { version: string; buildTime: string; commit: string; deployId?: string; }

function hardReload() {
  window.location.href = `${window.location.pathname}?_cb=${Date.now()}`;
}

export function useUpdateChecker(pollIntervalMs = 60_000) {
  const [updateAvailable, setUpdateAvailable] = useState(false);
  const [checking, setChecking]               = useState(false);
  const [updating, setUpdating]               = useState(false);
  const latestVersionRef = useRef<string>('');

  // On first load: if bundle fingerprint changed, announce it.
  useEffect(() => {
    const lastSeen = localStorage.getItem(SEEN_VERSION) ?? '';
    if (lastSeen !== LOCAL_FINGERPRINT) {
      localStorage.setItem(SEEN_VERSION, LOCAL_FINGERPRINT);
      localStorage.removeItem(SEEN_DEPLOY);
      if (lastSeen) {
        toast.success(`Updated to v${LOCAL_VERSION}`, {
          description: 'New fixes are now active.',
          duration: 5000,
        });
      }
    }
  }, []);

  const checkForUpdates = useCallback(async (): Promise<boolean> => {
    setChecking(true);
    try {
      // PRIMARY: server-side git check. The bot compares its own checkout
      // against origin/main using git — no CORS, no raw.githubusercontent
      // dependency, and it measures the real thing (is the SERVER behind?)
      // rather than browser-bundle-vs-server. This is authoritative.
      try {
        const gr = await fetch(`/api/update/check?t=${Date.now()}`, { cache: 'no-store' });
        if (gr.ok) {
          const gd = await gr.json();
          if (gd.ok) {
            if (gd.update_available) {
              latestVersionRef.current = gd.latest_subject || gd.remote_commit || 'new version';
              setUpdateAvailable(true);
              return true;
            }
            // Server is at origin/main HEAD. If the BROWSER bundle is older
            // than what the server now serves, a reload (not a pull) is needed.
            setUpdateAvailable(false);
            return false;
          }
        }
      } catch { /* fall through to version.json comparison */ }

      // FALLBACK (git check unavailable — pre-0.4.6 bots): compare versions.
      let data: VersionInfo | null = null;
      try {
        const ghResp = await fetch(`${GITHUB_VERSION_URL}?t=${Date.now()}`, { cache: 'no-store' });
        if (ghResp.ok) data = await ghResp.json();
      } catch { /* GitHub unreachable */ }
      if (!data) {
        const resp = await fetch(`/version.json?t=${Date.now()}`, { cache: 'no-store' });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        data = await resp.json();
      }
      if (data.version !== LOCAL_VERSION) {
        latestVersionRef.current = data.version;
        setUpdateAvailable(true);
        return true;
      }
      return false;
    } catch {
      throw new Error('Could not reach server');
    } finally {
      setChecking(false);
    }
  }, []);

  // Check 2 s after mount, then every 60 s.
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;
    let interval: ReturnType<typeof setInterval>;
    let settled = false;

    const tryCheck = async (retryDelay: number) => {
      try {
        await checkForUpdates();
        settled = true;
        interval = setInterval(() => checkForUpdates().catch(() => {}), pollIntervalMs);
      } catch {
        if (!settled) {
          const next = Math.min(retryDelay * 2, 30_000);
          timer = setTimeout(() => tryCheck(next), retryDelay);
        }
      }
    };

    timer = setTimeout(() => tryCheck(5_000), 2_000);
    return () => { clearTimeout(timer); clearInterval(interval); };
  }, [checkForUpdates, pollIntervalMs]);

  const applyUpdate = useCallback(async () => {
    setUpdating(true);
    localStorage.removeItem(SEEN_DEPLOY);
    try {
      // Ask the bot to pull latest code, rebuild, and restart itself.
      const res  = await fetch('/api/update', { method: 'POST', cache: 'no-store' });
      const body = await res.json().catch(() => ({}));
      if (body.success) {
        const newVer = latestVersionRef.current || 'new version';
        toast.loading(`Applying v${newVer} — bot will restart in ~30 s…`, { duration: 40_000 });
        // Poll until the bot comes back with the new version, then reload.
        const start = Date.now();
        const poll = setInterval(async () => {
          try {
            // The bot restarts mid-update; once /api/update/check reports 0
            // commits behind (server pulled + came back up), reload the page.
            const gr = await fetch(`/api/update/check?t=${Date.now()}`, { cache: 'no-store' })
              .then(r => r.json()).catch(() => null);
            const caughtUp = gr && gr.ok && gr.update_available === false;
            if (caughtUp || Date.now() - start > 90_000) {
              clearInterval(poll);
              hardReload();
            }
          } catch { /* bot still restarting */ }
        }, 4_000);
        return;
      }
    } catch { /* fall through */ }
    // Bot didn't support auto-update — just reload so the new static files are served.
    toast.loading('Reloading to apply update…');
    setTimeout(hardReload, 600);
  }, []);

  const dismiss = () => setUpdateAvailable(false);

  return { updateAvailable, checking, updating, checkForUpdates, applyUpdate, dismiss };
}
