import { Activity, Bot, Key, Wifi, WifiOff, RefreshCw, Download, ShieldCheck, ClipboardCopy, AlertTriangle } from 'lucide-react';
import { useUpdateChecker } from '@/hooks/useUpdateChecker';
import { useBackendVersion } from '@/hooks/useBackendVersion';
import { toast } from 'sonner';
import { useState } from 'react';

interface TopBarProps {
  isConnected: boolean;
  wsConnected?: boolean;
  onConnectClick?: () => void;
}

const APP_VERSION = __APP_VERSION__;

const TopBar = ({ isConnected, wsConnected, onConnectClick }: TopBarProps) => {
  const { updateAvailable, checking, updating, checkForUpdates, applyUpdate } = useUpdateChecker();
  const { backendVersion, mismatch, reloadNow } = useBackendVersion();
  const [reconciling, setReconciling] = useState(false);
  const [copyingDiag, setCopyingDiag] = useState(false);

  const handleCopyDiagnostics = async () => {
    setCopyingDiag(true);
    const toastId = toast.loading('Collecting diagnostics…');
    try {
      const res = await fetch('/api/diagnostics/bundle', { cache: 'no-store' });
      if (!res.ok) {
        toast.error('Diagnostics fetch failed', { id: toastId, description: `HTTP ${res.status}` });
        return;
      }
      const text = await res.text();
      try {
        await navigator.clipboard.writeText(text);
        toast.success(`Diagnostics copied (${(text.length / 1024).toFixed(1)} KB)`, {
          id: toastId,
          description: 'Paste it into the chat for analysis.',
          duration: 6000,
        });
      } catch {
        // Clipboard API blocked (non-HTTPS or permissions) — offer a download instead
        const blob = new Blob([text], { type: 'text/plain' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `wolfbot-diagnostics-${new Date().toISOString().slice(0, 19)}.txt`;
        a.click();
        URL.revokeObjectURL(url);
        toast.success('Clipboard blocked — diagnostics downloaded as a file instead', { id: toastId });
      }
    } catch {
      toast.error('Diagnostics request failed', { id: toastId });
    } finally {
      setCopyingDiag(false);
    }
  };

  const handleReconcile = async () => {
    setReconciling(true);
    const toastId = toast.loading('Reconciling positions with Binance…');
    try {
      const res  = await fetch('/api/reconcile', { cache: 'no-store' });
      if (!res.ok) {
        toast.error('Reconcile failed', { id: toastId, description: `Bot returned HTTP ${res.status}` });
        return;
      }
      const data = await res.json();
      if (data.error) {
        toast.error('Reconcile failed', { id: toastId, description: data.error });
        return;
      }
      const ghosts     = data.ghosts?.length ?? 0;
      const mismatches = data.mismatches?.length ?? 0;
      if (ghosts > 0 || mismatches > 0) {
        toast.warning(`Found ${ghosts} ghost(s), ${mismatches} mismatch(es)`, {
          id: toastId,
          description: 'Check activity log for details. Ghost positions removed.',
          duration: 8000,
        });
      } else {
        toast.success('All positions match Binance', { id: toastId });
      }
    } catch {
      toast.error('Reconcile request failed', { id: toastId });
    } finally {
      setReconciling(false);
    }
  };

  const handleCheckUpdate = async () => {
    const toastId = toast.loading('Checking for updates…');
    try {
      const found = await checkForUpdates();
      if (found) {
        // Update found — apply it immediately instead of waiting for a second click
        toast.success('Update found — reloading…', { id: toastId });
        await applyUpdate();
      } else {
        toast.success(`v${APP_VERSION} — up to date`, { id: toastId, description: 'Server is running the same version as your browser.' });
      }
    } catch {
      toast.error('Update check failed', { id: toastId, description: 'Check your connection and try again' });
    }
  };

  return (
    <div className="flex flex-col">
      <header className="h-14 border-b border-border bg-card flex items-center justify-between px-6">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <Bot className="w-6 h-6 text-primary" />
            <h1 className="text-lg font-semibold tracking-tight">TradeBot AI</h1>
          </div>
          {mismatch ? (
            <span
              className="text-xs font-mono text-amber-400 bg-amber-500/10 border border-amber-500/30 px-2 py-0.5 rounded"
              title="Your browser bundle is older than the running backend. Reload to update."
            >
              UI v{APP_VERSION} · server v{backendVersion}
            </span>
          ) : (
            <span className="text-xs font-mono text-muted-foreground bg-secondary px-2 py-0.5 rounded">
              v{APP_VERSION}
            </span>
          )}
        </div>

        <div className="flex items-center gap-4">
          {wsConnected !== undefined && (
            <div className="flex items-center gap-1.5 text-xs">
              {wsConnected ? (
                <>
                  <div className="w-1.5 h-1.5 rounded-full bg-gain animate-pulse" />
                  <span className="text-gain font-mono">Live</span>
                </>
              ) : (
                <>
                  <div className="w-1.5 h-1.5 rounded-full bg-warn" />
                  <span className="text-warn font-mono">Connecting...</span>
                </>
              )}
            </div>
          )}

          {/* Reconcile button — only show in live mode context */}
          {isConnected && (
            <button
              onClick={handleReconcile}
              disabled={reconciling}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
              title="Verify open positions match what Binance shows. Removes ghost positions if any."
            >
              <ShieldCheck className={`w-3.5 h-3.5 ${reconciling ? 'animate-spin' : ''}`} />
              <span className="hidden sm:inline">Sync with Binance</span>
            </button>
          )}

          {/* Copy diagnostics bundle — one click, paste into chat for remote diagnosis */}
          <button
            onClick={handleCopyDiagnostics}
            disabled={copyingDiag}
            className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
            title="Copy the full diagnostic report (version, health, errors, gate blockers, telemetry, analytics, config) to the clipboard"
          >
            <ClipboardCopy className={`w-3.5 h-3.5 ${copyingDiag ? 'animate-spin' : ''}`} />
            <span className="hidden sm:inline">Copy diagnostics</span>
          </button>

          {/* Backend-version mismatch: browser bundle is stale relative to the
              running backend. Distinct from the git-based update checker below. */}
          {mismatch && (
            <button
              onClick={reloadNow}
              className="flex items-center gap-1.5 px-3 py-1 rounded-md bg-loss/15 border border-loss/50 text-loss text-xs font-semibold hover:bg-loss/25 transition-colors animate-pulse"
              title="A newer backend version is running than the bundle your browser loaded. Click to hard-reload."
            >
              <AlertTriangle className="w-3.5 h-3.5" />
              New version {backendVersion} deployed — click to reload
            </button>
          )}

          {/* Check for updates button */}
          {updateAvailable ? (
            <button
              onClick={applyUpdate}
              disabled={updating}
              className="flex items-center gap-1.5 px-3 py-1 rounded-md bg-accent/20 border border-accent/40 text-accent text-xs font-semibold hover:bg-accent/30 transition-colors animate-pulse disabled:opacity-60 disabled:cursor-not-allowed"
            >
              <Download className={`w-3.5 h-3.5 ${updating ? 'animate-spin' : ''}`} />
              {updating ? 'Pulling update…' : 'Update available — click to apply'}
            </button>
          ) : (
            <button
              onClick={handleCheckUpdate}
              disabled={checking}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
              title="Check for updates"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${checking ? 'animate-spin' : ''}`} />
              <span className="hidden sm:inline">Check for updates</span>
            </button>
          )}

          <div className="flex items-center gap-2 text-sm">
            <Activity className="w-4 h-4 text-muted-foreground" />
            <span className="text-muted-foreground font-mono">Binance</span>
          </div>
          <button
            onClick={onConnectClick}
            className="flex items-center gap-2 hover:opacity-80 transition-opacity"
          >
            {isConnected ? (
              <>
                <div className="pulse-dot" />
                <Wifi className="w-4 h-4 text-gain" />
                <span className="text-xs text-gain font-medium">Connected</span>
              </>
            ) : (
              <>
                <Key className="w-4 h-4 text-warn" />
                <span className="text-xs text-warn font-medium">Connect API</span>
              </>
            )}
          </button>
        </div>
      </header>
    </div>
  );
};

export default TopBar;
