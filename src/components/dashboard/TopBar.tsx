import { Activity, Bot, Key, Wifi, WifiOff, RefreshCw, Download } from 'lucide-react';
import { useUpdateChecker } from '@/hooks/useUpdateChecker';
import { toast } from 'sonner';

interface TopBarProps {
  isConnected: boolean;
  wsConnected?: boolean;
  onConnectClick?: () => void;
}

const APP_VERSION = __APP_VERSION__;

const TopBar = ({ isConnected, wsConnected, onConnectClick }: TopBarProps) => {
  const { updateAvailable, checking, checkForUpdates, reload } = useUpdateChecker();

  const handleCheckUpdate = async () => {
    const toastId = toast.loading('Checking for updates…');
    try {
      const found = await checkForUpdates();
      if (found) {
        toast.success('Update available!', { id: toastId, description: 'Click the banner to reload' });
      } else {
        toast.success('You\'re up to date', { id: toastId, description: `Version ${APP_VERSION}` });
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
          <span className="text-xs font-mono text-muted-foreground bg-secondary px-2 py-0.5 rounded">
            v{APP_VERSION}
          </span>
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

          {/* Check for updates button */}
          {updateAvailable ? (
            <button
              onClick={reload}
              className="flex items-center gap-1.5 px-3 py-1 rounded-md bg-accent/20 border border-accent/40 text-accent text-xs font-semibold hover:bg-accent/30 transition-colors animate-pulse"
            >
              <Download className="w-3.5 h-3.5" />
              Update available — click to reload
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
