import { Activity, Bot, Key, Wifi, WifiOff } from 'lucide-react';

interface TopBarProps {
  isConnected: boolean;
  wsConnected?: boolean;
  onConnectClick?: () => void;
}

const TopBar = ({ isConnected, wsConnected, onConnectClick }: TopBarProps) => {
  return (
    <header className="h-14 border-b border-border bg-card flex items-center justify-between px-6">
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2">
          <Bot className="w-6 h-6 text-primary" />
          <h1 className="text-lg font-semibold tracking-tight">TradeBot AI</h1>
        </div>
        <span className="text-xs font-mono text-muted-foreground bg-secondary px-2 py-0.5 rounded">v2.0</span>
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
  );
};

export default TopBar;
