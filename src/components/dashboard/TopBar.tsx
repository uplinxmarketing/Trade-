import { Activity, Bot, Wifi, WifiOff } from 'lucide-react';

interface TopBarProps {
  isConnected: boolean;
}

const TopBar = ({ isConnected }: TopBarProps) => {
  return (
    <header className="h-14 border-b border-border bg-card flex items-center justify-between px-6">
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2">
          <Bot className="w-6 h-6 text-primary" />
          <h1 className="text-lg font-semibold tracking-tight">TradeBot AI</h1>
        </div>
        <span className="text-xs font-mono text-muted-foreground bg-secondary px-2 py-0.5 rounded">v1.0</span>
      </div>

      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2 text-sm">
          <Activity className="w-4 h-4 text-muted-foreground" />
          <span className="text-muted-foreground font-mono">Binance</span>
        </div>
        <div className="flex items-center gap-2">
          {isConnected ? (
            <>
              <div className="pulse-dot" />
              <Wifi className="w-4 h-4 text-gain" />
              <span className="text-xs text-gain font-medium">Connected</span>
            </>
          ) : (
            <>
              <WifiOff className="w-4 h-4 text-loss" />
              <span className="text-xs text-loss font-medium">Disconnected</span>
            </>
          )}
        </div>
      </div>
    </header>
  );
};

export default TopBar;
