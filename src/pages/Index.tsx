import { useState, useCallback, useEffect } from 'react';
import TopBar from '@/components/dashboard/TopBar';
import AiChatPanel from '@/components/dashboard/AiChatPanel';
import AITradingAgent from '@/components/dashboard/AITradingAgent';
import BinanceConnect from '@/components/dashboard/BinanceConnect';
import NotificationCenter from '@/components/dashboard/NotificationCenter';
import CoinSelectorPanel from '@/components/dashboard/CoinSelectorPanel';
import MarketStatsBar from '@/components/dashboard/MarketStatsBar';
import ChartPanelV2 from '@/components/dashboard/ChartPanelV2';
import OrderFormPanel from '@/components/dashboard/OrderFormPanel';
import WalletPanelV2 from '@/components/dashboard/WalletPanelV2';
import ReportDashboard from '@/components/dashboard/ReportDashboard';
import CoinSelector from '@/components/dashboard/CoinSelector';
import { useBinanceWebSocket } from '@/hooks/useBinanceWebSocket';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

const DEFAULT_COINS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'DOGEUSDT'];

const Index = () => {
  const [selectedCoins, setSelectedCoins] = useState<string[]>(DEFAULT_COINS);
  const [activeCoin, setActiveCoin]        = useState('BTCUSDT');
  const [binanceConnected, setBinanceConnected] = useState(false);
  const [showBinanceConnect, setShowBinanceConnect] = useState(false);

  const { prices, connected: wsConnected } = useBinanceWebSocket(selectedCoins);

  useEffect(() => {
    supabase.from('bot_config').upsert({
      user_session: 'default',
      selected_coins: DEFAULT_COINS,
      mode: 'test',
      is_running: false,
      current_balance: 10000,
      initial_balance: 10000,
      stop_loss_percent: 1.5,
      updated_at: new Date().toISOString(),
    }, { onConflict: 'user_session', ignoreDuplicates: true });
  }, []);

  const handleModeChange = useCallback((_m: 'test' | 'live') => {
    if (!binanceConnected) {
      toast.error('Connect your Binance API first', { description: 'Live trading requires API keys' });
      setShowBinanceConnect(true);
    }
  }, [binanceConnected]);

  return (
    <div className="min-h-screen bg-background flex flex-col overflow-hidden">
      <NotificationCenter />
      <TopBar
        isConnected={binanceConnected}
        wsConnected={wsConnected}
        onConnectClick={() => setShowBinanceConnect(true)}
      />

      <BinanceConnect
        isOpen={showBinanceConnect}
        onClose={() => setShowBinanceConnect(false)}
        onConnectionChange={setBinanceConnected}
      />

      {/* Main 3-column layout: coin list | content | chat */}
      <div className="flex flex-1 min-h-0 overflow-hidden">

        {/* Panel A — Coin Selector (left sidebar) */}
        <div className="w-52 flex-shrink-0 hidden lg:flex flex-col overflow-hidden">
          <CoinSelectorPanel
            selectedCoins={selectedCoins}
            activeCoin={activeCoin}
            onActiveCoin={setActiveCoin}
            prices={prices}
          />
        </div>

        {/* Main scrollable content */}
        <div className="flex-1 flex flex-col min-w-0 overflow-y-auto scrollbar-thin">
          {/* Panel B — Market Stats Bar */}
          <MarketStatsBar activeCoin={activeCoin} prices={prices} />

          {/* Panel C — Chart + Panel D — Order Form */}
          <div className="flex gap-0 border-b border-border" style={{ height: '420px' }}>
            {/* Chart takes ~65% */}
            <div className="flex-1 min-w-0 border-r border-border">
              <ChartPanelV2 activeCoin={activeCoin} prices={prices} />
            </div>
            {/* Order form takes ~35%, capped at 340px */}
            <div className="w-72 xl:w-80 flex-shrink-0">
              <OrderFormPanel activeCoin={activeCoin} prices={prices} binanceConnected={binanceConnected} />
            </div>
          </div>

          {/* Below chart: all panels */}
          <div className="p-4 space-y-4">
            {/* Wallet */}
            <WalletPanelV2
              binanceConnected={binanceConnected}
              prices={prices}
              mode={binanceConnected ? 'live' : 'test'}
            />

            {/* AI Trading Bot */}
            <AITradingAgent
              selectedCoins={selectedCoins}
              prices={prices}
              binanceConnected={binanceConnected}
              onConnectBinance={() => setShowBinanceConnect(true)}
            />

            {/* Coin selection (for bot) */}
            <div className="trading-card p-3">
              <div className="text-xs font-medium text-muted-foreground mb-2">Bot Watch List</div>
              <CoinSelector selected={selectedCoins} onChange={setSelectedCoins} />
            </div>

            {/* Reports */}
            <ReportDashboard />
          </div>
        </div>

        {/* AI Chat sidebar */}
        <div className="w-80 xl:w-96 border-l border-border hidden md:flex flex-col flex-shrink-0">
          <AiChatPanel />
        </div>
      </div>
    </div>
  );
};

export default Index;
