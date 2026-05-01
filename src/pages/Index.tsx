import { useState, useCallback, useEffect } from 'react';
import TopBar from '@/components/dashboard/TopBar';
import PortfolioSummary from '@/components/dashboard/PortfolioSummary';
import PriceChart from '@/components/dashboard/PriceChart';
import PnlChart from '@/components/dashboard/PnlChart';
import ReportDashboard from '@/components/dashboard/ReportDashboard';
import CoinSelector from '@/components/dashboard/CoinSelector';
import AiChatPanel from '@/components/dashboard/AiChatPanel';
import AiAnalysisPanel from '@/components/dashboard/AiAnalysisPanel';
import AITradingAgent from '@/components/dashboard/AITradingAgent';
import BinanceConnect from '@/components/dashboard/BinanceConnect';
import ProfitSettings from '@/components/dashboard/ProfitSettings';
import RecentTrades from '@/components/dashboard/RecentTrades';
import TradePanel from '@/components/dashboard/TradePanel';
import NotificationCenter from '@/components/dashboard/NotificationCenter';
import WalletPanel from '@/components/dashboard/WalletPanel';
import PaperWalletPanel from '@/components/dashboard/PaperWalletPanel';
import { useBinanceWebSocket } from '@/hooks/useBinanceWebSocket';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

const DEFAULT_COINS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'DOGEUSDT'];

const Index = () => {
  const [selectedCoins, setSelectedCoins] = useState<string[]>(DEFAULT_COINS);
  const [activeCoin, setActiveCoin] = useState('BTCUSDT');
  const [binanceConnected, setBinanceConnected] = useState(false);
  const [showBinanceConnect, setShowBinanceConnect] = useState(false);
  const [activeTab, setActiveTab] = useState<'dashboard' | 'bot' | 'report'>('dashboard');

  const { prices, connected: wsConnected } = useBinanceWebSocket(selectedCoins);
  const activePrice = prices[activeCoin];

  // Ensure a single bot_config row exists (ignoreDuplicates = don't overwrite existing)
  useEffect(() => {
    supabase.from('trading_bots').delete().eq('user_session', 'default').then(() => {});
    supabase.from('bot_config').upsert({
      user_session: 'default',
      selected_coins: DEFAULT_COINS,
      mode: 'test',
      is_running: false,
      current_balance: 10000,
      initial_balance: 10000,
      min_profit_percent: 0.5,
      stop_loss_percent: 1.5,
      updated_at: new Date().toISOString(),
    }, { onConflict: 'user_session', ignoreDuplicates: true });
  }, []);

  const handleModeChange = useCallback((_mode: 'test' | 'live') => {
    if (!binanceConnected) {
      toast.error('Connect your Binance API first', { description: 'Live trading requires API keys' });
      setShowBinanceConnect(true);
    }
  }, [binanceConnected]);

  const tabs = [
    { id: 'dashboard' as const, label: 'Dashboard' },
    { id: 'bot' as const, label: 'AI Bot' },
    { id: 'report' as const, label: 'Reports' },
  ];

  return (
    <div className="min-h-screen bg-background flex flex-col">
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

      {/* Tab navigation */}
      <div className="border-b border-border bg-card/50 px-4">
        <div className="flex gap-1">
          {tabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-2.5 text-xs font-medium transition-colors relative ${
                activeTab === tab.id ? 'text-accent' : 'text-muted-foreground hover:text-foreground'
              }`}
            >
              {tab.label}
              {activeTab === tab.id && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-accent rounded-t" />}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 flex">
        <div className="flex-1 p-4 space-y-4 overflow-y-auto scrollbar-thin">

          {/* Live coin ticker strip */}
          <div className="flex gap-1.5 overflow-x-auto scrollbar-thin pb-1">
            {selectedCoins.map(coin => {
              const p = prices[coin];
              const change = parseFloat(p?.priceChangePercent || '0');
              return (
                <button
                  key={coin}
                  onClick={() => setActiveCoin(coin)}
                  className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-mono whitespace-nowrap transition-colors active:scale-95 ${
                    activeCoin === coin ? 'bg-secondary text-foreground' : 'text-muted-foreground hover:text-foreground hover:bg-secondary/50'
                  }`}
                >
                  <span className="font-semibold">{coin.replace('USDT', '')}</span>
                  {p && (
                    <>
                      <span className="tabular-nums">${parseFloat(p.price).toLocaleString('en-US', { minimumFractionDigits: 2 })}</span>
                      <span className={`tabular-nums ${change >= 0 ? 'text-gain' : 'text-loss'}`}>
                        {change >= 0 ? '+' : ''}{change.toFixed(2)}%
                      </span>
                    </>
                  )}
                </button>
              );
            })}
          </div>

          {/* ── DASHBOARD TAB ── */}
          {activeTab === 'dashboard' && (
            <>
              <PortfolioSummary binanceConnected={binanceConnected} botCount={1} />

              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                <div className="lg:col-span-2">
                  <PriceChart
                    symbol={activeCoin.replace('USDT', '') + ' / USDT'}
                    currentPrice={activePrice?.price || '0'}
                    priceChange={activePrice?.priceChangePercent || '0'}
                  />
                </div>
                <TradePanel selectedCoins={selectedCoins} />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                <div className="space-y-4">
                  <PaperWalletPanel prices={prices} />
                  <CoinSelector selected={selectedCoins} onChange={setSelectedCoins} />
                  <ProfitSettings />
                  {binanceConnected && (
                    <WalletPanel
                      binanceConnected={binanceConnected}
                      selectedTradingCoins={selectedCoins}
                      onTradingCoinsChange={setSelectedCoins}
                    />
                  )}
                </div>
                <div className="lg:col-span-2 space-y-4">
                  <AITradingAgent selectedCoins={selectedCoins} prices={prices} binanceConnected={binanceConnected} onConnectBinance={() => setShowBinanceConnect(true)} />
                  <AiAnalysisPanel selectedCoins={selectedCoins} />
                </div>
              </div>
            </>
          )}

          {/* ── AI BOT TAB ── */}
          {activeTab === 'bot' && (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              <div className="lg:col-span-2 space-y-4">
                <PriceChart
                  symbol={activeCoin.replace('USDT', '') + ' / USDT'}
                  currentPrice={activePrice?.price || '0'}
                  priceChange={activePrice?.priceChangePercent || '0'}
                />
                <AITradingAgent selectedCoins={selectedCoins} prices={prices} binanceConnected={binanceConnected} onConnectBinance={() => setShowBinanceConnect(true)} />
              </div>
              <div className="space-y-4">
                <PaperWalletPanel prices={prices} />
                <AiAnalysisPanel selectedCoins={selectedCoins} />
                <ProfitSettings />
                <CoinSelector selected={selectedCoins} onChange={setSelectedCoins} />
              </div>
            </div>
          )}

          {/* ── REPORTS TAB ── */}
          {activeTab === 'report' && (
            <div className="space-y-4">
              <ReportDashboard />
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <PnlChart />
                <RecentTrades trades={[]} />
              </div>
            </div>
          )}
        </div>

        {/* AI Chat sidebar */}
        <div className="w-80 xl:w-96 border-l border-border hidden md:flex flex-col">
          <AiChatPanel />
        </div>
      </div>
    </div>
  );
};

export default Index;
