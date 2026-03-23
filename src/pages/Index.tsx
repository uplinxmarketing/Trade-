import { useState } from 'react';
import TopBar from '@/components/dashboard/TopBar';
import PortfolioSummary from '@/components/dashboard/PortfolioSummary';
import PriceChart from '@/components/dashboard/PriceChart';
import PnlChart from '@/components/dashboard/PnlChart';
import ReportDashboard from '@/components/dashboard/ReportDashboard';
import PositionsList from '@/components/dashboard/PositionsList';
import BotDashboard from '@/components/dashboard/BotDashboard';
import CoinSelector from '@/components/dashboard/CoinSelector';
import AiChatPanel from '@/components/dashboard/AiChatPanel';
import AiAnalysisPanel from '@/components/dashboard/AiAnalysisPanel';
import BinanceConnect from '@/components/dashboard/BinanceConnect';
import RecentTrades from '@/components/dashboard/RecentTrades';
import TradePanel from '@/components/dashboard/TradePanel';
import { useBinanceWebSocket } from '@/hooks/useBinanceWebSocket';
import type { Position } from '@/lib/binance-types';

const mockPositions: Position[] = [
  { symbol: 'BTCUSDT', side: 'LONG', entryPrice: '66850', markPrice: '67420', unrealizedPnl: '285.00', quantity: '0.5', leverage: 10 },
  { symbol: 'ETHUSDT', side: 'SHORT', entryPrice: '3520', markPrice: '3485', unrealizedPnl: '105.00', quantity: '3.0', leverage: 5 },
  { symbol: 'SOLUSDT', side: 'LONG', entryPrice: '148.50', markPrice: '146.20', unrealizedPnl: '-46.00', quantity: '20', leverage: 3 },
];

const mockTrades = [
  { id: '1', symbol: 'BTCUSDT', side: 'BUY' as const, price: '66850', qty: '0.5', time: '14:32', pnl: undefined },
  { id: '2', symbol: 'ETHUSDT', side: 'SELL' as const, price: '3520', qty: '3.0', time: '13:15', pnl: 78.40 },
  { id: '3', symbol: 'SOLUSDT', side: 'BUY' as const, price: '148.50', qty: '20', time: '11:44', pnl: undefined },
  { id: '4', symbol: 'BTCUSDT', side: 'SELL' as const, price: '66200', qty: '0.25', time: '09:20', pnl: -32.50 },
  { id: '5', symbol: 'DOGEUSDT', side: 'BUY' as const, price: '0.1245', qty: '10000', time: '08:05', pnl: undefined },
];

const Index = () => {
  const [selectedCoins, setSelectedCoins] = useState<string[]>([
    'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'DOGEUSDT'
  ]);
  const [activeCoin, setActiveCoin] = useState('BTCUSDT');
  const [botMode, setBotMode] = useState<'test' | 'live'>('test');
  const [binanceConnected, setBinanceConnected] = useState(false);
  const [showBinanceConnect, setShowBinanceConnect] = useState(false);

  const { prices, connected: wsConnected } = useBinanceWebSocket(selectedCoins);

  const activePrice = prices[activeCoin];

  return (
    <div className="min-h-screen bg-background flex flex-col">
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
                    activeCoin === coin
                      ? 'bg-secondary text-foreground'
                      : 'text-muted-foreground hover:text-foreground hover:bg-secondary/50'
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

          <PortfolioSummary
            totalBalance={24_847.32}
            dailyPnl={344.18}
            dailyPnlPercent={1.41}
            openPositions={mockPositions.length}
            activeBots={selectedCoins.length}
          />

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <PriceChart
              symbol={activeCoin.replace('USDT', '') + ' / USDT'}
              currentPrice={activePrice?.price || '0'}
              priceChange={activePrice?.priceChangePercent || '0'}
            />
            <PositionsList positions={mockPositions} />
            <TradePanel selectedCoins={selectedCoins} />
          </div>

          {/* P&L Chart + Report */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <PnlChart />
            <ReportDashboard />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <div className="space-y-4">
              <CoinSelector selected={selectedCoins} onChange={setSelectedCoins} />
              <BotDashboard
                selectedCoins={selectedCoins}
                mode={botMode}
                onModeChange={setBotMode}
                binanceConnected={binanceConnected}
              />
            </div>
            <div className="lg:col-span-2">
              <AiAnalysisPanel selectedCoins={selectedCoins} />
            </div>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <RecentTrades trades={mockTrades} />
          </div>
        </div>

        <div className="w-80 xl:w-96 border-l border-border hidden md:flex flex-col">
          <AiChatPanel />
        </div>
      </div>
    </div>
  );
};

export default Index;
