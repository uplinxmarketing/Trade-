import { useState, useCallback } from 'react';
import TopBar from '@/components/dashboard/TopBar';
import PortfolioSummary from '@/components/dashboard/PortfolioSummary';
import PriceChart from '@/components/dashboard/PriceChart';
import PositionsList from '@/components/dashboard/PositionsList';
import BotStrategies from '@/components/dashboard/BotStrategies';
import AiChatPanel from '@/components/dashboard/AiChatPanel';
import RecentTrades from '@/components/dashboard/RecentTrades';
import TradePanel from '@/components/dashboard/TradePanel';
import type { KlineData, Position, BotStrategy } from '@/lib/binance-types';

// Demo data — replaced by real Binance data when API keys are connected
const mockKlines: KlineData[] = Array.from({ length: 24 }, (_, i) => {
  const base = 67200 + Math.sin(i * 0.5) * 800 + Math.random() * 400;
  return { time: `${i}:00`, open: base, high: base + 200, low: base - 150, close: base + (Math.random() - 0.4) * 300, volume: 1200 + Math.random() * 800 };
});

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
  const [strategies, setStrategies] = useState<BotStrategy[]>([
    { id: '1', name: 'Grid Trading', status: 'active', pair: 'BTC/USDT', pnl: 342.18, trades: 47, winRate: 72 },
    { id: '2', name: 'DCA Bot', status: 'active', pair: 'ETH/USDT', pnl: 128.55, trades: 23, winRate: 65 },
    { id: '3', name: 'Momentum Scalper', status: 'paused', pair: 'SOL/USDT', pnl: -18.40, trades: 112, winRate: 54 },
  ]);

  const toggleBot = useCallback((id: string) => {
    setStrategies(prev => prev.map(s =>
      s.id === id ? { ...s, status: s.status === 'active' ? 'paused' : 'active' } : s
    ));
  }, []);

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <TopBar isConnected={true} />

      <div className="flex-1 flex">
        {/* Main Dashboard */}
        <div className="flex-1 p-4 space-y-4 overflow-y-auto scrollbar-thin">
          <PortfolioSummary
            totalBalance={24_847.32}
            dailyPnl={344.18}
            dailyPnlPercent={1.41}
            openPositions={mockPositions.length}
            activeBots={strategies.filter(s => s.status === 'active').length}
          />

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <PriceChart
              data={mockKlines}
              symbol="BTC / USDT"
              currentPrice="67420.50"
              priceChange="2.14"
            />
            <PositionsList positions={mockPositions} />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <BotStrategies strategies={strategies} onToggle={toggleBot} />
            <RecentTrades trades={mockTrades} />
          </div>
        </div>

        {/* AI Chat Sidebar */}
        <div className="w-80 xl:w-96 border-l border-border hidden md:flex flex-col">
          <AiChatPanel />
        </div>
      </div>
    </div>
  );
};

export default Index;
