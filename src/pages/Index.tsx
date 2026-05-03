import { useState, useCallback, useEffect } from 'react';
import TopBar from '@/components/dashboard/TopBar';
import AiChatPanel from '@/components/dashboard/AiChatPanel';
import AITradingAgent from '@/components/dashboard/AITradingAgent';
import CoinSelector from '@/components/dashboard/CoinSelector';
import BinanceConnect from '@/components/dashboard/BinanceConnect';
import NotificationCenter from '@/components/dashboard/NotificationCenter';
import CoinSelectorPanel from '@/components/dashboard/CoinSelectorPanel';
import MarketStatsBar from '@/components/dashboard/MarketStatsBar';
import ChartPanelV2 from '@/components/dashboard/ChartPanelV2';
import OrderFormPanel from '@/components/dashboard/OrderFormPanel';
import WalletPanelV2 from '@/components/dashboard/WalletPanelV2';
import ReportDashboard from '@/components/dashboard/ReportDashboard';
import { useBinanceWebSocket } from '@/hooks/useBinanceWebSocket';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

const DEFAULT_COINS = [
  'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'DOGEUSDT',
  'XRPUSDT', 'ADAUSDT', 'AVAXUSDT', 'LINKUSDT', 'POLUSDT',
  'ARBUSDT',  'OPUSDT',  'INJUSDT', 'NEARUSDT', 'FETUSDT',
];

const Index = () => {
  const [selectedCoins, setSelectedCoins] = useState<string[]>(DEFAULT_COINS);
  const [activeCoin, setActiveCoin]        = useState('BTCUSDT');
  const [binanceConnected, setBinanceConnected] = useState(false);
  const [showBinanceConnect, setShowBinanceConnect] = useState(false);
  const [agentPositions, setAgentPositions] = useState<{symbol:string;quantity:number;avg_entry_price:number}[]>([]);
  const [agentBalance, setAgentBalance] = useState(0);
  const [agentInitialBalance, setAgentInitialBalance] = useState(0);
  const [agentTrades, setAgentTrades] = useState<{side:'BUY'|'SELL';pnl:number|null;quantity:number;price:number}[]>([]);

  const { prices, connected: wsConnected } = useBinanceWebSocket(selectedCoins);

  useEffect(() => {
    (async () => {
      const { data } = await supabase
        .from('bot_config')
        .select('current_balance')
        .eq('user_session', 'default')
        .maybeSingle();
      if (!data || !Number(data.current_balance)) {
        let startBal = 1000;
        try {
          const cfg = JSON.parse(localStorage.getItem('paper_wallet_config') ?? '{}');
          startBal = cfg.startingBalance ?? 1000;
        } catch { /* use default */ }
        await supabase.from('bot_config').upsert({
          user_session: 'default',
          selected_coins: DEFAULT_COINS,
          mode: 'test',
          is_running: false,
          current_balance: startBal,
          initial_balance: startBal,
          stop_loss_percent: 1.5,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session' });
      }
    })();
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
            {/* Wallet — includes budget settings and reset */}
            <WalletPanelV2
              binanceConnected={binanceConnected}
              prices={prices}
              mode={binanceConnected ? 'live' : 'test'}
              selectedCoins={selectedCoins}
              agentPositions={agentPositions}
              agentBalance={agentBalance}
              agentInitialBalance={agentInitialBalance}
              agentTrades={agentTrades}
              onReset={() => { setAgentPositions([]); setAgentBalance(0); }}
            />

            {/* AI Trading Bot */}
            <AITradingAgent
              selectedCoins={selectedCoins}
              prices={prices}
              binanceConnected={binanceConnected}
              onConnectBinance={() => setShowBinanceConnect(true)}
              onStateChange={(pos, bal, initBal, trades) => {
                setAgentPositions(pos);
                setAgentBalance(bal);
                if (initBal) setAgentInitialBalance(initBal);
                if (trades) setAgentTrades(trades);
              }}
              onCoinsChange={setSelectedCoins}
            />

            {/* Bot Watch List */}
            <div className="trading-card p-3">
              <div className="text-xs font-medium text-muted-foreground mb-2">Bot Watch List</div>
              <CoinSelector selected={selectedCoins} onChange={setSelectedCoins} maxCoins={55} />
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
