import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import { API_BASE } from '@/config';
import TopBar from '@/components/dashboard/TopBar';
import VersionFooter from '@/components/dashboard/VersionFooter';
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
import OrderBookPanel from '@/components/dashboard/OrderBookPanel';
import FuturesAgent from '@/components/dashboard/FuturesAgent';
import { FunnelPanel } from '@/components/dashboard/FunnelPanel';
import { EvScorePanel } from '@/components/dashboard/EvScorePanel';
import { ShadowLabPanel } from '@/components/dashboard/ShadowLabPanel';
import { useBinanceWebSocket } from '@/hooks/useBinanceWebSocket';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';

const DEFAULT_COINS = [
  'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'DOGEUSDT',
  'XRPUSDT', 'ADAUSDT', 'AVAXUSDT', 'LINKUSDT', 'POLUSDT',
  'ARBUSDT',  'OPUSDT',  'INJUSDT', 'NEARUSDT', 'FETUSDT',
];

const COINS_KEY = 'trade_selected_coins';

const Index = () => {
  const [selectedCoins, setSelectedCoins] = useState<string[]>(() => {
    try {
      const saved = localStorage.getItem(COINS_KEY);
      if (saved) {
        const parsed = JSON.parse(saved) as string[];
        if (Array.isArray(parsed) && parsed.length > 0) return parsed;
      }
    } catch { /* fall through */ }
    return DEFAULT_COINS;
  });

  // Persist coin list whenever it changes so refreshes restore the same set
  useEffect(() => {
    try { localStorage.setItem(COINS_KEY, JSON.stringify(selectedCoins)); }
    catch { /* storage quota */ }
  }, [selectedCoins]);

  // If localStorage was empty (new browser / cleared), try Supabase wolfbot session
  useEffect(() => {
    const saved = localStorage.getItem(COINS_KEY);
    if (saved) return; // localStorage already has data — no need to fetch
    (async () => {
      try {
        const { data } = await supabase
          .from('bot_config')
          .select('selected_coins')
          .eq('user_session', 'wolfbot')
          .maybeSingle();
        const coins = data?.selected_coins;
        if (Array.isArray(coins) && coins.length > 0) {
          setSelectedCoins(coins as string[]);
        }
      } catch { /* non-fatal */ }
    })();
  }, []);
  const [activeCoin, setActiveCoin]        = useState('BTCUSDT');
  // Restore live mode from localStorage so the page loads in the correct state
  // instantly, without waiting for the API call (which can fail or be slow).
  const [binanceConnected, setBinanceConnected] = useState(() => {
    try { return localStorage.getItem('binance_mode') === 'live'; }
    catch { return false; }
  });
  const [showBinanceConnect, setShowBinanceConnect] = useState(false);
  const [agentPositions, setAgentPositions] = useState<{symbol:string;quantity:number;avg_entry_price:number}[]>([]);
  const [agentBalance, setAgentBalance] = useState(0);
  const [agentInitialBalance, setAgentInitialBalance] = useState(0);
  const [agentTrades, setAgentTrades] = useState<{side:'BUY'|'SELL';pnl:number|null;quantity:number;price:number}[]>([]);

  // WebSocket subscribes to the UNION of user-selected coins AND every open
  // position's symbol. Without including positions, the bot can hold a coin
  // (opened by Claude's strategy beyond the user's watchlist) for which the
  // frontend has no live price — the row then displays Now == Entry forever.
  // The symbol set is sorted+joined and compared by string so a new
  // agentPositions array reference (returned by every poll) does NOT cycle
  // the WebSocket connection — only an actual content change does.
  const positionSymbolKey = useMemo(
    () => agentPositions.map(p => p.symbol).filter(Boolean).sort().join(','),
    [agentPositions],
  );
  const selectedKey = useMemo(
    () => [...selectedCoins].sort().join(','),
    [selectedCoins],
  );
  const wsSymbols = useMemo(() => {
    const set = new Set<string>(selectedKey ? selectedKey.split(',') : []);
    if (positionSymbolKey) for (const s of positionSymbolKey.split(',')) set.add(s);
    return Array.from(set);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedKey, positionSymbolKey]);
  const { prices, connected: wsConnected } = useBinanceWebSocket(wsSymbols);

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

  // True once the mode has been confirmed by a successful backend response.
  // Until then the localStorage seed is display-only and must not be
  // re-persisted (a stale 'live' would otherwise refresh itself forever).
  const modeVerifiedRef = useRef(false);

  const setVerifiedConnected = useCallback((connected: boolean) => {
    modeVerifiedRef.current = true;
    setBinanceConnected(connected);
  }, []);

  // Persist mode to localStorage so any reload/device restores state instantly.
  // Only after backend verification — never write back the unverified seed.
  useEffect(() => {
    if (!modeVerifiedRef.current) return;
    try { localStorage.setItem('binance_mode', binanceConnected ? 'live' : 'paper'); }
    catch { /* quota */ }
  }, [binanceConnected]);

  // Verify actual bot mode on mount and correct if it drifted.
  // Never resets on network failure — keeps localStorage value so a slow or
  // restarting bot does not flash paper mode.
  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/api/status`, { cache: 'no-store' });
        if (!res.ok) return;
        const d = await res.json();
        setVerifiedConnected(d.mode === 'live');
      } catch { /* bot unreachable — keep localStorage value */ }
    })();
  }, [setVerifiedConnected]);

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
        onConnectionChange={setVerifiedConnected}
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
              onReset={(newBal) => { setAgentPositions([]); if (newBal) setAgentBalance(newBal); }}
            />

            {/* AI Trading Bot */}
            <AITradingAgent
              selectedCoins={selectedCoins}
              prices={prices}
              binanceConnected={binanceConnected}
              onConnectBinance={() => setShowBinanceConnect(true)}
              onLiveModeDetected={setVerifiedConnected}
              onStateChange={(pos, bal, initBal, trades) => {
                setAgentPositions(pos);
                setAgentBalance(bal);
                if (initBal) setAgentInitialBalance(initBal);
                if (trades) setAgentTrades(trades);
              }}
              onCoinsChange={setSelectedCoins}
            />

            {/* Entry Funnel — L1.2: today's ready→recheck→budget→posted→filled */}
            <FunnelPanel baseUrl={API_BASE} />

            {/* EV win-probability scores — S5: per-coin scores, ranking, expectancy, model */}
            <EvScorePanel baseUrl={API_BASE} />

            {/* Shadow-Lab — paper-shadow EV harvesting flywheel + copy-results */}
            <ShadowLabPanel baseUrl={API_BASE} />

            {/* Futures Paper-Trading Agent */}
            <FuturesAgent />

            {/* Order Book & 24h Stats */}
            <OrderBookPanel activeCoin={activeCoin} prices={prices} />

            {/* Bot Watch List */}
            <div className="trading-card p-3">
              <div className="text-xs font-medium text-muted-foreground mb-2">Bot Watch List</div>
              <CoinSelector selected={selectedCoins} onChange={setSelectedCoins} maxCoins={100} />
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

      {/* Persistent self-diagnosing version footer (I2) */}
      <VersionFooter />
    </div>
  );
};

export default Index;
