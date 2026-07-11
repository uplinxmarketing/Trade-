import { useState, useEffect, useCallback } from 'react';
import { RefreshCw, Loader2, Wifi, FlaskConical, RotateCcw, Lock, Percent, Zap, Layers, ChevronDown, ChevronUp, Check } from 'lucide-react';
import { formatTime } from '@/lib/format';
import { supabase } from '@/integrations/supabase/client';
import { TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { toast } from 'sonner';
import { API_BASE } from '@/config';
import { CoinIcon as SharedCoinIcon } from '@/components/CoinIcon';

const PAPER_CFG_KEY  = 'paper_wallet_config';

// All API calls are same-origin — bot runs on wolfbot.tech.
// Clean up any stale Railway/bot URL keys left in localStorage.
try {
  localStorage.removeItem('railway_bot_url');
  localStorage.removeItem('bot_server_url');
} catch { /* ignore */ }

const COIN_COLORS: Record<string, string> = {
  BTC: '#F7931A', ETH: '#627EEA', SOL: '#9945FF', BNB: '#F3BA2F',
  DOGE: '#C3A634', USDT: '#26A17B', ADA: '#0033AD', XRP: '#346AA9',
  DOT: '#E6007A', LINK: '#2A5ADA', AVAX: '#E84142', MATIC: '#8247E5',
  SHIB: '#FFA409', LTC: '#BFBBBB', UNI: '#FF007A', ATOM: '#2E3148',
};

const BREAK_EVEN = 1 / Math.pow(1 - TAKER_FEE, 2);

interface Position { symbol: string; quantity: number; avg_entry_price: number; }
interface LiveAsset { asset: string; free: string; locked: string; usdValue: number; }

type BudgetMode = 'fixed' | 'percent' | 'capped' | 'per_coin' | 'coin_pct';

interface WalletCfg {
  startingBalance: number;
  budgetMode: BudgetMode;
  budgetFixed: number;
  budgetPct: number;
  budgetCap: number;
  budgetPerCoin: Record<string, number>;
  budgetCoinPct: Record<string, number>;
}

const DEFAULT_CFG: WalletCfg = {
  startingBalance: 10000,
  budgetMode: 'percent',
  budgetFixed: 100,
  budgetPct: 5,
  budgetCap: 500,
  budgetPerCoin: { BTCUSDT: 200, ETHUSDT: 150, SOLUSDT: 100, BNBUSDT: 100, DOGEUSDT: 50 },
  budgetCoinPct: { BTCUSDT: 10, ETHUSDT: 8, SOLUSDT: 5, BNBUSDT: 5, DOGEUSDT: 3 },
};

const PRESET_BALANCES = [500, 1000, 5000, 10000];

const BUDGET_MODES: { key: BudgetMode; label: string; icon: React.ReactNode; desc: string }[] = [
  { key: 'fixed',    label: 'Fixed',      icon: <Lock className="w-3 h-3" />,    desc: 'Same USDT each trade' },
  { key: 'percent',  label: '% Bal',      icon: <Percent className="w-3 h-3" />, desc: 'Scales with balance' },
  { key: 'capped',   label: 'Cap',        icon: <Zap className="w-3 h-3" />,     desc: 'Max total deployed' },
  { key: 'per_coin', label: 'Per Coin',   icon: <Layers className="w-3 h-3" />,  desc: 'Fixed USDT per coin' },
  { key: 'coin_pct', label: 'Custom %',   icon: <Percent className="w-3 h-3" />, desc: '% of balance per coin' },
];

interface Props {
  binanceConnected: boolean;
  prices: LivePrices;
  mode: 'test' | 'live';
  selectedCoins?: string[];
  agentPositions?: { symbol: string; quantity: number; avg_entry_price: number }[];
  agentBalance?: number;
  agentInitialBalance?: number;
  agentTrades?: { side: 'BUY' | 'SELL'; pnl: number | null; quantity: number; price: number }[];
  onReset?: (newBalance: number) => void;
}

function CoinIcon({ coin }: { coin: string }) {
  // Delegate to the shared icon (real logo + initials fallback). `coin` here is a
  // base asset (BTC/USDT/ETH); the shared component tolerates base or full symbol.
  return <SharedCoinIcon symbol={coin} size={32} className="flex-shrink-0" />;
}

function PnlPill({ pnl, pct }: { pnl: number; pct: number }) {
  if (Math.abs(pnl) < 0.005) {
    return (
      <div className="rounded px-2 py-1 bg-muted/30 border border-border text-right min-w-[80px]">
        <div className="text-[10px] font-bold text-muted-foreground">0.00%</div>
        <div className="text-[9px] text-muted-foreground">+{pnl.toFixed(2)} USDT</div>
      </div>
    );
  }
  const gain = pnl > 0;
  return (
    <div className={`rounded px-2 py-1 border text-right min-w-[80px] ${gain ? 'bg-gain/10 border-gain/30' : 'bg-loss/10 border-loss/30'}`}>
      <div className={`text-[10px] font-bold ${gain ? 'text-gain' : 'text-loss'}`}>
        {gain ? '+' : ''}{pct.toFixed(2)}%
      </div>
      <div className={`text-[9px] ${gain ? 'text-gain' : 'text-loss'}`}>
        {gain ? '+' : ''}{pnl.toFixed(2)} USDT
      </div>
    </div>
  );
}

const WalletPanelV2 = ({ binanceConnected, prices, mode, selectedCoins, agentPositions, agentBalance, agentInitialBalance, agentTrades, onReset }: Props) => {
  const [usdtFree, setUsdtFree]         = useState(0);
  const [initialBalance, setInitialBalance] = useState(0);
  const [positions, setPositions]       = useState<Position[]>([]);
  const [liveAssets, setLiveAssets]     = useState<LiveAsset[]>([]);
  const [sessionGain, setSessionGain]   = useState(0);
  const [totalFees, setTotalFees]       = useState(0);
  const [loading, setLoading]           = useState(false);
  const [resetting, setResetting]       = useState(false);
  const [lastUpdated, setLastUpdated]   = useState('');
  const [showBudget, setShowBudget]     = useState(true);

  // ── Budget config (localStorage) ─────────────────────────────────────────────
  const [walletCfg, setWalletCfg] = useState<WalletCfg>(DEFAULT_CFG);
  // draftCfg holds uncommitted budget value edits; synced to server only on Apply
  const [draftCfg, setDraftCfg]   = useState<WalletCfg>(DEFAULT_CFG);

  useEffect(() => {
    try {
      const saved = JSON.parse(localStorage.getItem(PAPER_CFG_KEY) ?? '{}');
      const merged = { ...DEFAULT_CFG, ...saved };
      setWalletCfg(merged);
      setDraftCfg(merged);
    } catch { /* use defaults */ }
  }, []);

  // saveCfg: immediate local commit (mode, startingBalance) — no server call
  const saveCfg = useCallback((updates: Partial<WalletCfg>) => {
    setWalletCfg(prev => {
      const next = { ...prev, ...updates };
      try { localStorage.setItem(PAPER_CFG_KEY, JSON.stringify(next)); } catch { /* */ }
      return next;
    });
    // Keep draft in sync for non-value fields
    setDraftCfg(prev => ({ ...prev, ...updates }));
  }, []);

  // commitDraft: apply pending draft budget values → walletCfg + localStorage + server
  const commitDraft = useCallback(() => {
    setWalletCfg(prev => {
      const next = { ...prev, ...draftCfg };
      try { localStorage.setItem(PAPER_CFG_KEY, JSON.stringify(next)); } catch { /* */ }
      const patch = {
        budget_mode:           next.budgetMode,
        budget_fixed_usdt:     next.budgetFixed,
        budget_pct_of_free:    next.budgetPct,
        budget_total_cap_usdt: next.budgetCap,
        budget_per_coin:       next.budgetPerCoin,
        budget_coin_pct:       next.budgetCoinPct,
      };
      const url = API_BASE;
      fetch(`${url}/config`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch),
      }).then(r => r.json()).then(d => {
        if (d.ok) toast.success('Budget settings applied to bot');
        else toast.error('Server rejected: ' + (d.error ?? 'unknown'));
      }).catch(() => toast.error('Bot unreachable — check bot connection'));
      return next;
    });
  }, [draftCfg]);

  const hasPendingBudget =
    draftCfg.budgetMode    !== walletCfg.budgetMode    ||
    draftCfg.budgetFixed   !== walletCfg.budgetFixed   ||
    draftCfg.budgetPct     !== walletCfg.budgetPct     ||
    draftCfg.budgetCap     !== walletCfg.budgetCap     ||
    JSON.stringify(draftCfg.budgetPerCoin)  !== JSON.stringify(walletCfg.budgetPerCoin)  ||
    JSON.stringify(draftCfg.budgetCoinPct)  !== JSON.stringify(walletCfg.budgetCoinPct);

  // ── Paper / test mode ──────────────────────────────────────────────
  // No dependency on walletCfg state — reads localStorage directly so the callback
  // stays stable and the Supabase realtime subscription is never recreated on cfg changes.
  const loadPaper = useCallback(async () => {
    const localStartBal = (() => {
      try { return JSON.parse(localStorage.getItem(PAPER_CFG_KEY) ?? '{}').startingBalance ?? 1000; }
      catch { return 1000; }
    })();
    const [cfgRes, posRes, tradeRes] = await Promise.all([
      supabase.from('bot_config').select('current_balance,initial_balance,selected_coins').eq('user_session','default').maybeSingle(),
      supabase.from('paper_portfolio').select('*').eq('user_session','default').gt('quantity',0),
      supabase.from('bot_trade_history').select('side,pnl,quantity,price').eq('user_session','default'),
    ]);
    if (cfgRes.data) {
      const bal = Number(cfgRes.data.current_balance);
      const initBal = Number(cfgRes.data.initial_balance);
      setUsdtFree(bal > 0 ? bal : initBal > 0 ? initBal : localStartBal);
      setInitialBalance(initBal > 0 ? initBal : localStartBal);
    } else {
      setUsdtFree(localStartBal);
      setInitialBalance(localStartBal);
    }
    if (posRes.data) setPositions(posRes.data as Position[]);
    if (tradeRes.data) {
      const realized = tradeRes.data.filter(t=>t.side==='SELL'&&t.pnl!=null).reduce((s,t)=>s+(t.pnl||0),0);
      setSessionGain(Math.round(realized*100)/100);
      const fees = tradeRes.data.reduce((s,t)=>{
        const qty=Number(t.quantity), price=Number(t.price);
        if(t.side==='BUY')  return s+(qty*price/(1-TAKER_FEE))*TAKER_FEE;
        if(t.side==='SELL') return s+qty*price*TAKER_FEE;
        return s;
      },0);
      setTotalFees(Math.round(fees*10000)/10000);
    }
    setLastUpdated(formatTime(new Date()));
  }, []);

  // ── Live mode ──────────────────────────────────────────────────────────────
  // Reads directly from /api/wallet which calls the real Binance API
  // when the bot is in live mode (MODE=live env var). No Supabase proxy needed.
  const loadLive = useCallback(async () => {
    if (!binanceConnected) return;
    setLoading(true);
    try {
      const url = API_BASE;
      const res = await fetch(`${url}/api/wallet`, { cache: 'no-store' });
      if (!res.ok) { toast.error('Failed to load live wallet'); return; }
      const data = await res.json();
      if (data.account_fetch_ok === false) {
        toast.error('Binance account fetch failed — check API keys and server logs');
      }
      const nonZero = (data.balances || []).filter((b: any) =>
        parseFloat(String(b.free)) + parseFloat(String(b.locked)) > 0.0001
      );
      const assets: LiveAsset[] = [];
      for (const b of nonZero) {
        const asset = b.asset as string;
        const total = parseFloat(String(b.free)) + parseFloat(String(b.locked));
        let usdValue = 0;
        if (['USDT', 'BUSD', 'USDC'].includes(asset)) {
          usdValue = total;
        } else {
          const lp = prices[`${asset}USDT`];
          if (lp) {
            usdValue = total * parseFloat(lp.price);
          } else {
            // Coin not in watched list — fetch price from Binance REST
            try {
              if (asset === 'USD' || asset.startsWith('LD')) continue;
              const pr = await fetch(`/api/proxy/binance/ticker/price?symbol=${asset}USDT`);
              if (pr.ok) {
                const pd = await pr.json();
                if (pd.price) usdValue = total * parseFloat(pd.price);
              }
            } catch { /* skip — show with $0 value */ }
          }
        }
        // Show all coins with meaningful quantity, even if price unavailable
        if (total > 0.0001) assets.push({ asset, free: String(b.free), locked: String(b.locked), usdValue });
      }
      assets.sort((a, b) => b.usdValue - a.usdValue);
      setLiveAssets(assets);
      setLastUpdated(formatTime(new Date()));
    } catch { toast.error('Live wallet load error'); }
    finally { setLoading(false); }
  }, [binanceConnected, prices]);

  // ── Use agent-provided state when available (instant sync / server mode) ─────
  // Use agentInitialBalance (never reset to 0) as the Railway-mode flag so that
  // resetting the wallet (which briefly sets agentBalance=0) doesn't flip back to Supabase mode.
  // hasAgentData = true whenever the bot is the authoritative source.
  // Including binanceConnected here ensures paper/Supabase data is never
  // loaded on initial render when the user is in live mode — even before
  // the first poll returns agentBalance/agentInitialBalance.
  const hasAgentData = binanceConnected || (agentInitialBalance ?? 0) > 0 || (agentBalance ?? 0) > 0;

  const isPaper = mode === 'test' || !binanceConnected;

  // Paper subscription + polling — disabled when Railway is the backend
  // (hasAgentData=true means /api/wallet is the authoritative source)
  useEffect(() => {
    if (mode === 'live' && binanceConnected) return;
    if (hasAgentData) return; // server mode: no Supabase needed
    loadPaper();
    const ch = supabase.channel('walletv2')
      .on('postgres_changes',{event:'*',schema:'public',table:'bot_config'},loadPaper)
      .on('postgres_changes',{event:'*',schema:'public',table:'paper_portfolio'},loadPaper)
      .on('postgres_changes',{event:'*',schema:'public',table:'bot_trade_history'},loadPaper)
      .subscribe();
    const poll = setInterval(loadPaper, 5000);
    return () => { supabase.removeChannel(ch); clearInterval(poll); };
  }, [mode, binanceConnected, loadPaper, hasAgentData]);

  // Live mode — load immediately on connect, then poll every 15 s for fresh balances
  useEffect(() => {
    if (mode !== 'live' || !binanceConnected) return;
    loadLive();
    const id = setInterval(loadLive, 15_000);
    return () => clearInterval(id);
  }, [mode, binanceConnected]); // eslint-disable-line

  // ── Server wallet — poll /api/wallet for authoritative P&L numbers ─────────
  // Replaces Supabase-derived P&L when server is the backend.
  const [serverWallet, setServerWallet] = useState<{
    realized_pnl: number; session_pnl: number; starting_balance: number;
    total_fees: number | null; open_pos_value: number;
  } | null>(null);

  useEffect(() => {
    if (!hasAgentData) return;
    let cancelled = false;
    const poll = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/wallet`, { cache: 'no-store' });
        if (!res.ok || cancelled) return;
        const d = await res.json();
        if (!cancelled) setServerWallet({
          realized_pnl:     Number(d.realized_pnl     ?? 0),
          session_pnl:      Number(d.session_pnl      ?? 0),
          starting_balance: Number(d.starting_balance ?? 0),
          // null (not 0) when the backend omits the key so the local fee
          // estimate below can take over instead of showing a fake 0.
          total_fees:       d.total_fees !== undefined && d.total_fees !== null ? Number(d.total_fees) : null,
          open_pos_value:   Number(d.open_pos_value    ?? 0),
        });
      } catch {}
    };
    poll();
    const id = setInterval(poll, 5_000);
    return () => { cancelled = true; clearInterval(id); };
  }, [hasAgentData]); // eslint-disable-line
  // In server mode keep local `positions` state in sync with parent's agentPositions
  // so the refresh button (which writes to `positions` directly) and the AITradingAgent
  // poll both flow through the same variable.
  useEffect(() => {
    if (hasAgentData && agentPositions) setPositions(agentPositions);
  }, [agentPositions, hasAgentData]);

  // Keep usdtFree in sync with agent balance polls
  useEffect(() => {
    if (hasAgentData && (agentBalance ?? 0) > 0) setUsdtFree(agentBalance!);
  }, [agentBalance, hasAgentData]);

  const effectivePositions = positions;
  const effectiveUsdtFree  = hasAgentData ? (usdtFree > 0 ? usdtFree : (agentBalance ?? 0)) : usdtFree;
  const effectiveInitBal   = (agentInitialBalance && agentInitialBalance > 0) ? agentInitialBalance : initialBalance;

  // Session stats: server wallet (/api/wallet SQL) > agent trades > Supabase fallback
  const effectiveSells = agentTrades
    ? agentTrades.filter(t => (t.side === 'SELL' || (t.side as string).toLowerCase() === 'sell') && t.pnl != null)
    : [];
  const tradesRealizedPnl = effectiveSells.length > 0
    ? Math.round(effectiveSells.reduce((s, t) => s + (t.pnl ?? 0), 0) * 100) / 100
    : sessionGain;
  // Authoritative realized P&L: prefer /api/wallet SQL sum
  const effectiveSessionGain = serverWallet ? serverWallet.realized_pnl : tradesRealizedPnl;
  // Server total_fees covers both buy AND sell fees from the trades table.
  // Fallback: estimate from agentTrades sell side only (misses buy fees).
  const effectiveFees = serverWallet && serverWallet.total_fees !== null
    ? serverWallet.total_fees
    : effectiveSells.length > 0
      ? Math.round(effectiveSells.reduce((s, t) => {
          const qty = Number(t.quantity), price = Number(t.price);
          // Both sides: buy fee ≈ qty*price*rate, sell fee = qty*price*rate
          return s + qty * price * TAKER_FEE;
        }, 0) * 2 * 10000) / 10000  // ×2 approximates buy+sell fees
      : totalFees;

  // ── Reset paper wallet ───────────────────────────────────────────────────
  const resetWallet = async () => {
    // Use server's authoritative starting balance when available, else local config
    const resetBal = (serverWallet?.starting_balance ?? 0) > 0
      ? serverWallet!.starting_balance
      : walletCfg.startingBalance;
    if (!confirm(`Reset paper wallet to ${resetBal.toLocaleString()} USDT and clear all positions?`)) return;
    setResetting(true);
    try {
      // Only call server when in agent/server mode — pure-paper users without
      // a bot URL would otherwise always hit the network error path and
      // never reach the Supabase reset below.
      if (hasAgentData) {
        const res = await fetch(`${API_BASE}/api/reset`, { method: 'POST' }).catch(() => null);
        if (!res?.ok) {
          toast.error('Server reset failed — check bot logs');
          return;
        }
      }

      if (!hasAgentData) {
        // Pure paper mode (no server): sync Supabase too
        await Promise.all([
          supabase.from('paper_portfolio').delete().eq('user_session', 'default'),
          supabase.from('bot_trade_history').delete().eq('user_session', 'default'),
          supabase.from('bot_config').update({
            current_balance: resetBal,
            initial_balance: resetBal,
            is_running: false,
            updated_at: new Date().toISOString(),
          }).eq('user_session', 'default'),
        ]);
      }

      setPositions([]);
      setUsdtFree(resetBal);
      setInitialBalance(resetBal);
      setSessionGain(0);
      setTotalFees(0);
      setServerWallet(null);
      onReset?.(resetBal);
      toast.success(`Paper wallet reset · ${resetBal.toLocaleString()} USDT`);
    } finally { setResetting(false); }
  };

  // ── Compute portfolio totals (paper mode) ─────────────────────────────────────────
  // Number() casts guard against Supabase PostgREST returning NUMERIC columns as strings.
  const positionRows = effectivePositions.map(pos => {
    // toNum: guarantees a finite, non-negative number — malformed server/Supabase
    // rows (string "abc", null, or NaN-yielding parses) would otherwise propagate
    // into pnl/pct as NaN and render as "NaN%" / impossibly large losses.
    const toNum = (v: unknown) => {
      const n = typeof v === 'number' ? v : parseFloat(String(v ?? ''));
      return Number.isFinite(n) && n >= 0 ? n : 0;
    };
    const qty        = toNum(pos.quantity);
    const entryPrice = toNum(pos.avg_entry_price);
    const coin       = pos.symbol.replace('USDT','');
    const wsPrice    = toNum(prices[pos.symbol]?.price);
    const livePrice  = wsPrice > 0 ? wsPrice : entryPrice;
    const buyValue     = qty * entryPrice;
    const currentValue = qty * livePrice;
    // NET P&L (take-home if closed now): gross move minus the round-trip fee
    // (entry already paid + estimated exit) — matches the break-even line below.
    const pnl          = currentValue - buyValue - (currentValue + buyValue) * TAKER_FEE;
    let pct = entryPrice > 0 ? ((livePrice - entryPrice) / entryPrice) * 100 : 0;
    if (!Number.isFinite(pct)) pct = 0;
    if (pct < -100) pct = -100;
    const costBasis    = qty * entryPrice / (1 - TAKER_FEE);
    const breakEven    = entryPrice * BREAK_EVEN;
    return { coin, pos, qty, entryPrice, livePrice, currentValue, buyValue, costBasis, pnl, pct, breakEven };
  });

  const paperPositionTotal = positionRows.reduce((s,r)=>s+r.currentValue, 0);
  const totalPortfolio     = effectiveUsdtFree + paperPositionTotal;
  // Authoritative starting balance: /api/wallet > agentInitialBalance > Supabase
  const displayStartingBal = (serverWallet?.starting_balance ?? 0) > 0
    ? serverWallet!.starting_balance
    : effectiveInitBal;
  // Session P&L in USDT: /api/wallet computes total_value - starting_balance server-side
  const displaySessionPnl  = serverWallet ? serverWallet.session_pnl : (totalPortfolio - displayStartingBal);
  const sessionGainPct     = displayStartingBal > 0 ? (displaySessionPnl / displayStartingBal) * 100 : 0;

  // ── Live mode totals ─────────────────────────────────────────────────────
  const liveTotal = liveAssets.reduce((s,a)=>s+a.usdValue, 0);

  // ── Budget mode description for active mode ─────────────────────────────────────────
  const watchCoins = selectedCoins ?? Object.keys(walletCfg.budgetPerCoin);

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      <div className="px-4 py-2 border-b border-border bg-muted/20 flex items-center gap-2">
        <span className="text-[9px] uppercase tracking-widest text-muted-foreground font-semibold">Wallet (Spot)</span>
        <span className={`ml-auto text-[9px] px-2 py-0.5 rounded border font-semibold ${isPaper ? 'border-accent/40 text-accent bg-accent/10' : 'border-gain/40 text-gain bg-gain/10'}`}>
          {isPaper ? <><FlaskConical className="w-2.5 h-2.5 inline mr-1" />PAPER</> : <><Wifi className="w-2.5 h-2.5 inline mr-1" />LIVE</>}
        </span>
        <button onClick={async () => {
          if (hasAgentData) {
            // Server mode — fetch fresh data directly from the bot
            setLoading(true);
            try {
              const [walRes, posRes] = await Promise.all([
                fetch(`${API_BASE}/api/wallet`, { cache: 'no-store' }).then(r => r.ok ? r.json() : null).catch(() => null),
                fetch(`${API_BASE}/api/positions`, { cache: 'no-store' }).then(r => r.ok ? r.json() : null).catch(() => null),
              ]);
              if (walRes) setServerWallet({ realized_pnl: Number(walRes.realized_pnl ?? 0), session_pnl: Number(walRes.session_pnl ?? 0), starting_balance: Number(walRes.starting_balance ?? 0), total_fees: walRes.total_fees !== undefined && walRes.total_fees !== null ? Number(walRes.total_fees) : null, open_pos_value: Number(walRes.open_pos_value ?? 0) });
              if (walRes?.total_usdt !== undefined) setUsdtFree(Number(walRes.total_usdt));
              if (posRes?.positions) setPositions(posRes.positions as Position[]);
              setLastUpdated(formatTime(new Date()));
            } finally { setLoading(false); }
          } else if (isPaper) {
            loadPaper();
          } else {
            loadLive();
          }
        }}
          className="p-1 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground" title="Refresh">
          {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <RefreshCw className="w-3 h-3" />}
        </button>
      </div>

      {/* ── Header: total balance ── */}
      <div className="px-4 pt-3 pb-2 border-b border-border/50 flex items-end justify-between gap-4">
        <div>
          <div className="text-[10px] text-muted-foreground uppercase tracking-widest mb-0.5">Total spot balance</div>
          <div id="wallet-total" className="text-2xl font-bold font-mono tabular-nums">
            {(isPaper ? totalPortfolio : liveTotal).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}
            <span className="text-sm text-muted-foreground ml-1 font-normal">USDT</span>
          </div>
          <div className="text-[10px] text-muted-foreground font-mono mt-0.5">
            ≈ ${(isPaper ? totalPortfolio : liveTotal).toFixed(2)} USD · updated {lastUpdated || 'live via WebSocket'}
          </div>
        </div>
        <div className="text-right space-y-0.5 shrink-0">
          {isPaper && (
            <>
              <div className={`text-xs font-mono font-semibold ${effectiveSessionGain >= 0 ? 'text-gain' : 'text-loss'}`}>
                {effectiveSessionGain >= 0 ? '+' : ''}{effectiveSessionGain.toFixed(2)} USDT realized P&L
              </div>
              <div className={`text-[10px] font-mono ${displaySessionPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                {displaySessionPnl >= 0 ? '+' : ''}{displaySessionPnl.toFixed(2)} USDT
                {' '}({sessionGainPct >= 0 ? '+' : ''}{sessionGainPct.toFixed(2)}%) total session
              </div>
              <div className="text-[10px] text-muted-foreground">
                Fees (buy+sell): <span className="font-mono text-foreground">{effectiveFees.toFixed(4)} USDT</span>
              </div>
              {(serverWallet?.open_pos_value ?? 0) > 0 && (
                <div className="text-[10px] text-muted-foreground">
                  Open positions: <span className="font-mono text-foreground">{(serverWallet!.open_pos_value).toFixed(2)} USDT</span>
                </div>
              )}
            </>
          )}
        </div>
      </div>

      {/* ── Coin table header ── */}
      <div className="grid grid-cols-[auto_1fr_auto_auto_auto] gap-x-4 px-4 py-1.5 border-b border-border/30 bg-muted/10">
        {['COIN','QUANTITY HELD','VALUE (USDT)','COST BASIS','P&L'].map(h=>(
          <div key={h} className="text-[9px] uppercase tracking-widest text-muted-foreground font-semibold">{h}</div>
        ))}
      </div>

      {/* ── Paper mode rows ── */}
      {isPaper && (
        <div id="wallet-tbody" className="divide-y divide-border/30">
          {positionRows.map(({ coin, pos, qty, livePrice, currentValue, costBasis, pnl, pct }) => (
            <div key={pos.symbol} className="grid grid-cols-[auto_1fr_auto_auto_auto] gap-x-4 items-center px-4 py-2.5 hover:bg-muted/10 transition-colors">
              <div className="flex items-center gap-2">
                <CoinIcon coin={coin} />
                <div>
                  <div className="text-xs font-bold">{coin}</div>
                  <div className="text-[9px] text-muted-foreground">{coin} <span className="opacity-60">(paper)</span></div>
                </div>
              </div>
              <div className="font-mono text-xs tabular-nums">{qty < 0.01 ? qty.toFixed(6) : qty.toFixed(4)}</div>
              <div className="font-mono text-xs tabular-nums text-right">
                <div>{currentValue.toFixed(2)} USDT</div>
                <div className="text-[9px] text-muted-foreground">@ {livePrice.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:4})} USDT</div>
              </div>
              <div className="font-mono text-xs tabular-nums text-right">
                {costBasis.toFixed(2)} USDT
              </div>
              <PnlPill pnl={pnl} pct={pct} />
            </div>
          ))}

          {/* USDT free row */}
          <div className="grid grid-cols-[auto_1fr_auto_auto_auto] gap-x-4 items-center px-4 py-2.5 hover:bg-muted/10 transition-colors">
            <div className="flex items-center gap-2">
              <CoinIcon coin="USDT" />
              <div>
                <div className="text-xs font-bold">USDT</div>
                <div className="text-[9px] text-muted-foreground">Tether <span className="opacity-60">(paper)</span></div>
              </div>
            </div>
            <div className="font-mono text-xs tabular-nums text-muted-foreground">—</div>
            <div className="font-mono text-xs tabular-nums text-right">
              <div>{effectiveUsdtFree.toFixed(2)} USDT</div>
              <div className="text-[9px] text-muted-foreground">≈ ${effectiveUsdtFree.toFixed(2)} USD</div>
            </div>
            <div className="font-mono text-xs tabular-nums text-right text-muted-foreground">—</div>
            <div className="rounded px-2 py-1 bg-muted/20 border border-border text-right min-w-[80px]">
              <div className="text-[10px] text-muted-foreground font-semibold">stablecoin</div>
            </div>
          </div>

          {positionRows.length === 0 && effectiveUsdtFree === 0 && (
            <div className="px-4 py-6 text-center text-xs text-muted-foreground">
              No positions — start the AI agent to begin paper trading
            </div>
          )}
        </div>
      )}

      {/* ── Live mode rows ── */}
      {!isPaper && (
        <div className="divide-y divide-border/30">
          {liveAssets.map(a => {
            const lp = prices[`${a.asset}USDT`];
            const livePrice = lp ? parseFloat(lp.price) : 0;
            const isStable = ['USDT','BUSD','USDC'].includes(a.asset);
            return (
              <div key={a.asset} className="grid grid-cols-[auto_1fr_auto_auto_auto] gap-x-4 items-center px-4 py-2.5 hover:bg-muted/10">
                <div className="flex items-center gap-2">
                  <CoinIcon coin={a.asset} />
                  <div>
                    <div className="text-xs font-bold">{a.asset}</div>
                    <div className="text-[9px] text-muted-foreground">{a.asset}</div>
                  </div>
                </div>
                <div className="font-mono text-xs tabular-nums">{parseFloat(a.free).toFixed(6)}</div>
                <div className="font-mono text-xs tabular-nums text-right">
                  <div>{a.usdValue.toFixed(2)} USDT</div>
                  {livePrice > 0 && <div className="text-[9px] text-muted-foreground">@ {livePrice.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:4})} USDT</div>}
                </div>
                <div className="font-mono text-xs tabular-nums text-right text-muted-foreground">—</div>
                {isStable ? (
                  <div className="rounded px-2 py-1 bg-muted/20 border border-border text-right min-w-[80px]">
                    <div className="text-[10px] text-muted-foreground font-semibold">stablecoin</div>
                  </div>
                ) : (
                  <div className="rounded px-2 py-1 bg-muted/20 border border-border text-right min-w-[80px]">
                    <div className="text-[10px] text-muted-foreground">live</div>
                  </div>
                )}
              </div>
            );
          })}
          {liveAssets.length === 0 && (
            <div className="px-4 py-6 text-center text-xs text-muted-foreground">
              {!binanceConnected
                ? 'Connect Binance API to see live wallet'
                : loading
                ? 'Fetching live balances…'
                : 'No assets in spot wallet — transfer USDT from Earn/Funding to Spot on Binance'}
            </div>
          )}
        </div>
      )}

      {/* ── Paper wallet controls (budget + reset) ── */}
      {isPaper && (
        <div className="border-t border-border/50">
          {/* collapsible header */}
          <button
            onClick={() => setShowBudget(p => !p)}
            className="w-full flex items-center justify-between px-4 py-2 hover:bg-muted/10 text-left"
          >
            <span className="text-[9px] uppercase tracking-widest text-muted-foreground font-semibold">
              {hasAgentData ? 'Bot' : 'Paper'} Settings · Starting {((serverWallet?.starting_balance ?? 0) > 0 ? serverWallet!.starting_balance : walletCfg.startingBalance).toLocaleString()} USDT · Mode: {walletCfg.budgetMode}
            </span>
            <div className="flex items-center gap-2">
              <button
                onClick={e => { e.stopPropagation(); resetWallet(); }}
                disabled={resetting}
                className="flex items-center gap-1 text-[9px] text-loss hover:text-loss/80 px-2 py-0.5 rounded border border-loss/30 hover:border-loss/60 transition-colors disabled:opacity-40"
                title="Reset paper wallet"
              >
                <RotateCcw className={`w-2.5 h-2.5 ${resetting ? 'animate-spin' : ''}`} />
                Reset wallet
              </button>
              {showBudget ? <ChevronUp className="w-3 h-3 text-muted-foreground" /> : <ChevronDown className="w-3 h-3 text-muted-foreground" />}
            </div>
          </button>

          {showBudget && (
            <div className="px-4 pb-4 space-y-4 border-t border-border/30 pt-3">

              {/* Starting balance */}
              <div>
                <div className="text-[9px] uppercase tracking-widest text-muted-foreground mb-2">Starting Balance (USDT)</div>
                {hasAgentData ? (
                  <div className="text-[10px] text-muted-foreground bg-muted/30 border border-border rounded px-3 py-2">
                    <span className="font-mono font-semibold text-foreground">{((serverWallet?.starting_balance ?? 0) > 0 ? serverWallet!.starting_balance : walletCfg.startingBalance).toLocaleString()} USDT</span>
                    <span className="ml-2 opacity-70">— set by <code>STARTING_PAPER_USDT</code> server env var</span>
                  </div>
                ) : (
                  <div className="flex items-center gap-1.5 flex-wrap">
                    {PRESET_BALANCES.map(p => (
                      <button
                        key={p}
                        onClick={() => saveCfg({ startingBalance: p })}
                        className={`text-[10px] px-2.5 py-1 rounded font-mono transition-colors border
                          ${walletCfg.startingBalance === p
                            ? 'bg-accent text-accent-foreground border-accent'
                            : 'bg-muted/30 text-muted-foreground border-border hover:text-foreground hover:border-accent/50'
                          }`}
                      >
                        {p.toLocaleString()} USDT
                      </button>
                    ))}
                    <input
                      type="number"
                      min={100}
                      step={100}
                      value={walletCfg.startingBalance}
                      onChange={e => saveCfg({ startingBalance: Math.max(100, Number(e.target.value) || 10000) })}
                      className="w-28 bg-muted/40 border border-border rounded px-2 py-1 text-[10px] font-mono focus:outline-none focus:border-accent"
                      placeholder="Custom"
                    />
                  </div>
                )}
              </div>

              {/* Trade Size Mode + budget allocation now live in the AI Agent panel */}
              <div className="text-[9px] text-muted-foreground italic">
                Trade Size Mode &amp; Bot Allocation moved to the AI Agent panel — configure them there.
              </div>
            </div>
          )}
        </div>
      )}

      <div className="px-4 py-2 border-t border-border/30 bg-muted/10">
        <p className="text-[9px] text-muted-foreground">
          {isPaper
            ? 'Paper wallet — all positions are simulated · green = profitable · red = at loss · gray = at breakeven · values in USDT'
            : 'Live Binance spot wallet · values update with WebSocket prices'}
        </p>
      </div>
    </div>
  );
};

export default WalletPanelV2;
