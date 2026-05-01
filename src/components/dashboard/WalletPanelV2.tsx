import { useState, useEffect, useCallback } from 'react';
import { RefreshCw, Loader2, Wifi, FlaskConical } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';
import { TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { toast } from 'sonner';

const COIN_COLORS: Record<string, string> = {
  BTC: '#F7931A', ETH: '#627EEA', SOL: '#9945FF', BNB: '#F3BA2F',
  DOGE: '#C3A634', USDT: '#26A17B', ADA: '#0033AD', XRP: '#346AA9',
  DOT: '#E6007A', LINK: '#2A5ADA', AVAX: '#E84142', MATIC: '#8247E5',
  SHIB: '#FFA409', LTC: '#BFBBBB', UNI: '#FF007A', ATOM: '#2E3148',
};

const BREAK_EVEN = 1 / Math.pow(1 - TAKER_FEE, 2);

interface Position { symbol: string; quantity: number; avg_entry_price: number; }
interface LiveAsset { asset: string; free: string; locked: string; usdValue: number; }

interface Props {
  binanceConnected: boolean;
  prices: LivePrices;
  mode: 'test' | 'live';
}

function CoinIcon({ coin }: { coin: string }) {
  const color = COIN_COLORS[coin] ?? '#6b7280';
  return (
    <div style={{ background: color }} className="w-8 h-8 rounded-full flex items-center justify-center text-white text-[10px] font-bold flex-shrink-0 select-none">
      {coin.slice(0, 3)}
    </div>
  );
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

const WalletPanelV2 = ({ binanceConnected, prices, mode }: Props) => {
  const [usdtFree, setUsdtFree]         = useState(0);
  const [initialBalance, setInitialBalance] = useState(0);
  const [positions, setPositions]       = useState<Position[]>([]);
  const [liveAssets, setLiveAssets]     = useState<LiveAsset[]>([]);
  const [sessionGain, setSessionGain]   = useState(0);
  const [totalFees, setTotalFees]       = useState(0);
  const [loading, setLoading]           = useState(false);
  const [lastUpdated, setLastUpdated]   = useState('');

  // ── Paper / test mode ────────────────────────────────────────────────────────
  const loadPaper = useCallback(async () => {
    const [cfgRes, posRes, tradeRes] = await Promise.all([
      supabase.from('bot_config').select('current_balance,initial_balance').eq('user_session','default').maybeSingle(),
      supabase.from('paper_portfolio').select('*').eq('user_session','default').gt('quantity',0),
      supabase.from('bot_trade_history').select('side,pnl,quantity,price').eq('user_session','default'),
    ]);
    if (cfgRes.data) {
      setUsdtFree(Number(cfgRes.data.current_balance));
      setInitialBalance(Number(cfgRes.data.initial_balance));
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
    setLastUpdated(new Date().toLocaleTimeString());
  }, []);

  // ── Live mode ────────────────────────────────────────────────────────────────
  const loadLive = useCallback(async () => {
    if (!binanceConnected) return;
    setLoading(true);
    try {
      const { data, error } = await supabase.functions.invoke('binance-proxy', {
        body: { action: 'account', params: {} },
      });
      if (error || data?.error) { toast.error('Failed to load live wallet'); return; }
      const nonZero = (data.balances || []).filter((b:any) => parseFloat(b.free)+parseFloat(b.locked) > 0.0001);
      const assets: LiveAsset[] = [];
      for (const b of nonZero) {
        const asset = b.asset as string;
        const total = parseFloat(b.free)+parseFloat(b.locked);
        let usdValue = 0;
        if (['USDT','BUSD','USDC'].includes(asset)) {
          usdValue = total;
        } else {
          const lp = prices[`${asset}USDT`];
          if (lp) usdValue = total * parseFloat(lp.price);
        }
        if (usdValue >= 0.01) assets.push({ asset, free: b.free, locked: b.locked, usdValue });
      }
      assets.sort((a,b)=>b.usdValue-a.usdValue);
      setLiveAssets(assets);
      setLastUpdated(new Date().toLocaleTimeString());
    } finally { setLoading(false); }
  }, [binanceConnected, prices]);

  useEffect(() => {
    if (mode === 'test' || !binanceConnected) {
      loadPaper();
      const ch = supabase.channel('walletv2')
        .on('postgres_changes',{event:'*',schema:'public',table:'bot_config'},loadPaper)
        .on('postgres_changes',{event:'*',schema:'public',table:'paper_portfolio'},loadPaper)
        .on('postgres_changes',{event:'*',schema:'public',table:'bot_trade_history'},loadPaper)
        .subscribe();
      return () => { supabase.removeChannel(ch); };
    } else {
      loadLive();
    }
  }, [mode, binanceConnected, loadPaper, loadLive]);

  const isPaper = mode === 'test' || !binanceConnected;

  // ── Compute portfolio totals (paper mode) ────────────────────────────────────
  const positionRows = positions.map(pos => {
    const coin = pos.symbol.replace('USDT','');
    const livePrice = parseFloat(prices[pos.symbol]?.price||'0') || pos.avg_entry_price;
    const currentValue = pos.quantity * livePrice;
    const costBasis    = pos.quantity * pos.avg_entry_price / (1-TAKER_FEE);
    const pnl          = currentValue - costBasis;
    const pct          = costBasis > 0 ? (pnl/costBasis)*100 : 0;
    const breakEven    = pos.avg_entry_price * BREAK_EVEN;
    return { coin, pos, livePrice, currentValue, costBasis, pnl, pct, breakEven };
  });

  const paperPositionTotal = positionRows.reduce((s,r)=>s+r.currentValue, 0);
  const totalPortfolio     = usdtFree + paperPositionTotal;
  const sessionGainPct     = initialBalance > 0 ? ((totalPortfolio-initialBalance)/initialBalance)*100 : 0;

  // ── Live mode totals ─────────────────────────────────────────────────────────
  const liveTotal = liveAssets.reduce((s,a)=>s+a.usdValue, 0);

  return (
    <div className="bg-card border border-border rounded-lg overflow-hidden">
      <div className="px-4 py-2 border-b border-border bg-muted/20 flex items-center gap-2">
        <span className="text-[9px] uppercase tracking-widest text-muted-foreground font-semibold">A — Wallet Panel (Spot)</span>
        <span className={`ml-auto text-[9px] px-2 py-0.5 rounded border font-semibold ${isPaper ? 'border-accent/40 text-accent bg-accent/10' : 'border-gain/40 text-gain bg-gain/10'}`}>
          {isPaper ? <><FlaskConical className="w-2.5 h-2.5 inline mr-1" />PAPER</> : <><Wifi className="w-2.5 h-2.5 inline mr-1" />LIVE</>}
        </span>
        <button onClick={() => isPaper ? loadPaper() : loadLive()}
          className="p-1 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground" title="Refresh">
          {loading ? <Loader2 className="w-3 h-3 animate-spin" /> : <RefreshCw className="w-3 h-3" />}
        </button>
      </div>

      {/* ── Header: total balance ── */}
      <div className="px-4 pt-3 pb-2 border-b border-border/50 flex items-end justify-between gap-4">
        <div>
          <div className="text-[10px] text-muted-foreground uppercase tracking-widest mb-0.5">Total spot balance</div>
          <div className="text-2xl font-bold font-mono tabular-nums">
            {(isPaper ? totalPortfolio : liveTotal).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2})}
            <span className="text-sm text-muted-foreground ml-1 font-normal">USDT</span>
          </div>
          <div className="text-[10px] text-muted-foreground font-mono mt-0.5">
            ≈ {(isPaper ? totalPortfolio : liveTotal).toFixed(2)} USD · updated {lastUpdated || 'live via WebSocket'}
          </div>
        </div>
        <div className="text-right space-y-0.5 shrink-0">
          {isPaper && (
            <>
              <div className={`text-xs font-mono font-semibold ${sessionGain >= 0 ? 'text-gain' : 'text-loss'}`}>
                {sessionGain >= 0 ? '+' : ''}{sessionGain.toFixed(2)} USDT today
              </div>
              <div className={`text-[10px] font-mono ${sessionGainPct >= 0 ? 'text-gain' : 'text-loss'}`}>
                {sessionGainPct >= 0 ? '+' : ''}{sessionGainPct.toFixed(2)}% since session open
              </div>
              <div className="text-[10px] text-muted-foreground">
                Fees paid: <span className="font-mono text-foreground">{totalFees.toFixed(4)} USDT</span>
              </div>
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
        <div className="divide-y divide-border/30">
          {positionRows.map(({ coin, pos, livePrice, currentValue, costBasis, pnl, pct }) => (
            <div key={pos.symbol} className="grid grid-cols-[auto_1fr_auto_auto_auto] gap-x-4 items-center px-4 py-2.5 hover:bg-muted/10 transition-colors">
              <div className="flex items-center gap-2">
                <CoinIcon coin={coin} />
                <div>
                  <div className="text-xs font-bold">{coin}</div>
                  <div className="text-[9px] text-muted-foreground">{coin}</div>
                </div>
              </div>
              <div className="font-mono text-xs tabular-nums">{pos.quantity.toFixed(6)}</div>
              <div className="font-mono text-xs tabular-nums text-right">
                <div>{currentValue.toFixed(2)} USDT</div>
                <div className="text-[9px] text-muted-foreground">@ {livePrice.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:4})}</div>
              </div>
              <div className="font-mono text-xs tabular-nums text-right">
                {costBasis.toFixed(2)}
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
                <div className="text-[9px] text-muted-foreground">Tether</div>
              </div>
            </div>
            <div className="font-mono text-xs tabular-nums text-muted-foreground">—</div>
            <div className="font-mono text-xs tabular-nums text-right">
              <div>{usdtFree.toFixed(2)} USDT</div>
              <div className="text-[9px] text-muted-foreground">available to trade</div>
            </div>
            <div className="font-mono text-xs tabular-nums text-right text-muted-foreground">—</div>
            <div className="rounded px-2 py-1 bg-muted/20 border border-border text-right min-w-[80px]">
              <div className="text-[10px] text-muted-foreground font-semibold">stablecoin</div>
            </div>
          </div>

          {positionRows.length === 0 && usdtFree === 0 && (
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
                  {livePrice > 0 && <div className="text-[9px] text-muted-foreground">@ {livePrice.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:4})}</div>}
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
              {binanceConnected ? 'Loading live balances…' : 'Connect Binance API to see live wallet'}
            </div>
          )}
        </div>
      )}

      <div className="px-4 py-2 border-t border-border/30 bg-muted/10">
        <p className="text-[9px] text-muted-foreground">
          {isPaper
            ? 'Paper wallet — all positions are simulated · green = profitable · red = at loss · gray = at breakeven · Last updated: live via WebSocket'
            : 'Live Binance spot wallet · values update with WebSocket prices'}
        </p>
      </div>
    </div>
  );
};

export default WalletPanelV2;
