import { useState, useEffect, useCallback } from 'react';
import { Activity, Clock, XCircle, TrendingUp, TrendingDown, Loader2, CheckCircle2 } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';
import { TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import { toast } from 'sonner';

const BREAK_EVEN_MULT = 1 / Math.pow(1 - TAKER_FEE, 2);

type TabId = 'open' | 'history' | 'trades' | 'funds';

interface Trade {
  id: string;
  symbol: string;
  side: string;
  price: number;
  quantity: number;
  pnl: number | null;
  reason: string | null;
  created_at: string;
}

interface Position {
  symbol: string;
  quantity: number;
  avg_entry_price: number;
}

interface Props {
  prices: LivePrices;
}

function fmt(n: number, d = 2) { return n.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d }); }
function fmtTime(iso: string) {
  const d = new Date(iso);
  return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
}
function fmtDate(iso: string) {
  const d = new Date(iso);
  return `${d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} ${fmtTime(iso)}`;
}

function StatusPill({ pnl, side }: { pnl: number | null; side: string }) {
  if (side === 'BUY' && pnl === null) return (
    <span className="px-1.5 py-0.5 rounded text-[9px] font-semibold bg-accent/20 text-accent border border-accent/30">OPEN</span>
  );
  if (pnl === null) return null;
  if (pnl > 0) return (
    <span className="px-1.5 py-0.5 rounded text-[9px] font-semibold bg-gain/20 text-gain border border-gain/30">PROFIT</span>
  );
  return (
    <span className="px-1.5 py-0.5 rounded text-[9px] font-semibold bg-loss/20 text-loss border border-loss/30">LOSS</span>
  );
}

const OrderMonitor = ({ prices }: Props) => {
  const [tab, setTab]           = useState<TabId>('open');
  const [trades, setTrades]     = useState<Trade[]>([]);
  const [positions, setPositions] = useState<Position[]>([]);
  const [loading, setLoading]   = useState(true);

  const loadAll = useCallback(async () => {
    const [{ data: th }, { data: pp }] = await Promise.all([
      supabase.from('bot_trade_history').select('*').eq('user_session', 'default').order('created_at', { ascending: false }).limit(200),
      supabase.from('paper_portfolio').select('*').eq('user_session', 'default').gt('quantity', 0),
    ]);
    setTrades((th as Trade[]) ?? []);
    setPositions((pp as Position[]) ?? []);
    setLoading(false);
  }, []);

  useEffect(() => {
    loadAll();
    const ch = supabase.channel('om-updates')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadAll)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadAll)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadAll]);

  const openOrders  = trades.filter(t => t.side === 'BUY' && t.pnl === null);
  const closedTrades = trades.filter(t => t.pnl !== null);

  const cancelOrder = async (id: string, symbol: string) => {
    await supabase.from('bot_trade_history').delete().eq('id', id);
    await supabase.from('paper_portfolio').update({ quantity: 0, updated_at: new Date().toISOString() })
      .eq('user_session', 'default').eq('symbol', symbol);
    toast.info('Order cancelled');
    loadAll();
  };

  const TABS: { id: TabId; label: string; count?: number }[] = [
    { id: 'open',    label: 'Open Orders',   count: openOrders.length },
    { id: 'history', label: 'Order History',  count: closedTrades.length },
    { id: 'trades',  label: 'Trade History',  count: closedTrades.length },
    { id: 'funds',   label: 'Funds' },
  ];

  return (
    <div className="trading-card flex flex-col h-full">
      {/* Header */}
      <div className="p-3 border-b border-border flex items-center gap-2">
        <Activity className="w-4 h-4 text-primary" />
        <span className="text-sm font-medium">Orders & Monitor</span>
        {loading && <Loader2 className="w-3 h-3 animate-spin text-muted-foreground ml-auto" />}
      </div>

      {/* Tabs */}
      <div className="flex border-b border-border overflow-x-auto scrollbar-thin">
        {TABS.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)}
            className={`px-3 py-2 text-[10px] font-medium whitespace-nowrap flex items-center gap-1 transition-colors relative ${
              tab === t.id ? 'text-accent' : 'text-muted-foreground hover:text-foreground'
            }`}>
            {t.label}
            {t.count !== undefined && t.count > 0 && (
              <span className={`text-[9px] rounded-full px-1 font-bold ${tab === t.id ? 'bg-accent/20 text-accent' : 'bg-muted text-muted-foreground'}`}>{t.count}</span>
            )}
            {tab === t.id && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-accent rounded-t" />}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-y-auto scrollbar-thin">
        {/* ── OPEN ORDERS ── */}
        {tab === 'open' && (
          <div>
            {openOrders.length === 0 ? (
              <div className="p-6 text-center text-xs text-muted-foreground">No open orders</div>
            ) : (
              <>
                <div className="grid text-[9px] text-muted-foreground uppercase tracking-widest px-3 py-1.5 border-b border-border"
                  style={{ gridTemplateColumns: '1fr 3rem 3rem 4rem 4rem 3.5rem 2.5rem' }}>
                  <span>Time / Pair</span><span className="text-right">Type</span><span className="text-right">Side</span>
                  <span className="text-right">Price</span><span className="text-right">Amount</span>
                  <span className="text-right">Status</span><span className="text-right">Action</span>
                </div>
                {openOrders.map(o => (
                  <div key={o.id} className="grid items-center px-3 py-2 border-b border-border/50 hover:bg-muted/10 text-[10px]"
                    style={{ gridTemplateColumns: '1fr 3rem 3rem 4rem 4rem 3.5rem 2.5rem' }}>
                    <div>
                      <div className="text-foreground font-semibold">{o.symbol.replace('USDT', '')}/USDT</div>
                      <div className="text-muted-foreground">{fmtTime(o.created_at)}</div>
                    </div>
                    <div className="text-right text-muted-foreground">Market</div>
                    <div className={`text-right font-semibold ${o.side === 'BUY' ? 'text-gain' : 'text-loss'}`}>{o.side}</div>
                    <div className="text-right font-mono tabular-nums">${fmt(o.price, 4)}</div>
                    <div className="text-right font-mono tabular-nums">{o.quantity.toFixed(5)}</div>
                    <div className="flex justify-end"><StatusPill pnl={null} side="BUY" /></div>
                    <div className="flex justify-end">
                      <button onClick={() => cancelOrder(o.id, o.symbol)}
                        className="text-loss hover:text-loss/80 transition-colors" title="Cancel">
                        <XCircle className="w-3.5 h-3.5" />
                      </button>
                    </div>
                  </div>
                ))}
              </>
            )}

            {/* Active trade monitor */}
            {positions.length > 0 && (
              <div className="p-3 border-t border-border mt-1">
                <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 flex items-center gap-1">
                  <Activity className="w-3 h-3 text-accent" /> Active Position Monitor
                </div>
                {positions.map(pos => {
                  const lp = prices[pos.symbol];
                  if (!lp) return null;
                  const price   = parseFloat(lp.price);
                  const bep     = pos.avg_entry_price * BREAK_EVEN_MULT;
                  const pct     = ((price - pos.avg_entry_price) / pos.avg_entry_price) * 100;
                  const above   = price >= bep;
                  const progressPct = Math.min(100, Math.max(0, (pct / 2) * 100 + 50));
                  return (
                    <div key={pos.symbol} className="mb-3">
                      <div className="flex justify-between text-[10px] mb-1">
                        <span className="font-semibold text-foreground">{pos.symbol.replace('USDT', '')}</span>
                        <span className={above ? 'text-gain font-semibold' : 'text-loss font-semibold'}>
                          {above ? '✅ Profitable' : '⏳ Waiting'}
                        </span>
                      </div>
                      <div className="flex justify-between text-[9px] text-muted-foreground mb-1">
                        <span>Entry ${fmt(pos.avg_entry_price, 4)} · BEP ${fmt(bep, 4)}</span>
                        <span className={pct >= 0 ? 'text-gain' : 'text-loss'}>{pct >= 0 ? '+' : ''}{pct.toFixed(3)}%</span>
                      </div>
                      <div className="w-full h-1.5 bg-muted/30 rounded-full overflow-hidden">
                        <div
                          className={`h-full rounded-full transition-all duration-500 ${above ? 'bg-gain' : 'bg-loss'}`}
                          style={{ width: `${progressPct}%` }}
                        />
                      </div>
                      <div className="flex justify-between text-[9px] text-muted-foreground mt-0.5">
                        <span>Entry</span>
                        <span className="text-foreground font-mono">Current ${fmt(price, 4)}</span>
                        <span>+2%</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}

        {/* ── ORDER HISTORY ── */}
        {tab === 'history' && (
          <div>
            {closedTrades.length === 0 ? (
              <div className="p-6 text-center text-xs text-muted-foreground">No completed orders yet</div>
            ) : (
              <>
                <div className="grid text-[9px] text-muted-foreground uppercase tracking-widest px-3 py-1.5 border-b border-border"
                  style={{ gridTemplateColumns: '1fr 3rem 4rem 4rem 3.5rem 4rem' }}>
                  <span>Time / Pair</span><span className="text-right">Side</span>
                  <span className="text-right">Price</span><span className="text-right">Amount</span>
                  <span className="text-right">Fill %</span><span className="text-right">Status</span>
                </div>
                {closedTrades.map(o => (
                  <div key={o.id} className="grid items-center px-3 py-2 border-b border-border/50 hover:bg-muted/10 text-[10px]"
                    style={{ gridTemplateColumns: '1fr 3rem 4rem 4rem 3.5rem 4rem' }}>
                    <div>
                      <div className="text-foreground font-semibold">{o.symbol.replace('USDT', '')}/USDT</div>
                      <div className="text-muted-foreground">{fmtDate(o.created_at)}</div>
                    </div>
                    <div className={`text-right font-semibold ${o.side === 'BUY' ? 'text-gain' : 'text-loss'}`}>{o.side}</div>
                    <div className="text-right font-mono tabular-nums">${fmt(o.price, 4)}</div>
                    <div className="text-right font-mono tabular-nums">{o.quantity.toFixed(5)}</div>
                    <div className="text-right text-muted-foreground">100%</div>
                    <div className="flex justify-end"><StatusPill pnl={o.pnl} side={o.side} /></div>
                  </div>
                ))}
              </>
            )}
          </div>
        )}

        {/* ── TRADE HISTORY ── */}
        {tab === 'trades' && (
          <div>
            {closedTrades.length === 0 ? (
              <div className="p-6 text-center text-xs text-muted-foreground">No completed trades yet</div>
            ) : (
              <>
                <div className="grid text-[9px] text-muted-foreground uppercase tracking-widest px-3 py-1.5 border-b border-border"
                  style={{ gridTemplateColumns: '1fr 4rem 4rem 4rem 4rem' }}>
                  <span>Pair / Date</span><span className="text-right">Entry</span><span className="text-right">Exit</span>
                  <span className="text-right">Qty</span><span className="text-right">Net P&L</span>
                </div>
                {closedTrades.filter(t => t.side === 'SELL').map(o => (
                  <div key={o.id} className="grid items-center px-3 py-2 border-b border-border/50 hover:bg-muted/10 text-[10px]"
                    style={{ gridTemplateColumns: '1fr 4rem 4rem 4rem 4rem' }}>
                    <div>
                      <div className="text-foreground font-semibold">{o.symbol.replace('USDT', '')}</div>
                      <div className="text-muted-foreground">{fmtDate(o.created_at)}</div>
                    </div>
                    <div className="text-right font-mono tabular-nums text-muted-foreground">—</div>
                    <div className="text-right font-mono tabular-nums">${fmt(o.price, 4)}</div>
                    <div className="text-right font-mono tabular-nums">{o.quantity.toFixed(5)}</div>
                    <div className={`text-right font-mono tabular-nums font-semibold ${(o.pnl ?? 0) >= 0 ? 'text-gain' : 'text-loss'}`}>
                      {(o.pnl ?? 0) >= 0 ? '+' : ''}{fmt(o.pnl ?? 0, 4)}
                    </div>
                  </div>
                ))}
              </>
            )}
          </div>
        )}

        {/* ── FUNDS ── */}
        {tab === 'funds' && (
          <div className="p-3 space-y-2">
            {positions.length === 0 ? (
              <div className="p-4 text-center text-xs text-muted-foreground">No positions held</div>
            ) : (
              <>
                <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2">Active Positions</div>
                {positions.map(pos => {
                  const lp    = prices[pos.symbol];
                  const price = lp ? parseFloat(lp.price) : 0;
                  const value = pos.quantity * price;
                  const cost  = pos.quantity * pos.avg_entry_price / (1 - TAKER_FEE);
                  const pnl   = value * (1 - TAKER_FEE) - cost;
                  const above = pnl > 0;
                  return (
                    <div key={pos.symbol} className="bg-muted/20 border border-border rounded p-2.5">
                      <div className="flex justify-between items-start mb-1.5">
                        <div>
                          <div className="text-xs font-semibold text-foreground">{pos.symbol.replace('USDT', '')}/USDT</div>
                          <div className="text-[10px] text-muted-foreground">{pos.quantity.toFixed(6)} coins</div>
                        </div>
                        <div className="text-right">
                          <div className="text-xs font-mono font-semibold tabular-nums">${fmt(value)}</div>
                          <div className={`text-[10px] font-mono ${above ? 'text-gain' : 'text-loss'}`}>
                            {above ? '+' : ''}{fmt(pnl, 4)} USDT
                          </div>
                        </div>
                      </div>
                      <div className="flex justify-between text-[9px] text-muted-foreground">
                        <span>Avg entry ${fmt(pos.avg_entry_price, 4)}</span>
                        <span>Current ${price > 0 ? fmt(price, 4) : '—'}</span>
                      </div>
                    </div>
                  );
                })}
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default OrderMonitor;
