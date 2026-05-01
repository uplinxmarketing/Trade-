import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Play, Square, Brain, TrendingUp, TrendingDown, Zap,
  RotateCcw, ChevronDown, ChevronUp, FlaskConical,
  DollarSign, Pencil, Check, X, BookOpen, Eye,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { supabase } from '@/integrations/supabase/client';
import { toast } from 'sonner';
import { checkExits, TAKER_FEE } from '@/lib/trading-engine';
import type { LivePrices } from '@/lib/trading-engine';
import {
  runAgentCycle, executeBuyDecision, executeSellDecision,
  type AgentDecision, type OpenPosition, type RecentTrade,
} from '@/lib/ai-agent';

interface AITradingAgentProps {
  selectedCoins: string[];
  prices: LivePrices;
  binanceConnected?: boolean;
  onConnectBinance?: () => void;
}

const PRESET_BUDGETS = [1000, 5000, 10000, 25000, 50000];

const INSTRUCTIONS_KEY = 'ai_agent_instructions';
const AGENT_CYCLE_MS = 30_000; // 30 seconds between full Claude cycles
const breakEvenMultiplier = 1 / Math.pow(1 - TAKER_FEE, 2);

interface AgentLog {
  id: string;
  timestamp: string;
  type: 'cycle' | 'trade' | 'error';
  decisions?: AgentDecision[];
  learned?: string;
  nextWatch?: string;
  tradeSymbol?: string;
  tradeSide?: 'BUY' | 'SELL';
  tradePnl?: number | null;
  tradeReason?: string;
  error?: string;
}

const AITradingAgent = ({ selectedCoins, prices, binanceConnected, onConnectBinance }: AITradingAgentProps) => {
  const [mode, setMode] = useState<'test' | 'live'>('test');
  const [isRunning, setIsRunning] = useState(false);
  const [loading, setLoading] = useState(false);
  const [showAllTrades, setShowAllTrades] = useState(false);
  const [editingBudget, setEditingBudget] = useState(false);
  const [budgetDraft, setBudgetDraft] = useState('');
  const [instructions, setInstructions] = useState(
    () => localStorage.getItem(INSTRUCTIONS_KEY) ?? ''
  );
  const [editingInstructions, setEditingInstructions] = useState(false);
  const [instructionsDraft, setInstructionsDraft] = useState('');

  const [totalBudget, setTotalBudget] = useState(10000);
  const [trades, setTrades] = useState<RecentTrade[]>([]);
  const [positions, setPositions] = useState<OpenPosition[]>([]);
  const [balance, setBalance] = useState(10000);
  const [totalPnl, setTotalPnl] = useState(0);
  const [winRate, setWinRate] = useState(0);
  const [agentLogs, setAgentLogs] = useState<AgentLog[]>([]);
  const [expandedLog, setExpandedLog] = useState<string | null>(null);
  const [agentStatus, setAgentStatus] = useState('');
  const [cycleCountdown, setCycleCountdown] = useState(0);

  const isRunningRef = useRef(false);
  const processingRef = useRef(false);
  const balanceRef = useRef(balance);
  const cycleTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const countdownRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const stopLossRef = useRef(1.5);
  const minProfitRef = useRef(0.25);

  useEffect(() => { isRunningRef.current = isRunning; }, [isRunning]);
  useEffect(() => { balanceRef.current = balance; }, [balance]);

  // Persist instructions
  useEffect(() => {
    localStorage.setItem(INSTRUCTIONS_KEY, instructions);
  }, [instructions]);

  const loadData = useCallback(async () => {
    const [tradesRes, posRes, cfgRes] = await Promise.all([
      supabase.from('bot_trade_history').select('*').eq('user_session', 'default').order('created_at', { ascending: false }).limit(100),
      supabase.from('paper_portfolio').select('*').eq('user_session', 'default').gt('quantity', 0),
      supabase.from('bot_config').select('*').eq('user_session', 'default').maybeSingle(),
    ]);
    if (tradesRes.data) {
      const t = tradesRes.data as any[];
      setTrades(t);
      const sells = t.filter(x => x.side === 'SELL' && x.pnl !== null);
      setTotalPnl(Math.round(sells.reduce((s: number, x: any) => s + (x.pnl || 0), 0) * 10000) / 10000);
      const wins = sells.filter(x => (x.pnl || 0) > 0).length;
      setWinRate(sells.length ? Math.round((wins / sells.length) * 100) : 0);
    }
    if (posRes.data) setPositions(posRes.data as OpenPosition[]);
    if (cfgRes.data) {
      const running = cfgRes.data.is_running as boolean;
      isRunningRef.current = running;
      setIsRunning(running);
      const bal = Number(cfgRes.data.current_balance);
      setBalance(bal);
      balanceRef.current = bal;
    }
  }, []);

  useEffect(() => {
    supabase.from('bot_config')
      .select('stop_loss_percent, min_profit_percent')
      .eq('user_session', 'default').maybeSingle()
      .then(({ data }) => {
        if (data) {
          stopLossRef.current = Number(data.stop_loss_percent ?? 1.5);
          minProfitRef.current = Number(data.min_profit_percent ?? 0.25);
        }
      });
  }, []);

  useEffect(() => {
    loadData();
    const ch = supabase.channel('aiagentpanel')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'bot_trade_history' }, loadData)
      .on('postgres_changes', { event: '*', schema: 'public', table: 'paper_portfolio' }, loadData)
      .subscribe();
    return () => { supabase.removeChannel(ch); };
  }, [loadData]);

  // Always-on: check exits on every price tick (~1 s)
  const pricesRef = useRef(prices);
  useEffect(() => { pricesRef.current = prices; }, [prices]);

  useEffect(() => {
    if (!isRunning || processingRef.current) return;
    if (!Object.keys(prices).length) return;

    processingRef.current = true;
    checkExits(prices, supabase, stopLossRef.current, minProfitRef.current)
      .then(exits => {
        if (exits > 0) {
          loadData();
          toast.success(`Closed ${exits} position(s) via stop-loss / take-profit`, { duration: 2500 });
        }
      })
      .catch(() => {})
      .finally(() => { processingRef.current = false; });
  }, [prices]); // eslint-disable-line react-hooks/exhaustive-deps

  const runCycle = useCallback(async () => {
    if (!isRunningRef.current) return;
    setAgentStatus('Thinking…');
    const cycleId = crypto.randomUUID();

    try {
      const [posRes, cfgRes, histRes] = await Promise.all([
        supabase.from('paper_portfolio').select('*').eq('user_session', 'default').gt('quantity', 0),
        supabase.from('bot_config').select('current_balance').eq('user_session', 'default').maybeSingle(),
        supabase.from('bot_trade_history').select('*').eq('user_session', 'default').order('created_at', { ascending: false }).limit(20),
      ]);
      const currentPositions: OpenPosition[] = (posRes.data || []) as OpenPosition[];
      const currentBalance = Number(cfgRes.data?.current_balance ?? balanceRef.current);
      const recentTrades: RecentTrade[] = (histRes.data || []) as any[];

      const result = await runAgentCycle(
        selectedCoins,
        pricesRef.current,
        currentPositions,
        recentTrades,
        currentBalance,
        instructions,
      );

      const heldSymbols = new Set(currentPositions.map(p => p.symbol));
      let runningBalance = currentBalance;

      // Execute decisions sequentially
      for (const decision of result.decisions) {
        if (!isRunningRef.current) break;

        if (decision.action === 'BUY' && !heldSymbols.has(decision.symbol)) {
          const newBalance = await executeBuyDecision(
            decision, runningBalance, pricesRef.current, selectedCoins, supabase,
          );
          if (newBalance < runningBalance) {
            const spent = runningBalance - newBalance;
            const livePrice = parseFloat(pricesRef.current[decision.symbol]?.price || '0');
            setAgentLogs(prev => [{
              id: crypto.randomUUID(), timestamp: new Date().toISOString(),
              type: 'trade', tradeSymbol: decision.symbol, tradeSide: 'BUY',
              tradePnl: null,
              tradeReason: `${decision.reasoning.slice(0, 180)} · $${spent.toFixed(2)} @ $${livePrice.toFixed(4)}`,
              decisions: [decision],
            }, ...prev].slice(0, 100));
            toast.info(`AI bought ${decision.symbol.replace('USDT', '')} · ${decision.confidence}% conf`, { duration: 3000 });
            runningBalance = newBalance;
            heldSymbols.add(decision.symbol);
          }
        } else if (decision.action === 'SELL') {
          const pos = currentPositions.find(p => p.symbol === decision.symbol);
          if (pos) {
            const proceeds = await executeSellDecision(decision, pos, pricesRef.current, supabase);
            if (proceeds > 0) {
              const livePrice = parseFloat(pricesRef.current[decision.symbol]?.price || '0');
              const pnl = proceeds - (pos.quantity * pos.avg_entry_price / (1 - TAKER_FEE));
              setAgentLogs(prev => [{
                id: crypto.randomUUID(), timestamp: new Date().toISOString(),
                type: 'trade', tradeSymbol: decision.symbol, tradeSide: 'SELL',
                tradePnl: Math.round(pnl * 10000) / 10000,
                tradeReason: `${decision.reasoning.slice(0, 180)} · @ $${livePrice.toFixed(4)}`,
                decisions: [decision],
              }, ...prev].slice(0, 100));
              const pnlLabel = pnl >= 0 ? `+$${pnl.toFixed(4)}` : `-$${Math.abs(pnl).toFixed(4)}`;
              toast[pnl >= 0 ? 'success' : 'warning'](`AI sold ${decision.symbol.replace('USDT', '')} ${pnlLabel}`, { duration: 3000 });
              runningBalance += proceeds;
              heldSymbols.delete(decision.symbol);
            }
          }
        }
      }

      // Update balance in DB
      await supabase.from('bot_config').update({
        current_balance: Math.round(runningBalance * 10000) / 10000,
        updated_at: new Date().toISOString(),
      }).eq('user_session', 'default');

      // Log cycle summary
      setAgentLogs(prev => [{
        id: cycleId,
        timestamp: new Date().toISOString(),
        type: 'cycle',
        decisions: result.decisions,
        learned: result.learned,
        nextWatch: result.nextWatch,
      }, ...prev].slice(0, 100));

      setAgentStatus(`Cycle done · ${new Date().toLocaleTimeString()}`);
      await loadData();
    } catch (err: any) {
      const msg = err.message ?? String(err);
      setAgentLogs(prev => [{
        id: cycleId, timestamp: new Date().toISOString(), type: 'error', error: msg,
      }, ...prev].slice(0, 100));
      setAgentStatus(`Error: ${msg.slice(0, 60)}`);
      if (msg.includes('ANTHROPIC_API_KEY')) {
        toast.error('Anthropic API key not set', {
          description: 'Add ANTHROPIC_API_KEY=sk-ant-... to your .env file and restart.',
          duration: 10000,
        });
      }
    }
  }, [selectedCoins, instructions, loadData]);

  const scheduleNextCycle = useCallback(() => {
    if (cycleTimerRef.current) clearTimeout(cycleTimerRef.current);
    if (countdownRef.current) clearInterval(countdownRef.current);
    if (!isRunningRef.current) return;

    setCycleCountdown(AGENT_CYCLE_MS / 1000);
    countdownRef.current = setInterval(() => {
      setCycleCountdown(prev => {
        if (prev <= 1) { clearInterval(countdownRef.current!); return 0; }
        return prev - 1;
      });
    }, 1000);

    cycleTimerRef.current = setTimeout(() => {
      runCycle().then(() => scheduleNextCycle());
    }, AGENT_CYCLE_MS);
  }, [runCycle]);

  const toggleBot = async () => {
    if (mode === 'live' && !binanceConnected) {
      toast.error('Connect Binance API first');
      onConnectBinance?.();
      return;
    }
    setLoading(true);
    try {
      if (!isRunning) {
        await supabase.from('bot_config').upsert({
          user_session: 'default',
          selected_coins: selectedCoins,
          mode,
          is_running: true,
          current_balance: totalBudget,
          initial_balance: totalBudget,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'user_session' });

        isRunningRef.current = true;
        setIsRunning(true);
        setBalance(totalBudget);
        balanceRef.current = totalBudget;

        toast.success(`AI Agent started · ${mode.toUpperCase()} mode`, {
          description: `$${totalBudget.toLocaleString()} USDT · ${selectedCoins.length} coins · Claude decides every 30s`,
        });

        // First cycle immediately, then every 30 s
        runCycle().then(() => scheduleNextCycle());
      } else {
        if (cycleTimerRef.current) clearTimeout(cycleTimerRef.current);
        if (countdownRef.current) clearInterval(countdownRef.current);
        await supabase.from('bot_config').update({ is_running: false, updated_at: new Date().toISOString() })
          .eq('user_session', 'default');
        isRunningRef.current = false;
        setIsRunning(false);
        setCycleCountdown(0);
        toast.info('AI Agent paused');
      }
    } finally {
      setLoading(false);
    }
  };

  const resetBot = async () => {
    if (!confirm('Reset all trades and restore the full budget?')) return;
    if (cycleTimerRef.current) clearTimeout(cycleTimerRef.current);
    if (countdownRef.current) clearInterval(countdownRef.current);
    isRunningRef.current = false;
    await Promise.all([
      supabase.from('bot_trade_history').delete().eq('user_session', 'default'),
      supabase.from('paper_portfolio').delete().eq('user_session', 'default'),
      supabase.from('bot_config').update({
        current_balance: totalBudget, initial_balance: totalBudget,
        is_running: false, updated_at: new Date().toISOString(),
      }).eq('user_session', 'default'),
    ]);
    setTrades([]); setPositions([]); setBalance(totalBudget);
    setTotalPnl(0); setIsRunning(false); setAgentLogs([]); setCycleCountdown(0);
    toast.success(`Reset · $${totalBudget.toLocaleString()} USDT restored`);
  };

  const confirmBudgetEdit = () => {
    const val = Math.max(100, Number(budgetDraft));
    if (!isNaN(val)) { setTotalBudget(val); }
    setEditingBudget(false);
  };

  const pnlColor = totalPnl >= 0 ? 'text-gain' : 'text-loss';
  const pnlPct = totalBudget > 0 ? ((totalPnl / totalBudget) * 100).toFixed(2) : '0.00';
  const displayed = showAllTrades ? trades : trades.slice(0, 15);

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">

      {/* ── Header ── */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className={`w-2.5 h-2.5 rounded-full animate-pulse ${isRunning ? 'bg-accent' : 'bg-muted-foreground'}`} />
          <Brain className="w-3.5 h-3.5 text-accent" />
          <h3 className="text-sm font-semibold">Claude AI Agent</h3>
          <span className={`text-[9px] font-mono px-1.5 py-0.5 rounded ${mode === 'live' ? 'bg-loss/20 text-loss' : 'bg-accent/20 text-accent'}`}>
            {mode === 'live' ? 'LIVE' : 'PAPER'}
          </span>
          {isRunning && cycleCountdown > 0 && (
            <span className="text-[9px] text-muted-foreground font-mono">next cycle in {cycleCountdown}s</span>
          )}
        </div>
        <button onClick={resetBot} className="p-1.5 rounded hover:bg-muted/40 text-muted-foreground hover:text-foreground" title="Reset">
          <RotateCcw className="w-3.5 h-3.5" />
        </button>
      </div>

      {/* ── Mode toggle ── */}
      <div className="grid grid-cols-2 gap-1 bg-muted/30 rounded-md p-0.5">
        <button onClick={() => { if (!isRunning) setMode('test'); }} disabled={isRunning}
          className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold transition-colors disabled:opacity-60 ${mode === 'test' ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:text-foreground'}`}>
          <FlaskConical className="w-3.5 h-3.5" /> TEST — Paper
        </button>
        <button onClick={() => { if (!isRunning) setMode('live'); }} disabled={isRunning}
          className={`flex items-center justify-center gap-1.5 py-2 rounded text-xs font-semibold transition-colors disabled:opacity-60 ${mode === 'live' ? 'bg-loss/80 text-white' : 'text-muted-foreground hover:text-foreground'}`}>
          <Zap className="w-3.5 h-3.5" /> LIVE — Real
          {!binanceConnected && <span className="text-[9px] px-1 bg-warn/20 text-warn rounded">API needed</span>}
        </button>
      </div>

      {mode === 'live' && (
        <div className={`rounded-md px-3 py-2 text-xs ${binanceConnected ? 'bg-loss/10 border border-loss/30 text-loss' : 'bg-warn/10 border border-warn/30 text-warn'}`}>
          {binanceConnected ? '⚠️ LIVE MODE — real USDT will be used.'
            : <span>Binance API not connected. <button onClick={onConnectBinance} className="underline font-semibold">Connect now →</button></span>}
        </div>
      )}

      {/* ── Budget ── */}
      <div className="bg-muted/20 border border-border rounded-md px-3 py-2 space-y-2">
        <div className="flex items-center gap-2">
          <DollarSign className="w-3.5 h-3.5 text-muted-foreground" />
          <span className="text-[10px] uppercase tracking-widest text-muted-foreground">Paper Budget</span>
        </div>
        <div className="flex items-center gap-2">
          {editingBudget ? (
            <>
              <input type="number" value={budgetDraft} min={100} step={500} autoFocus
                onChange={e => setBudgetDraft(e.target.value)}
                onKeyDown={e => { if (e.key === 'Enter') confirmBudgetEdit(); if (e.key === 'Escape') setEditingBudget(false); }}
                className="flex-1 bg-muted/40 border border-accent rounded px-2 py-1 text-xs font-mono focus:outline-none" />
              <button onClick={confirmBudgetEdit} className="p-1 rounded hover:bg-gain/20 text-gain"><Check className="w-3.5 h-3.5" /></button>
              <button onClick={() => setEditingBudget(false)} className="p-1 rounded hover:bg-loss/20 text-loss"><X className="w-3.5 h-3.5" /></button>
            </>
          ) : (
            <>
              <div className="flex gap-1 flex-wrap flex-1">
                {PRESET_BUDGETS.map(p => (
                  <button key={p} onClick={() => { if (!isRunning) setTotalBudget(p); }} disabled={isRunning}
                    className={`text-[10px] px-2 py-1 rounded font-mono transition-colors disabled:opacity-50 ${totalBudget === p ? 'bg-accent text-accent-foreground' : 'bg-muted/50 text-muted-foreground hover:text-foreground'}`}>
                    ${p.toLocaleString()}
                  </button>
                ))}
              </div>
              <button onClick={() => { setBudgetDraft(String(totalBudget)); setEditingBudget(true); }} disabled={isRunning}
                className="p-1 rounded hover:bg-muted/40 text-muted-foreground disabled:opacity-50" title="Custom">
                <Pencil className="w-3 h-3" />
              </button>
            </>
          )}
        </div>
      </div>

      {/* ── User Instructions ── */}
      <div className="bg-muted/20 border border-border rounded-md px-3 py-2.5 space-y-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <BookOpen className="w-3.5 h-3.5 text-accent" />
            <span className="text-xs font-semibold text-accent">Trading Instructions</span>
          </div>
          {!editingInstructions ? (
            <button onClick={() => { setInstructionsDraft(instructions); setEditingInstructions(true); }}
              className="text-[10px] text-muted-foreground hover:text-foreground flex items-center gap-1">
              <Pencil className="w-3 h-3" /> Edit
            </button>
          ) : (
            <div className="flex gap-1">
              <button onClick={() => { setInstructions(instructionsDraft); setEditingInstructions(false); }}
                className="text-[10px] text-gain flex items-center gap-0.5"><Check className="w-3 h-3" /> Save</button>
              <button onClick={() => setEditingInstructions(false)}
                className="text-[10px] text-muted-foreground flex items-center gap-0.5 ml-2"><X className="w-3 h-3" /> Cancel</button>
            </div>
          )}
        </div>
        {editingInstructions ? (
          <textarea
            value={instructionsDraft}
            onChange={e => setInstructionsDraft(e.target.value)}
            placeholder="e.g. Focus on BTC and ETH. Buy on RSI dips below 45. Sell quickly for small profits. Avoid trading during high volatility. Prefer EMA crossover signals."
            className="w-full bg-muted/40 border border-accent/40 rounded px-2 py-2 text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-accent resize-none"
            rows={4}
          />
        ) : (
          <p className="text-xs text-muted-foreground leading-relaxed">
            {instructions || <span className="italic opacity-60">No instructions — Claude will use its own judgment. Click Edit to add strategy guidelines.</span>}
          </p>
        )}
        <p className="text-[9px] text-muted-foreground">The agent reads these instructions before every cycle and adjusts thresholds accordingly. No API key needed.</p>
      </div>

      {/* ── Stats ── */}
      <div className="grid grid-cols-4 gap-1.5">
        {[
          { label: 'USDT Free', value: balance.toLocaleString('en-US', { maximumFractionDigits: 2 }), unit: '', color: '' },
          { label: 'Net P&L', value: `${totalPnl >= 0 ? '+' : ''}${Math.abs(totalPnl).toFixed(2)}`, unit: ' USDT', color: pnlColor },
          { label: 'Return', value: `${totalPnl >= 0 ? '+' : ''}${pnlPct}%`, unit: '', color: pnlColor },
          { label: 'Win Rate', value: `${winRate}%`, unit: '', color: winRate >= 50 ? 'text-gain' : trades.filter(t => t.side === 'SELL').length > 0 ? 'text-loss' : '' },
        ].map(s => (
          <div key={s.label} className="bg-muted/20 rounded-md p-2 text-center">
            <div className="text-[9px] uppercase tracking-widest text-muted-foreground">{s.label}</div>
            <div className={`text-xs font-mono font-semibold tabular-nums ${s.color}`}>{s.value}<span className="text-[9px] opacity-70">{s.unit}</span></div>
          </div>
        ))}
      </div>

      {/* ── Start / Stop ── */}
      <Button onClick={toggleBot} disabled={loading || !selectedCoins.length}
        className={`w-full font-semibold py-5 ${isRunning ? 'bg-loss/90 hover:bg-loss text-white' : 'bg-gain/90 hover:bg-gain text-background'}`}>
        {loading ? <span className="animate-spin mr-1.5">⟳</span>
          : isRunning
            ? <><Square className="w-4 h-4 mr-1.5" />Stop Agent</>
            : <><Play className="w-4 h-4 mr-1.5" />Start AI Agent — Paper</>}
      </Button>
      {!isRunning && (
        <p className="text-[10px] text-center text-muted-foreground -mt-2">
          Analyses {selectedCoins.length} coins every 30 s · 0.1% fee · no API key required
        </p>
      )}
      {isRunning && agentStatus && (
        <p className="text-[10px] text-center text-accent -mt-2 font-mono">{agentStatus}</p>
      )}

      {/* ── Open Positions ── */}
      {positions.length > 0 && (
        <div>
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 flex items-center gap-1">
            <Zap className="w-3 h-3 text-warn" /> Open Positions ({positions.length})
          </div>
          <div className="space-y-1">
            {positions.map(pos => {
              const live = parseFloat(prices[pos.symbol]?.price || '0') || pos.avg_entry_price;
              const sellUsdt = pos.quantity * live * (1 - TAKER_FEE);
              const costUsdt = pos.quantity * pos.avg_entry_price / (1 - TAKER_FEE);
              const uPnl = sellUsdt - costUsdt;
              const pct = costUsdt > 0 ? (uPnl / costUsdt) * 100 : 0;
              const breakEven = pos.avg_entry_price * breakEvenMultiplier;
              const aboveBreak = live >= breakEven;
              return (
                <div key={pos.symbol} className="bg-muted/20 rounded px-2.5 py-2 text-xs">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <span className={`w-1.5 h-1.5 rounded-full animate-pulse ${uPnl >= 0 ? 'bg-gain' : 'bg-loss'}`} />
                      <span className="font-mono font-semibold">{pos.symbol.replace('USDT', '')}</span>
                      <span className="text-muted-foreground font-mono text-[10px]">×{Number(pos.quantity).toFixed(6)}</span>
                    </div>
                    <span className={`font-mono font-semibold ${uPnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                      {uPnl >= 0 ? '+' : ''}{uPnl.toFixed(4)} USDT
                      <span className="font-normal opacity-70 ml-1">({pct.toFixed(2)}%)</span>
                    </span>
                  </div>
                  <div className="flex items-center justify-between mt-1 text-[9px] text-muted-foreground font-mono">
                    <span>Entry ${pos.avg_entry_price.toLocaleString('en-US', { maximumFractionDigits: 4 })}</span>
                    <span className={aboveBreak ? 'text-gain' : 'text-warn'}>
                      Break-even ${breakEven.toLocaleString('en-US', { maximumFractionDigits: 4 })} {aboveBreak ? '✓' : '↑'}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Agent Thought Log ── */}
      {agentLogs.length > 0 && (
        <div>
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5 flex items-center gap-1">
            <Brain className="w-3 h-3 text-accent" /> Agent Activity Log
          </div>
          <div className="space-y-1 max-h-64 overflow-y-auto scrollbar-thin pr-1">
            {agentLogs.map(log => {
              const time = new Date(log.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
              const isExpanded = expandedLog === log.id;

              if (log.type === 'error') {
                return (
                  <div key={log.id} className="bg-loss/10 border border-loss/20 rounded px-2.5 py-2 text-xs">
                    <div className="flex items-center justify-between">
                      <span className="text-loss font-semibold text-[10px]">Error</span>
                      <span className="text-[9px] text-muted-foreground font-mono">{time}</span>
                    </div>
                    <p className="text-[10px] text-loss/80 mt-0.5">{log.error}</p>
                  </div>
                );
              }

              if (log.type === 'trade') {
                const isBuy = log.tradeSide === 'BUY';
                const profit = log.tradePnl !== null && log.tradePnl !== undefined && log.tradePnl > 0;
                const loss = log.tradePnl !== null && log.tradePnl !== undefined && log.tradePnl < 0;
                return (
                  <div key={log.id} className={`rounded px-2.5 py-2 text-xs border ${profit ? 'bg-gain/10 border-gain/20' : loss ? 'bg-loss/10 border-loss/20' : isBuy ? 'bg-accent/5 border-accent/20' : 'bg-muted/20 border-border/30'}`}>
                    <div className="flex items-start justify-between">
                      <div className="flex items-center gap-1.5">
                        {isBuy ? <TrendingUp className="w-3 h-3 text-accent flex-shrink-0" /> : profit ? <TrendingUp className="w-3 h-3 text-gain flex-shrink-0" /> : <TrendingDown className="w-3 h-3 text-loss flex-shrink-0" />}
                        <span className="font-mono font-semibold">{log.tradeSymbol?.replace('USDT', '')}</span>
                        <span className={`text-[9px] font-bold px-1 py-0.5 rounded ${isBuy ? 'bg-accent/20 text-accent' : profit ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'}`}>{log.tradeSide}</span>
                        {log.tradePnl !== null && log.tradePnl !== undefined && (
                          <span className={`font-mono text-[10px] font-bold ${log.tradePnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                            {log.tradePnl >= 0 ? '+' : ''}{log.tradePnl.toFixed(4)} USDT
                          </span>
                        )}
                      </div>
                      <span className="text-[9px] text-muted-foreground font-mono">{time}</span>
                    </div>
                    {log.tradeReason && (
                      <p className="text-[9px] text-muted-foreground mt-1 leading-relaxed">{log.tradeReason}</p>
                    )}
                  </div>
                );
              }

              // Cycle summary
              const buys = log.decisions?.filter(d => d.action === 'BUY').length ?? 0;
              const sells = log.decisions?.filter(d => d.action === 'SELL').length ?? 0;
              const holds = log.decisions?.filter(d => d.action === 'HOLD').length ?? 0;

              return (
                <div key={log.id} className="bg-muted/10 border border-border/40 rounded px-2.5 py-2 text-xs">
                  <button onClick={() => setExpandedLog(isExpanded ? null : log.id)} className="w-full text-left">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Eye className="w-3 h-3 text-accent" />
                        <span className="text-[10px] font-semibold text-accent">Analysis Cycle</span>
                        <span className="text-[9px] text-muted-foreground">
                          {buys > 0 && <span className="text-accent mr-1">+{buys}B</span>}
                          {sells > 0 && <span className="text-gain mr-1">{sells}S</span>}
                          <span className="opacity-60">{holds}H</span>
                        </span>
                      </div>
                      <div className="flex items-center gap-1">
                        <span className="text-[9px] text-muted-foreground font-mono">{time}</span>
                        {isExpanded ? <ChevronUp className="w-3 h-3 text-muted-foreground" /> : <ChevronDown className="w-3 h-3 text-muted-foreground" />}
                      </div>
                    </div>
                  </button>
                  {isExpanded && (
                    <div className="mt-2 space-y-1.5 border-t border-border/30 pt-2">
                      {log.decisions?.map(d => (
                        <div key={d.symbol} className="flex items-start gap-2">
                          <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded shrink-0 ${d.action === 'BUY' ? 'bg-accent/20 text-accent' : d.action === 'SELL' ? 'bg-gain/20 text-gain' : 'bg-muted/40 text-muted-foreground'}`}>{d.action}</span>
                          <div>
                            <span className="text-[10px] font-mono font-semibold">{d.symbol.replace('USDT', '')}</span>
                            <span className="text-[9px] text-muted-foreground ml-1">{d.confidence}% conf</span>
                            <p className="text-[9px] text-muted-foreground leading-relaxed">{d.reasoning}</p>
                          </div>
                        </div>
                      ))}
                      {log.learned && (
                        <div className="bg-accent/5 border border-accent/20 rounded px-2 py-1.5 mt-1">
                          <p className="text-[9px] text-accent/80 leading-relaxed"><span className="font-semibold">Learned: </span>{log.learned}</p>
                        </div>
                      )}
                      {log.nextWatch && (
                        <div className="bg-muted/20 rounded px-2 py-1">
                          <p className="text-[9px] text-muted-foreground leading-relaxed"><span className="font-semibold text-foreground">Watching: </span>{log.nextWatch}</p>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ── Trade History ── */}
      <div>
        <div className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1.5">
          Trade History ({trades.length})
        </div>
        {!trades.length ? (
          <p className="text-xs text-muted-foreground text-center py-4">
            {isRunning ? 'Analyzing market indicators — first cycle running…' : 'Start the agent to see trades here'}
          </p>
        ) : (
          <div className="space-y-0.5">
            {displayed.map((t: any) => {
              const isBuy = t.side === 'BUY';
              const profit = t.pnl !== null && t.pnl > 0;
              const loss = t.pnl !== null && t.pnl < 0;
              const time = new Date(t.created_at).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
              return (
                <div key={t.id} className={`flex items-start justify-between py-1.5 px-2.5 rounded text-xs ${profit ? 'bg-gain/10 border border-gain/20' : loss ? 'bg-loss/10 border border-loss/20' : isBuy ? 'bg-muted/20 border border-border/30' : 'bg-muted/10 border border-border/20'}`}>
                  <div className="flex items-start gap-2 min-w-0">
                    {isBuy ? <TrendingUp className="w-3 h-3 text-accent mt-0.5 flex-shrink-0" />
                      : profit ? <TrendingUp className="w-3 h-3 text-gain mt-0.5 flex-shrink-0" />
                      : <TrendingDown className="w-3 h-3 text-loss mt-0.5 flex-shrink-0" />}
                    <div className="min-w-0">
                      <div className="flex items-center gap-1.5 flex-wrap">
                        <span className="font-mono font-semibold">{t.symbol.replace('USDT', '')}</span>
                        <span className={`text-[9px] font-bold px-1 py-0.5 rounded ${isBuy ? 'bg-accent/20 text-accent' : profit ? 'bg-gain/20 text-gain' : 'bg-loss/20 text-loss'}`}>{t.side}</span>
                        <span className="text-muted-foreground font-mono">${Number(t.price).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 })}</span>
                      </div>
                      {t.reason && <p className="text-[9px] text-muted-foreground truncate max-w-[220px]">{t.reason}</p>}
                    </div>
                  </div>
                  <div className="text-right flex-shrink-0 ml-2">
                    {t.pnl !== null && (
                      <div className={`font-mono font-bold text-[10px] ${t.pnl >= 0 ? 'text-gain' : 'text-loss'}`}>
                        {t.pnl >= 0 ? '+' : ''}{t.pnl.toFixed(4)} USDT
                      </div>
                    )}
                    <div className="text-[9px] text-muted-foreground font-mono">{time}</div>
                  </div>
                </div>
              );
            })}
            {trades.length > 15 && (
              <button onClick={() => setShowAllTrades(!showAllTrades)}
                className="w-full text-[10px] text-accent hover:underline py-1 flex items-center justify-center gap-1">
                {showAllTrades ? <><ChevronUp className="w-3 h-3" />Show less</> : <><ChevronDown className="w-3 h-3" />Show all {trades.length} trades</>}
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default AITradingAgent;
