import { calcEMA, calcRSI, calcMACD, calcBollingerBands, calcSMA } from './indicators';
import { TAKER_FEE } from './trading-engine';
import type { LivePrices } from './trading-engine';

const B = 'https://api.binance.com/api/v3';

const MIN_NOTIONAL_USDT = 11;

export interface AgentDecision {
  symbol: string;
  action: 'BUY' | 'SELL' | 'HOLD';
  confidence: number;       // 0–100
  reasoning: string;        // Claude's full explanation
}

export interface AgentResponse {
  decisions: AgentDecision[];
  learned: string;          // what the agent observes / learns this cycle
  nextWatch: string;        // coins / conditions to watch next
}

export interface OpenPosition {
  symbol: string;
  quantity: number;
  avg_entry_price: number;
}

export interface RecentTrade {
  symbol: string;
  side: 'BUY' | 'SELL';
  price: number;
  pnl: number | null;
  reason: string | null;
  created_at: string;
}

const AGENT_SYSTEM = `You are an autonomous crypto paper-trading agent.
You start with ZERO knowledge and learn from every trade you make.
Your goal is profitable spot trading — enter strong setups, exit quickly once profitable.

Rules:
- Only BUY coins not already held. Only SELL coins you currently hold.
- Minimum trade size: $11 USDT.
- Fee is 0.1% each way. Break-even requires +0.20% price move.
- Never enter extremely overbought coins (RSI > 75) or fully-bearish trends (EMA9 < EMA21 < EMA50).
- Always respond with ONLY valid JSON — no markdown, no extra text.

Response format (strict JSON, no code fences):
{
  "decisions": [
    { "symbol": "BTCUSDT", "action": "BUY", "confidence": 72, "reasoning": "EMA9 crossed above EMA21, RSI at 48 (neutral), MACD histogram positive, volume 1.4x average. Clean setup." },
    { "symbol": "ETHUSDT", "action": "HOLD", "confidence": 55, "reasoning": "RSI 62 elevated, MACD histogram flat. Waiting for pullback." }
  ],
  "learned": "BTC consolidation breakout above 200 EMA looks strong. Previous ETH buy worked well at RSI 42.",
  "nextWatch": "SOL approaching key support at EMA50. BNB volume picking up."
}`;

async function fetchMarketData(symbol: string): Promise<string | null> {
  try {
    const [klineRes, depthRes] = await Promise.all([
      fetch(`${B}/klines?symbol=${symbol}&interval=1m&limit=100`),
      fetch(`${B}/depth?symbol=${symbol}&limit=10`),
    ]);
    const klines = await klineRes.json();
    const depth = await depthRes.json();
    if (!Array.isArray(klines) || klines.length < 30) return null;

    const closes  = klines.map((k: any[]) => parseFloat(k[4]));
    const highs   = klines.map((k: any[]) => parseFloat(k[2]));
    const lows    = klines.map((k: any[]) => parseFloat(k[3]));
    const volumes = klines.map((k: any[]) => parseFloat(k[5]));

    const price  = closes[closes.length - 1];
    const ema9   = calcEMA(closes, 9);
    const ema21  = calcEMA(closes, 21);
    const ema50  = calcEMA(closes, 50);
    const rsi    = calcRSI(closes, 14);
    const macd   = calcMACD(closes);
    const bb     = calcBollingerBands(closes);
    const volSma = calcSMA(volumes, 20);
    const volRatio = volSma > 0 ? (volumes[volumes.length - 1] / volSma) : 1;

    const bids = (depth.bids || []).slice(0, 5);
    const asks = (depth.asks || []).slice(0, 5);
    const bidVol = bids.reduce((s: number, b: string[]) => s + parseFloat(b[1]), 0);
    const askVol = asks.reduce((s: number, a: string[]) => s + parseFloat(a[1]), 0);
    const obRatio = askVol > 0 ? (bidVol / askVol).toFixed(2) : 'N/A';

    const trend = ema9 > ema21 && ema21 > ema50 ? 'BULL' : ema9 < ema21 && ema21 < ema50 ? 'BEAR' : 'MIXED';
    const bbPos = ((price - bb.lower) / (bb.upper - bb.lower) * 100).toFixed(0);

    // 15-min trend from last 15 candles
    const change15m = closes.length >= 15
      ? (((price - closes[closes.length - 15]) / closes[closes.length - 15]) * 100).toFixed(3)
      : '0';

    return `${symbol}: price=$${price.toFixed(4)} trend=${trend} ema9=${ema9.toFixed(4)} ema21=${ema21.toFixed(4)} ema50=${ema50.toFixed(4)} rsi=${rsi.toFixed(1)} macd_hist=${macd.histogram.toFixed(6)} macd_line=${macd.macdLine.toFixed(6)} signal=${macd.signalLine.toFixed(6)} bb_pos=${bbPos}% vol_ratio=${volRatio.toFixed(2)}x ob_ratio=${obRatio} change_15m=${change15m}%`;
  } catch {
    return null;
  }
}

export async function runAgentCycle(
  coins: string[],
  prices: LivePrices,
  positions: OpenPosition[],
  recentTrades: RecentTrade[],
  balance: number,
  userInstructions: string,
): Promise<AgentResponse> {
  // Gather market data for all coins in parallel
  const marketLines = await Promise.all(coins.map(fetchMarketData));
  const marketContext = marketLines.filter(Boolean).join('\n');

  const heldSymbols = new Set(positions.map(p => p.symbol));

  // Summarise open positions with live PnL
  const positionContext = positions.length === 0
    ? 'No open positions.'
    : positions.map(p => {
        const livePrice = parseFloat(prices[p.symbol]?.price || '0') || p.avg_entry_price;
        const sellProceeds = p.quantity * livePrice * (1 - TAKER_FEE);
        const costBasis = p.quantity * p.avg_entry_price / (1 - TAKER_FEE);
        const pnl = sellProceeds - costBasis;
        const pct = ((livePrice - p.avg_entry_price) / p.avg_entry_price * 100).toFixed(3);
        return `${p.symbol}: qty=${p.quantity.toFixed(6)} entry=$${p.avg_entry_price.toFixed(4)} live=$${livePrice.toFixed(4)} pnl=${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)} (${pct}%)`;
      }).join('\n');

  // Last 10 closed trades for learning context
  const tradeHistory = recentTrades
    .filter(t => t.side === 'SELL' && t.pnl !== null)
    .slice(0, 10)
    .map(t => `${t.symbol} SELL $${t.pnl! >= 0 ? '+' : ''}${t.pnl!.toFixed(4)} — ${t.reason ?? ''}`)
    .join('\n') || 'No completed trades yet.';

  const heldList = heldSymbols.size > 0 ? Array.from(heldSymbols).join(', ') : 'none';

  const prompt = `CURRENT STATE
Balance: $${balance.toFixed(2)} USDT free
Currently holding: ${heldList}

MARKET DATA (1-min candles, live indicators):
${marketContext || 'No market data available.'}

OPEN POSITIONS:
${positionContext}

RECENT TRADE OUTCOMES (for learning):
${tradeHistory}

USER TRADING INSTRUCTIONS:
${userInstructions || 'No specific instructions. Use your best judgement to make profitable trades.'}

Analyse the market data above. For every coin listed, decide BUY, SELL, or HOLD.
- You can only BUY coins not currently held AND where balance > $11 USDT.
- You can only SELL coins you currently hold.
- Set action=HOLD for everything else.
Remember to output ONLY valid JSON matching the required format.`;

  const resp = await fetch('/api/agent', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ system: AGENT_SYSTEM, prompt }),
  });

  if (!resp.ok) throw new Error(`Agent API error: ${resp.status}`);
  const data = await resp.json();

  if (data.error) throw new Error(data.error);

  // Validate structure
  const result = data as AgentResponse;
  if (!Array.isArray(result.decisions)) throw new Error('Invalid agent response structure');

  return result;
}

export async function executeBuyDecision(
  decision: AgentDecision,
  balance: number,
  prices: LivePrices,
  coins: string[],
  sb: import('@supabase/supabase-js').SupabaseClient,
): Promise<number> {
  if (decision.action !== 'BUY') return balance;

  const lp = prices[decision.symbol];
  if (!lp) return balance;
  const price = parseFloat(lp.price);
  if (!price || price <= 0) return balance;

  // Allocate evenly across selected coins, capped at available balance
  const allocation = Math.min(balance / Math.max(coins.length, 1), balance);
  if (allocation < MIN_NOTIONAL_USDT) return balance;

  const quantity = (allocation / price) * (1 - TAKER_FEE);
  const feeUSDT = allocation * TAKER_FEE;

  await sb.from('bot_trade_history').insert({
    user_session: 'default',
    symbol: decision.symbol,
    side: 'BUY',
    price,
    quantity,
    pnl: null,
    reason: `AI (${decision.confidence}% conf) · ${decision.reasoning.slice(0, 200)} · $${allocation.toFixed(2)} USDT · fee $${feeUSDT.toFixed(4)}`,
  });

  await sb.from('paper_portfolio').upsert({
    user_session: 'default',
    symbol: decision.symbol,
    quantity,
    avg_entry_price: price,
    updated_at: new Date().toISOString(),
  }, { onConflict: 'user_session,symbol' });

  return balance - allocation;
}

export async function executeSellDecision(
  decision: AgentDecision,
  position: OpenPosition,
  prices: LivePrices,
  sb: import('@supabase/supabase-js').SupabaseClient,
): Promise<number> {
  if (decision.action !== 'SELL') return 0;

  const lp = prices[position.symbol];
  if (!lp) return 0;
  const price = parseFloat(lp.price);
  if (!price || price <= 0) return 0;

  const sellProceeds = position.quantity * price * (1 - TAKER_FEE);
  const costBasis = position.quantity * position.avg_entry_price / (1 - TAKER_FEE);
  const pnl = Math.round((sellProceeds - costBasis) * 10000) / 10000;

  await sb.from('bot_trade_history').insert({
    user_session: 'default',
    symbol: position.symbol,
    side: 'SELL',
    price,
    quantity: position.quantity,
    pnl,
    reason: `AI (${decision.confidence}% conf) · ${decision.reasoning.slice(0, 200)}`,
  });

  await sb.from('paper_portfolio').delete()
    .eq('user_session', 'default')
    .eq('symbol', position.symbol);

  return sellProceeds;
}
