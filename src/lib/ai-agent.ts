import { calcEMA, calcRSI, calcMACD, calcBollingerBands, calcSMA, calcEntryScore } from './indicators';
import { TAKER_FEE } from './trading-engine';
import type { LivePrices } from './trading-engine';

const B = '/api/proxy/binance';
const MIN_NOTIONAL_USDT = 11;

export interface AgentDecision {
  symbol: string;
  action: 'BUY' | 'SELL' | 'HOLD';
  confidence: number;
  reasoning: string;
}

export interface AgentResponse {
  decisions: AgentDecision[];
  learned: string;
  nextWatch: string;
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

interface Indicators {
  price: number;
  ema9: number; ema21: number; ema50: number;
  rsi: number;
  macd: { macdLine: number; signalLine: number; histogram: number };
  bb: { upper: number; middle: number; lower: number; position: number };
  volumeRatio: number;
  volumeIncreasing: boolean;
  obRatio: number;
  spread: number;
  change15m: number;
}

function fmt(n: number, dp = 4): string {
  return n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: dp });
}

// Build human-readable reasoning from indicator values — no API needed
function buildReasoning(ind: Indicators, action: 'BUY' | 'SELL' | 'HOLD', extra = ''): string {
  const parts: string[] = [];

  const trendStr =
    ind.ema9 > ind.ema21 && ind.ema21 > ind.ema50 ? 'strong uptrend' :
    ind.ema9 > ind.ema21 ? 'short-term uptrend' :
    ind.ema9 < ind.ema21 && ind.ema21 < ind.ema50 ? 'downtrend' : 'mixed trend';
  parts.push(`EMA: ${trendStr} (9=${fmt(ind.ema9)} / 21=${fmt(ind.ema21)} / 50=${fmt(ind.ema50)})`);

  const rsiZone =
    ind.rsi < 30 ? 'oversold — bounce likely' :
    ind.rsi < 45 ? 'bullish zone — room to run' :
    ind.rsi < 60 ? 'neutral — balanced' :
    ind.rsi < 70 ? 'elevated — caution' : 'overbought — avoid entry';
  parts.push(`RSI ${ind.rsi.toFixed(1)} (${rsiZone})`);

  const macdDir = ind.macd.histogram > 0 ? 'positive momentum' : 'negative momentum';
  parts.push(`MACD ${macdDir} (hist ${ind.macd.histogram > 0 ? '+' : ''}${ind.macd.histogram.toFixed(6)})`);

  if (ind.volumeRatio > 1.3) parts.push(`Volume ${ind.volumeRatio.toFixed(1)}× avg — buyers active`);
  else if (ind.volumeRatio < 0.7) parts.push(`Volume ${ind.volumeRatio.toFixed(1)}× avg — low activity`);

  const bbPct = (ind.bb.position * 100).toFixed(0);
  parts.push(`BB position ${bbPct}% (${ind.bb.position < 0.3 ? 'near lower band' : ind.bb.position > 0.7 ? 'near upper band' : 'mid range'})`);

  if (Math.abs(ind.change15m) > 0.1) {
    parts.push(`15m move ${ind.change15m > 0 ? '+' : ''}${ind.change15m.toFixed(2)}%`);
  }

  if (action === 'BUY')  parts.push('→ Entry conditions met — opening position.');
  if (action === 'SELL') parts.push(`→ Exit: ${extra || 'indicators turning bearish'}.`);
  if (action === 'HOLD') parts.push(`→ Hold — ${extra || 'waiting for better setup'}.`);

  return parts.join('. ');
}

export async function fetchIndicators(symbol: string): Promise<{ ind: Indicators; score: number } | null> {
  try {
    const [klineRes, depthRes] = await Promise.all([
      fetch(`${B}/klines?symbol=${symbol}&interval=1m&limit=100`),
      fetch(`${B}/depth?symbol=${symbol}&limit=10`),
    ]);
    const klines = await klineRes.json();
    const depth  = await depthRes.json();
    if (!Array.isArray(klines) || klines.length < 50) return null;

    const closes  = klines.map((k: any[]) => parseFloat(k[4]));
    const volumes = klines.map((k: any[]) => parseFloat(k[5]));

    const price  = closes[closes.length - 1];
    const ema9   = calcEMA(closes, 9);
    const ema21  = calcEMA(closes, 21);
    const ema50  = calcEMA(closes, 50);
    const rsi    = calcRSI(closes, 14);
    const macd   = calcMACD(closes);
    const bb     = calcBollingerBands(closes);
    const volSma = calcSMA(volumes, 20);
    const currentVol = volumes[volumes.length - 1] ?? 0;
    const volumeRatio = volSma > 0 ? currentVol / volSma : 1;
    const recentVol = volumes.slice(-15).reduce((a, b) => a + b, 0);
    const prevVol   = volumes.slice(-30, -15).reduce((a, b) => a + b, 0);
    const volumeIncreasing = recentVol > prevVol;

    const bids = (depth.bids || []).slice(0, 5);
    const asks = (depth.asks || []).slice(0, 5);
    const bidVol = bids.reduce((s: number, b: string[]) => s + parseFloat(b[1]), 0);
    const askVol = asks.reduce((s: number, a: string[]) => s + parseFloat(a[1]), 0);
    const bestBid = bids.length ? parseFloat(bids[0][0]) : 0;
    const bestAsk = asks.length ? parseFloat(asks[0][0]) : 0;
    const spread  = bestAsk > 0 ? ((bestAsk - bestBid) / bestAsk) * 100 : 999;
    const obRatio = askVol > 0 ? bidVol / askVol : 1;

    const change15m = closes.length >= 15
      ? ((price - closes[closes.length - 15]) / closes[closes.length - 15]) * 100 : 0;

    const ind: Indicators = {
      price, ema9, ema21, ema50, rsi, macd, bb,
      volumeRatio, volumeIncreasing, obRatio, spread, change15m,
    };

    const score = calcEntryScore({
      ema9, ema21, ema50, price, rsi, macd, volumeRatio, obRatio, bb, volumeIncreasing,
    });

    return { ind, score };
  } catch {
    return null;
  }
}

// Parse the user's free-text instructions into simple trading parameters
function parseInstructions(text: string): { threshold: number; excludeCoins: string[] } {
  const t = text.toLowerCase();
  const threshold =
    t.includes('aggressive') || t.includes('quick') || t.includes('fast') ? 35 :
    t.includes('conservative') || t.includes('safe') || t.includes('careful') ? 60 : 40;

  // e.g. "avoid DOGE" or "skip BNB"
  const excludeCoins: string[] = [];
  const avoidMatch = t.match(/(?:avoid|skip|no)\s+([a-z]+)/g) || [];
  for (const m of avoidMatch) {
    const coin = m.replace(/(?:avoid|skip|no)\s+/, '').toUpperCase();
    excludeCoins.push(coin + 'USDT');
  }
  return { threshold, excludeCoins };
}

// ── Main agent cycle — runs every 30 s, uses local indicators only ────────────
// No Claude API required. Decisions come from indicator thresholds + rules.
export async function runAgentCycle(
  coins: string[],
  prices: LivePrices,
  positions: OpenPosition[],
  recentTrades: RecentTrade[],
  balance: number,
  userInstructions: string,
): Promise<AgentResponse> {
  const { threshold, excludeCoins } = parseInstructions(userInstructions);
  const heldSymbols = new Set(positions.map(p => p.symbol));

  const decisions: AgentDecision[] = [];
  const watchNotes: string[] = [];

  // Fetch indicators for all coins in parallel
  const results = await Promise.all(
    coins.map(async sym => ({ sym, data: await fetchIndicators(sym) }))
  );

  for (const { sym, data } of results) {
    if (!data) continue;
    if (excludeCoins.includes(sym)) continue;

    const { ind, score } = data;
    const isHeld = heldSymbols.has(sym);

    let action: 'BUY' | 'SELL' | 'HOLD' = 'HOLD';
    let confidence = 50;
    let reasoning = '';

    if (isHeld) {
      // Decide whether to keep holding or exit early (stop-loss runs separately)
      const pos = positions.find(p => p.symbol === sym)!;
      const livePrice = parseFloat(prices[sym]?.price || '0') || ind.price;
      const rawGainPct = ((livePrice - pos.avg_entry_price) / pos.avg_entry_price) * 100;

      const bearishFlip  = ind.ema9 < ind.ema21 && ind.macd.histogram < 0;
      const overbought   = ind.rsi > 72;

      if (bearishFlip && rawGainPct > 0.05) {
        action = 'SELL'; confidence = 78;
        reasoning = buildReasoning(ind, 'SELL', `EMA flipped bearish, ${rawGainPct.toFixed(2)}% gain secured`);
      } else if (overbought && rawGainPct > 0.1) {
        action = 'SELL'; confidence = 72;
        reasoning = buildReasoning(ind, 'SELL', `RSI overbought at ${ind.rsi.toFixed(1)}, locking in ${rawGainPct.toFixed(2)}% gain`);
      } else {
        action = 'HOLD'; confidence = 55;
        const gain = rawGainPct >= 0 ? `+${rawGainPct.toFixed(2)}%` : `${rawGainPct.toFixed(2)}%`;
        reasoning = buildReasoning(ind, 'HOLD', `position ${gain} — no exit signal yet`);
      }
    } else if (balance >= MIN_NOTIONAL_USDT && score >= threshold) {
      // Hard filters: reject clearly bad conditions
      const fullyBearish = ind.ema9 < ind.ema21 && ind.ema21 < ind.ema50;
      if (fullyBearish || ind.rsi > 75 || ind.spread > 0.5) {
        action = 'HOLD'; confidence = 40;
        const why = fullyBearish ? 'bearish EMA stack' : ind.rsi > 75 ? 'RSI overbought' : 'wide spread';
        reasoning = buildReasoning(ind, 'HOLD', `score ${score}/100 qualifies but filtered — ${why}`);
      } else {
        action = 'BUY'; confidence = Math.min(95, score);
        reasoning = buildReasoning(ind, 'BUY');
      }
    } else {
      action = 'HOLD'; confidence = 35;
      if (score >= threshold - 10) {
        watchNotes.push(`${sym.replace('USDT', '')} score ${score}/100 — approaching entry`);
        reasoning = buildReasoning(ind, 'HOLD', `score ${score}/${threshold} needed — close to entry`);
      } else {
        reasoning = buildReasoning(ind, 'HOLD', `score ${score}/${threshold} — waiting`);
      }
    }

    decisions.push({ symbol: sym, action, confidence, reasoning });
  }

  // Summarise recent performance for the "learned" field
  const recentSells = recentTrades.filter(t => t.side === 'SELL' && t.pnl !== null).slice(0, 10);
  const wins        = recentSells.filter(t => (t.pnl ?? 0) > 0).length;
  const totalPnl    = recentSells.reduce((s, t) => s + (t.pnl ?? 0), 0);
  const learned = recentSells.length === 0
    ? 'No completed trades yet — building trade history across all selected coins.'
    : `${wins}/${recentSells.length} recent trades profitable · net ${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(4)} USDT. ${
        wins / recentSells.length >= 0.6 ? 'Strategy performing well — maintaining settings.' :
        wins / recentSells.length >= 0.4 ? 'Win rate around 50% — normal for scalping.' :
        'Below 40% win rate — being more selective on entries.'}`;

  return {
    decisions,
    learned,
    nextWatch: watchNotes.join(' · ') || 'All coins below entry threshold — monitoring for signals.',
  };
}

// ── Trade execution helpers (unchanged) ──────────────────────────────────────

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

  const allocation = Math.min(balance / Math.max(coins.length, 1), balance);
  if (allocation < MIN_NOTIONAL_USDT) return balance;

  const quantity = (allocation / price) * (1 - TAKER_FEE);
  const feeUSDT  = allocation * TAKER_FEE;

  await sb.from('bot_trade_history').insert({
    user_session: 'default', symbol: decision.symbol,
    side: 'BUY', price, quantity, pnl: null,
    reason: `Score ${decision.confidence}/100 · ${decision.reasoning.slice(0, 200)} · $${allocation.toFixed(2)} @ $${price.toFixed(4)} · fee $${feeUSDT.toFixed(4)}`,
  });
  await sb.from('paper_portfolio').upsert({
    user_session: 'default', symbol: decision.symbol,
    quantity, avg_entry_price: price,
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
  const costBasis    = position.quantity * position.avg_entry_price / (1 - TAKER_FEE);
  const pnl          = Math.round((sellProceeds - costBasis) * 10000) / 10000;

  await sb.from('bot_trade_history').insert({
    user_session: 'default', symbol: position.symbol,
    side: 'SELL', price, quantity: position.quantity, pnl,
    reason: `${decision.reasoning.slice(0, 200)} · @ $${price.toFixed(4)}`,
  });
  await sb.from('paper_portfolio').delete()
    .eq('user_session', 'default').eq('symbol', position.symbol);

  return sellProceeds;
}
