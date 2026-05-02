import { SupabaseClient } from '@supabase/supabase-js';
import { calcEMA, calcRSI, calcMACD, calcBollingerBands, calcATR, calcSMA, calcEntryScore, calcTechnicalSignal } from './indicators';

const B = 'https://api.binance.com/api/v3';

// ── Binance fee constants ────────────────────────────────────────────────────
// Spot taker fee: 0.1% per trade.
// BUY:  fee taken from coins received  → coins_out = (usdt_in / price) × (1 − FEE)
// SELL: fee taken from USDT received  → usdt_out  = coins_in × price × (1 − FEE)
// Break-even: price must rise ≈ +0.2005% to cover both legs.
export const TAKER_FEE = 0.001;     // 0.1%
const MIN_NOTIONAL_USDT = 11;        // Binance minimum is $10; we add $1 buffer

// Minimum score to place an entry trade.
// 55 requires roughly 2-3 agreeing indicators (EMA + MACD, or EMA + RSI + vol).
export const ENTRY_SCORE_THRESHOLD = 55;

export interface SignalData {
  symbol: string;
  entryScore: number;
  rsi: number;
  ema9: number;
  ema21: number;
  ema50: number;
  macd: { histogram: number; macdLine: number; signalLine: number };
  bb: { position: number; upper: number; lower: number; middle: number };
  atr: number;
  volumeRatio: number;
  obRatio: number;
  spread: number;
  volumeIncreasing: boolean;
  aiBoost: number;
}

export type LivePrices = Record<string, { price: string; priceChangePercent: string }>;

// ── Fetch indicators from Binance klines ─────────────────────────────────────
export async function updateSignals(
  coins: string[],
  sb?: SupabaseClient
): Promise<Record<string, SignalData>> {
  const result: Record<string, SignalData> = {};

  // Latest AI signal boosts from Supabase (optional, non-blocking)
  const aiBoosts: Record<string, number> = {};
  if (sb) {
    const { data } = await sb
      .from('ai_analyses')
      .select('symbol, signal')
      .eq('user_session', 'default')
      .in('symbol', coins)
      .order('created_at', { ascending: false })
      .limit(coins.length * 2);
    if (data) {
      const seen = new Set<string>();
      for (const a of data) {
        if (!seen.has(a.symbol)) {
          seen.add(a.symbol);
          aiBoosts[a.symbol] = a.signal === 'strong_buy' ? 10 : a.signal === 'buy' ? 5 : 0;
        }
      }
    }
  }

  await Promise.all(coins.map(async (symbol) => {
    try {
      const [depthRes, klineRes] = await Promise.all([
        fetch(`${B}/depth?symbol=${symbol}&limit=20`),
        fetch(`${B}/klines?symbol=${symbol}&interval=1m&limit=200`),
      ]);
      const depth = await depthRes.json();
      const klines = await klineRes.json();
      if (!Array.isArray(klines) || klines.length < 50) return;

      const bids = (depth.bids || []).slice(0, 10);
      const asks = (depth.asks || []).slice(0, 10);
      const bidVol = bids.reduce((s: number, b: string[]) => s + parseFloat(b[1]), 0);
      const askVol = asks.reduce((s: number, a: string[]) => s + parseFloat(a[1]), 0);
      const bestBid = bids.length ? parseFloat(bids[0][0]) : 0;
      const bestAsk = asks.length ? parseFloat(asks[0][0]) : 0;
      const spread = bestAsk > 0 ? ((bestAsk - bestBid) / bestAsk) * 100 : 999;

      const closes  = klines.map((k: any[]) => parseFloat(k[4]));
      const highs   = klines.map((k: any[]) => parseFloat(k[2]));
      const lows    = klines.map((k: any[]) => parseFloat(k[3]));
      const volumes = klines.map((k: any[]) => parseFloat(k[5]));

      const ema9  = calcEMA(closes, 9);
      const ema21 = calcEMA(closes, 21);
      const ema50 = calcEMA(closes, 50);
      const rsi   = calcRSI(closes, 14);
      const macd  = calcMACD(closes);
      const bb    = calcBollingerBands(closes);
      const atr   = calcATR(highs, lows, closes, 14);
      const volumeSma     = calcSMA(volumes, 20);
      const currentVol    = volumes[volumes.length - 1] || 0;
      const volumeRatio   = volumeSma > 0 ? currentVol / volumeSma : 1;
      const recentVol     = volumes.slice(-15).reduce((a, b) => a + b, 0);
      const prevVol       = volumes.slice(-30, -15).reduce((a, b) => a + b, 0);
      const volumeIncreasing = recentVol > prevVol;
      const obRatio = bidVol / (askVol || 1);
      const price = closes[closes.length - 1];

      const entryScore = calcEntryScore({
        ema9, ema21, ema50, price, rsi, macd, volumeRatio, obRatio, bb, volumeIncreasing,
      });

      result[symbol] = {
        symbol, entryScore, rsi, ema9, ema21, ema50, macd, bb,
        atr, volumeRatio, obRatio, spread, volumeIncreasing,
        aiBoost: aiBoosts[symbol] || 0,
      };
    } catch { /* skip coin on error */ }
  }));

  return result;
}

// ── Check open positions — runs on every price tick (~1 s) ───────────────────
// Exits as soon as the fee-correct PnL turns positive — no minimum hold time.
// onSell is called for each closed position so callers can log the trade.
export async function checkExits(
  prices: LivePrices,
  sb: SupabaseClient,
  stopLossPct = 1.5,
  _minProfitPct = 0,          // reserved: absorbed from AIBotPanel call signature
  onSell?: (d: { symbol: string; price: number; usdtReceived: number; pnl: number }) => void,
): Promise<number> {
  const { data: holdings } = await sb
    .from('paper_portfolio').select('*').eq('user_session', 'default').gt('quantity', 0);
  if (!holdings || holdings.length === 0) return 0;

  const { data: cfg } = await sb
    .from('bot_config').select('current_balance').eq('user_session', 'default').maybeSingle();
  let balance = Number(cfg?.current_balance || 0);

  let executed = 0;

  for (const h of holdings) {
    const lp = prices[h.symbol];
    if (!lp) continue;
    const price = parseFloat(lp.price);
    if (!price) continue;

    const avgEntry = Number(h.avg_entry_price);
    const qty      = Number(h.quantity);

    const rawGainPct = ((price - avgEntry) / avgEntry) * 100;

    // Fee-correct PnL: what we actually receive minus what we actually spent
    const sellProceeds = qty * price * (1 - TAKER_FEE);
    const costBasis    = qty * avgEntry / (1 - TAKER_FEE);
    const pnl          = Math.round((sellProceeds - costBasis) * 10000) / 10000;

    let reason: string | null = null;
    if (rawGainPct <= -stopLossPct) {
      reason = `🛑 Stop loss ${rawGainPct.toFixed(2)}% · net ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(4)} USDT`;
    } else if (pnl > 0) {
      // Sell the instant the position is profitable after both fees
      reason = `✅ Profit +$${pnl.toFixed(4)} USDT · +${rawGainPct.toFixed(3)}%`;
    }

    if (reason) {
      await sb.from('bot_trade_history').insert({
        user_session: 'default', symbol: h.symbol,
        side: 'SELL', price, quantity: qty, pnl, reason,
      });
      await sb.from('paper_portfolio').delete()
        .eq('user_session', 'default').eq('symbol', h.symbol);
      balance += sellProceeds;
      executed++;
      onSell?.({ symbol: h.symbol, price, usdtReceived: sellProceeds, pnl });
    }
  }

  if (executed > 0) {
    await sb.from('bot_config').update({
      current_balance: Math.round(balance * 10000) / 10000,
      updated_at: new Date().toISOString(),
    }).eq('user_session', 'default');
  }

  return executed;
}

// ── Check entry opportunities — runs on every price tick ─────────────────────
export async function checkEntries(
  coins: string[],
  signals: Record<string, SignalData>,
  prices: LivePrices,
  coinBudgets: Record<string, number>,
  sb: SupabaseClient
): Promise<number> {
  if (Object.keys(signals).length === 0) return 0;

  const { data: cfg } = await sb
    .from('bot_config').select('current_balance').eq('user_session', 'default').maybeSingle();
  if (!cfg) return 0;
  let balance = Number(cfg.current_balance);
  if (balance < MIN_NOTIONAL_USDT) return 0;

  // Rate limit: max 30 buys per hour to avoid overtrading
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const { count } = await sb.from('bot_trade_history')
    .select('*', { count: 'exact', head: true })
    .eq('user_session', 'default').eq('side', 'BUY').gte('created_at', oneHourAgo);
  if ((count || 0) >= 30) return 0;

  const { data: holdings } = await sb.from('paper_portfolio')
    .select('symbol').eq('user_session', 'default').gt('quantity', 0);
  const held = new Set<string>((holdings || []).map((h: any) => h.symbol));

  const candidates = coins
    .filter(s => !held.has(s) && signals[s])
    .map(s => ({ symbol: s, score: signals[s].entryScore + signals[s].aiBoost }))
    .filter(c => c.score >= ENTRY_SCORE_THRESHOLD)
    .sort((a, b) => b.score - a.score);

  let executed = 0;

  for (const { symbol, score } of candidates) {
    const sig = signals[symbol];
    const lp  = prices[symbol];
    if (!lp) continue;
    const price = parseFloat(lp.price);
    if (!price || price <= 0) continue;

    // Hard filters — only exclude clearly bad conditions
    if (sig.spread > 0.5) continue;                                  // extremely wide spread
    if (sig.ema9 < sig.ema21 && sig.ema21 < sig.ema50) continue;    // fully bearish trend
    if (sig.rsi > 75) continue;                                      // extremely overbought

    // USDT to spend (capped at available balance only)
    const defaultBudget = balance / Math.max(coins.length, 1);
    const budget     = coinBudgets[symbol] ?? defaultBudget;
    const allocation = Math.min(budget, balance);
    if (allocation < MIN_NOTIONAL_USDT) continue;

    // Coins received after Binance takes 0.1% buy fee from the coin amount
    const quantity = (allocation / price) * (1 - TAKER_FEE);
    const feeUSDT  = allocation * TAKER_FEE;

    const reasons: string[] = [];
    if (sig.ema9 > sig.ema21)   reasons.push('EMA✓');
    if (sig.rsi < 50)           reasons.push(`RSI ${sig.rsi.toFixed(0)}`);
    if (sig.macd.histogram > 0) reasons.push('MACD✓');
    if (sig.volumeRatio > 1.2)  reasons.push(`Vol ${sig.volumeRatio.toFixed(1)}x`);
    if (sig.aiBoost > 0)        reasons.push('AI✓');

    await sb.from('bot_trade_history').insert({
      user_session: 'default', symbol,
      side: 'BUY', price, quantity, pnl: null,
      reason: `Score ${score}/100 · ${reasons.join(' · ')} · $${allocation.toFixed(2)} USDT · fee $${feeUSDT.toFixed(4)}`,
    });
    await sb.from('paper_portfolio').upsert({
      user_session: 'default', symbol, quantity,
      avg_entry_price: price,
      updated_at: new Date().toISOString(),
    }, { onConflict: 'user_session,symbol' });

    balance -= allocation;
    executed++;
  }

  if (executed > 0) {
    await sb.from('bot_config').update({
      current_balance: Math.round(balance * 10000) / 10000,
      updated_at: new Date().toISOString(),
    }).eq('user_session', 'default');
  }

  return executed;
}

// ── Background AI analysis ───────────────────────────────────────────────────
export async function runAnalysisCycle(
  coins: string[],
  sb: SupabaseClient,
  onStatus?: (msg: string) => void
): Promise<void> {
  onStatus?.('Running analysis...');
  for (const symbol of coins) {
    try {
      const [klineRes, tickerRes] = await Promise.all([
        fetch(`${B}/klines?symbol=${symbol}&interval=1h&limit=48`),
        fetch(`${B}/ticker/24hr?symbol=${symbol}`),
      ]);
      const klines = await klineRes.json();
      const ticker = await tickerRes.json();
      if (!Array.isArray(klines) || klines.length < 20) continue;

      const closes  = klines.map((k: any[]) => parseFloat(k[4]));
      const volumes = klines.map((k: any[]) => parseFloat(k[5]));
      const { signal, confidence, reasoning } = calcTechnicalSignal(closes, volumes);

      await sb.from('ai_analyses').insert({
        user_session: 'default', symbol,
        analysis_type: 'chart', signal, confidence, reasoning,
        price_at_analysis: parseFloat(ticker.lastPrice || '0'),
        predicted_direction: signal.includes('buy') ? 'up' : signal.includes('sell') ? 'down' : 'sideways',
        predicted_change_percent: signal.includes('strong') ? 1.5 : signal === 'hold' ? 0 : 0.5,
      });
    } catch { /* skip */ }
  }
  onStatus?.(`Analyzed ${coins.length} coins · ${new Date().toLocaleTimeString()}`);
}
