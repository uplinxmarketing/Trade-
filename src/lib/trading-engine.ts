import { SupabaseClient } from '@supabase/supabase-js';
import { calcEMA, calcRSI, calcMACD, calcBollingerBands, calcATR, calcSMA, calcEntryScore, calcTechnicalSignal } from './indicators';

const BINANCE_BASE = 'https://api.binance.com/api/v3';

async function fetchBinanceData(symbol: string) {
  try {
    const [priceRes, depthRes, klineRes] = await Promise.all([
      fetch(`${BINANCE_BASE}/ticker/price?symbol=${symbol}`),
      fetch(`${BINANCE_BASE}/depth?symbol=${symbol}&limit=20`),
      fetch(`${BINANCE_BASE}/klines?symbol=${symbol}&interval=1m&limit=200`),
    ]);
    const priceData = await priceRes.json();
    const depth = await depthRes.json();
    const klines = await klineRes.json();
    const price = parseFloat(priceData.price);
    const bids = (depth.bids || []).slice(0, 10);
    const asks = (depth.asks || []).slice(0, 10);
    const bidVol = bids.reduce((s: number, b: string[]) => s + parseFloat(b[1]), 0);
    const askVol = asks.reduce((s: number, a: string[]) => s + parseFloat(a[1]), 0);
    const bestBid = bids.length > 0 ? parseFloat(bids[0][0]) : 0;
    const bestAsk = asks.length > 0 ? parseFloat(asks[0][0]) : 0;
    const spread = bestAsk > 0 ? ((bestAsk - bestBid) / bestAsk) * 100 : 999;
    return { price, bidVol, askVol, spread, klines: Array.isArray(klines) ? klines : [] };
  } catch {
    return null;
  }
}

export async function runTradeCycle(
  coins: string[],
  sb: SupabaseClient,
  onStatus?: (msg: string) => void
): Promise<{ traded: number; message: string }> {
  onStatus?.('Fetching market data...');

  const [configRes] = await Promise.all([
    sb.from('bot_config').select('*').eq('user_session', 'default').maybeSingle(),
  ]);
  const config = configRes.data;
  if (!config) return { traded: 0, message: 'No bot config' };

  let currentBalance = Number(config.current_balance);
  const configMinProfit = Number(config.min_profit_percent ?? 0.25);
  const configStopLoss = Number(config.stop_loss_percent ?? 1.5);
  const feePerLeg = 0.10;
  const minSellTrigger = feePerLeg * 2 + 0.05;

  // Rate limit check
  const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  const { count: recentCount } = await sb.from('bot_trade_history')
    .select('*', { count: 'exact', head: true })
    .eq('user_session', 'default')
    .gte('created_at', oneHourAgo);
  if ((recentCount || 0) >= 20) return { traded: 0, message: 'Max 20 trades/hr reached' };

  // Get current holdings
  const { data: holdings } = await sb.from('paper_portfolio').select('*').eq('user_session', 'default');
  const holdingsMap: Record<string, { qty: number; avgPrice: number }> = {};
  if (holdings) {
    for (const h of holdings) {
      holdingsMap[h.symbol] = { qty: Number(h.quantity), avgPrice: Number(h.avg_entry_price) };
    }
  }

  // Get latest AI analyses
  const { data: aiAnalyses } = await sb.from('ai_analyses')
    .select('symbol, signal, confidence')
    .eq('user_session', 'default')
    .in('symbol', coins)
    .order('created_at', { ascending: false })
    .limit(coins.length * 3);
  const latestAnalysis: Record<string, any> = {};
  if (aiAnalyses) {
    for (const a of aiAnalyses) {
      if (!latestAnalysis[a.symbol]) latestAnalysis[a.symbol] = a;
    }
  }

  onStatus?.('Analyzing signals...');
  const decisions: any[] = [];

  for (const symbol of coins) {
    const data = await fetchBinanceData(symbol);
    if (!data || !data.price || data.klines.length < 50) continue;

    const { price, bidVol, askVol, spread, klines } = data;
    if (spread > 0.15) continue;

    const closes = klines.map((k: any[]) => parseFloat(k[4]));
    const highs = klines.map((k: any[]) => parseFloat(k[2]));
    const lows = klines.map((k: any[]) => parseFloat(k[3]));
    const volumes = klines.map((k: any[]) => parseFloat(k[5]));

    const lastClose = closes[closes.length - 1] || price;
    const prevClose = closes[closes.length - 2] || price;
    if (Math.abs(((lastClose - prevClose) / prevClose) * 100) > 2) continue;

    const ema9 = calcEMA(closes, 9);
    const ema21 = calcEMA(closes, 21);
    const ema50 = calcEMA(closes, 50);
    const rsi = calcRSI(closes, 14);
    const macd = calcMACD(closes);
    const bb = calcBollingerBands(closes);
    const atr = calcATR(highs, lows, closes, 14);
    const volumeSma = calcSMA(volumes, 20);
    const currentVolume = volumes[volumes.length - 1] || 0;
    const volumeRatio = volumeSma > 0 ? currentVolume / volumeSma : 1;
    const obRatio = bidVol / (askVol || 1);
    const recentVol = volumes.slice(-15).reduce((a, b) => a + b, 0);
    const prevVol = volumes.slice(-30, -15).reduce((a, b) => a + b, 0);
    const volumeIncreasing = recentVol > prevVol;

    const holding = holdingsMap[symbol];

    if (holding && holding.qty > 0) {
      const gainPercent = ((price - holding.avgPrice) / holding.avgPrice) * 100;
      const tpPercent = Math.max(configMinProfit, minSellTrigger);
      const atrStopLoss = atr > 0 ? (atr * 1.5 / holding.avgPrice) * 100 : configStopLoss;
      const effectiveSL = Math.min(configStopLoss, atrStopLoss);

      if (gainPercent <= -effectiveSL) {
        const pnl = (price - holding.avgPrice) * holding.qty;
        decisions.push({ symbol, side: 'SELL', price, quantity: holding.qty, pnl: Math.round(pnl * 100) / 100, reason: `🛑 Stop loss ${gainPercent.toFixed(2)}%`, score: -100 });
      } else if (gainPercent >= tpPercent) {
        const pnl = (price - holding.avgPrice) * holding.qty;
        decisions.push({ symbol, side: 'SELL', price, quantity: holding.qty, pnl: Math.round(pnl * 100) / 100, reason: `✅ Take profit ${gainPercent.toFixed(2)}%`, score: gainPercent * 10 });
      } else if (rsi > 75 && gainPercent > 0) {
        const sellQty = holding.qty * 0.5;
        const pnl = (price - holding.avgPrice) * sellQty;
        decisions.push({ symbol, side: 'SELL', price, quantity: sellQty, pnl: Math.round(pnl * 100) / 100, reason: `⚡ RSI extreme (${rsi.toFixed(0)}) — partial 50%`, score: 20 });
      }
    } else {
      if (ema9 < ema21 && ema21 < ema50) continue;
      if (rsi > 70) continue;
      if (volumeRatio < 1.0) continue;

      const entryScore = calcEntryScore({ ema9, ema21, ema50, price, rsi, macd, volumeRatio, obRatio, bb, volumeIncreasing });
      const aiSignal = latestAnalysis[symbol]?.signal || 'hold';
      const aiBoost = aiSignal === 'strong_buy' ? 10 : aiSignal === 'buy' ? 5 : 0;
      const finalScore = entryScore + aiBoost;

      if (finalScore >= 65) {
        const riskAmount = currentBalance * 0.02;
        const maxAllocation = Math.min(currentBalance * 0.25, currentBalance);
        const atrSL = atr > 0 ? (atr * 1.5 / price) * 100 : configStopLoss;
        const effectiveSL = Math.min(configStopLoss, atrSL);
        const allocation = Math.min(maxAllocation, Math.max(5, riskAmount / (effectiveSL / 100)));
        if (allocation >= 5 && currentBalance >= allocation) {
          const signalReasons: string[] = [];
          if (ema9 > ema21) signalReasons.push('EMA✓');
          if (rsi < 50) signalReasons.push(`RSI ${rsi.toFixed(0)}`);
          if (macd.histogram > 0) signalReasons.push('MACD✓');
          if (volumeRatio > 1.3) signalReasons.push(`Vol ${volumeRatio.toFixed(1)}x`);
          if (obRatio > 1.2) signalReasons.push('OB✓');
          if (aiSignal === 'buy' || aiSignal === 'strong_buy') signalReasons.push(`AI ${aiSignal}`);
          decisions.push({ symbol, side: 'BUY', price, quantity: Math.round((allocation / price) * 1e6) / 1e6, reason: `Score ${finalScore.toFixed(0)}/100 (${signalReasons.join(', ')})`, score: finalScore });
        }
      }
    }
  }

  decisions.sort((a, b) => {
    if (a.reason.includes('🛑') && !b.reason.includes('🛑')) return -1;
    if (!a.reason.includes('🛑') && b.reason.includes('🛑')) return 1;
    return (b.score || 0) - (a.score || 0);
  });

  const toExecute = [
    ...decisions.filter(d => d.reason.includes('🛑')),
    ...decisions.filter(d => d.side === 'SELL' && !d.reason.includes('🛑')).slice(0, 1),
    ...decisions.filter(d => d.side === 'BUY').slice(0, 1),
  ];

  if (toExecute.length === 0) return { traded: 0, message: `No signals (${decisions.length} assessed)` };

  onStatus?.(`Executing ${toExecute.length} trade(s)...`);

  for (const trade of toExecute) {
    await sb.from('bot_trade_history').insert({
      user_session: 'default',
      symbol: trade.symbol,
      side: trade.side,
      price: trade.price,
      quantity: trade.quantity,
      pnl: trade.pnl || null,
      reason: trade.reason,
    });

    if (trade.side === 'BUY') {
      const cost = trade.price * trade.quantity;
      currentBalance -= cost;
      const existing = holdingsMap[trade.symbol];
      const newQty = (existing?.qty || 0) + trade.quantity;
      const newAvg = existing
        ? (existing.avgPrice * existing.qty + trade.price * trade.quantity) / newQty
        : trade.price;
      await sb.from('paper_portfolio').upsert({
        user_session: 'default', symbol: trade.symbol,
        quantity: newQty, avg_entry_price: Math.round(newAvg * 100) / 100,
        updated_at: new Date().toISOString(),
      }, { onConflict: 'user_session,symbol' });
    } else {
      const proceeds = trade.price * trade.quantity;
      currentBalance += proceeds;
      const remaining = (holdingsMap[trade.symbol]?.qty || 0) - trade.quantity;
      if (remaining <= 0.000001) {
        await sb.from('paper_portfolio').delete().eq('user_session', 'default').eq('symbol', trade.symbol);
      } else {
        await sb.from('paper_portfolio').update({ quantity: remaining, updated_at: new Date().toISOString() })
          .eq('user_session', 'default').eq('symbol', trade.symbol);
      }
    }
  }

  await sb.from('bot_config').update({
    current_balance: Math.round(currentBalance * 100) / 100,
    last_trade_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }).eq('user_session', 'default');

  return { traded: toExecute.length, message: `Executed ${toExecute.length} trade(s)` };
}

export async function runAnalysisCycle(
  coins: string[],
  sb: SupabaseClient,
  onStatus?: (msg: string) => void
): Promise<void> {
  onStatus?.('Running AI analysis...');
  for (const symbol of coins) {
    try {
      const [klineRes, tickerRes] = await Promise.all([
        fetch(`${BINANCE_BASE}/klines?symbol=${symbol}&interval=1h&limit=48`),
        fetch(`${BINANCE_BASE}/ticker/24hr?symbol=${symbol}`),
      ]);
      const klines = await klineRes.json();
      const ticker = await tickerRes.json();
      if (!Array.isArray(klines) || klines.length < 20) continue;

      const closes = klines.map((k: any[]) => parseFloat(k[4]));
      const volumes = klines.map((k: any[]) => parseFloat(k[5]));
      const { signal, confidence, reasoning } = calcTechnicalSignal(closes, volumes);
      const currentPrice = parseFloat(ticker.lastPrice || '0');

      await sb.from('ai_analyses').insert({
        user_session: 'default',
        symbol,
        analysis_type: 'chart',
        signal,
        confidence,
        reasoning,
        price_at_analysis: currentPrice,
        predicted_direction: signal.includes('buy') ? 'up' : signal.includes('sell') ? 'down' : 'sideways',
        predicted_change_percent: signal.includes('strong') ? 1.5 : signal === 'hold' ? 0 : 0.5,
      });
    } catch {
      // skip this coin
    }
  }
  onStatus?.(`Analysis done: ${new Date().toLocaleTimeString()}`);
}
