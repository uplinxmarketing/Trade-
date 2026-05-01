export function calcEMA(data: number[], period: number): number {
  if (data.length === 0) return 0;
  if (data.length < period) return data.reduce((a, b) => a + b, 0) / data.length;
  const k = 2 / (period + 1);
  let ema = data.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < data.length; i++) ema = data[i] * k + ema * (1 - k);
  return ema;
}

export function calcSMA(data: number[], period: number): number {
  if (data.length < period) return data.reduce((a, b) => a + b, 0) / (data.length || 1);
  return data.slice(-period).reduce((a, b) => a + b, 0) / period;
}

export function calcRSI(closes: number[], period = 14): number {
  if (closes.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff > 0) gains += diff; else losses -= diff;
  }
  const avgGain = gains / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  return 100 - (100 / (1 + avgGain / avgLoss));
}

export function calcMACD(closes: number[], fast = 12, slow = 26, signal = 9) {
  const emaFast = calcEMA(closes, fast);
  const emaSlow = calcEMA(closes, slow);
  const macdLine = emaFast - emaSlow;
  const macdValues: number[] = [];
  for (let i = slow; i < closes.length; i++) {
    macdValues.push(calcEMA(closes.slice(0, i + 1), fast) - calcEMA(closes.slice(0, i + 1), slow));
  }
  const signalLine = macdValues.length >= signal ? calcEMA(macdValues, signal) : macdLine;
  return { macdLine, signalLine, histogram: macdLine - signalLine };
}

export function calcBollingerBands(closes: number[], period = 20, mult = 2) {
  const sma = calcSMA(closes, period);
  const slice = closes.slice(-period);
  const variance = slice.reduce((sum, val) => sum + Math.pow(val - sma, 2), 0) / slice.length;
  const stdDev = Math.sqrt(variance);
  const upper = sma + stdDev * mult;
  const lower = sma - stdDev * mult;
  const current = closes[closes.length - 1];
  const position = upper !== lower ? (current - lower) / (upper - lower) : 0.5;
  return { upper, middle: sma, lower, position };
}

export function calcATR(highs: number[], lows: number[], closes: number[], period = 14): number {
  if (highs.length < 2) return 0;
  const trs: number[] = [];
  for (let i = 1; i < highs.length; i++) {
    trs.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
  }
  return calcSMA(trs, period);
}

export interface EntryScoreParams {
  ema9: number; ema21: number; ema50: number; price: number;
  rsi: number; macd: { histogram: number; macdLine: number; signalLine: number };
  volumeRatio: number; obRatio: number;
  bb: { position: number };
  volumeIncreasing: boolean;
}

export function calcEntryScore(p: EntryScoreParams): number {
  let score = 0;
  if (p.ema9 > p.ema21) score += 10;
  if (p.ema21 > p.ema50) score += 5;
  if (p.price > p.ema9) score += 5;
  if (p.rsi > 30 && p.rsi < 50) score += 25;
  else if (p.rsi >= 50 && p.rsi < 60) score += 15;
  else if (p.rsi < 30) score += 10;
  if (p.macd.histogram > 0) score += 15;
  if (p.macd.macdLine > p.macd.signalLine) score += 10;
  if (p.volumeRatio > 1.5) score += 15;
  else if (p.volumeRatio > 1.3) score += 10;
  else if (p.volumeIncreasing) score += 5;
  if (p.obRatio > 1.5) score += 10;
  else if (p.obRatio > 1.2) score += 7;
  if (p.bb.position < 0.3) score += 5;
  else if (p.bb.position < 0.5) score += 3;
  return Math.min(100, score);
}

export function calcTechnicalSignal(closes: number[], volumes: number[]): { signal: string; confidence: number; reasoning: string } {
  const rsi = calcRSI(closes);
  const ema9 = calcEMA(closes, 9);
  const ema21 = calcEMA(closes, 21);
  const macd = calcMACD(closes);
  const bb = calcBollingerBands(closes);
  const recentVol = volumes.slice(-5).reduce((a, b) => a + b, 0) / 5;
  const avgVol = volumes.slice(-20).reduce((a, b) => a + b, 0) / 20;
  const volRatio = avgVol > 0 ? recentVol / avgVol : 1;

  let bullish = 0, bearish = 0;
  const reasons: string[] = [];

  if (ema9 > ema21) { bullish++; reasons.push('EMA bullish cross'); }
  else { bearish++; reasons.push('EMA bearish'); }

  if (rsi < 40) { bullish++; reasons.push(`RSI oversold (${rsi.toFixed(0)})`); }
  else if (rsi > 65) { bearish++; reasons.push(`RSI overbought (${rsi.toFixed(0)})`); }

  if (macd.histogram > 0) { bullish++; reasons.push('MACD positive'); }
  else { bearish++; }

  if (bb.position < 0.3) { bullish++; reasons.push('near BB lower'); }
  else if (bb.position > 0.7) { bearish++; }

  if (volRatio > 1.3) { bullish++; reasons.push(`volume ${volRatio.toFixed(1)}x`); }

  const total = bullish + bearish || 1;
  const confidence = Math.round((Math.max(bullish, bearish) / total) * 100);

  if (bullish > bearish + 1) return { signal: bullish >= 4 ? 'strong_buy' : 'buy', confidence, reasoning: reasons.join(', ') };
  if (bearish > bullish + 1) return { signal: bearish >= 4 ? 'strong_sell' : 'sell', confidence, reasoning: reasons.join(', ') };
  return { signal: 'hold', confidence: 50, reasoning: 'Mixed signals — holding' };
}
