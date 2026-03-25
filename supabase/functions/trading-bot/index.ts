import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
};

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const sb = createClient(supabaseUrl, supabaseKey);

  try {
    const { action, coins, mode, bot_id } = await req.json();

    if (action === "start") {
      const botId = bot_id || null;

      // 1. Fetch prices, order books, and klines (200 candles for indicators)
      const prices: Record<string, number> = {};
      const orderBooks: Record<string, { bidVol: number; askVol: number; spread: number }> = {};
      const klineData: Record<string, any[]> = {};

      await Promise.all(coins.map(async (symbol: string) => {
        try {
          const [priceResp, depthResp, klineResp] = await Promise.all([
            fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`),
            fetch(`https://api.binance.com/api/v3/depth?symbol=${symbol}&limit=20`),
            fetch(`https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=1m&limit=200`),
          ]);
          const priceData = await priceResp.json();
          const depth = await depthResp.json();
          const klines = await klineResp.json();

          prices[symbol] = parseFloat(priceData.price);

          const bids = (depth.bids || []).slice(0, 10);
          const asks = (depth.asks || []).slice(0, 10);
          const bidVol = bids.reduce((s: number, b: any) => s + parseFloat(b[1]), 0);
          const askVol = asks.reduce((s: number, a: any) => s + parseFloat(a[1]), 0);
          const bestBid = bids.length > 0 ? parseFloat(bids[0][0]) : 0;
          const bestAsk = asks.length > 0 ? parseFloat(asks[0][0]) : 0;
          const spread = bestAsk > 0 ? ((bestAsk - bestBid) / bestAsk) * 100 : 999;
          orderBooks[symbol] = { bidVol, askVol, spread };
          klineData[symbol] = klines;
        } catch {
          prices[symbol] = 0;
        }
      }));

      // 2. Get bot config
      let config: any;
      if (botId) {
        const { data } = await sb.from("trading_bots").select("*").eq("id", botId).maybeSingle();
        config = data;
      } else {
        const { data } = await sb.from("bot_config").select("*").eq("user_session", "default").maybeSingle();
        config = data;
      }

      if (!config) return jsonResp({ error: "No bot config found" }, 400);

      const isMultiBot = !!botId;
      const currentBalance = Number(isMultiBot ? config.available_balance : config.current_balance);
      const configMinProfit = Number(config.min_profit_percent ?? 0.25);
      const configStopLoss = Number(config.stop_loss_percent ?? 1.5);

      // Fee structure: detect BNB discount
      const feePerLeg = 0.10; // default 0.10%
      const roundTripFee = feePerLeg * 2; // 0.20%
      const profitBuffer = 0.05; // min net profit floor
      const minSellTrigger = roundTripFee + profitBuffer; // 0.25%

      const cooldownSeconds = Number(config.cooldown_seconds ?? 15);
      const maxTradesPerHour = Number(config.max_trades_per_hour ?? 6);
      const maxDailyLoss = Number(config.max_daily_loss ?? 50);
      const maxDrawdownPercent = Number(config.max_drawdown_percent ?? 10);
      const stopAfterLosses = Number(config.stop_after_consecutive_losses ?? 3);
      const consecutiveLosses = Number(config.consecutive_losses ?? 0);
      const dailyLoss = Number(config.daily_loss ?? 0);
      const allocatedBudget = Number(isMultiBot ? config.allocated_budget : config.initial_balance);

      // === RISK CHECKS ===
      const drawdownPercent = ((allocatedBudget - currentBalance) / allocatedBudget) * 100;
      if (drawdownPercent >= maxDrawdownPercent) {
        if (isMultiBot) await sb.from("trading_bots").update({ status: "stopped", updated_at: new Date().toISOString() }).eq("id", botId);
        return jsonResp({ success: false, message: `Bot stopped: max drawdown ${maxDrawdownPercent}% reached` });
      }
      if (dailyLoss >= maxDailyLoss) {
        if (isMultiBot) await sb.from("trading_bots").update({ status: "paused", updated_at: new Date().toISOString() }).eq("id", botId);
        return jsonResp({ success: false, message: `Bot paused: max daily loss $${maxDailyLoss} reached` });
      }
      if (consecutiveLosses >= stopAfterLosses) {
        if (isMultiBot) await sb.from("trading_bots").update({ status: "paused", updated_at: new Date().toISOString() }).eq("id", botId);
        return jsonResp({ success: false, message: `Bot paused: ${stopAfterLosses} consecutive losses` });
      }
      if (config.last_trade_at) {
        const timeSinceLast = (Date.now() - new Date(config.last_trade_at).getTime()) / 1000;
        if (timeSinceLast < cooldownSeconds) {
          return jsonResp({ success: true, message: `Cooldown: ${Math.ceil(cooldownSeconds - timeSinceLast)}s remaining` });
        }
      }
      const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
      const { count: recentTradeCount } = await sb
        .from("bot_trade_history")
        .select("*", { count: "exact", head: true })
        .eq("user_session", "default")
        .gte("created_at", oneHourAgo);
      if ((recentTradeCount || 0) >= maxTradesPerHour) {
        return jsonResp({ success: true, message: `Max ${maxTradesPerHour} trades/hr reached` });
      }

      // 3. Get current holdings
      const { data: holdings } = await sb.from("paper_portfolio").select("*").eq("user_session", "default");
      const holdingsMap: Record<string, { qty: number; avgPrice: number; buyFee: number }> = {};
      if (holdings) {
        for (const h of holdings) {
          holdingsMap[h.symbol] = { qty: Number(h.quantity), avgPrice: Number(h.avg_entry_price), buyFee: Number(h.avg_entry_price) * feePerLeg / 100 };
        }
      }

      // 4. Get latest AI analyses
      const { data: aiAnalyses } = await sb
        .from("ai_analyses")
        .select("symbol, signal, confidence, predicted_direction, predicted_change_percent")
        .eq("user_session", "default")
        .in("symbol", coins)
        .order("created_at", { ascending: false })
        .limit(coins.length * 2);
      const latestAnalysis: Record<string, any> = {};
      if (aiAnalyses) {
        for (const a of aiAnalyses) {
          if (!latestAnalysis[a.symbol]) latestAnalysis[a.symbol] = a;
        }
      }

      // === BTC MACRO TREND CHECK (global filter) ===
      let btcBearish = false;
      if (klineData["BTCUSDT"] && klineData["BTCUSDT"].length >= 50) {
        const btcCloses = klineData["BTCUSDT"].map((k: any) => parseFloat(k[4]));
        const btcEma9 = calcEMA(btcCloses, 9);
        const btcEma21 = calcEMA(btcCloses, 21);
        const btcEma50 = calcEMA(btcCloses, 50);
        btcBearish = btcEma9 < btcEma21 && btcEma21 < btcEma50;
      }

      // 5. Score each coin using multi-signal confluence
      const decisions: any[] = [];

      for (const symbol of coins) {
        const price = prices[symbol];
        if (!price) continue;

        const ob = orderBooks[symbol];
        const klines = klineData[symbol] || [];
        const holding = holdingsMap[symbol];
        const analysis = latestAnalysis[symbol];
        const aiSignal = analysis?.signal || "hold";
        const aiConfidence = analysis?.confidence || 0;

        // Extract OHLCV
        const closes = klines.map((k: any) => parseFloat(k[4]));
        const highs = klines.map((k: any) => parseFloat(k[2]));
        const lows = klines.map((k: any) => parseFloat(k[3]));
        const volumes = klines.map((k: any) => parseFloat(k[5]));

        if (closes.length < 50) continue;

        // === MARKET CONDITION FILTERS ===
        // Spread check: skip if > 0.05% (strict per spec)
        if (ob && ob.spread > 0.15) continue;

        // Volume filter: 24h volume must be sufficient
        const recentVol = volumes.slice(-15).reduce((a: number, b: number) => a + b, 0);
        const prevVol = volumes.slice(-30, -15).reduce((a: number, b: number) => a + b, 0);
        const volumeIncreasing = recentVol > prevVol;

        // Abnormal spike filter
        const lastClose = closes[closes.length - 1] || price;
        const prevClose = closes[closes.length - 2] || price;
        const spikePercent = Math.abs(((lastClose - prevClose) / prevClose) * 100);
        if (spikePercent > 2) continue;

        // BTC macro filter: if BTC bearish, only allow BTC trades
        if (btcBearish && symbol !== "BTCUSDT") continue;

        // === CALCULATE ALL INDICATORS ===
        const ema9 = calcEMA(closes, 9);
        const ema21 = calcEMA(closes, 21);
        const ema50 = calcEMA(closes, 50);
        const ema200 = closes.length >= 200 ? calcEMA(closes, 200) : calcEMA(closes, closes.length);
        const rsi = calcRSI(closes, 14);
        const macd = calcMACD(closes, 12, 26, 9);
        const stoch = calcStochastic(highs, lows, closes, 14, 3);
        const bb = calcBollingerBands(closes, 20, 2);
        const atr = calcATR(highs, lows, closes, 14);
        const volumeSma = calcSMA(volumes, 20);
        const currentVolume = volumes[volumes.length - 1] || 0;
        const volumeRatio = volumeSma > 0 ? currentVolume / volumeSma : 1;
        const obRatio = ob ? ob.bidVol / (ob.askVol || 1) : 1;

        // === VOLATILITY CAP ===
        const atrPercent = price > 0 ? (atr / price) * 100 : 0;
        const positionSizeMultiplier = atrPercent > 2 ? 0.5 : 1;

        if (holding && holding.qty > 0) {
          // === EXIT LOGIC (Priority-based) ===
          const exactBuyPrice = holding.avgPrice;
          const gainPercent = ((price - exactBuyPrice) / exactBuyPrice) * 100;

          // Dynamic take profit: entry × (1 + fee + fee + min_profit)
          const tpPercent = Math.max(configMinProfit, minSellTrigger);

          // P1 - STOP LOSS (ATR-based recommended)
          const atrStopLoss = atr > 0 ? (atr * 1.5 / exactBuyPrice) * 100 : configStopLoss;
          const effectiveStopLoss = Math.min(configStopLoss, atrStopLoss);
          if (gainPercent <= -effectiveStopLoss) {
            const pnl = (price - exactBuyPrice) * holding.qty;
            decisions.push({
              symbol, side: "SELL", price, quantity: holding.qty,
              reason: `🛑 Stop loss at ${gainPercent.toFixed(2)}% (SL: -${effectiveStopLoss.toFixed(2)}%)`,
              pnl: Math.round(pnl * 100) / 100, score: -100,
              indicators: { rsi, macd: macd.histogram, ema9, ema21, bb: bb.position },
            });
            continue;
          }

          // P2 - PROFIT TARGET
          if (gainPercent >= tpPercent) {
            const pnl = (price - exactBuyPrice) * holding.qty;
            decisions.push({
              symbol, side: "SELL", price, quantity: holding.qty,
              reason: `✅ Take profit at ${gainPercent.toFixed(2)}% (TP: ${tpPercent.toFixed(2)}%, covers fees + ${profitBuffer}% net)`,
              pnl: Math.round(pnl * 100) / 100, score: gainPercent * 10,
              indicators: { rsi, macd: macd.histogram, ema9, ema21 },
            });
            continue;
          }

          // P3 - TRAILING STOP (0.15% pullback from peak after reaching profit zone)
          if (gainPercent > 0.1) {
            const peakPrice = Math.max(...closes.slice(-10));
            const pullbackFromPeak = ((peakPrice - price) / peakPrice) * 100;
            if (pullbackFromPeak > 0.15 && gainPercent > 0) {
              const pnl = (price - exactBuyPrice) * holding.qty;
              decisions.push({
                symbol, side: "SELL", price, quantity: holding.qty,
                reason: `📉 Trailing stop: ${pullbackFromPeak.toFixed(2)}% pullback from peak`,
                pnl: Math.round(pnl * 100) / 100, score: gainPercent * 5,
                indicators: { rsi, pullbackFromPeak },
              });
              continue;
            }
          }

          // P4 - SIGNAL REVERSAL (entry score drops below 30)
          const sellScore = calcEntryScore({ ema9, ema21, ema50, price, rsi, macd, stoch, volumeRatio, obRatio, bb, volumeIncreasing });
          if (sellScore < 30 && gainPercent > -0.1) {
            const pnl = (price - exactBuyPrice) * holding.qty;
            decisions.push({
              symbol, side: "SELL", price, quantity: holding.qty,
              reason: `⚠️ Signal reversal (score: ${sellScore.toFixed(0)}/100)`,
              pnl: Math.round(pnl * 100) / 100, score: -50,
              indicators: { rsi, macd: macd.histogram, score: sellScore },
            });
            continue;
          }

          // P5 - RSI EXTREME (RSI > 75 while in profit → partial sell 50%)
          if (rsi > 75 && gainPercent > 0) {
            const sellQty = holding.qty * 0.5;
            const pnl = (price - exactBuyPrice) * sellQty;
            decisions.push({
              symbol, side: "SELL", price, quantity: sellQty,
              reason: `⚡ RSI extreme (${rsi.toFixed(0)}) — partial sell 50%`,
              pnl: Math.round(pnl * 100) / 100, score: 20,
              indicators: { rsi },
            });
            continue;
          }

          // P6 - EARLY EXIT: volume drop, sell wall, momentum weak
          const earlyExit = (!volumeIncreasing && gainPercent > 0.1) ||
            (obRatio < 0.8 && gainPercent > 0) ||
            (ema9 < ema21 && gainPercent > 0);
          if (earlyExit) {
            const pnl = (price - exactBuyPrice) * holding.qty;
            const exitReason = !volumeIncreasing ? 'volume drop' : obRatio < 0.8 ? 'sell wall' : 'momentum weak';
            decisions.push({
              symbol, side: "SELL", price, quantity: holding.qty,
              reason: `⚡ Early exit: ${exitReason} at ${gainPercent.toFixed(2)}%`,
              pnl: Math.round(pnl * 100) / 100, score: 10,
              indicators: { rsi, volumeRatio, obRatio },
            });
          }
        } else {
          // === ENTRY LOGIC (Multi-Signal Confluence Scoring) ===
          // NEVER enter on single indicator — require confluence of ≥3 signals
          const entryScore = calcEntryScore({ ema9, ema21, ema50, price, rsi, macd, stoch, volumeRatio, obRatio, bb, volumeIncreasing });

          // Hardcoded rules (trading principles)
          // Never buy when EMA9 < EMA21 < EMA50
          if (ema9 < ema21 && ema21 < ema50) continue;
          // Only buy if RSI < 50 and recovering, not still falling
          if (rsi > 70) continue;
          // Volume confirms: need >1.3x average
          if (volumeRatio < 1.0) continue;

          // Score threshold: 65 for entry (from spec)
          const threshold = 65;
          if (entryScore >= threshold) {
            // Position sizing: never risk more than 2% of total per trade, max 25% of balance
            const riskAmount = currentBalance * 0.02;
            const maxAllocation = Math.min(currentBalance * 0.25, currentBalance);
            const minOrderUsd = 5;
            const allocation = Math.min(maxAllocation, Math.max(minOrderUsd, riskAmount / (effectiveStopLossForEntry(atr, price, configStopLoss) / 100))) * positionSizeMultiplier;

            if (allocation >= minOrderUsd && currentBalance >= allocation) {
              const quantity = allocation / price;
              const signalReasons: string[] = [];
              if (ema9 > ema21) signalReasons.push('EMA✓');
              if (rsi < 50 && rsi > 30) signalReasons.push(`RSI ${rsi.toFixed(0)}`);
              if (macd.histogram > 0) signalReasons.push('MACD✓');
              if (volumeRatio > 1.3) signalReasons.push(`Vol ${volumeRatio.toFixed(1)}x`);
              if (obRatio > 1.2) signalReasons.push('OB✓');
              if (price < bb.middle) signalReasons.push('BB✓');

              decisions.push({
                symbol, side: "BUY", price,
                quantity: Math.round(quantity * 1e6) / 1e6,
                reason: `Score ${entryScore.toFixed(0)}/100 (${signalReasons.join(', ')})`,
                score: entryScore,
                indicators: { rsi, macd: macd.histogram, ema9, ema21, bb: bb.position, volumeRatio, obRatio, entryScore },
              });
            }
          }
        }
      }

      // 6. Rank and execute: stop losses first, then best opportunity
      decisions.sort((a, b) => {
        if (a.reason.includes('Stop loss') && !b.reason.includes('Stop loss')) return -1;
        if (!a.reason.includes('Stop loss') && b.reason.includes('Stop loss')) return 1;
        return (b.score || 0) - (a.score || 0);
      });
      const stopLosses = decisions.filter(d => d.reason.includes('Stop loss') || d.reason.includes('🛑'));
      const sells = decisions.filter(d => d.side === 'SELL' && !d.reason.includes('🛑'));
      const buys = decisions.filter(d => d.side === 'BUY');
      const toExecute = [...stopLosses, ...sells.slice(0, 1), ...buys.slice(0, 1)];

      const results = [];
      for (const trade of toExecute) {
        // Store indicator snapshot for AI learning
        await sb.from("bot_trade_history").insert({
          user_session: "default",
          symbol: trade.symbol,
          side: trade.side,
          price: trade.price,
          quantity: trade.quantity,
          pnl: trade.pnl || null,
          reason: trade.reason,
          ...(botId ? { bot_id: botId } : {}),
        });

        // Update AI learning - record indicator state at entry
        if (trade.side === "BUY" && trade.indicators) {
          await sb.from("ai_analyses").insert({
            user_session: "default",
            symbol: trade.symbol,
            signal: "buy",
            confidence: trade.score || 0,
            price_at_analysis: trade.price,
            predicted_direction: "up",
            predicted_change_percent: configMinProfit,
            reasoning: `Entry indicators: ${JSON.stringify(trade.indicators)}`,
            analysis_type: "entry_snapshot",
          });
        }

        // Update portfolio
        if (trade.side === "BUY") {
          const cost = trade.price * trade.quantity;
          const newBalance = currentBalance - cost;
          if (isMultiBot) {
            await sb.from("trading_bots").update({
              available_balance: Math.round(newBalance * 100) / 100,
              used_balance: Number(config.used_balance) + cost,
              last_trade_at: new Date().toISOString(),
              total_trades: Number(config.total_trades) + 1,
              updated_at: new Date().toISOString(),
            }).eq("id", botId);
          } else {
            await sb.from("bot_config").update({
              current_balance: Math.round(newBalance * 100) / 100,
              updated_at: new Date().toISOString(),
            }).eq("user_session", "default");
          }

          const existing = holdingsMap[trade.symbol];
          const newQty = (existing?.qty || 0) + trade.quantity;
          const newAvg = existing
            ? (existing.avgPrice * existing.qty + trade.price * trade.quantity) / newQty
            : trade.price;

          await sb.from("paper_portfolio").upsert({
            user_session: "default",
            symbol: trade.symbol,
            quantity: newQty,
            avg_entry_price: Math.round(newAvg * 100) / 100,
            updated_at: new Date().toISOString(),
            ...(botId ? { bot_id: botId } : {}),
          }, { onConflict: "user_session,symbol" });
        } else if (trade.side === "SELL") {
          const proceeds = trade.price * trade.quantity;
          const newBalance = currentBalance + proceeds;
          const pnl = trade.pnl || 0;
          const isLoss = pnl < 0;

          // AI Learning: mark entry analysis as correct/incorrect
          const { data: entryAnalysis } = await sb.from("ai_analyses")
            .select("id")
            .eq("user_session", "default")
            .eq("symbol", trade.symbol)
            .eq("analysis_type", "entry_snapshot")
            .order("created_at", { ascending: false })
            .limit(1)
            .maybeSingle();

          if (entryAnalysis) {
            await sb.from("ai_analyses").update({
              was_correct: !isLoss,
              actual_direction: pnl >= 0 ? "up" : "down",
              actual_change_percent: holdingsMap[trade.symbol]
                ? ((trade.price - holdingsMap[trade.symbol].avgPrice) / holdingsMap[trade.symbol].avgPrice) * 100
                : 0,
              reviewed_at: new Date().toISOString(),
            }).eq("id", entryAnalysis.id);
          }

          // Update learning metrics
          const { data: existingMetric } = await sb.from("learning_metrics")
            .select("*")
            .eq("user_session", "default")
            .eq("symbol", trade.symbol)
            .maybeSingle();

          if (existingMetric) {
            const newTotal = Number(existingMetric.total_predictions) + 1;
            const newCorrect = Number(existingMetric.correct_predictions) + (!isLoss ? 1 : 0);
            await sb.from("learning_metrics").update({
              total_predictions: newTotal,
              correct_predictions: newCorrect,
              accuracy_percent: (newCorrect / newTotal) * 100,
              updated_at: new Date().toISOString(),
            }).eq("id", existingMetric.id);
          } else {
            await sb.from("learning_metrics").insert({
              user_session: "default",
              symbol: trade.symbol,
              total_predictions: 1,
              correct_predictions: !isLoss ? 1 : 0,
              accuracy_percent: !isLoss ? 100 : 0,
            });
          }

          if (isMultiBot) {
            const newConsecutive = isLoss ? consecutiveLosses + 1 : 0;
            const newDailyLoss = isLoss ? dailyLoss + Math.abs(pnl) : dailyLoss;
            await sb.from("trading_bots").update({
              available_balance: Math.round(newBalance * 100) / 100,
              used_balance: Math.max(0, Number(config.used_balance) - proceeds),
              last_trade_at: new Date().toISOString(),
              total_trades: Number(config.total_trades) + 1,
              winning_trades: !isLoss ? Number(config.winning_trades) + 1 : Number(config.winning_trades),
              total_pnl: Number(config.total_pnl) + pnl,
              consecutive_losses: newConsecutive,
              daily_loss: newDailyLoss,
              updated_at: new Date().toISOString(),
            }).eq("id", botId);
          } else {
            await sb.from("bot_config").update({
              current_balance: Math.round(newBalance * 100) / 100,
              updated_at: new Date().toISOString(),
            }).eq("user_session", "default");
          }

          await sb.from("paper_portfolio").delete()
            .eq("user_session", "default")
            .eq("symbol", trade.symbol);
        }
        results.push(trade);
      }

      if (results.length > 0) {
        return jsonResp({ success: true, trades: results, allDecisions: decisions.length, prices });
      }

      return jsonResp({ success: true, message: "No trade opportunities passed filters", prices, balance: currentBalance });
    }

    return jsonResp({ error: "Unknown action" }, 400);
  } catch (e) {
    console.error("Trading bot error:", e);
    return jsonResp({ error: e instanceof Error ? e.message : "Unknown error" }, 500);
  }
});

// === INDICATOR FUNCTIONS ===

function calcEMA(data: number[], period: number): number {
  if (data.length === 0) return 0;
  if (data.length < period) return data.reduce((a, b) => a + b, 0) / data.length;
  const k = 2 / (period + 1);
  let ema = data.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < data.length; i++) {
    ema = data[i] * k + ema * (1 - k);
  }
  return ema;
}

function calcSMA(data: number[], period: number): number {
  if (data.length < period) return data.reduce((a, b) => a + b, 0) / (data.length || 1);
  return data.slice(-period).reduce((a, b) => a + b, 0) / period;
}

function calcRSI(closes: number[], period: number): number {
  if (closes.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff > 0) gains += diff;
    else losses -= diff;
  }
  const avgGain = gains / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

function calcMACD(closes: number[], fast: number, slow: number, signal: number) {
  const emaFast = calcEMA(closes, fast);
  const emaSlow = calcEMA(closes, slow);
  const macdLine = emaFast - emaSlow;

  // Calculate signal line from MACD values
  const macdValues: number[] = [];
  for (let i = slow; i < closes.length; i++) {
    const ef = calcEMA(closes.slice(0, i + 1), fast);
    const es = calcEMA(closes.slice(0, i + 1), slow);
    macdValues.push(ef - es);
  }
  const signalLine = macdValues.length >= signal ? calcEMA(macdValues, signal) : macdLine;
  return { macdLine, signalLine, histogram: macdLine - signalLine };
}

function calcStochastic(highs: number[], lows: number[], closes: number[], period: number, smooth: number) {
  if (closes.length < period) return { k: 50, d: 50 };
  const recentHighs = highs.slice(-period);
  const recentLows = lows.slice(-period);
  const highestHigh = Math.max(...recentHighs);
  const lowestLow = Math.min(...recentLows);
  const currentClose = closes[closes.length - 1];
  const range = highestHigh - lowestLow;
  const k = range > 0 ? ((currentClose - lowestLow) / range) * 100 : 50;
  return { k, d: k }; // simplified
}

function calcBollingerBands(closes: number[], period: number, stdDevMultiplier: number) {
  const sma = calcSMA(closes, period);
  const slice = closes.slice(-period);
  const variance = slice.reduce((sum, val) => sum + Math.pow(val - sma, 2), 0) / slice.length;
  const stdDev = Math.sqrt(variance);
  const upper = sma + stdDev * stdDevMultiplier;
  const lower = sma - stdDev * stdDevMultiplier;
  const current = closes[closes.length - 1];
  const position = upper !== lower ? (current - lower) / (upper - lower) : 0.5;
  return { upper, middle: sma, lower, stdDev, position };
}

function calcATR(highs: number[], lows: number[], closes: number[], period: number): number {
  if (highs.length < 2) return 0;
  const trs: number[] = [];
  for (let i = 1; i < highs.length; i++) {
    const tr = Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i] - closes[i - 1])
    );
    trs.push(tr);
  }
  return calcSMA(trs, period);
}

// === ENTRY SCORING SYSTEM (0-100) ===
function calcEntryScore(params: {
  ema9: number; ema21: number; ema50: number; price: number;
  rsi: number; macd: { histogram: number; macdLine: number; signalLine: number };
  stoch: { k: number; d: number }; volumeRatio: number; obRatio: number;
  bb: { position: number; upper: number; lower: number; middle: number };
  volumeIncreasing: boolean;
}): number {
  const { ema9, ema21, ema50, price, rsi, macd, stoch, volumeRatio, obRatio, bb, volumeIncreasing } = params;
  let score = 0;

  // Trend (EMA) — 20% weight
  if (ema9 > ema21) score += 10;
  if (ema21 > ema50) score += 5;
  if (price > ema9) score += 5;

  // RSI — 25% weight
  if (rsi > 30 && rsi < 50) score += 25; // Oversold recovery zone
  else if (rsi >= 50 && rsi < 60) score += 15;
  else if (rsi < 30) score += 10; // Deeply oversold

  // MACD — 25% weight
  if (macd.histogram > 0) score += 15;
  if (macd.macdLine > macd.signalLine) score += 10;

  // Volume — 15% weight
  if (volumeRatio > 1.5) score += 15;
  else if (volumeRatio > 1.3) score += 10;
  else if (volumeIncreasing) score += 5;

  // Order Book — 10% weight
  if (obRatio > 1.5) score += 10;
  else if (obRatio > 1.2) score += 7;

  // Bollinger Bands — 5% weight
  if (bb.position < 0.3) score += 5; // Near lower band
  else if (bb.position < 0.5) score += 3;

  return Math.min(100, score);
}

function effectiveStopLossForEntry(atr: number, price: number, configStopLoss: number): number {
  if (atr > 0 && price > 0) {
    const atrSl = (atr * 1.5 / price) * 100;
    return Math.min(configStopLoss, atrSl);
  }
  return configStopLoss;
}

function jsonResp(body: any, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
      "Content-Type": "application/json",
    },
  });
}
