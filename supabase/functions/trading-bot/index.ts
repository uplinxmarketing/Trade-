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
      // Support both legacy single-bot and new multi-bot
      const botId = bot_id || null;

      // 1. Fetch current prices + order books for market filtering
      const prices: Record<string, number> = {};
      const orderBooks: Record<string, { bidVol: number; askVol: number; spread: number }> = {};
      const klineData: Record<string, any[]> = {};

      await Promise.all(coins.map(async (symbol: string) => {
        try {
          const [priceResp, depthResp, klineResp] = await Promise.all([
            fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`),
            fetch(`https://api.binance.com/api/v3/depth?symbol=${symbol}&limit=20`),
            fetch(`https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=1m&limit=50`),
          ]);
          const priceData = await priceResp.json();
          const depth = await depthResp.json();
          const klines = await klineResp.json();

          prices[symbol] = parseFloat(priceData.price);

          // Order book analysis
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

      // 2. Get bot config (multi-bot or legacy)
      let config: any;
      if (botId) {
        const { data } = await sb.from("trading_bots").select("*").eq("id", botId).maybeSingle();
        config = data;
      } else {
        const { data } = await sb.from("bot_config").select("*").eq("user_session", "default").maybeSingle();
        config = data;
      }

      if (!config) {
        return jsonResp({ error: "No bot config found" }, 400);
      }

      // Multi-bot fields
      const isMultiBot = !!botId;
      const currentBalance = Number(isMultiBot ? config.available_balance : config.current_balance);
      const configMinProfit = Number(config.min_profit_percent ?? 0.5);
      const configStopLoss = Number(config.stop_loss_percent ?? 0.8);
      const fee = 0.2; // Binance fee %
      const buffer = 0.1;
      const cooldownSeconds = Number(config.cooldown_seconds ?? 15);
      const maxTradesPerHour = Number(config.max_trades_per_hour ?? 10);
      const maxDailyLoss = Number(config.max_daily_loss ?? 50);
      const maxDrawdownPercent = Number(config.max_drawdown_percent ?? 10);
      const stopAfterLosses = Number(config.stop_after_consecutive_losses ?? 5);
      const consecutiveLosses = Number(config.consecutive_losses ?? 0);
      const dailyLoss = Number(config.daily_loss ?? 0);
      const allocatedBudget = Number(isMultiBot ? config.allocated_budget : config.initial_balance);

      // RISK CHECK: Max drawdown
      const drawdownPercent = ((allocatedBudget - currentBalance) / allocatedBudget) * 100;
      if (drawdownPercent >= maxDrawdownPercent) {
        if (isMultiBot) await sb.from("trading_bots").update({ status: "stopped", updated_at: new Date().toISOString() }).eq("id", botId);
        return jsonResp({ success: false, message: `Bot stopped: max drawdown ${maxDrawdownPercent}% reached (${drawdownPercent.toFixed(1)}%)` });
      }

      // RISK CHECK: Max daily loss
      if (dailyLoss >= maxDailyLoss) {
        if (isMultiBot) await sb.from("trading_bots").update({ status: "paused", updated_at: new Date().toISOString() }).eq("id", botId);
        return jsonResp({ success: false, message: `Bot paused: max daily loss $${maxDailyLoss} reached` });
      }

      // RISK CHECK: Consecutive losses
      if (consecutiveLosses >= stopAfterLosses) {
        if (isMultiBot) await sb.from("trading_bots").update({ status: "paused", updated_at: new Date().toISOString() }).eq("id", botId);
        return jsonResp({ success: false, message: `Bot paused: ${stopAfterLosses} consecutive losses` });
      }

      // RISK CHECK: Cooldown
      if (config.last_trade_at) {
        const timeSinceLast = (Date.now() - new Date(config.last_trade_at).getTime()) / 1000;
        if (timeSinceLast < cooldownSeconds) {
          return jsonResp({ success: true, message: `Cooldown: ${Math.ceil(cooldownSeconds - timeSinceLast)}s remaining` });
        }
      }

      // RISK CHECK: Max trades per hour
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
      const { data: holdings } = await sb
        .from("paper_portfolio")
        .select("*")
        .eq("user_session", "default");
      const holdingsMap: Record<string, { qty: number; avgPrice: number }> = {};
      if (holdings) {
        for (const h of holdings) {
          holdingsMap[h.symbol] = { qty: Number(h.quantity), avgPrice: Number(h.avg_entry_price) };
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

      // 5. Make decisions with MARKET FILTERING + EMA + ORDER BOOK
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

        // === MARKET FILTER (MANDATORY) ===
        // Spread check
        if (ob && ob.spread > 0.15) continue; // Skip if spread > 0.15%

        // Volume analysis from klines
        const closes = klines.map((k: any) => parseFloat(k[4]));
        const volumes = klines.map((k: any) => parseFloat(k[5]));

        if (volumes.length < 10) continue;

        // Volume trend: last 30 candles vs previous 30
        const recentVol = volumes.slice(-15).reduce((a: number, b: number) => a + b, 0);
        const prevVol = volumes.slice(-30, -15).reduce((a: number, b: number) => a + b, 0);
        const volumeIncreasing = recentVol > prevVol;

        // Check for abnormal price spikes (>2% in single candle)
        const lastClose = closes[closes.length - 1] || price;
        const prevClose = closes[closes.length - 2] || price;
        const spikePercent = Math.abs(((lastClose - prevClose) / prevClose) * 100);
        if (spikePercent > 2) continue; // Skip abnormal spikes

        // === EMA TREND DETECTION ===
        const ema9 = calcEMA(closes, 9);
        const ema21 = calcEMA(closes, 21);
        const isBullish = ema9 > ema21 && price > ema9;
        const isBearish = ema9 < ema21 && price < ema9;

        // === ORDER BOOK PRESSURE ===
        const obRatio = ob ? ob.bidVol / (ob.askVol || 1) : 1;
        const buyPressure = obRatio > 1.2;
        const sellPressure = obRatio < 0.8;

        if (holding && holding.qty > 0) {
          // === EXIT LOGIC ===
          const gainPercent = ((price - holding.avgPrice) / holding.avgPrice) * 100;

          // Dynamic take profit: entry × (1 + fee + min_profit + buffer)
          const tpPercent = fee + configMinProfit + buffer;
          const sellThreshold = (aiSignal === "sell" || aiSignal === "strong_sell") && aiConfidence > 50
            ? Math.max(configMinProfit, tpPercent * 0.7)
            : tpPercent;

          const slPercent = configStopLoss;

          // Early exit conditions
          const earlyExit = (!volumeIncreasing && gainPercent > 0.1) || // Volume dropping with small profit
            (sellPressure && gainPercent > 0) || // Sell wall appeared
            (isBearish && gainPercent > 0); // Momentum weakening

          if (gainPercent >= sellThreshold || earlyExit && gainPercent > 0.1) {
            const pnl = (price - holding.avgPrice) * holding.qty;
            decisions.push({
              symbol, side: "SELL", price, quantity: holding.qty,
              reason: earlyExit
                ? `Early exit at ${gainPercent.toFixed(2)}% (${!volumeIncreasing ? 'volume drop' : sellPressure ? 'sell wall' : 'momentum weak'})`
                : `Take profit at ${gainPercent.toFixed(2)}% (TP: ${sellThreshold.toFixed(2)}%)`,
              pnl: Math.round(pnl * 100) / 100,
              aiSignal, aiConfidence, score: gainPercent,
            });
          } else if (gainPercent <= -slPercent) {
            const pnl = (price - holding.avgPrice) * holding.qty;
            decisions.push({
              symbol, side: "SELL", price, quantity: holding.qty,
              reason: `Stop loss at ${gainPercent.toFixed(2)}% (SL: -${slPercent}%)`,
              pnl: Math.round(pnl * 100) / 100,
              aiSignal, aiConfidence, score: -100, // Priority: always execute SL
            });
          }
        } else {
          // === ENTRY LOGIC ===
          // Must have bullish trend + volume + buy pressure + AI confirmation
          const trendOk = isBullish;
          const volumeOk = volumeIncreasing;
          const obOk = buyPressure;
          // Price pulled back near EMA9
          const pullbackToEma = price <= ema9 * 1.003; // Within 0.3% of EMA9

          const aiOk = !analysis ||
            (aiSignal === "strong_buy" && aiConfidence > 40) ||
            (aiSignal === "buy" && aiConfidence > 55);

          // Score the opportunity
          let score = 0;
          if (trendOk) score += 30;
          if (volumeOk) score += 20;
          if (obOk) score += 20;
          if (pullbackToEma) score += 15;
          if (aiOk) score += aiConfidence * 0.15;

          if (score >= 50 && trendOk) {
            const allocationPercent = score > 80 ? 0.3 : score > 65 ? 0.2 : 0.1;
            const maxAllocation = currentBalance * allocationPercent;
            const minOrderUsd = 5;
            if (maxAllocation >= minOrderUsd) {
              const quantity = maxAllocation / price;
              decisions.push({
                symbol, side: "BUY", price,
                quantity: Math.round(quantity * 1e6) / 1e6,
                reason: `Scalp entry (score: ${score.toFixed(0)}, EMA✓, Vol${volumeOk ? '✓' : '✗'}, OB${obOk ? '✓' : '✗'}${pullbackToEma ? ', Pullback✓' : ''})`,
                aiSignal, aiConfidence, score,
              });
            }
          }
        }
      }

      // 6. Rank and pick best
      decisions.sort((a, b) => (b.score || 0) - (a.score || 0));
      // Execute stop losses first, then best opportunity
      const stopLosses = decisions.filter(d => d.reason.includes('Stop loss'));
      const others = decisions.filter(d => !d.reason.includes('Stop loss'));
      const toExecute = [...stopLosses, ...others.slice(0, 1)];

      const results = [];
      for (const trade of toExecute) {
        // Record the trade
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

        // Update portfolio
        if (trade.side === "BUY") {
          const newBalance = currentBalance - trade.price * trade.quantity;
          if (isMultiBot) {
            await sb.from("trading_bots").update({
              available_balance: Math.round(newBalance * 100) / 100,
              used_balance: Number(config.used_balance) + trade.price * trade.quantity,
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
        return jsonResp({
          success: true,
          trades: results,
          allDecisions: decisions.length,
          prices,
        });
      }

      return jsonResp({
        success: true,
        message: "No trade opportunities passed market filter",
        prices,
        balance: currentBalance,
      });
    }

    return jsonResp({ error: "Unknown action" }, 400);
  } catch (e) {
    console.error("Trading bot error:", e);
    return jsonResp({ error: e instanceof Error ? e.message : "Unknown error" }, 500);
  }
});

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
