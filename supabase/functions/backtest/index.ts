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
    const { coins, days = 7 } = await req.json();

    if (!coins || coins.length === 0) {
      return new Response(JSON.stringify({ error: "No coins specified" }), {
        status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const allTrades: Array<{ symbol: string; side: string; price: number; pnl: number; time: string }> = [];
    let simulatedBalance = 10000;
    const holdings: Record<string, { qty: number; avgPrice: number }> = {};

    // For each coin, fetch historical klines and simulate trading
    for (const symbol of coins) {
      const limit = Math.min(days * 24, 500); // 1h candles
      const resp = await fetch(
        `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=1h&limit=${limit}`
      );
      const klines = await resp.json();

      if (!Array.isArray(klines) || klines.length < 20) continue;

      // Calculate SMA and RSI across the history, simulate trades
      for (let i = 20; i < klines.length; i++) {
        const closes = klines.slice(i - 20, i).map((k: any[]) => parseFloat(k[4]));
        const currentPrice = parseFloat(klines[i][4]);
        const sma7 = closes.slice(-7).reduce((a: number, b: number) => a + b, 0) / 7;
        const sma20 = closes.reduce((a: number, b: number) => a + b, 0) / 20;

        // RSI
        let gains = 0, losses = 0;
        for (let j = 1; j < closes.length; j++) {
          const diff = closes[j] - closes[j - 1];
          if (diff > 0) gains += diff; else losses -= diff;
        }
        const rsi = losses === 0 ? 100 : 100 - (100 / (1 + gains / losses));

        const time = new Date(klines[i][0]).toISOString();
        const holding = holdings[symbol];

        if (holding && holding.qty > 0) {
          const gainPercent = ((currentPrice - holding.avgPrice) / holding.avgPrice) * 100;

          // Sell conditions: take profit > 1.5% or stop loss < -3% or RSI > 70
          if (gainPercent > 1.5 || gainPercent < -3 || rsi > 70) {
            const pnl = (currentPrice - holding.avgPrice) * holding.qty;
            simulatedBalance += currentPrice * holding.qty;
            allTrades.push({
              symbol, side: "SELL", price: currentPrice,
              pnl: Math.round(pnl * 100) / 100, time,
            });
            delete holdings[symbol];
          }
        } else {
          // Buy conditions: price below SMA20, RSI < 35, trending up short-term
          if (currentPrice < sma20 * 0.99 && rsi < 35 && currentPrice > sma7 * 0.98) {
            const allocation = simulatedBalance * 0.15;
            if (allocation > 10) {
              const qty = allocation / currentPrice;
              holdings[symbol] = { qty, avgPrice: currentPrice };
              simulatedBalance -= allocation;
              allTrades.push({
                symbol, side: "BUY", price: currentPrice, pnl: 0, time,
              });
            }
          }
        }
      }
    }

    // Close any remaining positions at last price
    for (const [symbol, holding] of Object.entries(holdings)) {
      const resp = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`);
      const data = await resp.json();
      const currentPrice = parseFloat(data.price);
      const pnl = (currentPrice - holding.avgPrice) * holding.qty;
      simulatedBalance += currentPrice * holding.qty;
      allTrades.push({
        symbol, side: "SELL (close)", price: currentPrice,
        pnl: Math.round(pnl * 100) / 100, time: new Date().toISOString(),
      });
    }

    const totalPnl = simulatedBalance - 10000;
    const winningTrades = allTrades.filter(t => t.pnl > 0).length;
    const closedTrades = allTrades.filter(t => t.side.startsWith("SELL")).length;
    const winRate = closedTrades > 0 ? (winningTrades / closedTrades) * 100 : 0;

    // Store backtest trades as learning data
    for (const t of allTrades.slice(0, 50)) {
      await sb.from("bot_trade_history").insert({
        user_session: "backtest",
        symbol: t.symbol,
        side: t.side.replace(" (close)", ""),
        price: t.price,
        quantity: 0,
        pnl: t.pnl || null,
        reason: `Backtest: ${days}d historical`,
      });
    }

    return new Response(JSON.stringify({
      totalTrades: allTrades.length,
      totalPnl: Math.round(totalPnl * 100) / 100,
      winRate: Math.round(winRate * 10) / 10,
      finalBalance: Math.round(simulatedBalance * 100) / 100,
      trades: allTrades.slice(-20),
    }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("Backtest error:", e);
    return new Response(JSON.stringify({ error: e instanceof Error ? e.message : "Unknown error" }), {
      status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
