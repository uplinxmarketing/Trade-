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
    const { action, coins, mode } = await req.json();

    if (action === "start") {
      // Fetch current prices for selected coins from Binance public API
      const prices: Record<string, number> = {};
      for (const symbol of coins) {
        try {
          const resp = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`);
          const data = await resp.json();
          prices[symbol] = parseFloat(data.price);
        } catch {
          prices[symbol] = 0;
        }
      }

      // Get bot config
      const { data: config } = await sb
        .from("bot_config")
        .select("*")
        .eq("user_session", "default")
        .maybeSingle();

      if (!config) {
        return new Response(JSON.stringify({ error: "No bot config found" }), {
          status: 400,
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }

      const currentBalance = Number(config.current_balance);

      // Get current holdings
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

      // Simple spot strategy: buy low (when price is near recent low), sell high
      // For test mode, simulate analysis and make a decision
      const decisions: Array<{ symbol: string; side: string; price: number; quantity: number; reason: string; pnl?: number }> = [];

      for (const symbol of coins) {
        const price = prices[symbol];
        if (!price) continue;

        const holding = holdingsMap[symbol];

        if (holding && holding.qty > 0) {
          // We hold this coin — check if we should sell
          const gainPercent = ((price - holding.avgPrice) / holding.avgPrice) * 100;

          if (gainPercent > 1.5) {
            // Sell — take profit
            const pnl = (price - holding.avgPrice) * holding.qty;
            decisions.push({
              symbol,
              side: "SELL",
              price,
              quantity: holding.qty,
              reason: `Take profit at ${gainPercent.toFixed(2)}% gain`,
              pnl: Math.round(pnl * 100) / 100,
            });
          } else if (gainPercent < -3) {
            // Stop loss
            const pnl = (price - holding.avgPrice) * holding.qty;
            decisions.push({
              symbol,
              side: "SELL",
              price,
              quantity: holding.qty,
              reason: `Stop loss at ${gainPercent.toFixed(2)}% loss`,
              pnl: Math.round(pnl * 100) / 100,
            });
          }
        } else {
          // We don't hold — consider buying
          // Allocate up to 20% of balance per coin
          const maxAllocation = currentBalance * 0.2;
          if (maxAllocation > 10) {
            const quantity = maxAllocation / price;
            decisions.push({
              symbol,
              side: "BUY",
              price,
              quantity: Math.round(quantity * 1e6) / 1e6,
              reason: `Spot buy — diversifying into ${symbol.replace("USDT", "")}`,
            });
          }
        }
      }

      // Pick the best opportunity (for now just take the first decision)
      // In future iterations, the ML model will rank these
      const trade = decisions.length > 0 ? decisions[0] : null;

      if (trade) {
        // Record the trade
        await sb.from("bot_trade_history").insert({
          user_session: "default",
          symbol: trade.symbol,
          side: trade.side,
          price: trade.price,
          quantity: trade.quantity,
          pnl: trade.pnl || null,
          reason: trade.reason,
        });

        // Update portfolio
        if (trade.side === "BUY") {
          const newBalance = currentBalance - trade.price * trade.quantity;
          await sb.from("bot_config").update({
            current_balance: Math.round(newBalance * 100) / 100,
            updated_at: new Date().toISOString(),
          }).eq("user_session", "default");

          // Upsert holding
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
          }, { onConflict: "user_session,symbol" });
        } else if (trade.side === "SELL") {
          const proceeds = trade.price * trade.quantity;
          const newBalance = currentBalance + proceeds;
          await sb.from("bot_config").update({
            current_balance: Math.round(newBalance * 100) / 100,
            updated_at: new Date().toISOString(),
          }).eq("user_session", "default");

          // Remove holding
          await sb.from("paper_portfolio").delete()
            .eq("user_session", "default")
            .eq("symbol", trade.symbol);
        }

        return new Response(JSON.stringify({
          success: true,
          trade,
          prices,
          balance: trade.side === "BUY"
            ? currentBalance - trade.price * trade.quantity
            : currentBalance + trade.price * trade.quantity,
        }), {
          headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }

      return new Response(JSON.stringify({
        success: true,
        message: "No trade opportunities found at current prices",
        prices,
        balance: currentBalance,
      }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ error: "Unknown action" }), {
      status: 400,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("Trading bot error:", e);
    return new Response(JSON.stringify({ error: e instanceof Error ? e.message : "Unknown error" }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
