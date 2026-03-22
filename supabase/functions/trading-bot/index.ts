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
      // 1. Fetch current prices
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

      // 2. Get bot config
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

      // 4. Get latest AI analyses for smarter decisions
      const { data: aiAnalyses } = await sb
        .from("ai_analyses")
        .select("symbol, signal, confidence, predicted_direction, predicted_change_percent")
        .eq("user_session", "default")
        .in("symbol", coins)
        .order("created_at", { ascending: false })
        .limit(coins.length * 2);

      // Deduplicate: latest per symbol
      const latestAnalysis: Record<string, any> = {};
      if (aiAnalyses) {
        for (const a of aiAnalyses) {
          if (!latestAnalysis[a.symbol]) latestAnalysis[a.symbol] = a;
        }
      }

      // 5. Make AI-informed decisions
      const decisions: Array<{
        symbol: string; side: string; price: number;
        quantity: number; reason: string; pnl?: number;
        aiSignal?: string; aiConfidence?: number;
      }> = [];

      for (const symbol of coins) {
        const price = prices[symbol];
        if (!price) continue;

        const holding = holdingsMap[symbol];
        const analysis = latestAnalysis[symbol];
        const aiSignal = analysis?.signal || "hold";
        const aiConfidence = analysis?.confidence || 0;

        if (holding && holding.qty > 0) {
          // Check if we should sell
          const gainPercent = ((price - holding.avgPrice) / holding.avgPrice) * 100;

          // AI-informed sell: lower threshold if AI says sell
          const sellThreshold = (aiSignal === "sell" || aiSignal === "strong_sell") && aiConfidence > 50
            ? 0.8 // Sell earlier when AI says sell
            : 1.5; // Normal take profit

          const stopLoss = (aiSignal === "sell" || aiSignal === "strong_sell") && aiConfidence > 70
            ? -1.5 // Tighter stop when AI bearish
            : -3;

          if (gainPercent > sellThreshold) {
            const pnl = (price - holding.avgPrice) * holding.qty;
            decisions.push({
              symbol, side: "SELL", price, quantity: holding.qty,
              reason: `Take profit at ${gainPercent.toFixed(2)}% ${aiSignal !== 'hold' ? `(AI: ${aiSignal} ${aiConfidence}%)` : ''}`,
              pnl: Math.round(pnl * 100) / 100,
              aiSignal, aiConfidence,
            });
          } else if (gainPercent < stopLoss) {
            const pnl = (price - holding.avgPrice) * holding.qty;
            decisions.push({
              symbol, side: "SELL", price, quantity: holding.qty,
              reason: `Stop loss at ${gainPercent.toFixed(2)}% ${aiSignal !== 'hold' ? `(AI: ${aiSignal} ${aiConfidence}%)` : ''}`,
              pnl: Math.round(pnl * 100) / 100,
              aiSignal, aiConfidence,
            });
          }
        } else {
          // Consider buying — only if AI says buy or strong_buy with decent confidence
          const shouldBuy = !analysis || // No analysis yet = cautious buy
            (aiSignal === "strong_buy" && aiConfidence > 40) ||
            (aiSignal === "buy" && aiConfidence > 55);

          if (shouldBuy) {
            // Allocate based on AI confidence
            const allocationPercent = aiConfidence > 70 ? 0.25 : aiConfidence > 50 ? 0.15 : 0.1;
            const maxAllocation = currentBalance * allocationPercent;
            if (maxAllocation > 10) {
              const quantity = maxAllocation / price;
              decisions.push({
                symbol, side: "BUY", price,
                quantity: Math.round(quantity * 1e6) / 1e6,
                reason: `Spot buy ${analysis ? `(AI: ${aiSignal} ${aiConfidence}%)` : '(no AI data yet)'}`,
                aiSignal, aiConfidence,
              });
            }
          }
        }
      }

      // 6. Rank decisions by AI confidence and pick the best
      decisions.sort((a, b) => (b.aiConfidence || 0) - (a.aiConfidence || 0));
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

          await sb.from("paper_portfolio").delete()
            .eq("user_session", "default")
            .eq("symbol", trade.symbol);
        }

        return new Response(JSON.stringify({
          success: true,
          trade,
          allDecisions: decisions,
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
        message: "No trade opportunities — AI signals are neutral or bearish with low confidence",
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
