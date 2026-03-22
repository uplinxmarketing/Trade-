import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
};

const AI_GATEWAY = "https://ai.gateway.lovable.dev/v1/chat/completions";

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  const LOVABLE_API_KEY = Deno.env.get("LOVABLE_API_KEY");
  if (!LOVABLE_API_KEY) {
    return new Response(JSON.stringify({ error: "LOVABLE_API_KEY not configured" }), {
      status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const supabaseKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
  const sb = createClient(supabaseUrl, supabaseKey);

  try {
    const { action, symbols } = await req.json();

    if (action === "analyze") {
      const results = [];

      for (const symbol of symbols) {
        // 1. Fetch kline data (last 48 1h candles) from Binance public API
        const klineResp = await fetch(
          `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=1h&limit=48`
        );
        const klines = await klineResp.json();

        // 2. Fetch 24h ticker
        const tickerResp = await fetch(
          `https://api.binance.com/api/v3/ticker/24hr?symbol=${symbol}`
        );
        const ticker = await tickerResp.json();

        // 3. Get past learning metrics for this coin
        const { data: metrics } = await sb
          .from("learning_metrics")
          .select("*")
          .eq("user_session", "default")
          .eq("symbol", symbol)
          .maybeSingle();

        // 4. Get recent analysis accuracy
        const { data: recentAnalyses } = await sb
          .from("ai_analyses")
          .select("signal, was_correct, confidence")
          .eq("user_session", "default")
          .eq("symbol", symbol)
          .order("created_at", { ascending: false })
          .limit(10);

        // Build chart data summary for AI
        const chartSummary = klines.slice(-24).map((k: any[]) => ({
          time: new Date(k[0]).toISOString(),
          open: parseFloat(k[1]),
          high: parseFloat(k[2]),
          low: parseFloat(k[3]),
          close: parseFloat(k[4]),
          volume: parseFloat(k[5]),
        }));

        const currentPrice = parseFloat(ticker.lastPrice);
        const priceChange24h = parseFloat(ticker.priceChangePercent);
        const volume24h = parseFloat(ticker.volume);
        const high24h = parseFloat(ticker.highPrice);
        const low24h = parseFloat(ticker.lowPrice);

        // Calculate simple technical indicators
        const closes = chartSummary.map((c: any) => c.close);
        const sma7 = closes.slice(-7).reduce((a: number, b: number) => a + b, 0) / 7;
        const sma20 = closes.slice(-20).reduce((a: number, b: number) => a + b, 0) / Math.min(20, closes.length);
        const rsi = calculateRSI(closes);
        const volatility = ((high24h - low24h) / currentPrice * 100).toFixed(2);

        const learningContext = metrics
          ? `Past accuracy: ${metrics.accuracy_percent}% over ${metrics.total_predictions} predictions. Best pattern: ${metrics.best_pattern || 'unknown'}.`
          : "No prior predictions. This is a fresh analysis.";

        const recentAccuracy = recentAnalyses && recentAnalyses.length > 0
          ? `Recent signals: ${recentAnalyses.map((a: any) => `${a.signal}(${a.was_correct === true ? '✓' : a.was_correct === false ? '✗' : '?'})`).join(', ')}`
          : "";

        // 5. Call AI for analysis with tool calling for structured output
        const aiResp = await fetch(AI_GATEWAY, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${LOVABLE_API_KEY}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            model: "google/gemini-3-flash-preview",
            messages: [
              {
                role: "system",
                content: `You are an expert crypto market analyst. Analyze the provided chart data and technical indicators to generate a trading signal for SPOT trading (buy low, sell high).

Your job is to:
1. Analyze price action, volume, and technical indicators
2. Consider the 24h trend and volatility
3. Factor in the learning context (past prediction accuracy)
4. Generate a clear signal with confidence level

${learningContext}
${recentAccuracy}

Be conservative. Only recommend strong_buy or strong_sell when indicators clearly align. For spot trading, focus on entry points (buying dips) and exit points (selling peaks).`
              },
              {
                role: "user",
                content: `Analyze ${symbol} for spot trading:

Current Price: $${currentPrice}
24h Change: ${priceChange24h}%
24h Volume: ${volume24h.toLocaleString()}
24h High: $${high24h} | Low: $${low24h}
Volatility: ${volatility}%

Technical Indicators:
- SMA(7): $${sma7.toFixed(2)} (price ${currentPrice > sma7 ? 'above' : 'below'})
- SMA(20): $${sma20.toFixed(2)} (price ${currentPrice > sma20 ? 'above' : 'below'})
- RSI(14): ${rsi.toFixed(1)}
- Trend: ${currentPrice > sma7 && sma7 > sma20 ? 'Bullish' : currentPrice < sma7 && sma7 < sma20 ? 'Bearish' : 'Neutral'}

Recent 24h candle data (OHLCV):
${JSON.stringify(chartSummary.slice(-12), null, 1)}`
              }
            ],
            tools: [{
              type: "function",
              function: {
                name: "trading_signal",
                description: "Generate a trading signal with reasoning",
                parameters: {
                  type: "object",
                  properties: {
                    signal: { type: "string", enum: ["strong_buy", "buy", "hold", "sell", "strong_sell"] },
                    confidence: { type: "number", description: "Confidence 0-100" },
                    reasoning: { type: "string", description: "2-3 sentence analysis explaining the signal" },
                    predicted_direction: { type: "string", enum: ["up", "down", "sideways"] },
                    predicted_change_percent: { type: "number", description: "Expected price change % in next 4-8 hours" },
                    key_levels: {
                      type: "object",
                      properties: {
                        support: { type: "number" },
                        resistance: { type: "number" },
                        stop_loss: { type: "number" },
                        take_profit: { type: "number" }
                      },
                      required: ["support", "resistance"]
                    }
                  },
                  required: ["signal", "confidence", "reasoning", "predicted_direction", "predicted_change_percent", "key_levels"],
                  additionalProperties: false
                }
              }
            }],
            tool_choice: { type: "function", function: { name: "trading_signal" } },
          }),
        });

        if (!aiResp.ok) {
          const errText = await aiResp.text();
          console.error(`AI analysis failed for ${symbol}:`, aiResp.status, errText);
          if (aiResp.status === 429) {
            return new Response(JSON.stringify({ error: "Rate limited. Please try again in a moment." }), {
              status: 429, headers: { ...corsHeaders, "Content-Type": "application/json" },
            });
          }
          if (aiResp.status === 402) {
            return new Response(JSON.stringify({ error: "AI credits exhausted. Please add funds in Settings > Workspace > Usage." }), {
              status: 402, headers: { ...corsHeaders, "Content-Type": "application/json" },
            });
          }
          continue;
        }

        const aiData = await aiResp.json();
        let analysis;
        try {
          const toolCall = aiData.choices[0]?.message?.tool_calls?.[0];
          analysis = JSON.parse(toolCall.function.arguments);
        } catch {
          console.error(`Failed to parse AI response for ${symbol}`);
          continue;
        }

        // 6. Store analysis
        await sb.from("ai_analyses").insert({
          user_session: "default",
          symbol,
          analysis_type: "chart",
          signal: analysis.signal,
          confidence: analysis.confidence,
          reasoning: analysis.reasoning,
          price_at_analysis: currentPrice,
          predicted_direction: analysis.predicted_direction,
          predicted_change_percent: analysis.predicted_change_percent,
        });

        results.push({
          symbol,
          currentPrice,
          priceChange24h,
          volume24h,
          sma7,
          sma20,
          rsi,
          volatility: parseFloat(volatility),
          ...analysis,
        });
      }

      // 7. Review past predictions (check ones older than 4 hours)
      await reviewPastPredictions(sb);

      return new Response(JSON.stringify({ success: true, analyses: results }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    if (action === "get_latest") {
      const { data } = await sb
        .from("ai_analyses")
        .select("*")
        .eq("user_session", "default")
        .in("symbol", symbols)
        .order("created_at", { ascending: false })
        .limit(symbols.length * 2);

      // Deduplicate: latest per symbol
      const latest: Record<string, any> = {};
      if (data) {
        for (const row of data) {
          if (!latest[row.symbol]) latest[row.symbol] = row;
        }
      }

      // Get learning metrics
      const { data: metricsData } = await sb
        .from("learning_metrics")
        .select("*")
        .eq("user_session", "default");

      return new Response(JSON.stringify({
        analyses: Object.values(latest),
        metrics: metricsData || [],
      }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ error: "Unknown action" }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("AI analysis error:", e);
    return new Response(JSON.stringify({ error: e instanceof Error ? e.message : "Unknown error" }), {
      status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});

function calculateRSI(closes: number[], period = 14): number {
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

async function reviewPastPredictions(sb: any) {
  // Find unreviewed predictions older than 4 hours
  const fourHoursAgo = new Date(Date.now() - 4 * 60 * 60 * 1000).toISOString();
  const { data: unreviewedPredictions } = await sb
    .from("ai_analyses")
    .select("*")
    .eq("user_session", "default")
    .is("was_correct", null)
    .lt("created_at", fourHoursAgo)
    .limit(20);

  if (!unreviewedPredictions || unreviewedPredictions.length === 0) return;

  for (const pred of unreviewedPredictions) {
    try {
      // Get current price
      const resp = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${pred.symbol}`);
      const data = await resp.json();
      const currentPrice = parseFloat(data.price);
      const priceAtAnalysis = Number(pred.price_at_analysis);

      const actualChange = ((currentPrice - priceAtAnalysis) / priceAtAnalysis) * 100;
      const actualDirection = actualChange > 0.5 ? "up" : actualChange < -0.5 ? "down" : "sideways";
      const wasCorrect = actualDirection === pred.predicted_direction;

      // Update the prediction
      await sb.from("ai_analyses").update({
        actual_direction: actualDirection,
        actual_change_percent: Math.round(actualChange * 100) / 100,
        was_correct: wasCorrect,
        reviewed_at: new Date().toISOString(),
      }).eq("id", pred.id);

      // Update learning metrics
      const { data: existing } = await sb
        .from("learning_metrics")
        .select("*")
        .eq("user_session", "default")
        .eq("symbol", pred.symbol)
        .maybeSingle();

      if (existing) {
        const newTotal = Number(existing.total_predictions) + 1;
        const newCorrect = Number(existing.correct_predictions) + (wasCorrect ? 1 : 0);
        await sb.from("learning_metrics").update({
          total_predictions: newTotal,
          correct_predictions: newCorrect,
          accuracy_percent: Math.round((newCorrect / newTotal) * 10000) / 100,
          updated_at: new Date().toISOString(),
        }).eq("id", existing.id);
      } else {
        await sb.from("learning_metrics").insert({
          user_session: "default",
          symbol: pred.symbol,
          total_predictions: 1,
          correct_predictions: wasCorrect ? 1 : 0,
          accuracy_percent: wasCorrect ? 100 : 0,
        });
      }
    } catch (err) {
      console.error(`Failed to review prediction for ${pred.symbol}:`, err);
    }
  }
}
