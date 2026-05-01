import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
};

const SYSTEM_PROMPT = `You are DeepTrade AI, an expert crypto trading assistant built into a live paper-trading bot dashboard.

You help the user by:
- Explaining what the bot is doing and why it entered/exited trades
- Analysing specific coins on request (RSI, MACD, EMA trends, volume, support/resistance)
- Suggesting bot settings (interval, profit target, stop-loss)
- Teaching trading concepts clearly with concrete numbers
- Warning about risk — always mention that this is paper trading

Bot context:
- Single AI bot running test/paper trades on Binance spot pairs
- Uses multi-signal confluence scoring (EMA, RSI, MACD, Bollinger Bands, volume, order book)
- Minimum entry score: 65/100
- Always on: passive AI analysis every 5 minutes
- Trade interval: configurable (30s – 10m)

Be concise, use markdown for structure, and use specific numbers. Never give financial advice — always frame as analysis.`;

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  try {
    const { messages } = await req.json();

    const ANTHROPIC_KEY = Deno.env.get("ANTHROPIC_API_KEY");
    const LOVABLE_KEY = Deno.env.get("LOVABLE_API_KEY");

    // Try Anthropic first, then fall back to Lovable gateway
    if (ANTHROPIC_KEY) {
      return await callAnthropic(messages, ANTHROPIC_KEY);
    } else if (LOVABLE_KEY) {
      return await callLovableGateway(messages, LOVABLE_KEY);
    } else {
      // No AI key configured — return a helpful static response
      const reply = "**AI Chat not configured**\n\nTo enable the AI assistant, add your Anthropic API key to Supabase:\n\n1. Go to your [Supabase Dashboard → Project Settings → Edge Functions → Secrets](https://supabase.com/dashboard)\n2. Add a secret named `ANTHROPIC_API_KEY` with your key from [console.anthropic.com](https://console.anthropic.com)\n3. Restart the app\n\nThe trading bot itself works without this — only the chat assistant needs it.";
      return openAiStream(reply, corsHeaders);
    }
  } catch (e) {
    console.error("trading-chat error:", e);
    const msg = e instanceof Error ? e.message : "Unknown error";
    return new Response(JSON.stringify({ error: msg }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});

// ── Anthropic Claude (streaming, converted to OpenAI SSE format) ──────────────
async function callAnthropic(messages: any[], apiKey: string) {
  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 1024,
      system: SYSTEM_PROMPT,
      stream: true,
      messages: messages.map((m: any) => ({ role: m.role, content: m.content })),
    }),
  });

  if (!response.ok || !response.body) {
    const t = await response.text();
    throw new Error(`Anthropic API error ${response.status}: ${t}`);
  }

  // Convert Anthropic SSE → OpenAI SSE so existing frontend parser works
  const readable = new ReadableStream({
    async start(controller) {
      const reader = response.body!.getReader();
      const decoder = new TextDecoder();
      let buf = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });

        let nl: number;
        while ((nl = buf.indexOf("\n")) !== -1) {
          const line = buf.slice(0, nl).trimEnd();
          buf = buf.slice(nl + 1);
          if (!line.startsWith("data: ")) continue;
          const json = line.slice(6).trim();
          if (json === "[DONE]") continue;
          try {
            const evt = JSON.parse(json);
            // Anthropic content_block_delta → OpenAI delta
            if (evt.type === "content_block_delta" && evt.delta?.type === "text_delta") {
              const openAiChunk = { choices: [{ delta: { content: evt.delta.text } }] };
              controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(openAiChunk)}\n\n`));
            }
            if (evt.type === "message_stop") {
              controller.enqueue(new TextEncoder().encode("data: [DONE]\n\n"));
            }
          } catch { /* skip malformed */ }
        }
      }
      controller.close();
    },
  });

  return new Response(readable, {
    headers: { ...corsHeaders, "Content-Type": "text/event-stream", "Cache-Control": "no-cache" },
  });
}

// ── Lovable AI gateway (original, OpenAI-compatible streaming) ────────────────
async function callLovableGateway(messages: any[], apiKey: string) {
  const response = await fetch("https://ai.gateway.lovable.dev/v1/chat/completions", {
    method: "POST",
    headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      messages: [{ role: "system", content: SYSTEM_PROMPT }, ...messages],
      stream: true,
    }),
  });

  if (!response.ok) {
    const t = await response.text();
    throw new Error(`Gateway error ${response.status}: ${t}`);
  }
  return new Response(response.body, {
    headers: { ...corsHeaders, "Content-Type": "text/event-stream" },
  });
}

// ── Emit a static message as a single SSE chunk ───────────────────────────────
function openAiStream(text: string, headers: Record<string, string>): Response {
  const chunk = { choices: [{ delta: { content: text } }] };
  const body = `data: ${JSON.stringify(chunk)}\n\ndata: [DONE]\n\n`;
  return new Response(body, {
    headers: { ...headers, "Content-Type": "text/event-stream" },
  });
}
