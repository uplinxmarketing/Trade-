import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-supabase-client-platform, x-supabase-client-platform-version, x-supabase-client-runtime, x-supabase-client-runtime-version",
};

const BINANCE_BASE = "https://api.binance.com";

serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { headers: corsHeaders });

  try {
    const { action, params } = await req.json();

    // Handle save_keys action — store keys as Deno env (in practice these are stored as secrets)
    if (action === "save_keys") {
      // In production, keys should be stored via the secrets management system
      // For now, we validate the keys work by making a test call
      const { apiKey, apiSecret } = params;
      if (!apiKey || !apiSecret) {
        return new Response(JSON.stringify({ error: "API Key and Secret are required" }), {
          status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }

      // Test the keys by calling account endpoint
      const timestamp = Date.now().toString();
      const queryString = `timestamp=${timestamp}&recvWindow=5000`;
      const encoder = new TextEncoder();
      const keyData = encoder.encode(apiSecret);
      const msgData = encoder.encode(queryString);
      const cryptoKey = await crypto.subtle.importKey("raw", keyData, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      const sig = await crypto.subtle.sign("HMAC", cryptoKey, msgData);
      const signature = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');

      const response = await fetch(`${BINANCE_BASE}/api/v3/account?${queryString}&signature=${signature}`, {
        headers: { "X-MBX-APIKEY": apiKey },
      });
      const data = await response.json();

      if (!response.ok) {
        return new Response(JSON.stringify({ error: `Binance rejected keys: ${data.msg || 'Invalid API key'}` }), {
          status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
      }

      // Keys are valid — in a real setup we'd persist them via secrets API
      return new Response(JSON.stringify({ success: true, canTrade: data.canTrade }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const BINANCE_API_KEY = Deno.env.get("BINANCE_API_KEY");
    const BINANCE_SECRET = Deno.env.get("BINANCE_SECRET");

    if (!BINANCE_API_KEY || !BINANCE_SECRET) {
      return new Response(JSON.stringify({ error: "Binance API credentials not configured. Add your keys in the settings panel." }), {
        status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    let url: string;
    let method = "GET";
    const headers: Record<string, string> = {
      "X-MBX-APIKEY": BINANCE_API_KEY,
    };

    const timestamp = Date.now().toString();

    switch (action) {
      case "ticker": {
        const symbol = params?.symbol || "BTCUSDT";
        url = `${BINANCE_BASE}/api/v3/ticker/24hr?symbol=${symbol}`;
        break;
      }
      case "klines": {
        const symbol = params?.symbol || "BTCUSDT";
        const interval = params?.interval || "1h";
        const limit = params?.limit || 24;
        url = `${BINANCE_BASE}/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
        break;
      }
      case "account": {
        const queryString = `timestamp=${timestamp}&recvWindow=5000`;
        const encoder = new TextEncoder();
        const keyData = encoder.encode(BINANCE_SECRET);
        const msgData = encoder.encode(queryString);
        const cryptoKey = await crypto.subtle.importKey("raw", keyData, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
        const sig = await crypto.subtle.sign("HMAC", cryptoKey, msgData);
        const signature = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');
        url = `${BINANCE_BASE}/api/v3/account?${queryString}&signature=${signature}`;
        break;
      }
      case "order": {
        method = "POST";
        const orderParams = new URLSearchParams({
          symbol: params.symbol,
          side: params.side,
          type: params.type,
          quantity: params.quantity,
          timestamp,
          recvWindow: "5000",
        });
        if (params.price) orderParams.set("price", params.price);
        if (params.stopPrice) orderParams.set("stopPrice", params.stopPrice);
        if (params.type === "LIMIT" || params.type === "STOP_LOSS_LIMIT") orderParams.set("timeInForce", "GTC");

        const qs = orderParams.toString();
        const encoder = new TextEncoder();
        const keyData = encoder.encode(BINANCE_SECRET);
        const msgData = encoder.encode(qs);
        const cryptoKey = await crypto.subtle.importKey("raw", keyData, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
        const sig = await crypto.subtle.sign("HMAC", cryptoKey, msgData);
        const signature = Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('');
        url = `${BINANCE_BASE}/api/v3/order?${qs}&signature=${signature}`;
        break;
      }
      default:
        return new Response(JSON.stringify({ error: `Unknown action: ${action}` }), {
          status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
        });
    }

    const response = await fetch(url, { method, headers });
    const data = await response.json();

    if (!response.ok) {
      console.error("Binance API error:", response.status, data);
      return new Response(JSON.stringify({ error: `Binance error: ${data.msg || 'Unknown'}`, code: data.code }), {
        status: response.status, headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify(data), {
      status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  } catch (e) {
    console.error("binance-proxy error:", e);
    return new Response(JSON.stringify({ error: e instanceof Error ? e.message : "Unknown error" }), {
      status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
});
