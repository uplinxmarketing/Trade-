import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import fs from "fs";
import { componentTagger } from "lovable-tagger";
import type { Plugin, ViteDevServer } from "vite";
import type { IncomingMessage, ServerResponse } from "http";

const versionJson = JSON.parse(fs.readFileSync("./public/version.json", "utf-8"));

const SYSTEM_PROMPT = `You are DeepTrade AI, an expert crypto trading assistant built into a live paper-trading bot dashboard.
You help the user by:
- Explaining what the bot is doing and why it entered/exited trades
- Analysing specific coins on request (RSI, MACD, EMA trends, volume, support/resistance)
- Suggesting bot settings (interval, profit target, stop-loss)
- Teaching trading concepts clearly with concrete numbers
- Warning about risk — always mention that this is paper trading
Be concise, use markdown for structure, use specific numbers. Never give financial advice — always frame as analysis.`;

function chatProxyPlugin(): Plugin {
  return {
    name: "chat-proxy",
    configureServer(server) {
      server.middlewares.use(
        "/api/chat",
        async (req: IncomingMessage, res: ServerResponse) => {
          if (req.method === "OPTIONS") {
            res.writeHead(200, { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "content-type" });
            res.end();
            return;
          }
          if (req.method !== "POST") { res.writeHead(405); res.end(); return; }

          let body = "";
          for await (const chunk of req) body += chunk;

          const apiKey = process.env.ANTHROPIC_API_KEY;

          if (!apiKey) {
            const msg = "**AI Chat setup required**\n\nTo enable the AI assistant:\n1. Get a free API key from [console.anthropic.com](https://console.anthropic.com)\n2. Add this line to your `.env` file:\n   ```\n   ANTHROPIC_API_KEY=sk-ant-...\n   ```\n3. Restart the app\n\nThe trading bot itself works without this — only the chat needs it.";
            const chunk = JSON.stringify({ choices: [{ delta: { content: msg } }] });
            res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" });
            res.write(`data: ${chunk}\n\ndata: [DONE]\n\n`);
            res.end();
            return;
          }

          try {
            const { messages } = JSON.parse(body);
            const upstream = await fetch("https://api.anthropic.com/v1/messages", {
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
                messages: messages.map((m: { role: string; content: string }) => ({ role: m.role, content: m.content })),
              }),
            });

            if (!upstream.ok) {
              const txt = await upstream.text();
              throw new Error(`Anthropic ${upstream.status}: ${txt}`);
            }

            res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" });

            const reader = upstream.body!.getReader();
            const dec = new TextDecoder();
            let buf = "";

            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              buf += dec.decode(value, { stream: true });
              let nl: number;
              while ((nl = buf.indexOf("\n")) !== -1) {
                const line = buf.slice(0, nl).trimEnd();
                buf = buf.slice(nl + 1);
                if (!line.startsWith("data: ")) continue;
                const json = line.slice(6).trim();
                if (json === "[DONE]") continue;
                try {
                  const evt = JSON.parse(json);
                  if (evt.type === "content_block_delta" && evt.delta?.type === "text_delta") {
                    res.write(`data: ${JSON.stringify({ choices: [{ delta: { content: evt.delta.text } }] })}\n\n`);
                  }
                  if (evt.type === "message_stop") res.write("data: [DONE]\n\n");
                } catch { /* skip malformed */ }
              }
            }
            res.end();
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : "Unknown error";
            res.writeHead(500, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ error: msg }));
          }
        }
      );
    },
  };
}

function updatePlugin(): Plugin {
  return {
    name: "update-puller",
    configureServer(server: ViteDevServer) {
      // Simple ping — client polls this to detect when server is back after restart
      server.middlewares.use("/api/ping", (_req: IncomingMessage, res: ServerResponse) => {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      });

      server.middlewares.use("/api/update", async (req: IncomingMessage, res: ServerResponse) => {
        if (req.method !== "POST") { res.writeHead(405); res.end(); return; }
        try {
          const { execSync } = await import("child_process");
          const output = execSync("git pull --ff-only origin main", {
            encoding: "utf-8",
            timeout: 30_000,
            cwd: process.cwd(),
          });
          const trimmed = output.trim();
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: true, output: trimmed }));
          // Restart Vite 1.5 s after responding so the response is fully sent first.
          // The client then polls /api/ping every second and reloads when it gets a reply.
          setTimeout(() => server.restart(), 1500);
        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : String(e);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: msg }));
        }
      });
    },
  };
}

export default defineConfig(({ mode }) => ({
  server: {
    host: "0.0.0.0",
    port: 8080,
    hmr: { overlay: false },
  },
  // Bake version into the JS bundle so update checker works even after git pull
  define: {
    __APP_COMMIT__: JSON.stringify(versionJson.commit),
    __APP_BUILD_TIME__: JSON.stringify(versionJson.buildTime),
    __APP_VERSION__: JSON.stringify(versionJson.version),
  },
  plugins: [
    react(),
    chatProxyPlugin(),
    updatePlugin(),
    mode === "development" && componentTagger(),
  ].filter(Boolean),
  resolve: {
    alias: { "@": path.resolve(__dirname, "./src") },
  },
}));
