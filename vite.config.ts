import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import fs from "fs";
import { componentTagger } from "lovable-tagger";
import type { Plugin } from "vite";
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

          // API key: use env var if set, otherwise accept from the request body
          // (user pastes it into the UI — stored in localStorage, sent per-request)
          let parsed: { messages: unknown[]; apiKey?: string };
          try { parsed = JSON.parse(body); } catch { res.writeHead(400); res.end(); return; }
          const apiKey = process.env.ANTHROPIC_API_KEY || parsed.apiKey || "";

          if (!apiKey) {
            const msg = "**Add your Anthropic API key to use this chat.**\n\nClick the **Add key** button at the top of this panel and paste your key from [console.anthropic.com](https://console.anthropic.com).\n\nThe trading bot itself works completely without any API key.";
            const chunk = JSON.stringify({ choices: [{ delta: { content: msg } }] });
            res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache" });
            res.write(`data: ${chunk}\n\ndata: [DONE]\n\n`);
            res.end();
            return;
          }

          try {
            const { messages } = parsed;
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

function agentPlugin(): Plugin {
  return {
    name: "ai-agent",
    configureServer(server) {
      server.middlewares.use("/api/agent", async (req: IncomingMessage, res: ServerResponse) => {
        if (req.method !== "POST") { res.writeHead(405); res.end(); return; }

        let body = "";
        for await (const chunk of req) body += chunk;

        const apiKey = process.env.ANTHROPIC_API_KEY;
        if (!apiKey) {
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "ANTHROPIC_API_KEY not set" }));
          return;
        }

        try {
          const payload = JSON.parse(body);
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
              system: payload.system,
              messages: [{ role: "user", content: payload.prompt }],
            }),
          });

          if (!upstream.ok) {
            const txt = await upstream.text();
            throw new Error(`Anthropic ${upstream.status}: ${txt}`);
          }

          const data = await upstream.json();
          const text = data.content?.[0]?.text ?? "";

          // Extract JSON from the response (may be wrapped in markdown)
          const jsonMatch = text.match(/\{[\s\S]*\}/);
          if (!jsonMatch) throw new Error("No JSON in agent response");
          const parsed = JSON.parse(jsonMatch[0]);

          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify(parsed));
        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : String(e);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: msg }));
        }
      });
    },
  };
}

function updatePlugin(): Plugin {
  return {
    name: "update-puller",
    configureServer(server) {
      server.middlewares.use("/api/ping", (_req: IncomingMessage, res: ServerResponse) => {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      });

      server.middlewares.use("/api/update", async (req: IncomingMessage, res: ServerResponse) => {
        if (req.method !== "POST") { res.writeHead(405); res.end(); return; }
        try {
          const { execSync } = await import("child_process");
          const nodePath     = await import("path");

          // Use the same update.js script that start.bat uses.
          // It downloads the ZIP from GitHub using only Node.js + PowerShell —
          // no git installation required.
          const updateScript = nodePath.join(process.cwd(), "scripts", "update.js");
          const output = execSync(`node "${updateScript}"`, {
            encoding: "utf-8",
            timeout: 120_000,   // ZIP download can take a minute on slow connections
            cwd: process.cwd(),
          });

          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: true, output: output.trim() }));
          // Restart Vite so the new bundle picks up the freshly-copied source files.
          // Client polls /api/ping every second and reloads once the server is back.
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
    agentPlugin(),
    updatePlugin(),
    mode === "development" && componentTagger(),
  ].filter(Boolean),
  resolve: {
    alias: { "@": path.resolve(__dirname, "./src") },
  },
}));
