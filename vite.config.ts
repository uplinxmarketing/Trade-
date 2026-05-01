import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import fs from "fs";
import { componentTagger } from "lovable-tagger";
import type { Plugin } from "vite";
import type { IncomingMessage, ServerResponse } from "http";

const versionJson = JSON.parse(fs.readFileSync("./public/version.json", "utf-8"));

// Use real git short hash if available — guarantees the fingerprint changes on every push.
// Falls back to version.json commit field if git isn't installed (Windows without git).
let gitHash = versionJson.commit;
try {
  const { execSync: _exec } = require("child_process");
  gitHash = _exec("git rev-parse --short HEAD", { encoding: "utf-8", stdio: ["pipe","pipe","ignore"] }).trim() || versionJson.commit;
  // Also keep version.json in sync so GitHub raw shows the same hash
  if (gitHash !== versionJson.commit) {
    versionJson.commit = gitHash;
    fs.writeFileSync("./public/version.json", JSON.stringify(versionJson, null, 2) + "\n");
  }
} catch { /* git not available — use static commit from version.json */ }

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
          const nodeOs   = await import("os");
          const nodePath = await import("path");
          const nodeFs   = await import("fs");
          const https    = await import("https");
          const http     = await import("http");
          const { execSync } = await import("child_process");

          const ZIP_URL = "https://github.com/uplinxmarketing/Trade-/archive/refs/heads/main.zip";
          const appDir  = process.cwd();
          const uid     = Date.now();
          const zipPath = nodePath.join(nodeOs.tmpdir(), `tb_upd_${uid}.zip`);
          const extPath = nodePath.join(nodeOs.tmpdir(), `tb_upd_ext_${uid}`);

          // Download ZIP following redirects (GitHub → S3)
          await new Promise<void>((resolve, reject) => {
            function get(url: string) {
              const mod = url.startsWith("https") ? https : http;
              (mod as typeof https).get(url, { headers: { "User-Agent": "TradeBot-Updater/1.0" } }, (r) => {
                if (r.statusCode && r.statusCode >= 300 && r.statusCode < 400 && r.headers.location) {
                  return get(r.headers.location);
                }
                const out = nodeFs.createWriteStream(zipPath);
                r.pipe(out);
                out.on("finish", () => out.close(() => resolve()));
                out.on("error", reject);
                r.on("error", reject);
              }).on("error", reject);
            }
            get(ZIP_URL);
          });

          // Extract with PowerShell (built into every modern Windows)
          if (nodeFs.existsSync(extPath)) {
            execSync(`powershell -NoProfile -ExecutionPolicy Bypass -Command "Remove-Item '${extPath}' -Recurse -Force"`, { timeout: 30_000 });
          }
          execSync(
            `powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -Path '${zipPath}' -DestinationPath '${extPath}' -Force"`,
            { timeout: 120_000 }
          );

          // Copy all files except protected dirs — use Node.js built-in fs.cpSync (Node 18+)
          const entries = nodeFs.readdirSync(extPath);
          if (!entries.length) throw new Error("Extracted ZIP is empty");
          const srcDir = nodePath.join(extPath, entries[0]);
          const skip   = new Set(["node_modules", "logs", ".env", "dist", ".git"]);

          for (const entry of nodeFs.readdirSync(srcDir)) {
            if (skip.has(entry)) continue;
            const src = nodePath.join(srcDir, entry);
            const dst = nodePath.join(appDir, entry);
            nodeFs.cpSync(src, dst, { recursive: true, force: true });
          }

          // Cleanup temp files
          try { nodeFs.unlinkSync(zipPath); } catch { /* ok */ }
          try {
            execSync(`powershell -NoProfile -ExecutionPolicy Bypass -Command "Remove-Item '${extPath}' -Recurse -Force -ErrorAction SilentlyContinue"`, { timeout: 20_000, stdio: "ignore" });
          } catch { /* ok */ }

          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: true, output: "Update downloaded and applied." }));
          // Vite restart rebundles with the new version.json.
          // Client polls /api/ping and reloads once the server is back up.
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
    __APP_COMMIT__: JSON.stringify(gitHash),
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
