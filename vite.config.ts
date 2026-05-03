import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import fs from "fs";
import { execSync } from "child_process";

const versionJson = JSON.parse(fs.readFileSync("./public/version.json", "utf-8"));

// Generate a unique fingerprint for each build using the real git commit and
// current timestamp. Write it back to version.json so the served static file
// always matches the bundle baked into this deploy.
let buildCommit = versionJson.commit;
let buildTime   = versionJson.buildTime;
try {
  buildCommit = execSync("git rev-parse --short HEAD", { stdio: ["pipe","pipe","ignore"] }).toString().trim();
  buildTime   = new Date().toISOString();
  fs.writeFileSync("./public/version.json", JSON.stringify(
    { version: versionJson.version, buildTime, commit: buildCommit },
    null, 2
  ) + "\n");
} catch {
  // git unavailable (rare) — fall back to static values already in version.json
}

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "./src") },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  // Bake version into the JS bundle so update checker can compare
  define: {
    __APP_COMMIT__: JSON.stringify(buildCommit),
    __APP_BUILD_TIME__: JSON.stringify(buildTime),
    __APP_VERSION__: JSON.stringify(versionJson.version),
  },
  server: {
    host: "0.0.0.0",
    port: 8080,
    hmr: { overlay: false },
  },
});
