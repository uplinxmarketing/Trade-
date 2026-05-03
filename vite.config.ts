import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import fs from "fs";
import { execSync } from "child_process";

const versionJson = JSON.parse(fs.readFileSync("./public/version.json", "utf-8"));

// Prefer RAILWAY_GIT_COMMIT_SHA (injected by Railway Dockerfile ARG) so the
// fingerprint is based on the git commit rather than wall-clock time.
// Falls back to git CLI, then to the last committed value.
// buildTime is always a fresh timestamp for the "Updated to vX.Y.Z" toast.
const railwayShort = (process.env.RAILWAY_GIT_COMMIT_SHA || '').slice(0, 8);
let buildCommit = railwayShort || versionJson.commit;
let buildTime   = new Date().toISOString();

if (!railwayShort) {
  try {
    buildCommit = execSync("git rev-parse --short HEAD", { stdio: ["pipe","pipe","ignore"] }).toString().trim();
  } catch {
    // git unavailable — buildCommit keeps last committed value
  }
}

// Always write version.json so dist/ matches the baked bundle constants
fs.writeFileSync("./public/version.json", JSON.stringify(
  { version: versionJson.version, buildTime, commit: buildCommit },
  null, 2
) + "\n");

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
