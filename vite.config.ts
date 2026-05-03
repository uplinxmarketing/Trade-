import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import fs from "fs";

const versionJson = JSON.parse(fs.readFileSync("./public/version.json", "utf-8"));

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
    __APP_COMMIT__: JSON.stringify(versionJson.commit),
    __APP_BUILD_TIME__: JSON.stringify(versionJson.buildTime),
    __APP_VERSION__: JSON.stringify(versionJson.version),
  },
  server: {
    host: "0.0.0.0",
    port: 8080,
    hmr: { overlay: false },
  },
});
