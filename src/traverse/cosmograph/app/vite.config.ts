import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  plugins: [
    react(),
    {
      name: "clean-old-assets",
      buildStart() {
        // Remove only stale asset bundles, preserve JSON data + manifest
        const assetsDir = path.resolve(__dirname, "dist/assets");
        if (fs.existsSync(assetsDir)) {
          for (const f of fs.readdirSync(assetsDir)) {
            fs.unlinkSync(path.join(assetsDir, f));
          }
        }
      },
    },
  ],
  build: {
    emptyOutDir: false,
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8080",
    },
  },
});
