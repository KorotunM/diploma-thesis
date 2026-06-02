import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const backendProxyTarget = process.env.VITE_BACKEND_PROXY_TARGET ?? "http://127.0.0.1:8004";
const schedulerProxyTarget = process.env.VITE_SCHEDULER_PROXY_TARGET ?? "http://127.0.0.1:8001";
const parserProxyTarget = process.env.VITE_PARSER_PROXY_TARGET ?? "http://127.0.0.1:8002";
const normalizerProxyTarget = process.env.VITE_NORMALIZER_PROXY_TARGET ?? "http://127.0.0.1:8003";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/backend": {
        target: backendProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/backend/, ""),
      },
      "/scheduler": {
        target: schedulerProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/scheduler/, ""),
      },
      "/parser": {
        target: parserProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/parser/, ""),
      },
      "/normalizer": {
        target: normalizerProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/normalizer/, ""),
      },
    },
  },
});
