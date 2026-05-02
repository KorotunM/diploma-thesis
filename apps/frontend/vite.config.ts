import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/backend": {
        target: "http://backend:8004",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/backend/, ""),
      },
      "/scheduler": {
        target: "http://scheduler:8001",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/scheduler/, ""),
      },
      "/parser": {
        target: "http://parser:8002",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/parser/, ""),
      },
      "/normalizer": {
        target: "http://normalizer:8003",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/normalizer/, ""),
      },
    },
  },
});
