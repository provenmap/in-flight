import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// `npm run dev:ui` serves the view with hot reload and passes the stream and the API to `serve` on 8088.
const server = process.env.RIF_SERVER ?? 'http://localhost:8088';

export default defineConfig({
  root: 'ui',
  plugins: [react(), tailwindcss()],
  build: { outDir: '../dist/ui', emptyOutDir: true, chunkSizeWarningLimit: 1500 },
  server: {
    proxy: {
      '/ws': { target: server.replace(/^http/, 'ws'), ws: true },
      '/api': server,
      '/mcp': server,
    },
  },
});
