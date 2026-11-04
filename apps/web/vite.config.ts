import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import process from 'node:process'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    // The browser only calls its own origin. No JWT secret, internal service addresses or DB credentials here.
    proxy: { '/v1': { target: process.env.GATEWAY_URL || 'http://localhost:8000', changeOrigin: true } },
  },
  test: { environment: 'jsdom', clearMocks: true, restoreMocks: true },
})
