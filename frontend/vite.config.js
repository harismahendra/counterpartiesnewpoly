import { defineConfig } from 'vite';

export default defineConfig({
  // Use ESM to avoid CJS deprecation warning
  esbuild: {
    target: 'esnext',
    logOverride: { 'this-is-undefined-in-esm': 'silent' }
  },
  server: {
    port: 3000,
    hmr: {
      overlay: false,
      clientPort: 3000,
      protocol: 'ws',
      host: 'localhost'
    },
    watch: {
      ignored: ['**/node_modules/**', '**/.git/**']
    },
    proxy: {
      '/socket.io': {
        target: 'http://localhost:8000',
        ws: true,
        changeOrigin: true
      }
    }
  },
  logLevel: 'warn',
  optimizeDeps: {
    exclude: ['socket.io-client']
  },
  build: {
    rollupOptions: {
      onwarn(warning, warn) {
        if (warning.code === 'EVAL' || warning.message.includes('SES')) {
          return;
        }
        warn(warning);
      }
    }
  }
});
