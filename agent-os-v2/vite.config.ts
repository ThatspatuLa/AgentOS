import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 3030,
    host: '127.0.0.1',
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
})
