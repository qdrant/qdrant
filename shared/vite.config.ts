import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import mkcert from "vite-plugin-mkcert";
import tsPaths from "vite-tsconfig-paths";

export default defineConfig({
  server: {
    https: {},
    port: 5173,
  },
  plugins: [
    react(),
    mkcert({
      source: "coding",
    }),
    tsPaths(),
  ],
  build: {
    cssMinify: "lightningcss",
    rollupOptions: {
      output: {
        manualChunks: {
          "react-vendor": ["react", "react-dom", "react-router-dom"],
        },
      },
    },
  },
  optimizeDeps: {
    include: ["react/jsx-runtime"],
  },
});
