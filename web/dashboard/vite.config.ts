import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [
    react(),
    {
      name: "dashboard-data-missing",
      configureServer(server) {
        // Development has no implicit access to trading files or credentials.
        server.middlewares.use("/data/", (_request, response) => {
          response.statusCode = 404;
          response.setHeader("Content-Type", "application/json");
          response.end('{"error":"data_not_configured"}');
        });
      },
    },
  ],
  server: { host: "127.0.0.1", port: 5173, strictPort: true },
  preview: { host: "127.0.0.1", port: 4173, strictPort: true },
  build: { sourcemap: false },
});
