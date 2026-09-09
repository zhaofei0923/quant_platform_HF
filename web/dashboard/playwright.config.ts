import { defineConfig, devices } from "@playwright/test";
// Local browser QA must not send loopback requests to the host's outbound proxy.
for (const key of [
  "HTTP_PROXY",
  "HTTPS_PROXY",
  "http_proxy",
  "https_proxy",
  "ALL_PROXY",
  "all_proxy",
]) {
  delete process.env[key];
}
export default defineConfig({
  testDir: "./tests",
  testMatch: "**/*.spec.ts",
  fullyParallel: true,
  workers: 3,
  timeout: 30_000,
  reporter: "list",
  outputDir: "/tmp/quant-dashboard-browser-tests",
  use: {
    baseURL: "http://127.0.0.1:5173",
    timezoneId: "Asia/Shanghai",
    locale: "zh-CN",
    trace: "off",
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        viewport: { width: 1548, height: 1016 },
      },
    },
  ],
  webServer: {
    command: "npm run dev",
    url: "http://127.0.0.1:5173",
    reuseExistingServer: !process.env.CI,
  },
});
