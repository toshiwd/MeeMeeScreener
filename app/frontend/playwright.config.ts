import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  outputDir: "../../test-results/app-frontend-playwright",
  expect: {
    timeout: 10_000
  },
  projects: [
    {
      name: "chromium",
      use: {
        browserName: "chromium"
      }
    }
  ],
  use: {
    baseURL: "http://127.0.0.1:4173",
    trace: "retain-on-failure",
    locale: "ja-JP",
    timezoneId: "Asia/Tokyo",
    colorScheme: "light",
    viewport: {
      width: 1440,
      height: 1200
    }
  },
  webServer: {
    command: "npm run dev -- --host 127.0.0.1 --port 4173",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    env: {
      VITE_SHOW_OPERATOR_CONSOLE: "1",
      VITE_OPERATOR_CONSOLE_GATE_MODE: "header"
    }
  }
});
