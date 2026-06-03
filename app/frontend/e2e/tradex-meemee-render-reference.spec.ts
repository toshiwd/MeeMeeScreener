import { expect, test } from "@playwright/test";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

type ManifestRow = {
  image_sample_key?: string;
  code: string;
  as_of: number;
  scale: string;
  bars: number;
  image_relpath?: string;
  bars_payload?: number[][];
};

const manifestPath = process.env.TRADEX_RENDER_MANIFEST;
const outputDir = process.env.TRADEX_BROWSER_RENDER_OUTPUT;

test("exports browser reference PNGs from ThumbnailCanvas.drawChart", async ({ page }) => {
  expect(manifestPath).toBeTruthy();
  expect(outputDir).toBeTruthy();
  const rows = readFileSync(resolve(manifestPath!), "utf-8")
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => JSON.parse(line) as ManifestRow)
    .filter((row) => Array.isArray(row.bars_payload));
  mkdirSync(resolve(outputDir!), { recursive: true });
  await page.goto("/");

  for (const row of rows) {
    const dataUrl = await page.evaluate(async ({ payloadRows, maxBars }) => {
      const { drawChart } = await import("/src/components/ThumbnailCanvas.tsx");
      const canvas = document.createElement("canvas");
      canvas.width = 1280;
      canvas.height = 720;
      drawChart(
        canvas,
        { bars: payloadRows, ma: { ma7: [], ma20: [], ma60: [] }, provenance: null },
        [],
        false,
        [
          { period: 7, visible: true, color: "#f59e0b" },
          { period: 20, visible: true, color: "#3b82f6" },
          { period: 60, visible: true, color: "#8b5cf6" },
          { period: 100, visible: true, color: "#ec4899" },
          { period: 200, visible: true, color: "#64748b" }
        ],
        1280,
        720,
        maxBars,
        true,
        "light"
      );
      return canvas.toDataURL("image/png");
    }, { payloadRows: row.bars_payload!, maxBars: row.bars });
    const target = resolve(outputDir!, row.image_relpath ?? `${row.code}_${row.as_of}_${row.scale}_${row.bars}.png`);
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, Buffer.from(dataUrl.split(",", 2)[1], "base64"));
  }
  expect(rows.length).toBeGreaterThan(0);
});
