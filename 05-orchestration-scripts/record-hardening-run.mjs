import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const playwrightEntry = path.resolve("FlowPlaneUI/FlowPlaneUI/node_modules/playwright-core/index.mjs");
const { chromium } = await import(pathToFileURL(playwrightEntry).href);

const uiUrl = process.env.FLOWPLANE_UI_URL ?? "http://localhost:5174";
const controlCenterUrl = process.env.FLOWPLANE_CONTROL_CENTER_URL ?? "http://localhost:9022";
const durationSeconds = Number.parseInt(process.env.FLOWPLANE_RECORD_SECONDS ?? "3900", 10);
const outDir = path.resolve(process.env.FLOWPLANE_QA_OUT_DIR ?? "output/qa-runs/hardening-video");
const edgePath = process.env.FLOWPLANE_BROWSER_PATH ?? "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe";

fs.mkdirSync(outDir, { recursive: true });

const browser = await chromium.launch({
  executablePath: edgePath,
  headless: true
});

const context = await browser.newContext({
  viewport: { width: 1600, height: 1000 },
  recordVideo: { dir: outDir, size: { width: 1600, height: 1000 } }
});

const page = await context.newPage();
page.setDefaultTimeout(15000);

async function log(message) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

async function navigate(label, url) {
  await log(`recording ${label}: ${url}`);
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 }).catch(async (error) => {
    await log(`navigation warning for ${label}: ${error.message}`);
  });
  await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => {});
  await page.waitForTimeout(8000);
}

async function ensureFlowPlaneLogin() {
  await page.goto(uiUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => {});

  if (!(await page.getByText("Sign in", { exact: false }).count())) {
    return;
  }

  await page.getByLabel(/email|username/i).fill("admin@flowplane.local").catch(async () => {
    await page.locator("input").first().fill("admin@flowplane.local");
  });
  const password = page.getByLabel(/password/i);
  if (await password.count()) {
    await password.fill("admin123");
  } else {
    await page.locator("input[type='password']").fill("admin123");
  }
  await page.getByRole("button", { name: /sign in|login/i }).click();
  await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => {});
}

const startedAt = Date.now();
const stopAt = startedAt + durationSeconds * 1000;

await log(`starting hardening recorder for ${durationSeconds}s`);
await ensureFlowPlaneLogin();

while (Date.now() < stopAt) {
  await navigate("FLOWPLANE runtimes", `${uiUrl}/runtimes`);
  await navigate("FLOWPLANE failures", `${uiUrl}/failures`);
  await navigate("FLOWPLANE deployments", `${uiUrl}/deployments`);
  await navigate("Control Center overview", controlCenterUrl);
  await navigate("Control Center topics", `${controlCenterUrl}/clusters`);
}

await context.close();
await browser.close();

const videos = fs.readdirSync(outDir)
  .filter((file) => file.endsWith(".webm"))
  .map((file) => path.join(outDir, file))
  .sort((a, b) => fs.statSync(a).mtimeMs - fs.statSync(b).mtimeMs);

await log(`video=${videos[videos.length - 1] ?? outDir}`);
