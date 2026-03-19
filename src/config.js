import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, "..");
const dataDir = path.join(projectRoot, "data");

loadEnvFile(path.join(projectRoot, ".env"));

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return;
  }

  const lines = fs.readFileSync(filePath, "utf-8").split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const separator = trimmed.indexOf("=");
    if (separator === -1) {
      continue;
    }

    const key = trimmed.slice(0, separator).trim();
    const rawValue = trimmed.slice(separator + 1).trim();
    if (!process.env[key]) {
      process.env[key] = rawValue.replace(/^['"]|['"]$/g, "");
    }
  }
}

function env(name, fallback) {
  const value = process.env[name];
  return value === undefined || value === "" ? fallback : value;
}

function envBool(name, fallback) {
  const value = env(name, String(fallback));
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function envInt(name, fallback) {
  const value = Number.parseInt(env(name, String(fallback)), 10);
  return Number.isFinite(value) ? value : fallback;
}

function envFloat(name, fallback) {
  const value = Number.parseFloat(env(name, String(fallback)));
  return Number.isFinite(value) ? value : fallback;
}

export const config = {
  projectRoot,
  dataDir,
  publicDir: path.join(projectRoot, "public"),
  dbPath: path.join(dataDir, "tro-watch.sqlite"),
  seedDbArchivePath: env("SEED_DB_ARCHIVE_PATH", path.join(projectRoot, "seed", "tro-watch.sqlite.gz")),
  seedDbMinimumCases: envInt("SEED_DB_MIN_CASES", 10000),
  server: {
    port: envInt("PORT", 4127),
    adminToken: env("ADMIN_TOKEN", "")
  },
  sync: {
    enableScheduler: envBool("ENABLE_SCHEDULER", true),
    enableBackfillScheduler: envBool("ENABLE_BACKFILL_SCHEDULER", true),
    bootstrapSync: envBool("BOOTSTRAP_SYNC", true),
    bootstrapSyncDelayMs: envInt("BOOTSTRAP_SYNC_DELAY_MS", 30 * 1000),
    bootstrapBackfillDelayMs: envInt("BOOTSTRAP_BACKFILL_DELAY_MS", 60 * 1000),
    startDate: env("START_DATE", "2025-01-01"),
    pollIntervalMs: envInt("POLL_INTERVAL_MS", 30 * 60 * 1000),
    backfillIntervalMs: envInt("BACKFILL_INTERVAL_MS", 60 * 60 * 1000),
    discoveryMaxPagesPerRun: envInt("DISCOVERY_MAX_PAGES_PER_RUN", 3),
    backfillMaxPagesPerRun: envInt("BACKFILL_MAX_PAGES_PER_RUN", 50)
  },
  courtListener: {
    baseUrl: env("COURTLISTENER_BASE_URL", "https://www.courtlistener.com/api/rest/v4"),
    apiToken: env("COURTLISTENER_API_TOKEN", ""),
    enableDocketSync: envBool("COURTLISTENER_ENABLE_DOCKET_SYNC", false),
    docketMaxCasesPerRun: envInt("COURTLISTENER_DOCKET_MAX_CASES_PER_RUN", 8)
  },
  worldtro: {
    enabled: envBool("WORLDTRO_ENABLED", true),
    baseUrl: env("WORLDTRO_BASE_URL", "https://worldtro.com"),
    minIntervalMs: envInt("WORLDTRO_MIN_INTERVAL_MS", 1500),
    timeoutMs: envInt("WORLDTRO_TIMEOUT_MS", 15000),
    maxCasesPerRun: envInt("WORLDTRO_MAX_CASES_PER_RUN", 3),
    backfillMaxCasesPerRun: envInt("WORLDTRO_BACKFILL_MAX_CASES_PER_RUN", 30),
    staleAfterHours: envInt("WORLDTRO_STALE_AFTER_HOURS", 12)
  },
  translation: {
    provider: env("TRANSLATION_PROVIDER", "openai"),
    baseUrl: env("OPENAI_BASE_URL", "https://api.openai.com/v1"),
    apiKey: env("OPENAI_API_KEY", ""),
    model: env("OPENAI_MODEL", "gpt-4.1-mini"),
    batchLimit: envInt("TRANSLATION_BATCH_LIMIT", 20)
  },
  pacer: {
    enabled: envBool("PACER_ENABLED", false),
    dailyBudgetUsd: envFloat("PACER_DAILY_BUDGET_USD", 15),
    perRunBudgetUsd: envFloat("PACER_PER_RUN_BUDGET_USD", 3),
    maxCasesPerRun: envInt("PACER_MAX_CASES_PER_RUN", 2)
  },
  pacerMonitor: {
    enabled: envBool("PACERMONITOR_ENABLED", false),
    baseUrl: env("PACERMONITOR_API_BASE_URL", ""),
    apiKey: env("PACERMONITOR_API_KEY", "")
  }
};
