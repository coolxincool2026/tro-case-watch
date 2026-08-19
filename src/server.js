import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import crypto from "node:crypto";
import os from "node:os";
import { spawn } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import zlib from "node:zlib";
import { URL, fileURLToPath } from "node:url";
import { config } from "./config.js";
import { Store } from "./db.js";
import { CourtListenerClient, extractCourtListenerWebhookDocketId } from "./providers/courtlistener.js";
import { CourtFeedClient } from "./providers/courtfeed.js";
import { RecentFilingsClient } from "./providers/recentfilings.js";
import { LawFirmClient } from "./providers/lawfirm.js";
import { CatalogClient } from "./providers/catalog.js";
import { SignalFeedClient, SIGNAL_FEED_PROVIDER_KEY } from "./providers/signal-feed.js";
import { PacerAdapter } from "./providers/pacer.js";
import { PacerMonitorAdapter } from "./providers/pacermonitor.js";
import { DocketAlarmClient } from "./providers/docketalarm.js";
import { UniCourtClient } from "./providers/unicourt.js";
import {
  FALLBACK_PROVIDER_KEY,
  OFFICIAL_DOCKET_PROVIDER_KEY,
  PRIORITY_FEED_PROVIDER_KEY,
  PRIORITY_FEED_SOURCE,
  caseHasPriorityFeedUrl,
  getPriorityFeedRaw,
  publicProviderLabel
} from "./priority-feed.js";
import { TranslationService } from "./translation.js";
import { CaseSyncService } from "./sync.js";
import { docketLooksLike } from "./insights.js";
import { DailyEmailReportService } from "./daily-report.js";
import { TroDailyRoundupService } from "./tro-daily-roundup.js";
import { loadTroDailyUpdates } from "./tro-daily-updates.js";
import {
  claimWebhookEnrichmentJobState,
  enqueueWebhookEnrichmentJobState,
  finishWebhookEnrichmentJobState,
  getWebhookEnrichmentQueueStats
} from "./webhook-enrichment.js";

const mimeTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".jpg": "image/jpeg",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".jpeg": "image/jpeg",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".webp": "image/webp"
};

const currentScriptPath = fileURLToPath(import.meta.url);
const detachedTaskExitGraceMs = 30 * 1000;
const detachedTaskKillGraceMs = 10 * 1000;

ensureSeedDatabase();

const store = createStoreWithRecovery();
const courtListener = new CourtListenerClient(config.courtListener, config.pacer);
const courtFeeds = new CourtFeedClient(config.courtFeeds);
const recentFilings = new RecentFilingsClient(config.recentFilings);
const lawFirms = new LawFirmClient(config.lawFirms);
const signalFeed = new SignalFeedClient(config.signalFeed);
const priorityFeed = new CatalogClient(config.priorityFeed);
const pacerMonitor = new PacerMonitorAdapter(config.pacerMonitor);
const docketAlarm = new DocketAlarmClient(config.docketAlarm);
const uniCourt = new UniCourtClient(config.uniCourt);
const pacer = new PacerAdapter(config.pacer, store);
const translator = new TranslationService(config.translation, store);
const dailyEmailReport = new DailyEmailReportService({ config, store });
const troDailyRoundup = new TroDailyRoundupService({ config, store });
const syncService = new CaseSyncService({
  config,
  store,
  courtFeeds,
  recentFilings,
  lawFirms,
  courtListener,
  signalFeed,
  priorityFeed,
  pacerMonitor,
  docketAlarm,
  uniCourt,
  pacer,
  translator
});
const backgroundCaseHydrations = new Map();
const publicResponseCache = new Map();
const publicRateLimitBuckets = new Map();
const publicBehaviorBuckets = new Map();
const publicChallengeBuckets = new Map();
const pendingLookupImports = new Map();
let lastTroDailyUpdatesRefreshQueuedAt = 0;
let lastWebhookEnrichmentWorkerQueuedAt = 0;
const browserGuardCookieName = "__tt_guard";
const publicApiTokenMetaName = "tt-public-api-token";
const publicApiTokenHeaderName = "x-tt-public-token";
const scannerPathFragments = [
  "/.env",
  "/.git",
  "/wp-admin",
  "/wp-login",
  "/phpmyadmin",
  "/pma",
  "/adminer",
  "/vendor/phpunit",
  "/cgi-bin/",
  "/.aws/",
  "/.ssh/",
  "/server-status",
  "/actuator",
  "/boaform",
  "/hnap1",
  "/jmx-console",
  "/solr/",
  "/_ignition/",
  "/autodiscover",
  "/owa/",
  "/debug/default/view",
  "/containers/json",
  "/v2/_catalog"
];
const scannerQueryFragments = [
  "../",
  "..%2f",
  "${jndi:",
  "union select",
  "<script",
  "xdebug_session_start",
  "phpunit",
  "thinkphp"
];
const publicSiteOrigins = new Set([
  "https://trotracker.com",
  "https://www.trotracker.com",
  "https://tro-case-watch-production.up.railway.app",
  "http://localhost:4127"
]);
const courtListenerWebhookPrefix = "/api/webhook/courtlistener/";
const webhookEnrichmentQueueCheckpointKey = "system:webhook-enrichment-queue";
const supportedWebhookEnrichmentProviders = new Set([
  SIGNAL_FEED_PROVIDER_KEY,
  PRIORITY_FEED_SOURCE,
  "61tro",
  "recentfilings",
  "courtlistener",
  "pacermonitor",
  "docketalarm",
  "unicourt"
]);
const standaloneSyncTaskLockMinutes = new Map([
  [SIGNAL_FEED_PROVIDER_KEY, 15],
  ["catalog", 30],
  ["courtfeeds", 20],
  ["recentfilings", 20],
  ["lawfirms", 30],
  ["pacermonitor", 30],
  ["pacer", 30],
  ["docketalarm", 30],
  ["unicourt", 30],
  ["courtlistener-docket", 30],
  ["courtlistener-alerts", 30],
  ["recompute-case-summaries", 60],
  ["reconcile-duplicates", 30],
  ["dedupe-docket-entries", 45],
  ["rebuild-case-fast-path", 45],
  ["cleanup-window-email", 15],
  ["restore-missing-from-backup", 120],
  ["inspect-missing-from-backup", 30],
  ["daily-report", 20],
  ["tro-daily-roundup", 20],
  ["tro-daily-updates", 15]
]);

function clearPublicResponseCache(pathPrefixes = []) {
  if (!Array.isArray(pathPrefixes) || !pathPrefixes.length) {
    publicResponseCache.clear();
    return;
  }

  for (const key of [...publicResponseCache.keys()]) {
    const separatorIndex = key.indexOf("::");
    const pathname = separatorIndex === -1 ? key : key.slice(0, separatorIndex);
    if (pathPrefixes.some((prefix) => pathname === prefix || pathname.startsWith(prefix))) {
      publicResponseCache.delete(key);
    }
  }
}

function clearPublicCaseCaches() {
  clearPublicResponseCache(["/api/cases"]);
}

function isSqliteBusyError(error) {
  const message = String(error?.message || "").toLowerCase();
  return (
    error?.code === "ERR_SQLITE_ERROR" &&
    (Number(error?.errcode || 0) === 5 || message.includes("database is locked") || message.includes("database table is locked"))
  );
}

function normalizeSyncOnlyMode(rawMode = "") {
  if (rawMode === PRIORITY_FEED_SOURCE) {
    return "catalog";
  }
  if (rawMode === `${PRIORITY_FEED_SOURCE}-until-idle`) {
    return "catalog-until-idle";
  }
  return String(rawMode || "").trim();
}

function getDetachedSyncOnlyMode(args = []) {
  const syncOnlyIndex = args.indexOf("--sync-only");
  if (syncOnlyIndex === -1) {
    return null;
  }

  return normalizeSyncOnlyMode(args[syncOnlyIndex + 1]);
}

function getStandaloneSyncLockMinutes(mode) {
  return standaloneSyncTaskLockMinutes.get(mode) || 30;
}

function canPreclaimDetachedTask(mode) {
  return standaloneSyncTaskLockMinutes.has(mode);
}

function getDetachedTaskMaxRuntimeMs(mode) {
  if (mode !== "recent" && mode !== "backfill") {
    return 0;
  }
  return Math.max(getSyncModeMaxRuntimeMs(mode), 60 * 1000);
}

function stopDetachedTaskProcessGroup(child, signal) {
  if (!child?.pid || child.exitCode !== null || child.signalCode !== null) {
    return false;
  }
  try {
    process.kill(-child.pid, signal);
    return true;
  } catch (error) {
    if (error?.code !== "ESRCH") {
      console.error(`[spawn-guard] failed signal=${signal} pid=${child.pid}: ${error.message}`);
    }
    return false;
  }
}

function superviseDetachedTask(child, mode) {
  const maxRuntimeMs = getDetachedTaskMaxRuntimeMs(mode);
  if (!maxRuntimeMs) {
    return;
  }

  let killTimer = null;
  const timeoutTimer = setTimeout(() => {
    console.error(`[spawn-guard] terminating overdue task=${mode} pid=${child.pid}`);
    if (!stopDetachedTaskProcessGroup(child, "SIGTERM")) {
      return;
    }
    killTimer = setTimeout(() => {
      if (stopDetachedTaskProcessGroup(child, "SIGKILL")) {
        console.error(`[spawn-guard] killed unresponsive task=${mode} pid=${child.pid}`);
      }
    }, detachedTaskKillGraceMs);
    killTimer.unref?.();
  }, maxRuntimeMs + detachedTaskExitGraceMs);
  timeoutTimer.unref?.();

  child.once("exit", (code, signal) => {
    clearTimeout(timeoutTimer);
    if (killTimer) {
      clearTimeout(killTimer);
    }
    if (code || signal) {
      console.warn(`[spawn-guard] task=${mode} pid=${child.pid} exited code=${code ?? "null"} signal=${signal || "none"}`);
    }
  });
}

function spawnDetachedTask(args = [], extraEnv = {}) {
  const mode = getDetachedSyncOnlyMode(args);
  const env = {
    ...process.env,
    ...extraEnv
  };

  if (mode && canPreclaimDetachedTask(mode) && !env.DETACHED_TASK_RUN_ID) {
    const runId = store.claimSyncRun("system", mode, getStandaloneSyncLockMinutes(mode));
    if (!runId) {
      console.warn(`[spawn-guard] skipped task=${mode} reason=already-running`);
      return {
        spawned: false,
        mode,
        reason: "already-running"
      };
    }

    env.DETACHED_TASK_RUN_ID = String(runId);
    env.DETACHED_TASK_MODE = mode;
  }

  const child = spawn(process.execPath, [currentScriptPath, ...args], {
    cwd: path.dirname(config.publicDir),
    env,
    detached: true,
    stdio: ["ignore", "inherit", "inherit"]
  });
  superviseDetachedTask(child, mode);
  child.unref();
  return {
    spawned: true,
    pid: child.pid,
    mode
  };
}

function isSuccessfulSyncModeDue(mode, minimumIntervalMs) {
  const latest = store.db
    .prepare(`
      SELECT finished_at, started_at
      FROM sync_runs
      WHERE provider = 'system'
        AND mode = ?
        AND status IN ('succeeded', 'completed')
      ORDER BY COALESCE(finished_at, started_at) DESC
      LIMIT 1
    `)
    .get(String(mode || "").trim());
  const latestAt = Date.parse(latest?.finished_at || latest?.started_at || "");
  if (!Number.isFinite(latestAt)) {
    return true;
  }
  return Date.now() - latestAt >= Math.max(Number(minimumIntervalMs || 0), 60 * 1000);
}

function spawnDetachedTaskIfDue(mode, minimumIntervalMs) {
  if (!isSuccessfulSyncModeDue(mode, minimumIntervalMs)) {
    return { spawned: false, mode, reason: "not-due" };
  }
  return spawnDetachedTask(["--sync-only", mode]);
}

function getSyncModeMaxRuntimeMs(mode = "recent") {
  return mode === "backfill"
    ? Number(config.sync?.backfillMaxRuntimeMs || 0)
    : Number(config.sync?.recentMaxRuntimeMs || 0);
}

function reapAndRecoverStaleSyncRuns() {
  if (!config.sync?.watchdogEnabled) {
    return;
  }

  const heartbeatTimeoutMs = Math.max(Number(config.sync?.runHeartbeatTimeoutMs || 0), 30 * 1000);
  let recentReaped = [];
  try {
    recentReaped = store.reapStaleSyncRuns("system", {
      mode: "recent",
      heartbeatTimeoutMs,
      maxRuntimeMs: Math.max(getSyncModeMaxRuntimeMs("recent") + 60 * 1000, 60 * 1000),
      reasonPrefix: "recent watchdog auto-cleared stale run"
    });
  } catch (error) {
    if (isSqliteBusyError(error)) {
      console.warn(`[watchdog] skipped recent reap: ${error.message}`);
    } else {
      console.error("[watchdog] recent reap failed", error);
    }
  }
  if (recentReaped.length) {
    console.warn(`[watchdog] reaped stale recent runs ${recentReaped.map((row) => `#${row.id}`).join(", ")}`);
    if (config.sync.enableScheduler) {
      spawnDetachedTask(["--sync-only", "recent"]);
    }
  }

  let backfillReaped = [];
  try {
    backfillReaped = store.reapStaleSyncRuns("system", {
      mode: "backfill",
      heartbeatTimeoutMs,
      maxRuntimeMs: Math.max(getSyncModeMaxRuntimeMs("backfill") + 60 * 1000, 60 * 1000),
      reasonPrefix: "backfill watchdog auto-cleared stale run"
    });
  } catch (error) {
    if (isSqliteBusyError(error)) {
      console.warn(`[watchdog] skipped backfill reap: ${error.message}`);
    } else {
      console.error("[watchdog] backfill reap failed", error);
    }
  }
  if (backfillReaped.length) {
    console.warn(`[watchdog] reaped stale backfill runs ${backfillReaped.map((row) => `#${row.id}`).join(", ")}`);
    if (config.sync.enableBackfillScheduler && syncService.getBackfillStatus().pending) {
      spawnDetachedTask(["--sync-only", "backfill"]);
    }
  }
}

function queueLookupImport(term, { courtName = "", caseName = "" } = {}) {
  const lookupTerm = String(term || "").trim();
  if (!lookupTerm) {
    return false;
  }

  const dedupeKey = JSON.stringify({
    term: lookupTerm.toLowerCase(),
    courtName: String(courtName || "").trim().toLowerCase(),
    caseName: String(caseName || "").trim().slice(0, 120).toLowerCase()
  });
  const existing = pendingLookupImports.get(dedupeKey);
  if (existing && Date.now() - existing.startedAt < 90 * 1000) {
    return false;
  }

  pendingLookupImports.set(dedupeKey, {
    startedAt: Date.now()
  });

  const args = ["--import-lookup", lookupTerm];
  if (String(courtName || "").trim()) {
    args.push("--court-name", String(courtName).trim());
  }
  if (String(caseName || "").trim()) {
    args.push("--case-name", String(caseName).trim().slice(0, 120));
  }

  spawnDetachedTask(args);
  setTimeout(() => {
    pendingLookupImports.delete(dedupeKey);
  }, 90 * 1000);
  return true;
}

function getWebhookEnrichmentProviders(providers = null) {
  const rawProviders = Array.isArray(providers) && providers.length
    ? providers
    : config.sync?.webhookEnrichmentProviders || [];

  return [...new Set(
    rawProviders
      .map((value) => String(value || "").trim())
      .filter((value) => supportedWebhookEnrichmentProviders.has(value))
  )];
}

function buildWebhookEnrichmentWorkerEnv() {
  const maxOldSpaceMb = Math.max(Number(config.sync?.webhookEnrichmentMaxOldSpaceMb || 0), 128);
  const existingNodeOptions = String(process.env.NODE_OPTIONS || "").trim();
  if (existingNodeOptions.includes("--max-old-space-size=")) {
    return {};
  }

  return {
    NODE_OPTIONS: `${existingNodeOptions} --max-old-space-size=${maxOldSpaceMb}`.trim()
  };
}

function getWebhookEnrichmentWorkStats() {
  try {
    return getWebhookEnrichmentQueueStats(store.getCheckpoint(webhookEnrichmentQueueCheckpointKey), {
      now: new Date().toISOString()
    });
  } catch (error) {
    if (isSqliteBusyError(error)) {
      console.warn(`[webhook-enrichment] skipped queue probe: ${error.message}`);
      return null;
    }
    throw error;
  }
}

function scheduleWebhookEnrichmentWorker({ force = false } = {}) {
  if (!config.sync?.webhookEnrichmentEnabled) {
    return false;
  }

  const queueStats = getWebhookEnrichmentWorkStats();
  if (!queueStats) {
    return false;
  }

  if (
    Number(queueStats.readyCount || 0) <= 0 &&
    Number(queueStats.expiredLeaseCount || 0) <= 0
  ) {
    return false;
  }

  const debounceMs = Math.max(Number(config.sync?.webhookEnrichmentSpawnDebounceMs || 0), 1000);
  if (!force && Date.now() - lastWebhookEnrichmentWorkerQueuedAt < debounceMs) {
    return false;
  }

  lastWebhookEnrichmentWorkerQueuedAt = Date.now();
  const spawned = spawnDetachedTask(["--sync-only", "webhook-enrichment"], buildWebhookEnrichmentWorkerEnv());
  return Boolean(spawned.spawned);
}

function getWebhookEnrichmentPriorityAt(caseRow, { lastWebhookFiledAt = null, lastWebhookAt = null } = {}) {
  const candidates = [
    String(lastWebhookFiledAt || "").trim(),
    String(caseRow?.latest_docket_filed_at || "").trim(),
    String(caseRow?.date_filed || "").trim(),
    String(lastWebhookAt || "").trim(),
    String(caseRow?.last_seen_at || "").trim(),
    String(caseRow?.updated_at || "").trim()
  ].filter(Boolean);

  let selected = null;
  let selectedTs = -Infinity;
  for (const value of candidates) {
    const timestamp = Date.parse(value);
    if (Number.isFinite(timestamp) && timestamp > selectedTs) {
      selected = value;
      selectedTs = timestamp;
    }
  }

  return selected || new Date().toISOString();
}

function getWebhookEnrichmentMemorySnapshot() {
  const usage = process.memoryUsage();
  return {
    rssMb: Number((usage.rss / (1024 * 1024)).toFixed(1)),
    heapUsedMb: Number((usage.heapUsed / (1024 * 1024)).toFixed(1)),
    freeSystemMb: Number((os.freemem() / (1024 * 1024)).toFixed(1))
  };
}

function checkWebhookEnrichmentMemoryPressure() {
  const snapshot = getWebhookEnrichmentMemorySnapshot();
  const maxRssMb = Math.max(Number(config.sync?.webhookEnrichmentMaxRssMb || 0), 0);
  const minFreeSystemMb = Math.max(Number(config.sync?.webhookEnrichmentMinFreeSystemMb || 0), 0);

  if (maxRssMb > 0 && snapshot.rssMb >= maxRssMb) {
    return {
      ok: false,
      reason: "rss-limit",
      snapshot
    };
  }

  if (minFreeSystemMb > 0 && snapshot.freeSystemMb <= minFreeSystemMb) {
    return {
      ok: false,
      reason: "low-system-memory",
      snapshot
    };
  }

  return {
    ok: true,
    reason: null,
    snapshot
  };
}

function enqueueWebhookEnrichmentCase(caseId, {
  providers = null,
  lastWebhookAt = new Date().toISOString(),
  lastWebhookFiledAt = null,
  latestEntryId = null,
  entryCount = 0,
  eventType = null,
  idempotencyKey = null,
  delayMs = null
} = {}) {
  if (!config.sync?.webhookEnrichmentEnabled) {
    return {
      enqueued: false,
      reason: "disabled"
    };
  }

  const normalizedCaseId = Number(caseId || 0);
  if (!Number.isFinite(normalizedCaseId) || normalizedCaseId <= 0) {
    return {
      enqueued: false,
      reason: "invalid-case-id"
    };
  }

  const caseRow = store.getCase(normalizedCaseId);
  if (!caseRow) {
    return {
      enqueued: false,
      reason: "missing-case"
    };
  }

  const selectedProviders = getWebhookEnrichmentProviders(providers);
  if (!selectedProviders.length) {
    return {
      enqueued: false,
      reason: "no-providers"
    };
  }

  const normalizedDelayMs = Math.max(
    Number((delayMs ?? config.sync?.webhookEnrichmentSettleDelayMs) || 0),
    15 * 1000
  );
  const now = new Date();
  const queuedJob = store.updateCheckpointAtomically(webhookEnrichmentQueueCheckpointKey, (currentValue) => {
    const result = enqueueWebhookEnrichmentJobState(currentValue, {
      caseId: normalizedCaseId,
      providers: selectedProviders,
      enqueuedAt: now.toISOString(),
      runAfter: new Date(now.getTime() + normalizedDelayMs).toISOString(),
      priorityAt: getWebhookEnrichmentPriorityAt(caseRow, {
        lastWebhookFiledAt,
        lastWebhookAt
      }),
      lastWebhookAt,
      lastWebhookFiledAt,
      latestEntryId,
      entryCount,
      eventType,
      idempotencyKey
    }, {
      maxPending: Math.max(Number(config.sync?.webhookEnrichmentMaxPendingJobs || 0), 0),
      now: now.toISOString()
    });

    return {
      value: result.state,
      result
    };
  });

  scheduleWebhookEnrichmentWorker();
  return {
    enqueued: Boolean(queuedJob?.queuedJob),
    pendingCount: Number(queuedJob?.pendingCount || 0),
    queuedJob: queuedJob?.queuedJob || null
  };
}

function runSyncModeChild(mode, extraArgs = [], extraEnv = {}, { streamLogs = false } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [currentScriptPath, "--sync-only", mode, "--result-json", ...extraArgs],
      {
        cwd: path.dirname(config.publicDir),
        env: {
          ...process.env,
          ...extraEnv
        },
        stdio: ["ignore", "pipe", "pipe"]
      }
    );

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      const text = String(chunk || "");
      stdout += text;
      if (streamLogs) {
        process.stdout.write(text);
      }
    });
    child.stderr.on("data", (chunk) => {
      const text = String(chunk || "");
      stderr += text;
      if (streamLogs) {
        process.stderr.write(text);
      }
    });

    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (code !== 0) {
        const reason = signal ? `signal ${signal}` : `exit ${code}`;
        reject(new Error(`child sync failed (${reason})\n${stderr || stdout}`.trim()));
        return;
      }

      const lines = stdout
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);
      const lastLine = lines.at(-1) || "{}";
      try {
        resolve(JSON.parse(lastLine));
      } catch {
        reject(new Error(`child sync returned non-json output\n${stdout}`.trim()));
      }
    });
  });
}

async function withStandaloneSyncTaskLock(mode, task) {
  const normalizedMode = normalizeSyncOnlyMode(mode);
  const preclaimedRunId = Number(process.env.DETACHED_TASK_RUN_ID || 0);
  const preclaimedMode = String(process.env.DETACHED_TASK_MODE || "").trim();
  const maxAgeMinutes = getStandaloneSyncLockMinutes(normalizedMode);
  const runId =
    preclaimedRunId > 0 && preclaimedMode === normalizedMode
      ? preclaimedRunId
      : store.claimSyncRun("system", normalizedMode, maxAgeMinutes);

  if (!runId) {
    return {
      skipped: true,
      reason: "already-running",
      mode: normalizedMode
    };
  }

  const heartbeatIntervalMs = Math.max(Number(config.sync?.runHeartbeatIntervalMs || 30 * 1000), 5 * 1000);
  const maxRuntimeMs = Math.max(maxAgeMinutes * 60 * 1000, 60 * 1000);
  const touchHeartbeat = () => {
    store.touchSyncRun(runId, {
      provider: "system",
      mode: normalizedMode,
      pid: process.pid
    });
  };
  touchHeartbeat();

  const heartbeatTimer = setInterval(() => {
    try {
      touchHeartbeat();
    } catch (error) {
      console.warn(`[sync-lock] heartbeat failed task=${normalizedMode}: ${error.message}`);
    }
  }, heartbeatIntervalMs);
  heartbeatTimer.unref?.();

  const timeoutTimer = setTimeout(() => {
    const message = `${normalizedMode} task exceeded ${Math.round(maxRuntimeMs / 60000)} minutes`;
    try {
      store.finishSyncRun(runId, "failed", { mode: normalizedMode }, message);
      store.clearSyncRunHeartbeat(runId);
    } catch {}
    console.error(`[sync-lock] ${message}`);
    process.exit(124);
  }, maxRuntimeMs);
  timeoutTimer.unref?.();

  try {
    const result = await task();
    store.finishSyncRun(runId, "succeeded", result && typeof result === "object" ? result : { result });
    return result;
  } catch (error) {
    store.finishSyncRun(runId, "failed", { mode: normalizedMode }, error?.message || String(error));
    throw error;
  } finally {
    clearInterval(heartbeatTimer);
    clearTimeout(timeoutTimer);
    try {
      store.clearSyncRunHeartbeat(runId);
    } catch {}
  }
}

function printSyncOnlyResult(label, result, { resultJson = false } = {}) {
  if (resultJson) {
    console.log(JSON.stringify(result));
    return;
  }

  if (result?.skipped) {
    console.log(`[sync] skipped ${label} ${JSON.stringify({ reason: result.reason || "already-running" })}`);
    return;
  }

  console.log(`[sync] completed ${label} ${JSON.stringify(result)}`);
}

function ensureSeedDatabase() {
  if (!config.seedDbArchivePath || !fs.existsSync(config.seedDbArchivePath)) {
    return;
  }

  const shouldRestore = needsSeedRestore();
  if (!shouldRestore.restore) {
    return;
  }

  try {
    restoreSeedDatabase(shouldRestore.reason);
  } catch (error) {
    if (!isNoSpaceError(error) || !switchToFallbackDbPath()) {
      throw error;
    }

    const fallbackRestore = needsSeedRestore();
    if (fallbackRestore.restore) {
      restoreSeedDatabase(`${shouldRestore.reason}:fallback-enospc`);
    }
  }
}

function createStoreWithRecovery() {
  try {
    return new Store(config.dbPath);
  } catch (error) {
    if (!isRecoverableSqliteError(error) || !config.seedDbArchivePath || !fs.existsSync(config.seedDbArchivePath)) {
      throw error;
    }

    console.error(`[bootstrap-db] store open failed, attempting seed restore (${error.message})`);
    try {
      restoreSeedDatabase(`store-open-failed:${error.code || "unknown"}`);
    } catch (restoreError) {
      if (!isNoSpaceError(restoreError) || !switchToFallbackDbPath()) {
        throw restoreError;
      }

      restoreSeedDatabase(`store-open-failed:${error.code || "unknown"}:fallback-enospc`);
    }
    return new Store(config.dbPath);
  }
}

function isRecoverableSqliteError(error) {
  const message = String(error?.message || "").toLowerCase();
  return (
    error?.code === "ERR_SQLITE_ERROR" &&
    (message.includes("malformed") || message.includes("disk image") || message.includes("not a database"))
  );
}

function isNoSpaceError(error) {
  return error?.code === "ENOSPC" || String(error?.message || "").toLowerCase().includes("no space left on device");
}

function switchToFallbackDbPath() {
  const nextDbPath = String(config.fallbackDbPath || "").trim();
  if (!nextDbPath || nextDbPath === config.dbPath) {
    return false;
  }

  console.warn(`[bootstrap-db] primary db path is full, switching to fallback path ${nextDbPath}`);
  config.dbPath = nextDbPath;
  return true;
}

function restoreSeedDatabase(reason = "manual") {
  fs.mkdirSync(path.dirname(config.dbPath), { recursive: true });
  cleanupDatabaseFiles(config.dbPath, { includePrimary: true });

  const archive = fs.readFileSync(config.seedDbArchivePath);
  const dbBuffer = zlib.gunzipSync(archive);
  fs.writeFileSync(config.dbPath, dbBuffer);
  cleanupDatabaseFiles(config.dbPath, { includePrimary: false });
  verifyDatabase(config.dbPath, config.seedDbMinimumCases);
  console.log(`[bootstrap-db] restored seed database from ${config.seedDbArchivePath} (${reason})`);
}

function cleanupDatabaseFiles(dbPath, { includePrimary = false } = {}) {
  const targets = includePrimary ? [dbPath] : [];
  targets.push(`${dbPath}-wal`, `${dbPath}-shm`, `${dbPath}-journal`);
  const directory = path.dirname(dbPath);
  if (fs.existsSync(directory)) {
    targets.push(
      ...fs
        .readdirSync(directory, { withFileTypes: true })
        .filter((entry) => entry.isFile() && entry.name.startsWith(`${path.basename(dbPath)}.restore-`))
        .map((entry) => path.join(directory, entry.name))
    );
  }

  for (const target of targets) {
    try {
      if (fs.existsSync(target)) {
        fs.rmSync(target, { force: true });
      }
    } catch (error) {
      console.warn(`[bootstrap-db] could not remove ${target}: ${error.message}`);
    }
  }
}

function verifyDatabase(dbPath, minimumCases = 0) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    const quickCheck = db.prepare("PRAGMA quick_check").get();
    const quickCheckValue = String(quickCheck?.quick_check || "").toLowerCase();
    if (quickCheckValue !== "ok") {
      throw new Error(`seed-integrity-failed:${quickCheck?.quick_check || "unknown"}`);
    }

    if (minimumCases > 0) {
      const row = db.prepare("SELECT COUNT(*) AS total FROM cases").get();
      const total = Number(row?.total || 0);
      if (total < minimumCases) {
        throw new Error(`seed-too-small:${total}`);
      }
    }
  } finally {
    db.close();
  }
}

function needsSeedRestore() {
  if (!fs.existsSync(config.dbPath)) {
    return { restore: true, reason: "db-missing" };
  }

  const stats = fs.statSync(config.dbPath);
  if (stats.size === 0) {
    return { restore: true, reason: "db-empty" };
  }

  try {
    const db = new DatabaseSync(config.dbPath, { readOnly: true });
    const quickCheck = db.prepare("PRAGMA quick_check").get();
    if (String(quickCheck?.quick_check || "").toLowerCase() !== "ok") {
      db.close();
      return { restore: true, reason: `db-integrity:${quickCheck?.quick_check || "unknown"}` };
    }

    const row = db.prepare("SELECT COUNT(*) AS total FROM cases").get();
    db.close();
    const total = Number(row?.total || 0);
    if (total < config.seedDbMinimumCases) {
      return { restore: true, reason: `db-too-small:${total}` };
    }
  } catch {
    return { restore: true, reason: "db-unreadable" };
  }

  return { restore: false, reason: "db-ready" };
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, buildApiHeaders());
  response.end(JSON.stringify(payload));
}

function buildWeakEtag(stat) {
  return `W/"${Number(stat.size || 0).toString(16)}-${Math.floor(Number(stat.mtimeMs || 0)).toString(16)}"`;
}

function isImmutableStaticAsset(target = "", extension = "") {
  return extension === ".js" || extension === ".css";
}

function isCacheableStaticAsset(target = "", extension = "") {
  return isImmutableStaticAsset(target, extension) ||
    extension === ".jpg" ||
    extension === ".jpeg" ||
    extension === ".png" ||
    extension === ".svg" ||
    extension === ".webp" ||
    extension === ".ico";
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildApiHeaders(origin = "") {
  const headers = {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "x-robots-tag": "noindex, nofollow, noarchive",
    "x-frame-options": "DENY",
    "referrer-policy": "same-origin",
    "permissions-policy": "camera=(), microphone=(), geolocation=()"
  };

  if (publicSiteOrigins.has(origin)) {
    headers["access-control-allow-origin"] = origin;
    headers["access-control-allow-methods"] = "GET,POST,OPTIONS";
    headers["access-control-allow-headers"] = `content-type,x-admin-token,${publicApiTokenHeaderName}`;
    headers["vary"] = "Origin";
  }

  return headers;
}

function buildBrowserGuardSecret() {
  const seed = [
    config.server.adminToken || "",
    config.dbPath || "",
    config.server.port || "",
    "public-browser-guard"
  ].join("|");
  return crypto.createHash("sha256").update(seed).digest("hex");
}

function normalizeHostHeader(value = "") {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/:\d+$/, "");
}

function shouldRedirectToWww(hostname) {
  return hostname === "trotracker.com";
}

function redirectToWww(request, response) {
  const target = `https://www.trotracker.com${request.url || "/"}`;
  response.writeHead(301, {
    location: target,
    "cache-control": "public, max-age=300"
  });
  response.end();
}

async function readRequestBody(request) {
  const chunks = [];
  for await (const chunk of request) {
    chunks.push(chunk);
  }

  const text = Buffer.concat(chunks).toString("utf-8");
  return text ? JSON.parse(text) : {};
}

async function handleCourtListenerWebhook(request, response, pathname) {
  const webhookSecret = String(config.courtListener?.webhookSecret || "").trim();
  if (!webhookSecret) {
    return sendJson(response, 503, { error: "Webhook not configured" });
  }

  const urlSecret = pathname.slice(courtListenerWebhookPrefix.length);
  if (!urlSecret || urlSecret !== webhookSecret) {
    return sendJson(response, 403, { error: "Invalid webhook secret" });
  }

  try {
    const body = await readRequestBody(request);
    const results = Array.isArray(body?.payload?.results)
      ? body.payload.results
      : Array.isArray(body?.results)
        ? body.results
        : [];
    const webhookMeta = body?.webhook || {};
    const idempotencyKey = request.headers["idempotency-key"] || null;
    console.log(
      `[webhook] CL: type=${webhookMeta.event_type || "unknown"}, results=${results.length}, key=${idempotencyKey || "none"}`
    );

    let processed = 0;
    let skipped = 0;
    let reupped = 0;
    const timestamp = new Date().toISOString();
    const findCaseByCLId = store.db.prepare(
      "SELECT id, case_name, docket_number FROM cases WHERE courtlistener_docket_id = ? LIMIT 1"
    );
    const casesToRefresh = new Map();

    const oldAlerts = [
      ...(Array.isArray(body?.payload?.old_alerts) ? body.payload.old_alerts : []),
      ...(Array.isArray(body?.payload?.disabled_alerts) ? body.payload.disabled_alerts : [])
    ];
    if (oldAlerts.length) {
      const reupResult = await syncService.reupCourtListenerAlertsFromWebhook(oldAlerts);
      reupped += Number(reupResult.processed || 0);
      skipped += Number(reupResult.skipped || 0);
    }

    for (const entry of results) {
      try {
        const docketId = extractCourtListenerWebhookDocketId(entry);
        if (!docketId) {
          skipped += 1;
          continue;
        }

        const caseRow = findCaseByCLId.get(docketId);
        if (!caseRow) {
          skipped += 1;
          continue;
        }

        const entryId = entry.id ?? null;
        const entryNumber = entry.entry_number ?? entry.document_number ?? null;
        const entryKey = `courtlistener:docket-entry:${entryId ?? `${docketId}:${entryNumber ?? Date.now()}`}`;
        store.upsertDocketEntry({
          case_id: caseRow.id,
          source_entry_key: entryKey,
          primary_source: "courtlistener",
          source_entry_id: entryId != null ? String(entryId) : null,
          document_type: null,
          entry_number: entryNumber != null ? String(entryNumber) : null,
          document_number: entry.document_number != null ? String(entry.document_number) : null,
          filed_at: entry.date_filed || entry.entry_date_filed || null,
          description: entry.description || null,
          absolute_url: entry.absolute_url ? courtListener.absoluteUrl(entry.absolute_url) : null,
          is_available: entry.recap_documents?.some((item) => item.is_available) ? 1 : 0,
          page_count: entry.page_count ?? null,
          pacer_doc_id: entry.pacer_doc_id != null ? String(entry.pacer_doc_id) : null,
          raw_json: JSON.stringify(entry),
          last_synced_at: timestamp
        });
        processed += 1;
        const normalizedCaseId = Number(caseRow.id || 0);
        const existingMeta = casesToRefresh.get(normalizedCaseId) || {
          entryCount: 0,
          latestEntryId: null,
          lastWebhookFiledAt: null
        };
        const entryFiledAt = String(entry.date_filed || entry.entry_date_filed || "").trim() || null;
        const previousFiledAt = Date.parse(String(existingMeta.lastWebhookFiledAt || ""));
        const currentFiledAt = Date.parse(String(entryFiledAt || ""));
        casesToRefresh.set(normalizedCaseId, {
          entryCount: existingMeta.entryCount + 1,
          latestEntryId:
            existingMeta.latestEntryId && Number(existingMeta.latestEntryId) > Number(entryId || 0)
              ? existingMeta.latestEntryId
              : Number(entryId || 0) || existingMeta.latestEntryId || null,
          lastWebhookFiledAt:
            Number.isFinite(currentFiledAt) && currentFiledAt >= previousFiledAt
              ? entryFiledAt
              : existingMeta.lastWebhookFiledAt || entryFiledAt || null
        });
      } catch (error) {
        console.error("[webhook] entry error:", error);
        skipped += 1;
      }
    }

    if (processed > 0) {
      clearPublicCaseCaches();
    }

    for (const [caseId, caseMeta] of casesToRefresh.entries()) {
      syncService.handleCourtListenerWebhookEvent(caseId, {
        eventType: webhookMeta.event_type || null,
        idempotencyKey,
        latestEntryId: caseMeta.latestEntryId || null,
        entryCount: caseMeta.entryCount || 0
      }).then(() => {
        const queued = enqueueWebhookEnrichmentCase(caseId, {
          eventType: webhookMeta.event_type || null,
          idempotencyKey,
          latestEntryId: caseMeta.latestEntryId || null,
          entryCount: caseMeta.entryCount || 0,
          lastWebhookAt: new Date().toISOString(),
          lastWebhookFiledAt: caseMeta.lastWebhookFiledAt || null
        });
        if (queued.enqueued) {
          console.log(
            `[webhook] queued high-quality enrichment for case ${caseId} with ${queued.queuedJob?.providers?.join(",") || "none"}`
          );
        }
      }).catch((error) => {
        console.error(`[webhook] failed to persist webhook state for case ${caseId}:`, error);
      });
    }

    console.log(`[webhook] Done: processed=${processed}, skipped=${skipped}, reupped=${reupped}`);
    return sendJson(response, 200, { received: true, processed, skipped, reupped, idempotencyKey });
  } catch (error) {
    console.error("[webhook] Error:", error);
    return sendJson(response, 500, { error: "Webhook processing error" });
  }
}

function authorize(request) {
  const configuredToken = String(config.server.adminToken || "").trim();
  if (configuredToken) {
    return request.headers["x-admin-token"] === configuredToken;
  }

  const remoteAddress = String(request.socket?.remoteAddress || "").trim();
  return remoteAddress === "127.0.0.1" || remoteAddress === "::1" || remoteAddress === "::ffff:127.0.0.1";
}

function getClientIp(request) {
  const cloudflareIp = String(request.headers["cf-connecting-ip"] || request.headers["true-client-ip"] || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)[0];
  if (cloudflareIp) {
    return cloudflareIp;
  }

  const forwarded = String(request.headers["x-forwarded-for"] || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)[0];
  return forwarded || request.socket?.remoteAddress || "unknown";
}

function parseCookies(request) {
  const raw = String(request.headers.cookie || "");
  if (!raw) {
    return {};
  }

  return raw
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean)
    .reduce((acc, item) => {
      const separator = item.indexOf("=");
      if (separator === -1) {
        return acc;
      }

      const key = item.slice(0, separator).trim();
      const value = item.slice(separator + 1).trim();
      if (key) {
        acc[key] = decodeURIComponent(value);
      }
      return acc;
    }, {});
}

function fingerprintClient(request) {
  return crypto
    .createHash("sha256")
    .update(`${getClientIp(request)}|${String(request.headers["user-agent"] || "")}`)
    .digest("base64url");
}

function createBrowserGuardToken(request, expiresAtMs) {
  const body = `${Math.floor(expiresAtMs)}.${fingerprintClient(request)}`;
  const signature = crypto
    .createHmac("sha256", buildBrowserGuardSecret())
    .update(body)
    .digest("base64url");
  return `${body}.${signature}`;
}

function createPublicApiToken(request, expiresAtMs) {
  const body = `${Math.floor(expiresAtMs)}.${fingerprintClient(request)}.public-api`;
  const signature = crypto
    .createHmac("sha256", buildBrowserGuardSecret())
    .update(body)
    .digest("base64url");
  return `${body}.${signature}`;
}

function hasValidBrowserGuard(request) {
  if (authorize(request)) {
    return true;
  }

  const token = parseCookies(request)[browserGuardCookieName];
  if (!token) {
    return false;
  }

  const [expiresAtRaw, fingerprint, signature] = String(token).split(".");
  const expiresAt = Number(expiresAtRaw || 0);
  if (!expiresAt || !fingerprint || !signature || expiresAt <= Date.now()) {
    return false;
  }

  const body = `${expiresAt}.${fingerprint}`;
  const expectedSignature = crypto
    .createHmac("sha256", buildBrowserGuardSecret())
    .update(body)
    .digest("base64url");

  if (signature !== expectedSignature) {
    return false;
  }

  return fingerprint === fingerprintClient(request);
}

function attachBrowserGuardCookie(request, headers = {}) {
  const maxAgeSeconds = 2 * 60 * 60;
  const expiresAtMs = Date.now() + maxAgeSeconds * 1000;
  headers["set-cookie"] = `${browserGuardCookieName}=${encodeURIComponent(createBrowserGuardToken(request, expiresAtMs))}; Path=/; Max-Age=${maxAgeSeconds}; HttpOnly; Secure; SameSite=Lax`;
  return headers;
}

function hasValidPublicApiToken(request) {
  if (authorize(request)) {
    return true;
  }

  const token = String(request.headers[publicApiTokenHeaderName] || "").trim();
  if (!token) {
    return false;
  }

  const parts = token.split(".");
  if (parts.length !== 4) {
    return false;
  }

  const [expiresAtRaw, fingerprint, scope, signature] = parts;
  const expiresAt = Number(expiresAtRaw || 0);
  if (!expiresAt || expiresAt <= Date.now() || scope !== "public-api") {
    return false;
  }

  const body = `${expiresAt}.${fingerprint}.${scope}`;
  const expectedSignature = crypto
    .createHmac("sha256", buildBrowserGuardSecret())
    .update(body)
    .digest("base64url");

  if (signature !== expectedSignature) {
    return false;
  }

  return fingerprint === fingerprintClient(request);
}

function escapeHtmlAttribute(value = "") {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("\"", "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function injectPublicApiTokenIntoHtml(request, htmlBuffer) {
  const expiresAtMs = Date.now() + 2 * 60 * 60 * 1000;
  const token = createPublicApiToken(request, expiresAtMs);
  const metaTag = `    <meta name="${publicApiTokenMetaName}" content="${escapeHtmlAttribute(token)}" />\n`;
  const html = Buffer.isBuffer(htmlBuffer) ? htmlBuffer.toString("utf-8") : String(htmlBuffer || "");
  if (html.includes(`name="${publicApiTokenMetaName}"`)) {
    return html.replace(
      new RegExp(`<meta name="${publicApiTokenMetaName}" content="[^"]*" ?/?>`),
      metaTag.trim()
    );
  }

  if (html.includes("</head>")) {
    return html.replace("</head>", `${metaTag}</head>`);
  }

  return `${metaTag}${html}`;
}

function escapeBootstrapJson(value) {
  return JSON.stringify(value)
    .replaceAll("<", "\\u003c")
    .replaceAll(">", "\\u003e")
    .replaceAll("&", "\\u0026")
    .replaceAll("\u2028", "\\u2028")
    .replaceAll("\u2029", "\\u2029");
}

function injectPublicBootstrapIntoHtml(htmlBuffer) {
  const html = Buffer.isBuffer(htmlBuffer) ? htmlBuffer.toString("utf-8") : String(htmlBuffer || "");

  try {
    const payload = {
      cases: serializePublicCasesPayload(store.listCases({
        startDate: config.sync.startDate,
        category: "watchlist",
        court: "",
        search: "",
        page: 1,
        pageSize: 15
      })),
      status: serializePublicStatus(syncService.getPublicStatus()),
      generatedAt: new Date().toISOString()
    };
    const bootstrapTag =
      `<script id="tt-bootstrap" type="application/json">${escapeBootstrapJson(payload)}</script>\n`;

    if (html.includes("</head>")) {
      return html.replace("</head>", `${bootstrapTag}</head>`);
    }

    return `${bootstrapTag}${html}`;
  } catch (error) {
    console.error("[bootstrap] failed to build public payload:", error);
    return html;
  }
}

function renderPublicHtml(request, htmlBuffer, { includeBootstrap = false } = {}) {
  const tokenizedHtml = injectPublicApiTokenIntoHtml(request, htmlBuffer);
  return includeBootstrap ? injectPublicBootstrapIntoHtml(tokenizedHtml) : tokenizedHtml;
}

function isSuspiciousUserAgent(request) {
  const userAgent = String(request.headers["user-agent"] || "").toLowerCase();
  if (!userAgent) {
    return true;
  }

  return config.server.suspiciousUserAgentPatterns.some((pattern) =>
    userAgent.includes(String(pattern || "").toLowerCase())
  );
}

function getRequestOriginFromReferer(request) {
  const referer = String(request.headers.referer || "").trim();
  if (!referer) {
    return "";
  }

  try {
    return new URL(referer).origin;
  } catch {
    return "";
  }
}

function hasAllowedPublicOriginContext(request) {
  if (authorize(request)) {
    return true;
  }

  const origin = String(request.headers.origin || "").trim();
  if (origin && publicSiteOrigins.has(origin)) {
    return true;
  }

  const refererOrigin = getRequestOriginFromReferer(request);
  if (refererOrigin && publicSiteOrigins.has(refererOrigin)) {
    return true;
  }

  const secFetchSite = String(request.headers["sec-fetch-site"] || "").trim().toLowerCase();
  return secFetchSite === "same-origin" || secFetchSite === "same-site";
}

function getPublicRateLimitPolicy(pathname) {
  if (pathname === "/api/cases") {
    return {
      scope: "cases",
      limit: config.server.publicRateLimitCasesPerWindow
    };
  }

  if (pathname.startsWith("/api/cases/")) {
    return {
      scope: "case-detail",
      limit: config.server.publicRateLimitCaseDetailPerWindow
    };
  }

  if (pathname === "/api/sync/status") {
    return {
      scope: "status",
      limit: config.server.publicRateLimitStatusPerWindow
    };
  }

  if (pathname === "/api/tro-daily-updates") {
    return {
      scope: "tro-daily-updates",
      limit: config.server.publicRateLimitStatusPerWindow
    };
  }

  if (pathname === "/api/health") {
    return {
      scope: "health",
      limit: config.server.publicRateLimitHealthPerWindow
    };
  }

  return null;
}

function pruneRateLimitBuckets() {
  const cutoff = Date.now() - config.server.publicRateLimitWindowMs * 2;
  for (const [key, bucket] of publicRateLimitBuckets.entries()) {
    if (bucket.windowStartedAt < cutoff) {
      publicRateLimitBuckets.delete(key);
    }
  }
}

function getPublicBehaviorKey(request) {
  const browserGuard = parseCookies(request)[browserGuardCookieName];
  return browserGuard || fingerprintClient(request);
}

function pruneBehaviorBuckets() {
  const cutoff = Date.now() - config.server.publicRateLimitWindowMs * 2;
  for (const [key, bucket] of publicBehaviorBuckets.entries()) {
    if (bucket.windowStartedAt < cutoff) {
      publicBehaviorBuckets.delete(key);
    }
  }
}

function getBehaviorBucket(request) {
  if (publicBehaviorBuckets.size > 5000) {
    pruneBehaviorBuckets();
  }

  const key = getPublicBehaviorKey(request);
  const now = Date.now();
  const windowMs = config.server.publicRateLimitWindowMs;
  const existing = publicBehaviorBuckets.get(key);
  if (existing && now - existing.windowStartedAt < windowMs) {
    return existing;
  }

  const nextBucket = {
    windowStartedAt: now,
    searches: new Set(),
    caseDetails: new Set(),
    fullCaseDetails: new Set(),
    recentCaseDetailHits: [],
    recentFullCaseDetailHits: [],
    lightDetailGrants: new Map()
  };
  publicBehaviorBuckets.set(key, nextBucket);
  return nextBucket;
}

function pruneChallengeBuckets() {
  const cutoff = Date.now() - config.server.publicTemporaryBlockMs * 2;
  for (const [key, bucket] of publicChallengeBuckets.entries()) {
    if ((bucket.blockedUntil || 0) < cutoff && (bucket.firstStrikeAt || 0) < cutoff) {
      publicChallengeBuckets.delete(key);
    }
  }
}

function getChallengeBucket(request) {
  if (publicChallengeBuckets.size > 5000) {
    pruneChallengeBuckets();
  }

  const key = getPublicBehaviorKey(request);
  const existing = publicChallengeBuckets.get(key);
  if (existing) {
    return existing;
  }

  const nextBucket = {
    strikeCount: 0,
    firstStrikeAt: 0,
    blockedUntil: 0
  };
  publicChallengeBuckets.set(key, nextBucket);
  return nextBucket;
}

function registerBehaviorStrike(request, { immediateBlockMs = 0 } = {}) {
  const bucket = getChallengeBucket(request);
  const now = Date.now();
  if (immediateBlockMs > 0) {
    bucket.blockedUntil = now + immediateBlockMs;
    bucket.strikeCount = 0;
    bucket.firstStrikeAt = 0;
    return bucket;
  }

  const resetAfterMs = config.server.publicRateLimitWindowMs * 4;
  if (!bucket.firstStrikeAt || now - bucket.firstStrikeAt > resetAfterMs) {
    bucket.strikeCount = 1;
    bucket.firstStrikeAt = now;
  } else {
    bucket.strikeCount += 1;
  }

  if (bucket.strikeCount >= config.server.publicBehaviorStrikeLimit) {
    bucket.blockedUntil = now + config.server.publicTemporaryBlockMs;
    bucket.strikeCount = 0;
    bucket.firstStrikeAt = 0;
  }

  return bucket;
}

function sendTemporaryBlock(response, blockedUntil) {
  const retryAfter = Math.max(1, Math.ceil((blockedUntil - Date.now()) / 1000));
  response.writeHead(429, {
    ...buildApiHeaders(),
    "retry-after": String(retryAfter)
  });
  response.end(JSON.stringify({
    error: "Temporarily blocked",
    retry_after_seconds: retryAfter
  }));
}

function enforceTemporaryPublicBlock(request, response, pathname) {
  if (request.method !== "GET" || authorize(request) || !requiresBrowserGuard(pathname)) {
    return false;
  }

  const bucket = getChallengeBucket(request);
  if ((bucket.blockedUntil || 0) > Date.now()) {
    sendTemporaryBlock(response, bucket.blockedUntil);
    return true;
  }

  return false;
}

function matchesScannerProbe(pathname = "", searchParams) {
  const loweredPath = String(pathname || "").trim().toLowerCase();
  if (scannerPathFragments.some((fragment) => loweredPath.includes(fragment))) {
    return true;
  }

  const flattenedQuery = searchParams ? searchParams.toString().toLowerCase() : "";
  return scannerQueryFragments.some((fragment) => flattenedQuery.includes(fragment));
}

function enforceScannerShield(request, response, pathname, searchParams) {
  if (request.method !== "GET" || authorize(request)) {
    return false;
  }

  if (!matchesScannerProbe(pathname, searchParams)) {
    return false;
  }

  registerBehaviorStrike(request, {
    immediateBlockMs: config.server.publicScannerBlockMs
  });
  response.writeHead(404, {
    "content-type": "text/plain; charset=utf-8",
    "cache-control": "no-store"
  });
  response.end("Not Found");
  return true;
}

function sendBehaviorThrottle(response, bucket, label) {
  const retryAfter = Math.max(
    1,
    Math.ceil((bucket.windowStartedAt + config.server.publicRateLimitWindowMs - Date.now()) / 1000)
  );
  response.writeHead(429, {
    ...buildApiHeaders(),
    "retry-after": String(retryAfter)
  });
  response.end(JSON.stringify({
    error: `${label} rate limited`,
    retry_after_seconds: retryAfter
  }));
}

function hasRecentLightDetailGrant(bucket, pathname) {
  const grantedAt = Number(bucket.lightDetailGrants.get(pathname) || 0);
  if (!grantedAt) {
    return false;
  }

  return Date.now() - grantedAt <= config.server.publicFullDetailGrantTtlMs;
}

function shouldApplySuspiciousDelay(request, pathname, searchParams) {
  if (!isSuspiciousUserAgent(request)) {
    return false;
  }

  if (pathname.startsWith("/api/cases/")) {
    return true;
  }

  if (pathname === "/api/cases" && String(searchParams?.get("search") || "").trim()) {
    return true;
  }

  return false;
}

async function enforcePublicBehaviorThrottle(request, response, pathname, searchParams) {
  if (request.method !== "GET" || authorize(request)) {
    return false;
  }

  if (pathname !== "/api/cases" && !pathname.startsWith("/api/cases/")) {
    return false;
  }

  const bucket = getBehaviorBucket(request);

  if (pathname === "/api/cases") {
    const normalizedSearch = String(searchParams?.get("search") || "")
      .trim()
      .toLowerCase()
      .slice(0, 80);
    if (normalizedSearch) {
      bucket.searches.add(normalizedSearch);
      if (bucket.searches.size > config.server.publicBehaviorDistinctSearchesPerWindow) {
        registerBehaviorStrike(request);
        sendBehaviorThrottle(response, bucket, "Search");
        return true;
      }
    }
  }

  if (pathname.startsWith("/api/cases/")) {
    bucket.caseDetails.add(pathname);
    const now = Date.now();
    const burstWindowMs = config.server.publicCaseDetailBurstWindowMs;
    bucket.recentCaseDetailHits = bucket.recentCaseDetailHits.filter((timestamp) => now - timestamp < burstWindowMs);
    bucket.recentCaseDetailHits.push(now);
    if (bucket.recentCaseDetailHits.length > config.server.publicCaseDetailBurstMaxRequests) {
      registerBehaviorStrike(request, {
        immediateBlockMs: config.server.publicTemporaryBlockMs
      });
      sendTemporaryBlock(response, Date.now() + config.server.publicTemporaryBlockMs);
      return true;
    }

    const fullDetail = searchParams?.get("full") === "1";
    if (fullDetail) {
      if (!hasRecentLightDetailGrant(bucket, pathname)) {
        registerBehaviorStrike(request);
        sendBehaviorThrottle(response, bucket, "Full docket");
        return true;
      }

      bucket.fullCaseDetails.add(pathname);
      bucket.recentFullCaseDetailHits = bucket.recentFullCaseDetailHits.filter(
        (timestamp) => now - timestamp < config.server.publicFullCaseDetailBurstWindowMs
      );
      bucket.recentFullCaseDetailHits.push(now);
      if (bucket.recentFullCaseDetailHits.length > config.server.publicFullCaseDetailBurstMaxRequests) {
        registerBehaviorStrike(request, {
          immediateBlockMs: config.server.publicTemporaryBlockMs
        });
        sendTemporaryBlock(response, Date.now() + config.server.publicTemporaryBlockMs);
        return true;
      }

      if (bucket.fullCaseDetails.size > config.server.publicBehaviorDistinctFullCaseDetailsPerWindow) {
        registerBehaviorStrike(request);
        sendBehaviorThrottle(response, bucket, "Full docket");
        return true;
      }
    } else {
      bucket.lightDetailGrants.set(pathname, now);
      for (const [grantedPath, grantedAt] of bucket.lightDetailGrants.entries()) {
        if (now - grantedAt > config.server.publicFullDetailGrantTtlMs) {
          bucket.lightDetailGrants.delete(grantedPath);
        }
      }
    }

    if (bucket.caseDetails.size > config.server.publicBehaviorDistinctCaseDetailsPerWindow) {
      registerBehaviorStrike(request);
      sendBehaviorThrottle(response, bucket, "Case detail");
      return true;
    }
  }

  if (shouldApplySuspiciousDelay(request, pathname, searchParams)) {
    await new Promise((resolve) => setTimeout(resolve, config.server.suspiciousPenaltyDelayMs));
  }

  return false;
}

function enforcePublicReadRateLimit(request, response, pathname) {
  if (request.method !== "GET" || authorize(request)) {
    return false;
  }

  const policy = getPublicRateLimitPolicy(pathname);
  if (!policy) {
    return false;
  }

  if (publicRateLimitBuckets.size > 5000) {
    pruneRateLimitBuckets();
  }

  const suspicious = isSuspiciousUserAgent(request);
  const effectiveLimit = suspicious
    ? Math.min(policy.limit, config.server.suspiciousRateLimitPerWindow)
    : policy.limit;
  const key = `${getClientIp(request)}:${policy.scope}`;
  const now = Date.now();
  const windowMs = config.server.publicRateLimitWindowMs;
  const bucket = publicRateLimitBuckets.get(key);

  if (!bucket || now - bucket.windowStartedAt >= windowMs) {
    publicRateLimitBuckets.set(key, {
      count: 1,
      windowStartedAt: now
    });
    return false;
  }

  bucket.count += 1;
  if (bucket.count <= effectiveLimit) {
    return false;
  }

  const retryAfter = Math.max(1, Math.ceil((bucket.windowStartedAt + windowMs - now) / 1000));
  response.writeHead(429, {
    ...buildApiHeaders(),
    "retry-after": String(retryAfter)
  });
  response.end(JSON.stringify({
    error: "Too many requests",
    retry_after_seconds: retryAfter
  }));
  return true;
}

function requiresBrowserGuard(pathname = "") {
  return (
    pathname === "/api/cases" ||
    pathname.startsWith("/api/cases/") ||
    pathname === "/api/sync/status" ||
    pathname === "/api/tro-daily-updates"
  );
}

function enforceBrowserGuard(request, response, pathname) {
  if (request.method !== "GET" || !requiresBrowserGuard(pathname) || authorize(request)) {
    return false;
  }

  if (!hasAllowedPublicOriginContext(request)) {
    response.writeHead(403, buildApiHeaders());
    response.end(JSON.stringify({
      error: "Same-site browser context required"
    }));
    return true;
  }

  if (hasValidBrowserGuard(request)) {
    return false;
  }

  response.writeHead(403, buildApiHeaders());
  response.end(JSON.stringify({
    error: "Browser session required"
  }));
  return true;
}

function enforcePublicApiToken(request, response, pathname) {
  if (request.method !== "GET" || !requiresBrowserGuard(pathname) || authorize(request)) {
    return false;
  }

  if (hasValidPublicApiToken(request)) {
    return false;
  }

  response.writeHead(403, buildApiHeaders());
  response.end(JSON.stringify({
    error: "Signed page session required"
  }));
  return true;
}

function getPublicCacheTtlMs(pathname) {
  if (pathname === "/api/health") {
    return config.server.publicHealthCacheTtlMs;
  }

  if (pathname === "/api/sync/status") {
    return config.server.publicStatusCacheTtlMs;
  }

  if (pathname === "/api/tro-daily-updates") {
    return config.server.publicTroDailyUpdatesCacheTtlMs;
  }

  if (pathname === "/api/cases") {
    return config.server.publicCasesCacheTtlMs;
  }

  if (pathname.startsWith("/api/cases/")) {
    return config.server.publicCaseDetailCacheTtlMs;
  }

  return 0;
}

function getPublicCacheKey(request, pathname) {
  return `${pathname}::${request.url || pathname}`;
}

function getCachedPublicPayload(request, pathname) {
  if (request.method !== "GET" || authorize(request)) {
    return null;
  }

  const ttlMs = getPublicCacheTtlMs(pathname);
  if (ttlMs <= 0) {
    return null;
  }

  const key = getPublicCacheKey(request, pathname);
  const cached = publicResponseCache.get(key);
  if (!cached) {
    return null;
  }

  if (cached.expiresAt <= Date.now()) {
    publicResponseCache.delete(key);
    return null;
  }

  return cached.payload;
}

function setCachedPublicPayload(request, pathname, payload) {
  if (request.method !== "GET" || authorize(request)) {
    return;
  }

  const ttlMs = getPublicCacheTtlMs(pathname);
  if (ttlMs <= 0) {
    return;
  }

  const key = getPublicCacheKey(request, pathname);
  publicResponseCache.set(key, {
    expiresAt: Date.now() + ttlMs,
    payload
  });

  if (publicResponseCache.size <= config.server.publicApiCacheMaxEntries) {
    return;
  }

  const oldestKey = publicResponseCache.keys().next().value;
  if (oldestKey) {
    publicResponseCache.delete(oldestKey);
  }
}

function sanitizeInsights(insights = {}) {
  return {
    plaintiff_name: insights.plaintiff_name || null,
    brand_name: insights.brand_name || null,
    ip_case_type_label: insights.ip_case_type_label || null,
    lead_law_firm: insights.lead_law_firm || null,
    defendant_count: insights.defendant_count || 0,
    defendant_preview: Array.isArray(insights.defendant_preview) ? insights.defendant_preview : [],
    status: insights.status
      ? {
          key: insights.status.key || null,
          label: insights.status.label || "持续观察",
          tone: insights.status.tone || "neutral"
        }
      : {
          key: null,
          label: "持续观察",
          tone: "neutral"
        },
    highlights: Array.isArray(insights.highlights)
      ? insights.highlights.map((value) => sanitizePublicText(value)).filter(Boolean)
      : [],
    narrative: sanitizePublicText(insights.narrative),
    badges: Array.isArray(insights.badges) ? insights.badges.map((value) => sanitizePublicText(value)).filter(Boolean) : []
  };
}

function sanitizePublicText(value) {
  const text = String(value || "").trim();
  if (!text) {
    return null;
  }

  return text
    .replace(new RegExp(PRIORITY_FEED_SOURCE, "gi"), "站内归档")
    .replace(/worldtro/gi, "站内归档")
    .replace(/worldtro\.com/gi, "站内归档")
    .replace(/courtlistener/gi, "公开摘要")
    .replace(/61tro/gi, "站内归档")
    .replace(/sriplaw/gi, "站内归档")
    .replace(/\bgbc\b/gi, "站内归档")
    .replace(/pacermonitor/gi, "公开来源")
    .replace(/docketalarm/gi, "外部补充源")
    .replace(/unicourt/gi, "外部补充源");
}

function sanitizePublicUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }

  try {
    const url = new URL(raw);
    if (!["http:", "https:"].includes(url.protocol)) {
      return null;
    }
    return url.toString();
  } catch {
    return null;
  }
}

function resolveTroDailyUpdateCaseId(item = {}) {
  const numericCaseId = Number(item.caseId || 0);
  if (numericCaseId > 0 && store.getCase(numericCaseId)) {
    return numericCaseId;
  }

  const docketNumber = String(item.docketNumber || "").trim();
  if (docketNumber) {
    const payload = store.listCases({
      startDate: "2020-01-01",
      category: "all",
      search: docketNumber,
      page: 1,
      pageSize: 5
    });

    const normalizedNeedle = docketNumber.toLowerCase().replace(/\s+/g, "");
    const exact = (payload.items || []).find((candidate) =>
      String(candidate.docket_number || "").toLowerCase().replace(/\s+/g, "") === normalizedNeedle
    );
    if (exact?.id) {
      return Number(exact.id);
    }

    if (payload.items?.[0]?.id) {
      return Number(payload.items[0].id);
    }
  }

  const caseName = String(item.caseName || "").trim();
  if (caseName) {
    const payload = store.listCases({
      startDate: "2020-01-01",
      category: "all",
      search: caseName,
      page: 1,
      pageSize: 3
    });
    if (payload.items?.[0]?.id) {
      return Number(payload.items[0].id);
    }
  }

  return null;
}

function serializeTroDailyUpdates(payload = {}) {
  const items = Array.isArray(payload.items) ? payload.items : [];
  return {
    updatedAt: payload.updatedAt || null,
    total: Number(payload.total || items.length || 0),
    items: items
      .slice(0, Number(config.reports.troDailyUpdates.maxItems || 3))
      .map((item) => {
        const matchedCaseId = resolveTroDailyUpdateCaseId(item);
        const href = matchedCaseId ? `/case/${matchedCaseId}` : "/#wechat-contact";
        return {
        id: String(item.id || ""),
        title: sanitizePublicText(item.title) || "今日动态",
        summary: sanitizePublicText(item.summary) || null,
        url: sanitizePublicUrl(item.url),
        href,
        matchedCaseId,
        docketNumber: sanitizePublicText(item.docketNumber) || null,
        sources: Array.isArray(item.sources) ? item.sources.map((value) => sanitizePublicText(value)).filter(Boolean) : [],
        publishedAt: item.publishedAt || null,
        heat: Number(item.heat || 0),
        caseRefs: Array.isArray(item.caseRefs) ? item.caseRefs.map((value) => sanitizePublicText(value)).filter(Boolean) : []
      };
      })
  };
}

function troDailyUpdatesRefreshIsStale(payload = {}) {
  const updatedAtMs = Date.parse(String(payload.updatedAt || ""));
  if (!Number.isFinite(updatedAtMs)) {
    return true;
  }
  return Date.now() - updatedAtMs >= 2 * 60 * 60 * 1000;
}

function queueTroDailyUpdatesRefreshIfNeeded(payload = {}) {
  if (!config.reports?.troDailyRoundup?.enabled) {
    return;
  }

  const isStale = troDailyUpdatesRefreshIsStale(payload);
  const isEmpty = Number(payload.total || 0) === 0;
  if (!isStale && !isEmpty) {
    return;
  }

  if (Date.now() - lastTroDailyUpdatesRefreshQueuedAt < 10 * 60 * 1000) {
    return;
  }

  lastTroDailyUpdatesRefreshQueuedAt = Date.now();
  spawnDetachedTask(["--sync-only", "tro-daily-updates"]);
}

function sanitizeEntryDocumentType(value) {
  const type = String(value || "").trim();
  if (!type) {
    return "Docket Entry";
  }

  if (new RegExp(PRIORITY_FEED_SOURCE, "i").test(type) || /priority|catalog/i.test(type)) {
    return "Docket Entry";
  }

  if (/pacermonitor/i.test(type)) {
    return /document/i.test(type) ? "Docket Document" : "Docket Entry";
  }

  if (/pacer document/i.test(type)) {
    return "Docket Document";
  }

  return type
    .replace(new RegExp(PRIORITY_FEED_SOURCE, "gi"), "Docket")
    .replace(/pacermonitor/gi, "Docket");
}

function normalizeDisplayNumber(value) {
  const text = String(value || "").trim();
  if (!text) {
    return null;
  }

  return text.replace(/\.0+$/g, "");
}

function sanitizeTimelineLabel(entry = {}) {
  return "站内归档";
}

function publicCasePresentationMode(item = {}) {
  if (hasPriorityFeedCoverage(item)) {
    return "brand_first";
  }

  return "case_first";
}

function hasPriorityFeedCoverage(item = {}) {
  if (Number(getPriorityFeedRaw(item.raw)?.rowCount || 0) > 0) {
    return true;
  }

  if (caseHasPriorityFeedUrl(item)) {
    return true;
  }

  return Array.isArray(item.entries) && item.entries.some((entry) => entry.primary_source === PRIORITY_FEED_SOURCE);
}

function shouldHydratePriorityFeedOnDemand(item = {}) {
  if (!config.priorityFeed.enabled || !item.insights?.is_seller_case) {
    return false;
  }

  const entryCount = Number(item.entries?.length || 0);
  const priorityFeedRowCount = Number(getPriorityFeedRaw(item.raw)?.rowCount || 0);
  const minimumExpectedEntries = Math.max(12, Number(item.docket_count || 0), 6);

  if (!hasPriorityFeedCoverage(item)) {
    return entryCount < minimumExpectedEntries;
  }

  return priorityFeedRowCount > 0 && entryCount < priorityFeedRowCount;
}

function shouldForcePriorityFeedRefresh(item = {}) {
  const priorityFeedRowCount = Number(getPriorityFeedRaw(item.raw)?.rowCount || 0);
  return priorityFeedRowCount > 0 && Number(item.entries?.length || 0) < priorityFeedRowCount;
}

function laterIso(left, right) {
  if (!left) {
    return right || null;
  }

  if (!right) {
    return left || null;
  }

  return String(left).localeCompare(String(right)) >= 0 ? left : right;
}

function latestEntryFiledAtForItem(item = {}) {
  const entries = Array.isArray(item.entries) ? item.entries : [];
  return entries.reduce((latest, entry) => {
    const filedAt = String(entry?.filed_at || "").trim();
    if (!filedAt) {
      return latest;
    }

    if (!latest || filedAt.localeCompare(latest) > 0) {
      return filedAt;
    }

    return latest;
  }, null);
}

function hasCaseLevelActivityLead(item = {}) {
  const latestActivityAt = String(
    laterIso(laterIso(item?.updated_at, item?.latest_docket_filed_at), item?.date_filed) || ""
  ).trim();
  if (!latestActivityAt) {
    return false;
  }

  const latestEntryFiledAt = latestEntryFiledAtForItem(item);
  if (!latestEntryFiledAt) {
    return true;
  }

  return latestActivityAt.localeCompare(latestEntryFiledAt) > 0;
}

function caseMatchesRecentFilingsSource(item = {}) {
  const docketNumber = String(item?.docket_number || "");
  if (!/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
    return false;
  }

  const caseCourt = String(item?.court_name || item?.courtName || "").trim().toLowerCase();
  if (!caseCourt) {
    return false;
  }

  return recentFilings.listSources().some((source) => {
    const sourceCourt = String(source?.courtName || "").trim().toLowerCase();
    return sourceCourt && (sourceCourt === caseCourt || sourceCourt.includes(caseCourt) || caseCourt.includes(sourceCourt));
  });
}

function shouldHydrateRecentFilingsOnDemand(item = {}) {
  if (!config.recentFilings?.enabled || !caseMatchesRecentFilingsSource(item)) {
    return false;
  }

  const hasActivityLead = hasCaseLevelActivityLead(item);
  const recentFilingsState = Object.values(item?.raw?.recent_filings || {}).reduce((latest, value) => {
    const syncedAt = Date.parse(String(value?.syncedAt || value?.federalRecordLookup?.updatedAt || ""));
    return Number.isFinite(syncedAt) ? Math.max(latest, syncedAt) : latest;
  }, 0);

  if (!hasActivityLead && recentFilingsState && Date.now() - recentFilingsState < 4 * 60 * 60 * 1000) {
    return false;
  }

  return true;
}

function shouldHydrate61troOnDemand(item = {}) {
  if (!config.lawFirms?.enabled || !/\b\d{2}-cv-\d{3,6}\b/i.test(String(item?.docket_number || ""))) {
    return false;
  }

  const hasPriorityProfile = Boolean(item?.insights?.is_seller_case || item?.insights?.is_tro_case || item?.insights?.is_schedule_a_case);
  const hasActivityLead = hasCaseLevelActivityLead(item);
  if (!hasPriorityProfile && !hasActivityLead) {
    return false;
  }

  const syncedAt = Date.parse(String(item?.raw?.law_firm_sites?.["61tro"]?.syncedAt || ""));
  if (!hasActivityLead && Number.isFinite(syncedAt) && Date.now() - syncedAt < 4 * 60 * 60 * 1000) {
    return false;
  }

  return true;
}

function shouldHydrateCourtListenerOnDemand(item = {}) {
  if (!courtListener.hasDocketAccess()) {
    return false;
  }

  const docketNumber = String(item?.docket_number || "");
  if (!item?.courtlistener_docket_id && !/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
    return false;
  }

  const entryCount = Number(item.entries?.length || 0);
  const latestNumberMatch = String(item.latest_docket_number || "").match(/^(\d+)/);
  const latestNumber = latestNumberMatch ? Number.parseInt(latestNumberMatch[1], 10) : 0;
  const expectedEntries = Math.max(
    item.insights?.is_seller_case ? 12 : 8,
    item.insights?.is_tro_case ? 10 : 0,
    item.insights?.is_schedule_a_case ? 10 : 0,
    Number(item.docket_count || 0),
    Number(getPriorityFeedRaw(item.raw)?.rowCount || 0),
    latestNumber
  );

  const hasActivityLead = hasCaseLevelActivityLead(item);
  if (!hasActivityLead && entryCount >= expectedEntries) {
    return false;
  }

  const syncedAt = item.last_docket_sync_at ? Date.parse(item.last_docket_sync_at) : 0;
  if (!hasActivityLead && syncedAt && Date.now() - syncedAt < 2 * 60 * 60 * 1000) {
    return false;
  }

  return Boolean(item.insights?.is_seller_case || item.insights?.is_tro_case || item.insights?.is_schedule_a_case);
}

function shouldHydratePacerMonitorOnDemand(item = {}) {
  if (!config.pacerMonitor.enabled) {
    return false;
  }

  const docketNumber = String(item.docket_number || "");
  if (!/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
    return false;
  }

  const entryCount = Number(item.entries?.length || 0);
  const expectedEntries = Math.max(
    10,
    Number(item.docket_count || 0),
    Number(getPriorityFeedRaw(item.raw)?.rowCount || 0),
    6
  );

  if (entryCount >= expectedEntries) {
    return false;
  }

  const syncedAt = item.raw?.pacermonitor?.syncedAt ? Date.parse(item.raw.pacermonitor.syncedAt) : 0;
  const state = String(item.raw?.pacermonitor?.state || "").toLowerCase();
  const retryHours =
    state === "challenge" || state === "rate_limited"
      ? config.pacerMonitor.blockedRetryAfterHours
      : state === "not_found"
        ? config.pacerMonitor.notFoundRetryAfterHours
      : config.pacerMonitor.staleAfterHours;

  if (syncedAt && Date.now() - syncedAt < retryHours * 60 * 60 * 1000) {
    return false;
  }

  return true;
}

function shouldHydrateDocketAlarmOnDemand(item = {}) {
  if (!config.docketAlarm.enabled || !docketAlarm.hasCredentials()) {
    return false;
  }

  const docketNumber = String(item.docket_number || "");
  if (!/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
    return false;
  }

  const entryCount = Number(item.entries?.length || 0);
  const expectedEntries = Math.max(
    10,
    Number(item.docket_count || 0),
    Number(getPriorityFeedRaw(item.raw)?.rowCount || 0),
    6
  );

  const hasActivityLead = hasCaseLevelActivityLead(item);
  if (!hasActivityLead && entryCount >= expectedEntries) {
    return false;
  }

  const syncedAt = item.raw?.docketalarm?.syncedAt ? Date.parse(item.raw.docketalarm.syncedAt) : 0;
  const state = String(item.raw?.docketalarm?.state || "").toLowerCase();
  const retryHours =
    state === "not_found"
      ? config.docketAlarm.notFoundRetryAfterHours
      : config.docketAlarm.staleAfterHours;

  if (!hasActivityLead && syncedAt && Date.now() - syncedAt < retryHours * 60 * 60 * 1000) {
    return false;
  }

  return true;
}

function shouldHydrateUniCourtOnDemand(item = {}) {
  if (!config.uniCourt?.enabled || !uniCourt.hasCredentials()) {
    return false;
  }

  const docketNumber = String(item.docket_number || "");
  if (!/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
    return false;
  }

  const entryCount = Number(item.entries?.length || 0);
  const expectedEntries = Math.max(
    10,
    Number(item.docket_count || 0),
    Number(getPriorityFeedRaw(item.raw)?.rowCount || 0),
    6
  );

  const hasActivityLead = hasCaseLevelActivityLead(item);
  if (!hasActivityLead && entryCount >= expectedEntries) {
    return false;
  }

  const syncedAt = item.raw?.unicourt?.syncedAt ? Date.parse(item.raw.unicourt.syncedAt) : 0;
  const state = String(item.raw?.unicourt?.state || "").toLowerCase();
  const retryHours =
    state === "not_found"
      ? Number(config.uniCourt?.notFoundRetryAfterHours || 24)
      : Number(config.uniCourt?.staleAfterHours || 24);

  if (!hasActivityLead && syncedAt && Date.now() - syncedAt < retryHours * 60 * 60 * 1000) {
    return false;
  }

  return true;
}

function buildCaseHydrationPlan(item = {}) {
  const recentfilings = shouldHydrateRecentFilingsOnDemand(item);
  const lawfirm61tro = shouldHydrate61troOnDemand(item);
  const courtlistener = shouldHydrateCourtListenerOnDemand(item);
  const priorityFeed = shouldHydratePriorityFeedOnDemand(item);
  const pacermonitor = shouldHydratePacerMonitorOnDemand(item);
  const docketalarm = shouldHydrateDocketAlarmOnDemand(item);
  const unicourtPlan = shouldHydrateUniCourtOnDemand(item);
  const lookup = Boolean(String(item?.docket_number || item?.case_name || "").trim()) &&
    (recentfilings || lawfirm61tro || courtlistener || priorityFeed || pacermonitor || docketalarm || unicourtPlan);
  return {
    pending: lookup || recentfilings || lawfirm61tro || courtlistener || priorityFeed || pacermonitor || docketalarm || unicourtPlan,
    lookup,
    recentfilings,
    lawfirm61tro,
    courtlistener,
    priority: priorityFeed,
    pacermonitor,
    docketalarm,
    unicourt: unicourtPlan
  };
}

function shouldHydratePublicCaseDetailOnDemand(item = {}) {
  const hasActivityLead = hasCaseLevelActivityLead(item);
  const entryCount = Number(item.entries?.length || 0);
  if (!hasActivityLead && entryCount >= 4) {
    return false;
  }

  const syncedAt = item.last_docket_sync_at ? Date.parse(item.last_docket_sync_at) : 0;
  if (!hasActivityLead && syncedAt && Date.now() - syncedAt < 4 * 60 * 60 * 1000) {
    return false;
  }

  return buildCaseHydrationPlan(item).pending;
}

function queueCaseHydration(caseId, initialItem) {
  const plan = buildCaseHydrationPlan(initialItem);
  if (!plan.pending) {
    return plan;
  }

  if (backgroundCaseHydrations.has(caseId)) {
    return plan;
  }

  const task = (async () => {
    let current = store.getCase(caseId) || initialItem;

    if (plan.lookup) {
      try {
        const lookupTerm = String(current?.docket_number || current?.case_name || "").trim();
        if (lookupTerm) {
          await syncService.importLookup(lookupTerm, {
            courtName: current?.court_name || "",
            caseName: current?.case_name || "",
            sources: ["courtlistener"]
          });
          current = store.getCase(caseId) || current;
        }
      } catch {
        current = store.getCase(caseId) || current;
      }
    }

    if (plan.recentfilings && shouldHydrateRecentFilingsOnDemand(current)) {
      try {
        await syncService.enrichCaseWithRecentFilings(caseId);
        current = store.getCase(caseId) || current;
      } catch {
        current = store.getCase(caseId) || current;
      }
    }

    if (plan.lawfirm61tro && shouldHydrate61troOnDemand(current)) {
      try {
        await syncService.enrichCaseWithLawFirmLookup(caseId, {
          sourceIds: ["61tro"]
        });
        current = store.getCase(caseId) || current;
      } catch {
        current = store.getCase(caseId) || current;
      }
    }

    if (plan.courtlistener && shouldHydrateCourtListenerOnDemand(current)) {
      try {
        await syncService.enrichCaseWithCourtListener(caseId);
        current = store.getCase(caseId) || current;
      } catch {
        current = store.getCase(caseId) || current;
      }
    }

    if (plan.priority && shouldHydratePriorityFeedOnDemand(current)) {
      try {
        await syncService.enrichCaseWithPriorityFeed(caseId, {
          force: shouldForcePriorityFeedRefresh(current)
        });
        current = store.getCase(caseId) || current;
      } catch {
        current = store.getCase(caseId) || current;
      }
    }

    if (shouldHydratePacerMonitorOnDemand(current)) {
      try {
        await syncService.enrichCaseWithPacerMonitor(caseId);
        current = store.getCase(caseId) || current;
      } catch {
        // Keep the existing detail payload available even when the fallback source misses.
      }
    }

    if (shouldHydrateDocketAlarmOnDemand(current)) {
      try {
        await syncService.enrichCaseWithDocketAlarm(caseId);
        current = store.getCase(caseId) || current;
      } catch {
        current = store.getCase(caseId) || current;
      }
    }

    if (shouldHydrateUniCourtOnDemand(current)) {
      try {
        await syncService.enrichCaseWithUniCourt(caseId);
      } catch {
        // Keep the existing detail payload available even when the fallback source misses.
      }
    }
  })()
    .catch(() => {})
    .finally(() => {
      backgroundCaseHydrations.delete(caseId);
      clearPublicCaseCaches();
    });

  backgroundCaseHydrations.set(caseId, task);
  return plan;
}

async function refreshCaseAcrossSources(caseId, {
  initialItem = null,
  providers = [SIGNAL_FEED_PROVIDER_KEY, "lookup:courtlistener", "recentfilings", "61tro", PRIORITY_FEED_SOURCE, "courtlistener", "pacermonitor", "docketalarm", "unicourt"],
  interProviderDelayMs = 0,
  shouldContinue = null
} = {}) {
  const expandedProviders = [...new Set(
    providers.flatMap((provider) => {
      if (provider === "lookup") {
        return ["lookup:courtlistener", "recentfilings", "61tro"];
      }
      if (provider === "lookup:recentfilings") {
        return ["recentfilings"];
      }
      if (provider === "lookup:61tro") {
        return ["61tro"];
      }
      return [provider];
    })
  )];
  const completedProviders = [];
  const failedProviders = [];
  const providerResults = [];
  let lookupResult = null;
  let current = store.getCase(caseId) || initialItem || null;
  let stoppedReason = null;

  const refreshCurrent = () => {
    current = store.getCase(caseId) || current;
    return current;
  };

  const shouldRunProvider = async (provider) => {
    if (typeof shouldContinue !== "function") {
      return {
        ok: true,
        reason: null
      };
    }

    const decision = await shouldContinue({
      caseId,
      provider,
      current: refreshCurrent(),
      completedProviders: [...completedProviders],
      failedProviders: [...failedProviders],
      providerResults: [...providerResults]
    });
    if (decision === false) {
      return {
        ok: false,
        reason: "stopped"
      };
    }

    if (decision && typeof decision === "object" && decision.ok === false) {
      return {
        ok: false,
        reason: decision.reason || "stopped"
      };
    }

    return {
      ok: true,
      reason: null
    };
  };

  const runProvider = async (provider, task) => {
    const decision = await shouldRunProvider(provider);
    if (!decision.ok) {
      stoppedReason = decision.reason;
      return false;
    }

    try {
      const result = await task();
      providerResults.push({
        provider,
        ...(result && typeof result === "object" ? result : { result })
      });
      completedProviders.push(provider);
      refreshCurrent();
      if (interProviderDelayMs > 0) {
        await wait(interProviderDelayMs);
      }
    } catch (error) {
      failedProviders.push({
        provider,
        error: error?.message || String(error),
        status: Number(error?.status || 0) || null,
        retryAfterMs: Number(error?.retryAfterMs || 0) || null
      });
      console.warn(`[sync] provider ${provider} failed for case ${caseId}: ${error?.message || error}`);
    }
    return true;
  };

  if (!stoppedReason && expandedProviders.includes(SIGNAL_FEED_PROVIDER_KEY)) {
    await runProvider(SIGNAL_FEED_PROVIDER_KEY, () => syncService.enrichCaseWithSignalFeed(caseId, { force: true }));
  }

  const lookupProviders = expandedProviders.filter((provider) => provider.startsWith("lookup:"));
  if (lookupProviders.length && current) {
    const decision = await shouldRunProvider("lookup");
    if (!decision.ok) {
      stoppedReason = decision.reason;
    } else {
      try {
        const lookupTerm = String(current?.docket_number || current?.case_name || "").trim();
        if (lookupTerm) {
          const lookupSources = lookupProviders
            .map((provider) => provider.split(":")[1] || "")
            .filter(Boolean);
          lookupResult = await syncService.importLookup(lookupTerm, {
            courtName: current?.court_name || "",
            caseName: current?.case_name || "",
            sources: lookupSources
          });
          providerResults.push({
            provider: "lookup",
            lookupResult
          });
          refreshCurrent();
        }
      } catch (error) {
        failedProviders.push({
          provider: "lookup",
          error: error?.message || String(error),
          status: Number(error?.status || 0) || null,
          retryAfterMs: Number(error?.retryAfterMs || 0) || null
        });
        console.warn(`[sync] provider lookup failed for case ${caseId}: ${error?.message || error}`);
      }
      if (lookupResult?.sourceResults) {
        for (const provider of lookupProviders) {
          const sourceKey = provider.split(":")[1] || "";
          const sourceResult = lookupResult.sourceResults?.[sourceKey] || null;
          if (!sourceResult?.attempted) {
            continue;
          }
          completedProviders.push(provider);
        }
      }
      if (interProviderDelayMs > 0 && lookupProviders.length) {
        await wait(interProviderDelayMs);
      }
    }
  }

  if (!stoppedReason && expandedProviders.includes("recentfilings")) {
    await runProvider("recentfilings", () => syncService.enrichCaseWithRecentFilings(caseId, { force: true }));
  }

  if (!stoppedReason && expandedProviders.includes("61tro")) {
    await runProvider("61tro", () => syncService.enrichCaseWithLawFirmLookup(caseId, {
      sourceIds: ["61tro"],
      force: true
    }));
  }

  if (!stoppedReason && expandedProviders.includes(PRIORITY_FEED_SOURCE)) {
    await runProvider(PRIORITY_FEED_SOURCE, async () => {
      await syncService.enrichCaseWithPriorityFeed(caseId, {
        force: true
      });
    });
  }

  if (!stoppedReason && expandedProviders.includes("courtlistener")) {
    await runProvider("courtlistener", () => syncService.enrichCaseWithCourtListener(caseId, { force: true }));
  }

  if (!stoppedReason && expandedProviders.includes("pacermonitor")) {
    await runProvider("pacermonitor", () => syncService.enrichCaseWithPacerMonitor(caseId, { force: true }));
  }

  if (!stoppedReason && expandedProviders.includes("docketalarm")) {
    await runProvider("docketalarm", () => syncService.enrichCaseWithDocketAlarm(caseId, { force: true }));
  }

  if (!stoppedReason && expandedProviders.includes("unicourt")) {
    await runProvider("unicourt", () => syncService.enrichCaseWithUniCourt(caseId, { force: true }));
  }

  store.refreshCaseDocketSummary(caseId);
  refreshCurrent();

  return {
    caseId,
    requestedProviders: expandedProviders,
    completedProviders,
    failedProviders,
    providerResults,
    stoppedReason,
    lookupResult
  };
}

function isTransientEnrichmentFailure(failure = {}) {
  const status = Number(failure?.status || 0);
  if ([408, 409, 425, 429, 500, 502, 503, 504].includes(status)) {
    return true;
  }

  return /\b(408|409|425|429|500|502|503|504)\b/.test(String(failure?.error || ""));
}

function shouldClearCachesForEnrichment(result = {}) {
  return (result.providerResults || []).some((item) =>
    item?.enriched ||
    Number(item?.casesUpserted || 0) > 0 ||
    Number(item?.docketEntriesUpserted || 0) > 0
  );
}

async function runWebhookEnrichmentWorker() {
  if (!config.sync?.webhookEnrichmentEnabled) {
    return {
      skipped: true,
      reason: "disabled"
    };
  }

  const runId = store.claimSyncRun("system", "webhook-enrichment", 45);
  if (!runId) {
    return {
      skipped: true,
      reason: "already-running"
    };
  }

  let claimedJob = null;

  try {
    const claimed = store.updateCheckpointAtomically(webhookEnrichmentQueueCheckpointKey, (currentValue) => {
      const result = claimWebhookEnrichmentJobState(currentValue, {
        now: new Date().toISOString(),
        leaseTtlMs: Math.max(Number(config.sync?.webhookEnrichmentLeaseTtlMs || 0), 60 * 1000),
        maxPending: Math.max(Number(config.sync?.webhookEnrichmentMaxPendingJobs || 0), 0)
      });
      return {
        value: result.state,
        result
      };
    });

    claimedJob = claimed?.job || null;
    if (!claimedJob) {
      const queueStats = getWebhookEnrichmentQueueStats(store.getCheckpoint(webhookEnrichmentQueueCheckpointKey), {
        now: new Date().toISOString()
      });
      const stats = {
        claimed: 0,
        pendingCount: queueStats.pendingCount,
        readyCount: queueStats.readyCount,
        leasedCount: queueStats.leasedCount,
        note: "webhook enrichment queue empty"
      };
      store.finishSyncRun(runId, "completed", stats);
      return {
        skipped: true,
        reason: "queue-empty",
        ...stats
      };
    }

    const initialMemory = checkWebhookEnrichmentMemoryPressure();
    if (!initialMemory.ok) {
      const finished = store.updateCheckpointAtomically(webhookEnrichmentQueueCheckpointKey, (currentValue) => {
        const result = finishWebhookEnrichmentJobState(currentValue, {
          caseId: claimedJob.caseId,
          leaseId: claimedJob.leaseId,
          now: new Date().toISOString(),
          requeueAfterMs: Math.max(Number(config.sync?.webhookEnrichmentMemoryRetryDelayMs || 0), 60 * 1000),
          requeueJob: {
            providers: claimedJob.providers
          },
          maxPending: Math.max(Number(config.sync?.webhookEnrichmentMaxPendingJobs || 0), 0)
        });
        return {
          value: result.state,
          result
        };
      });
      const queueStats = getWebhookEnrichmentQueueStats(finished?.state || store.getCheckpoint(webhookEnrichmentQueueCheckpointKey), {
        now: new Date().toISOString()
      });
      const stats = {
        claimed: 1,
        caseId: claimedJob.caseId,
        requeued: 1,
        reason: initialMemory.reason,
        memory: initialMemory.snapshot,
        pendingCount: queueStats.pendingCount,
        readyCount: queueStats.readyCount,
        leasedCount: queueStats.leasedCount
      };
      store.finishSyncRun(runId, "completed", stats);
      return stats;
    }

    const result = await refreshCaseAcrossSources(claimedJob.caseId, {
      initialItem: store.getCase(claimedJob.caseId),
      providers: claimedJob.providers,
      interProviderDelayMs: Math.max(Number(config.sync?.webhookEnrichmentProviderDelayMs || 0), 0),
      shouldContinue: () => {
        const guard = checkWebhookEnrichmentMemoryPressure();
        return guard.ok
          ? { ok: true }
          : {
              ok: false,
              reason: "memory-pressure"
            };
      }
    });

    const remainingProviders = (claimedJob.providers || []).filter((provider) => !result.completedProviders.includes(provider));
    const transientFailures = (result.failedProviders || []).filter(isTransientEnrichmentFailure);
    const retryAfterMs = transientFailures.reduce((maxDelay, failure) => {
      return Math.max(maxDelay, Number(failure?.retryAfterMs || 0), 0);
    }, 0);
    const requeueAfterMs = result.stoppedReason === "memory-pressure"
      ? Math.max(Number(config.sync?.webhookEnrichmentMemoryRetryDelayMs || 0), 60 * 1000)
      : transientFailures.length && remainingProviders.length
        ? Math.max(Number(config.sync?.webhookEnrichmentRetryDelayMs || 0), retryAfterMs, 60 * 1000)
        : 0;

    const finished = store.updateCheckpointAtomically(webhookEnrichmentQueueCheckpointKey, (currentValue) => {
      const completion = finishWebhookEnrichmentJobState(currentValue, {
        caseId: claimedJob.caseId,
        leaseId: claimedJob.leaseId,
        now: new Date().toISOString(),
        requeueAfterMs,
        requeueJob: requeueAfterMs > 0
          ? {
              providers: remainingProviders.length ? remainingProviders : claimedJob.providers,
              priorityAt: claimedJob.priorityAt,
              lastWebhookAt: claimedJob.lastWebhookAt,
              lastWebhookFiledAt: claimedJob.lastWebhookFiledAt,
              latestEntryId: claimedJob.latestEntryId,
              entryCount: claimedJob.entryCount,
              eventType: claimedJob.eventType,
              idempotencyKey: claimedJob.idempotencyKey
            }
          : null,
        maxPending: Math.max(Number(config.sync?.webhookEnrichmentMaxPendingJobs || 0), 0)
      });
      return {
        value: completion.state,
        result: completion
      };
    });

    if (shouldClearCachesForEnrichment(result)) {
      clearPublicCaseCaches();
    }

    const queueStats = getWebhookEnrichmentQueueStats(finished?.state || store.getCheckpoint(webhookEnrichmentQueueCheckpointKey), {
      now: new Date().toISOString()
    });
    const stats = {
      claimed: 1,
      caseId: claimedJob.caseId,
      completedProviders: result.completedProviders,
      failedProviders: result.failedProviders,
      stoppedReason: result.stoppedReason,
      requeued: requeueAfterMs > 0 ? 1 : 0,
      requeueAfterMs: requeueAfterMs || null,
      pendingCount: queueStats.pendingCount,
      readyCount: queueStats.readyCount,
      leasedCount: queueStats.leasedCount
    };
    store.finishSyncRun(runId, "completed", stats);

    if (queueStats.readyCount > 0) {
      scheduleWebhookEnrichmentWorker({ force: true });
    }

    return {
      ...stats,
      providerResults: result.providerResults
    };
  } catch (error) {
    if (claimedJob?.caseId) {
      try {
        store.updateCheckpointAtomically(webhookEnrichmentQueueCheckpointKey, (currentValue) => {
          const completion = finishWebhookEnrichmentJobState(currentValue, {
            caseId: claimedJob.caseId,
            leaseId: claimedJob.leaseId,
            now: new Date().toISOString(),
            requeueAfterMs: Math.max(Number(config.sync?.webhookEnrichmentRetryDelayMs || 0), 60 * 1000),
            requeueJob: {
              providers: claimedJob.providers,
              priorityAt: claimedJob.priorityAt,
              lastWebhookAt: claimedJob.lastWebhookAt,
              lastWebhookFiledAt: claimedJob.lastWebhookFiledAt,
              latestEntryId: claimedJob.latestEntryId,
              entryCount: claimedJob.entryCount,
              eventType: claimedJob.eventType,
              idempotencyKey: claimedJob.idempotencyKey
            },
            maxPending: Math.max(Number(config.sync?.webhookEnrichmentMaxPendingJobs || 0), 0)
          });
          return {
            value: completion.state,
            result: completion
          };
        });
      } catch (checkpointError) {
        console.error("[webhook-enrichment] failed to requeue crashed job", checkpointError);
      }
    }

    store.finishSyncRun(runId, "failed", {
      claimed: claimedJob?.caseId ? 1 : 0,
      caseId: claimedJob?.caseId || null
    }, error?.message || String(error));
    throw error;
  }
}

function serializePublicEntry(entry = {}) {
  return {
    id: entry.id,
    filed_at: entry.filed_at || null,
    entry_number: normalizeDisplayNumber(entry.entry_number),
    document_number: normalizeDisplayNumber(entry.document_number),
    document_type: sanitizeEntryDocumentType(entry.document_type),
    description: sanitizePublicText(entry.description),
    description_zh: sanitizePublicText(entry.description_zh),
    timeline_label: sanitizeTimelineLabel(entry)
  };
}

function serializePublicCaseSummary(item = {}) {
  return {
    id: item.id,
    presentation_mode: publicCasePresentationMode(item),
    case_name: item.case_name || null,
    case_name_zh: item.case_name_zh || null,
    court_id: item.court_id || null,
    court_name: item.court_name || null,
    docket_number: item.docket_number || null,
    date_filed: item.date_filed || null,
    date_terminated: item.date_terminated || null,
    status: item.status || null,
    recent_activity_summary: sanitizePublicText(item.recent_activity_summary),
    recent_activity_summary_zh: sanitizePublicText(item.recent_activity_summary_zh),
    latest_docket_filed_at: item.latest_docket_filed_at || null,
    latest_docket_number: item.latest_docket_number || null,
    docket_count: Number(item.docket_count || 0),
    insights: sanitizeInsights(item.insights)
  };
}

function areEntryNumbersSequential(entries) {
  if (!entries.length) return false;
  const nums = entries
    .map((e) => {
      const raw = e.document_number ?? e.entry_number;
      return raw != null ? Number(raw) : NaN;
    })
    .filter((n) => !isNaN(n) && n > 0);
  // Need at least 3 numbered entries
  if (nums.length < 3) return false;
  // At least 80% of all entries must carry a number;
  // if many entries lack numbers it means sources are mixed
  if (nums.length / entries.length < 0.8) return false;
  const sorted = [...nums].sort((a, b) => a - b);
  let gaps = 0;
  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i] - sorted[i - 1] !== 1) gaps++;
  }
  // Strict: tolerate at most 15 % non-consecutive pairs
  return gaps / (sorted.length - 1) < 0.15;
}

function serializePublicCaseDetail(item = {}) {
  const rawEntries = Array.isArray(item.entries) ? item.entries : [];
  const serializedEntries = rawEntries.map(serializePublicEntry);
  return {
    ...serializePublicCaseSummary(item),
    entries_truncated: Boolean(item.entries_truncated),
    hydration_pending: item.hydration_pending
      ? {
          pending: Boolean(item.hydration_pending.pending)
        }
      : null,
    entries_sequential: areEntryNumbersSequential(rawEntries),
    entries: serializedEntries
  };
}

function serializePublicCasesPayload(payload = {}) {
  return {
    items: Array.isArray(payload.items) ? payload.items.map(serializePublicCaseSummary) : [],
    total: Number(payload.total || 0),
    page: Number(payload.page || 1),
    pageSize: Number(payload.pageSize || 25),
    pageCount: Number(payload.pageCount || 1),
    courts: Array.isArray(payload.courts)
      ? payload.courts.map((court) => ({
          court_id: court.court_id || "",
          court_name: court.court_name || "",
          total: Number(court.total || 0)
        }))
      : [],
    categoryRelaxed: Boolean(payload.categoryRelaxed),
    relaxedCategory: payload.relaxedCategory || null,
    liveImported: payload.liveImported
      ? {
          imported: Number(payload.liveImported.imported || 0),
          matched: Number(payload.liveImported.matched || 0)
        }
      : null,
    lookupPending: Boolean(payload.lookupPending),
    lookupError: payload.lookupError || null
  };
}

function serializePublicStatus(status = {}) {
  const dashboard = status.dashboard || {};
  const recentSync = dashboard.recentSync || null;

  return {
    isRunning: Boolean(status.isRunning),
    currentMode: status.currentMode || null,
    lastStartedAt: status.lastStartedAt || null,
    lastFinishedAt: status.lastFinishedAt || null,
    dashboard: {
      totals: {
        total_cases: Number(dashboard.totals?.total_cases || 0),
        watchlist_cases: Number(dashboard.totals?.watchlist_cases || 0),
        tro_cases: Number(dashboard.totals?.tro_cases || 0),
        schedule_a_cases: Number(dashboard.totals?.schedule_a_cases || 0),
        seller_cases: Number(dashboard.totals?.seller_cases || 0),
        today_added_watchlist: Number(dashboard.totals?.today_added_watchlist || 0)
      },
      latestCase: dashboard.latestCase
        ? {
            updated_at: dashboard.latestCase.updated_at || null,
            case_name: dashboard.latestCase.case_name || null,
            docket_number: dashboard.latestCase.docket_number || null
          }
        : null,
      recentSync: recentSync
        ? {
            id: recentSync.id,
            mode: recentSync.mode || "recent",
            status: recentSync.status || "unknown",
            started_at: recentSync.started_at || null,
            finished_at: recentSync.finished_at || null
          }
        : null
    }
  };
}

function serializeAdminStatus(status = {}) {
  return {
    ...serializePublicStatus(status),
    providers: {
      priority: status.providers?.priorityFeed
        ? { enabled: Boolean(status.providers.priorityFeed.enabled), state: status.providers.priorityFeed.state || null }
        : null,
      official: status.providers?.courtlistener
        ? { enabled: Boolean(status.providers.courtlistener.enabled), state: status.providers.courtlistener.state || null }
        : null,
      fallback: status.providers?.pacermonitor
        ? { enabled: Boolean(status.providers.pacermonitor.enabled), state: status.providers.pacermonitor.state || null }
        : null,
      pacer: status.providers?.pacer
        ? { enabled: Boolean(status.providers.pacer.enabled), state: status.providers.pacer.state || null }
        : null,
      docketalarm: status.providers?.docketalarm
        ? { enabled: Boolean(status.providers.docketalarm.enabled), state: status.providers.docketalarm.state || null }
        : null,
      unicourt: status.providers?.unicourt
        ? { enabled: Boolean(status.providers.unicourt.enabled), state: status.providers.unicourt.state || null }
        : null,
      courtfeeds: status.providers?.courtfeeds
        ? { enabled: Boolean(status.providers.courtfeeds.enabled), state: status.providers.courtfeeds.state || null }
        : null,
      recentfilings: status.providers?.recentfilings
        ? { enabled: Boolean(status.providers.recentfilings.enabled), state: status.providers.recentfilings.state || null }
        : null,
      advisories: status.providers?.lawfirms
        ? { enabled: Boolean(status.providers.lawfirms.enabled), state: status.providers.lawfirms.state || null }
        : null
    }
  };
}

function serializeGapPayload(payload = {}) {
  return {
    summary: {
      total: Number(payload.summary?.total || 0),
      courtlistener: Number(payload.summary?.courtlistener || 0),
      priority: Number(payload.summary?.priority || 0),
      pacermonitor: Number(payload.summary?.pacermonitor || 0),
      challenge: Number(payload.summary?.challenge || 0)
    },
    items: Array.isArray(payload.items)
      ? payload.items.map((item) => ({
          id: Number(item.id || 0),
          docket_number: item.docket_number || null,
          case_name: item.case_name || null,
          court_name: item.court_name || null,
          latest_docket_filed_at: item.latest_docket_filed_at || null,
          lead_law_firm: item.lead_law_firm || null,
          defendant_count: Number(item.defendant_count || 0),
          docket_count: Number(item.docket_count || 0),
          total_entries: Number(item.total_entries || 0),
          courtlistener_entries: Number(item.courtlistener_entries || 0),
          expected_entries: Number(item.expected_entries || 0),
          gap: Number(item.gap || 0),
          courtlistener_gap: Number(item.courtlistener_gap || 0),
          priority_row_count: Number(item.priority_row_count || 0),
          priority_entries: Number(item.priority_entries || 0),
          pacermonitor_entries: Number(item.pacermonitor_entries || 0),
          priority_synced_at: item.priority_synced_at || null,
          pacermonitor_synced_at: item.pacermonitor_synced_at || null,
          pacermonitor_state: item.pacermonitor_state || null,
          is_recent_case: Boolean(item.is_recent_case),
          providers_needed: Array.isArray(item.providers_needed)
            ? item.providers_needed.map((value) => publicProviderLabel(value))
            : [],
          reasons: Array.isArray(item.reasons)
            ? item.reasons.map((value) =>
                String(value || "")
                  .replace(new RegExp(PRIORITY_FEED_SOURCE, "gi"), PRIORITY_FEED_PUBLIC_LABEL)
                  .replace(/courtlistener/gi, "官方摘要")
                  .replace(/pacermonitor/gi, "备用公开源")
              )
            : []
        }))
      : []
  };
}

function looksLikeBareDocketSequence(value = "") {
  return /^\d{5,6}$/.test(String(value || "").trim());
}

function isStrictCivilDocketLookup(value = "") {
  const search = String(value || "").trim();
  return /^(?:\d+:)?\d{2}-cv-\d{5,6}$/i.test(search);
}

function isMalformedCivilDocketLookup(value = "") {
  const search = String(value || "").trim();
  return /(?:\d+:)?\d{2}-cv-/i.test(search) && !isStrictCivilDocketLookup(search);
}

function normalizeCategory(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return ["watchlist", "seller_watch", "tro", "schedule_a", "all"].includes(normalized) ? normalized : "";
}

function resolveSearchCategory(search = "", requestedCategory = "") {
  const explicitCategory = normalizeCategory(requestedCategory);
  if (explicitCategory) {
    return explicitCategory;
  }

  if (docketLooksLike(search) || looksLikeBareDocketSequence(search)) {
    return "all";
  }

  return "watchlist";
}

function findRelaxedPayload(store, filters) {
  for (const category of ["watchlist", "seller_watch", "tro", "schedule_a", "all"]) {
    if (category === filters.category) {
      continue;
    }

    const payload = store.listCases({
      ...filters,
      category
    });

    if (payload.total > 0) {
      payload.categoryRelaxed = true;
      payload.relaxedCategory = category;
      return payload;
    }
  }

  return null;
}

async function handleApi(request, response, pathname, searchParams) {
  if (request.method === "OPTIONS") {
    response.writeHead(204, buildApiHeaders(request.headers.origin || ""));
    response.end();
    return;
  }

  if (request.method === "POST" && pathname.startsWith(courtListenerWebhookPrefix)) {
    return handleCourtListenerWebhook(request, response, pathname);
  }

  if (enforceTemporaryPublicBlock(request, response, pathname)) {
    return;
  }

  if (enforcePublicReadRateLimit(request, response, pathname)) {
    return;
  }

  if (enforceBrowserGuard(request, response, pathname)) {
    return;
  }

  if (enforcePublicApiToken(request, response, pathname)) {
    return;
  }

  if (await enforcePublicBehaviorThrottle(request, response, pathname, searchParams)) {
    return;
  }

  if (request.method === "GET" && pathname === "/api/health") {
    const cached = getCachedPublicPayload(request, pathname);
    if (cached) {
      return sendJson(response, 200, cached);
    }

    const payload = {
      ok: true,
      startDate: config.sync.startDate,
      runtime: {
        isRunning: syncService.state.isRunning,
        currentMode: syncService.state.currentMode,
        lastStartedAt: syncService.state.lastStartedAt,
        lastFinishedAt: syncService.state.lastFinishedAt,
        lastError: syncService.state.lastError
      }
    };
    setCachedPublicPayload(request, pathname, payload);
    return sendJson(response, 200, payload);
  }

  if (request.method === "GET" && pathname === "/api/cases") {
    const rawSearch = searchParams.get("search") || "";
    const useCache = true;
    const cached = useCache ? getCachedPublicPayload(request, pathname) : null;
    if (cached) {
      return sendJson(response, 200, cached);
    }

    const filters = {
      startDate: config.sync.startDate,
      category: resolveSearchCategory(rawSearch, searchParams.get("category") || ""),
      search: rawSearch,
      court: searchParams.get("court") || "",
      page: Number(searchParams.get("page") || 1),
      pageSize: Math.min(Number(searchParams.get("pageSize") || 25), config.server.publicCasesMaxPageSize)
    };

    let payload = store.listCases(filters);
    const isDirectDocketLookup = docketLooksLike(filters.search);
    const isBareDocketSequence = looksLikeBareDocketSequence(filters.search);
    const isStrictDirectDocketLookup = isStrictCivilDocketLookup(filters.search);
    const isMalformedDirectDocketLookup = isMalformedCivilDocketLookup(filters.search);

    if (filters.search && payload.total === 0 && isDirectDocketLookup) {
      const relaxedPayload = findRelaxedPayload(store, filters);
      if (relaxedPayload) {
        payload = relaxedPayload;
      }
    }

    if (filters.search && payload.total === 0 && isMalformedDirectDocketLookup) {
      payload.lookupError = "案号格式建议输入 00123 或 26-cv-00123，避免输入 26-cv-123。";
    }

    if (filters.search && payload.total === 0 && isStrictDirectDocketLookup) {
      queueLookupImport(filters.search);
      payload.lookupPending = true;
    }

    const serialized = serializePublicCasesPayload(payload);
    if (useCache && !serialized.lookupPending) {
      setCachedPublicPayload(request, pathname, serialized);
    }
    return sendJson(response, 200, serialized);
  }

  if (request.method === "GET" && pathname.startsWith("/api/cases/")) {
    const cached = getCachedPublicPayload(request, pathname);
    if (cached) {
      return sendJson(response, 200, cached);
    }

    const caseId = Number(pathname.split("/").pop());
    const fullDetail = searchParams.get("full") === "1";
    let item = store.getCase(caseId, {
      recentEntriesLimit: fullDetail ? 0 : config.server.publicCaseDetailInitialEntries
    });

    if (!item) {
      return sendJson(response, 404, { error: "Case not found" });
    }

    const hydrationPlan = buildCaseHydrationPlan(item);
    const payload = serializePublicCaseDetail({
      ...item,
      hydration_pending: hydrationPlan
    });
    setCachedPublicPayload(request, pathname, payload);
    return sendJson(response, 200, payload);
  }

  if (request.method === "GET" && pathname === "/api/sync/status") {
    const cached = getCachedPublicPayload(request, pathname);
    if (cached) {
      return sendJson(response, 200, cached);
    }

    const payload = serializePublicStatus(syncService.getPublicStatus());
    setCachedPublicPayload(request, pathname, payload);
    return sendJson(response, 200, payload);
  }

  if (request.method === "GET" && pathname === "/api/tro-daily-updates") {
    const cached = getCachedPublicPayload(request, pathname);
    if (cached) {
      queueTroDailyUpdatesRefreshIfNeeded(cached);
      return sendJson(response, 200, cached);
    }

    const payload = serializeTroDailyUpdates(loadTroDailyUpdates(config.reports.troDailyUpdates));
    queueTroDailyUpdatesRefreshIfNeeded(payload);
    setCachedPublicPayload(request, pathname, payload);
    return sendJson(response, 200, payload);
  }

  if (request.method === "GET" && pathname === "/api/admin/status") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    return sendJson(response, 200, serializeAdminStatus(syncService.getPublicStatus()));
  }

  if (request.method === "GET" && pathname === "/api/admin/gaps") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    const limit = Math.min(Math.max(Number(searchParams.get("limit") || 25), 1), 100);
    return sendJson(response, 200, serializeGapPayload(store.getCoverageGapCases(limit, {
      recentWindowDays: config.pacerMonitor.recentWindowDays
    })));
  }

  if (request.method === "POST" && pathname === "/api/admin/sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    const body = await readRequestBody(request);
    const mode = body.mode === "backfill" ? "backfill" : "recent";

    spawnDetachedTask(["--sync-only", mode]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/priority-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "catalog"]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "priority-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/fallback-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "pacermonitor"]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "fallback-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/pacer-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "pacer"]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "pacer-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/docketalarm-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "docketalarm"]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "docketalarm-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/unicourt-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "unicourt"]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "unicourt-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/official-docket-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "courtlistener-docket"]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "official-docket-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/courtlistener-alert-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "courtlistener-alerts"]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "courtlistener-alert-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/court-feed-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "courtfeeds"]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "court-feed-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/law-firm-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "lawfirms"]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "advisory-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/reconcile-duplicates") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    const body = await readRequestBody(request);
    const limit = Math.min(Math.max(Number(body.limit || 100), 1), 500);
    spawnDetachedTask(["--sync-only", "reconcile-duplicates", "--limit", String(limit)]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "reconcile-duplicates",
      limit
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/purge-non-watchlist") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    if (!config.server.enablePurgeNonWatchlist) {
      return sendJson(response, 403, { error: "purge-non-watchlist disabled" });
    }

    const body = await readRequestBody(request);
    const limit = Math.max(Number(body.limit || 0), 0);
    const dryRun = body.dryRun === undefined ? true : Boolean(body.dryRun);
    const startDate = String(body.startDate || "").trim();
    const args = ["--sync-only", "purge-non-watchlist"];
    if (dryRun) {
      args.push("--dry-run");
    }
    if (limit > 0) {
      args.push("--limit", String(limit));
    }
    if (startDate) {
      args.push("--start-date", startDate);
    }

    spawnDetachedTask(args);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      mode: "purge-non-watchlist",
      dryRun,
      limit,
      startDate: startDate || null
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/enrich-case") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    const body = await readRequestBody(request);
    const requestedProviders = Array.isArray(body.providers)
      ? body.providers
      : [OFFICIAL_DOCKET_PROVIDER_KEY, PRIORITY_FEED_PROVIDER_KEY, FALLBACK_PROVIDER_KEY, "docketalarm", "unicourt"];
    const providers = [...new Set(
      requestedProviders.filter((item) =>
        item === "courtlistener" ||
        item === "pacermonitor" ||
        item === "docketalarm" ||
        item === "unicourt" ||
        item === OFFICIAL_DOCKET_PROVIDER_KEY ||
        item === PRIORITY_FEED_PROVIDER_KEY ||
        item === FALLBACK_PROVIDER_KEY
      )
    )].map((item) => {
      if (item === OFFICIAL_DOCKET_PROVIDER_KEY) {
        return "courtlistener";
      }
      if (item === PRIORITY_FEED_PROVIDER_KEY) {
        return PRIORITY_FEED_SOURCE;
      }
      if (item === FALLBACK_PROVIDER_KEY) {
        return "pacermonitor";
      }
      return item;
    });

    let item = Number(body.caseId) > 0 ? store.getCase(Number(body.caseId)) : null;
    if (!item && body.search) {
      const payload = store.listCases({
        startDate: config.sync.startDate,
        category: "all",
        search: String(body.search || "").trim(),
        page: 1,
        pageSize: 5
      });
      const first = payload.items?.[0];
      item = first ? store.getCase(first.id) : null;
    }

    if (!item) {
      return sendJson(response, 404, { error: "Case not found" });
    }

    spawnDetachedTask([
      "--enrich-case-id",
      String(item.id),
      "--providers",
      providers.join(",")
    ]);
    clearPublicCaseCaches();

    return sendJson(response, 202, {
      accepted: true,
      case: {
        id: item.id,
        docket_number: item.docket_number || null,
        case_name: item.case_name || null
      },
      providers
    });
  }

  sendJson(response, 404, { error: "Not found" });
}

function serveStatic(request, response, pathname) {
  const target = pathname === "/" ? "/index.html" : pathname;
  const filePath = path.join(config.publicDir, target);

  if (!filePath.startsWith(config.publicDir)) {
    response.writeHead(403);
    response.end("Forbidden");
    return;
  }

  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    const fallback = path.join(config.publicDir, "index.html");
    response.writeHead(200, attachBrowserGuardCookie(request, {
      "content-type": "text/html; charset=utf-8",
      "x-content-type-options": "nosniff",
      "cache-control": "no-store"
    }));
    response.end(renderPublicHtml(request, fs.readFileSync(fallback), { includeBootstrap: true }));
    return;
  }

  const extension = path.extname(filePath);
  const stat = fs.statSync(filePath);
  const etag = buildWeakEtag(stat);
  const headers = {
    "content-type": mimeTypes[extension] || "application/octet-stream",
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "same-origin",
    etag,
    "last-modified": stat.mtime.toUTCString()
  };

  if (target === "/ops.html") {
    headers["cache-control"] = "no-store";
    headers["x-robots-tag"] = "noindex, nofollow, noarchive";
  }

  if (extension === ".html") {
    headers["content-security-policy"] =
      "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self' mailto:";
    headers["cache-control"] = "no-store";
  } else if (isImmutableStaticAsset(target, extension)) {
    headers["cache-control"] = "public, max-age=31536000, immutable";
  } else if (isCacheableStaticAsset(target, extension)) {
    headers["cache-control"] = "public, max-age=2592000";
  } else {
    headers["cache-control"] = "public, max-age=3600";
  }
  if (extension === ".html") {
    attachBrowserGuardCookie(request, headers);
  }

  if (extension !== ".html" && String(request.headers["if-none-match"] || "").trim() === etag) {
    response.writeHead(304, headers);
    response.end();
    return;
  }

  response.writeHead(200, headers);
  response.end(
    extension === ".html"
      ? renderPublicHtml(request, fs.readFileSync(filePath), { includeBootstrap: target === "/index.html" })
      : fs.readFileSync(filePath)
  );
}

const server = http.createServer(async (request, response) => {
  try {
    const hostname = normalizeHostHeader(request.headers.host);
    if (shouldRedirectToWww(hostname)) {
      redirectToWww(request, response);
      return;
    }

    const url = new URL(request.url, `http://${request.headers.host}`);

    if (enforceScannerShield(request, response, url.pathname, url.searchParams)) {
      return;
    }

    if (url.pathname.startsWith("/api/")) {
      response.setHeader("access-control-allow-origin", buildApiHeaders(request.headers.origin || "")["access-control-allow-origin"] || "");
      response.setHeader("access-control-allow-methods", "GET,POST,OPTIONS");
      response.setHeader("access-control-allow-headers", `content-type,x-admin-token,${publicApiTokenHeaderName}`);
      await handleApi(request, response, url.pathname, url.searchParams);
      return;
    }

    serveStatic(request, response, url.pathname);
  } catch (error) {
    console.error("[server]", error);
    sendJson(response, 500, { error: error.message });
  }
});

async function main() {
  const refreshCaseDocketIndex = process.argv.indexOf("--refresh-case-docket");
  if (refreshCaseDocketIndex !== -1) {
    const docketNumber = String(process.argv[refreshCaseDocketIndex + 1] || "").trim();
    const courtNameIndex = process.argv.indexOf("--court-name");
    const caseNameIndex = process.argv.indexOf("--case-name");
    const courtName = courtNameIndex !== -1 ? String(process.argv[courtNameIndex + 1] || "").trim() : "";
    const caseName = caseNameIndex !== -1 ? String(process.argv[caseNameIndex + 1] || "").trim() : "";
    const providersIndex = process.argv.indexOf("--providers");
    const providers = providersIndex !== -1
      ? String(process.argv[providersIndex + 1] || "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean)
      : [SIGNAL_FEED_PROVIDER_KEY, "lookup:courtlistener", "recentfilings", "61tro", PRIORITY_FEED_SOURCE, "courtlistener", "pacermonitor", "docketalarm", "unicourt"];

    const lookupResult = await syncService.importLookup(docketNumber, { courtName, caseName });
    const startDate = config.sync.discoveryStartDate || config.sync.startDate || "2025-01-01";
    const caseRow =
      store.findCaseByCourtAndDocket({ courtName, docketNumber, startDate }) ||
      store.findCaseByDocketNumber(docketNumber, startDate);

    if (!caseRow?.id) {
      const completedLookupProviders = Object.entries(lookupResult?.sourceResults || {})
        .filter(([, result]) => result?.attempted)
        .map(([sourceKey]) => `lookup:${sourceKey}`);
      console.log(JSON.stringify({
        docketNumber,
        courtName,
        caseName,
        ...lookupResult,
        resolvedCaseId: null,
        requestedProviders: providers,
        completedProviders: completedLookupProviders,
        failedProviders: []
      }));
      process.exit(lookupResult.matched > 0 ? 0 : 1);
    }

    const result = await refreshCaseAcrossSources(Number(caseRow.id), {
      initialItem: caseRow,
      providers
    });
    console.log(JSON.stringify({
      docketNumber,
      courtName,
      caseName,
      resolvedCaseId: Number(caseRow.id),
      initialLookupResult: lookupResult,
      ...result
    }));
    console.log(`[sync] refreshed case ${caseRow.id} (${docketNumber}) with ${result.completedProviders.join(",") || "none"}`);
    process.exit(result.completedProviders.length ? 0 : 1);
  }

  const enrichCaseIdIndex = process.argv.indexOf("--enrich-case-id");
  if (enrichCaseIdIndex !== -1) {
    const caseId = Number(process.argv[enrichCaseIdIndex + 1]);
    const providersIndex = process.argv.indexOf("--providers");
    const providers = providersIndex !== -1
      ? String(process.argv[providersIndex + 1] || "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean)
      : [SIGNAL_FEED_PROVIDER_KEY, "lookup:courtlistener", "recentfilings", "61tro", PRIORITY_FEED_SOURCE, "courtlistener", "pacermonitor", "docketalarm", "unicourt"];

    const result = await refreshCaseAcrossSources(caseId, {
      initialItem: store.getCase(caseId),
      providers
    });
    console.log(JSON.stringify(result));
    console.log(`[sync] enriched case ${caseId} with ${result.completedProviders.join(",") || "none"}`);
    process.exit(result.completedProviders.length ? 0 : 1);
  }

  const importLookupIndex = process.argv.indexOf("--import-lookup");
  if (importLookupIndex !== -1) {
    const term = String(process.argv[importLookupIndex + 1] || "").trim();
    const courtNameIndex = process.argv.indexOf("--court-name");
    const caseNameIndex = process.argv.indexOf("--case-name");
    const courtName = courtNameIndex !== -1 ? String(process.argv[courtNameIndex + 1] || "").trim() : "";
    const caseName = caseNameIndex !== -1 ? String(process.argv[caseNameIndex + 1] || "").trim() : "";
    const result = await syncService.importLookup(term, { courtName, caseName });
    console.log(JSON.stringify({
      term,
      courtName,
      caseName,
      ...result
    }));
    process.exit(0);
  }

  const syncOnlyIndex = process.argv.indexOf("--sync-only");
  if (syncOnlyIndex !== -1) {
    const rawMode = process.argv[syncOnlyIndex + 1];
    const resultJson = process.argv.includes("--result-json");
    const normalizedMode =
      rawMode === PRIORITY_FEED_SOURCE
        ? "catalog"
        : rawMode === `${PRIORITY_FEED_SOURCE}-until-idle`
          ? "catalog-until-idle"
          : rawMode;
    if (normalizedMode === "catalog") {
      const forceDiscovery = process.argv.includes("--force-discovery");
      const result = await withStandaloneSyncTaskLock("catalog", () =>
        syncService.syncPriorityFeedRecent("backfill", { forceDiscovery })
      );
      printSyncOnlyResult("catalog", result, { resultJson });
      process.exit(0);
    }

    if (normalizedMode === "catalog-until-idle") {
      const maxRoundsIndex = process.argv.indexOf("--max-rounds");
      const idleRoundsIndex = process.argv.indexOf("--idle-rounds");
      const sleepMsIndex = process.argv.indexOf("--sleep-ms");
      const batchSizeIndex = process.argv.indexOf("--batch-size");
      const maxRounds = maxRoundsIndex !== -1
        ? Math.max(Number(process.argv[maxRoundsIndex + 1] || 0), 1)
        : 200;
      const idleRounds = idleRoundsIndex !== -1
        ? Math.max(Number(process.argv[idleRoundsIndex + 1] || 0), 1)
        : 3;
      const sleepMs = sleepMsIndex !== -1
        ? Math.max(Number(process.argv[sleepMsIndex + 1] || 0), 0)
        : 3000;
      const batchSize = batchSizeIndex !== -1
        ? Math.max(Number(process.argv[batchSizeIndex + 1] || 0), 1)
        : 25;

      let rounds = 0;
      let idleStreak = 0;
      let totalSyncedCases = 0;
      let totalFailedCases = 0;
      let totalNotFoundCases = 0;
      let discoverySnapshot = null;

      const forceDiscovery = process.argv.includes("--force-discovery");
      while (rounds < maxRounds && idleStreak < idleRounds) {
        rounds += 1;
        console.log(`[sync] catalog round ${rounds} starting ${JSON.stringify({ batchSize })}`);
        const forceDiscoveryEnv = forceDiscovery && rounds === 1 ? "1" : "0";
        const result = await runSyncModeChild(
          "catalog",
          [],
          {
            PRIORITY_FEED_BACKFILL_MAX_CASES_PER_RUN: String(batchSize),
            PRIORITY_FEED_PROGRESS: "1",
            PRIORITY_FEED_FORCE_DISCOVERY: forceDiscoveryEnv
          },
          { streamLogs: true }
        );
        totalSyncedCases += Number(result.syncedCases || 0);
        totalFailedCases += Number(result.failedCases || 0);
        totalNotFoundCases += Number(result.notFoundCases || 0);
        discoverySnapshot = {
          discoveredCases: Number(result.discoveredCases || 0),
          attachedCases: Number(result.attachedCases || 0),
          createdCases: Number(result.createdCases || 0),
          totalCatalogCases: Number(result.totalCatalogCases || 0),
          discoverySkipped: Boolean(result.discoverySkipped)
        };

        const idleRound = Number(result.candidateCount || 0) === 0 && Number(result.discoveredCases || 0) === 0;
        idleStreak = idleRound ? idleStreak + 1 : 0;

        console.log(`[sync] catalog round ${rounds} ${JSON.stringify({
          ...result,
          idleRound,
          idleStreak
        })}`);

        if (idleStreak >= idleRounds || rounds >= maxRounds) {
          break;
        }

        if (sleepMs > 0) {
          await wait(sleepMs);
        }
      }

      console.log(`[sync] completed catalog-until-idle ${JSON.stringify({
        rounds,
        idleStreak,
        maxRounds,
        idleRounds,
        batchSize,
        totalSyncedCases,
        totalFailedCases,
        totalNotFoundCases,
        discoverySnapshot
      })}`);
      process.exit(0);
    }

    if (rawMode === "courtfeeds") {
      const result = await withStandaloneSyncTaskLock("courtfeeds", () => syncService.syncCourtFeedsRecent("recent"));
      printSyncOnlyResult("courtfeeds", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "recentfilings") {
      const result = await withStandaloneSyncTaskLock("recentfilings", () => syncService.syncRecentFilingsRecent("recent"));
      printSyncOnlyResult("recentfilings", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === SIGNAL_FEED_PROVIDER_KEY) {
      const result = await withStandaloneSyncTaskLock(SIGNAL_FEED_PROVIDER_KEY, () => syncService.syncSignalFeedRecent("recent"));
      printSyncOnlyResult(SIGNAL_FEED_PROVIDER_KEY, result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "lawfirms") {
      const result = await withStandaloneSyncTaskLock("lawfirms", () => syncService.syncLawFirmRecent("recent"));
      printSyncOnlyResult("lawfirms", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "pacermonitor") {
      const result = await withStandaloneSyncTaskLock("pacermonitor", () => syncService.syncPacerMonitorRecent("backfill"));
      printSyncOnlyResult("pacermonitor", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "pacer") {
      const result = await withStandaloneSyncTaskLock("pacer", () => syncService.syncPacerRecent("backfill"));
      printSyncOnlyResult("pacer", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "docketalarm") {
      const result = await withStandaloneSyncTaskLock("docketalarm", () => syncService.syncDocketAlarmRecent("backfill"));
      printSyncOnlyResult("docketalarm", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "unicourt") {
      const result = await withStandaloneSyncTaskLock("unicourt", () => syncService.syncUniCourtRecent("backfill"));
      printSyncOnlyResult("unicourt", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "courtlistener-docket") {
      const result = await withStandaloneSyncTaskLock("courtlistener-docket", () => syncService.syncCourtListenerDockets());
      printSyncOnlyResult("courtlistener-docket", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "courtlistener-alerts") {
      const limitIndex = process.argv.indexOf("--limit");
      const startDateIndex = process.argv.indexOf("--start-date");
      const force = process.argv.includes("--force");
      const result = await withStandaloneSyncTaskLock("courtlistener-alerts", () => syncService.syncCourtListenerAlertSubscriptions({
        limit: limitIndex !== -1 ? Math.max(Number(process.argv[limitIndex + 1] || 0), 1) : null,
        startDate: startDateIndex !== -1 ? String(process.argv[startDateIndex + 1] || "").trim() : null,
        force
      }));
      printSyncOnlyResult("courtlistener-alerts", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "recompute-case-summaries") {
      const batchSizeIndex = process.argv.indexOf("--batch-size");
      const limitIndex = process.argv.indexOf("--limit");
      const touchUpdatedAt = process.argv.includes("--touch-updated-at");
      const result = await withStandaloneSyncTaskLock("recompute-case-summaries", () => store.recomputeAllCaseDocketSummaries({
        batchSize: batchSizeIndex !== -1 ? Number(process.argv[batchSizeIndex + 1] || 500) : 500,
        limit: limitIndex !== -1 ? Number(process.argv[limitIndex + 1] || 0) : 0,
        touchUpdatedAt
      }));
      printSyncOnlyResult("recompute-case-summaries", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "reconcile-duplicates") {
      const limitIndex = process.argv.indexOf("--limit");
      const limit = limitIndex !== -1 ? Math.min(Math.max(Number(process.argv[limitIndex + 1] || 100), 1), 500) : 100;
      const result = await withStandaloneSyncTaskLock("reconcile-duplicates", () => store.reconcileDuplicateCases({
        startDate: config.sync.startDate,
        category: "watchlist",
        limit
      }));
      printSyncOnlyResult("reconcile-duplicates", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "dedupe-docket-entries") {
      const limitIndex = process.argv.indexOf("--limit");
      const caseIdIndex = process.argv.indexOf("--case-id");
      const sourceIndex = process.argv.indexOf("--source");
      const startDateIndex = process.argv.indexOf("--start-date");
      const result = await withStandaloneSyncTaskLock("dedupe-docket-entries", () => store.dedupeStoredDocketEntries({
        limit: limitIndex !== -1 ? Math.max(Number(process.argv[limitIndex + 1] || 0), 0) : 0,
        caseId: caseIdIndex !== -1 ? Math.max(Number(process.argv[caseIdIndex + 1] || 0), 0) : 0,
        primarySource: sourceIndex !== -1 ? String(process.argv[sourceIndex + 1] || "").trim() || null : null,
        startDate: startDateIndex !== -1 ? String(process.argv[startDateIndex + 1] || "").trim() || config.sync.startDate : config.sync.startDate
      }));
      printSyncOnlyResult("dedupe-docket-entries", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "rebuild-case-fast-path") {
      const batchSizeIndex = process.argv.indexOf("--batch-size");
      const limitIndex = process.argv.indexOf("--limit");
      const result = await withStandaloneSyncTaskLock("rebuild-case-fast-path", () => store.rebuildCaseFastPathColumns({
        batchSize: batchSizeIndex !== -1 ? Math.max(Number(process.argv[batchSizeIndex + 1] || 1000), 1) : 1000,
        limit: limitIndex !== -1 ? Math.max(Number(process.argv[limitIndex + 1] || 0), 0) : 0
      }));
      printSyncOnlyResult("rebuild-case-fast-path", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "cleanup-window-email") {
      const result = await withStandaloneSyncTaskLock("cleanup-window-email", () => store.cleanupWindowEmailArtifacts({
        vacuum: process.argv.includes("--vacuum")
      }));
      printSyncOnlyResult("cleanup-window-email", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "restore-missing-from-backup") {
      const sourceDbIndex = process.argv.indexOf("--source-db");
      const limitIndex = process.argv.indexOf("--limit");
      const result = await withStandaloneSyncTaskLock("restore-missing-from-backup", () => store.restoreMissingFromBackup({
        sourceDbPath: sourceDbIndex !== -1 ? String(process.argv[sourceDbIndex + 1] || "").trim() : "",
        dryRun: process.argv.includes("--dry-run"),
        limit: limitIndex !== -1 ? Math.max(Number(process.argv[limitIndex + 1] || 0), 0) : 0,
        retainedOnly: process.argv.includes("--retained-only")
      }));
      printSyncOnlyResult("restore-missing-from-backup", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "inspect-missing-from-backup") {
      const sourceDbIndex = process.argv.indexOf("--source-db");
      const limitIndex = process.argv.indexOf("--limit");
      const result = await withStandaloneSyncTaskLock("inspect-missing-from-backup", () => store.inspectMissingFromBackup({
        sourceDbPath: sourceDbIndex !== -1 ? String(process.argv[sourceDbIndex + 1] || "").trim() : "",
        limit: limitIndex !== -1 ? Math.max(Number(process.argv[limitIndex + 1] || 0), 0) : 0,
        retainedOnly: process.argv.includes("--retained-only")
      }));
      printSyncOnlyResult("inspect-missing-from-backup", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "purge-non-watchlist") {
      if (!config.server.enablePurgeNonWatchlist) {
        throw new Error("purge-non-watchlist disabled");
      }
      const limitIndex = process.argv.indexOf("--limit");
      const startDateIndex = process.argv.indexOf("--start-date");
      const limit = limitIndex !== -1 ? Math.max(Number(process.argv[limitIndex + 1] || 0), 0) : 0;
      const startDate = startDateIndex !== -1 ? String(process.argv[startDateIndex + 1] || "").trim() : "";
      const dryRun = process.argv.includes("--dry-run");
      const result = await store.purgeNonWatchlistCases({
        limit,
        dryRun,
        startDate: startDate || null
      });
      console.log(`[sync] completed purge-non-watchlist ${JSON.stringify(result)}`);
      process.exit(0);
    }

    if (rawMode === "daily-report") {
      const result = await withStandaloneSyncTaskLock("daily-report", () => dailyEmailReport.maybeSendScheduledReport());
      printSyncOnlyResult("daily-report", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "tro-daily-roundup") {
      const result = await withStandaloneSyncTaskLock("tro-daily-roundup", () => troDailyRoundup.maybeSendScheduledRoundup());
      printSyncOnlyResult("tro-daily-roundup", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "tro-daily-updates") {
      const result = await withStandaloneSyncTaskLock("tro-daily-updates", () => troDailyRoundup.refreshUpdates({}));
      printSyncOnlyResult("tro-daily-updates", result, { resultJson });
      process.exit(0);
    }

    if (rawMode === "webhook-enrichment") {
      const result = await runWebhookEnrichmentWorker();
      if (result?.skipped) {
        console.log(`[sync] skipped webhook-enrichment ${JSON.stringify({ reason: result.reason || "queue-empty" })}`);
      } else {
        console.log(`[sync] completed webhook-enrichment ${JSON.stringify(result)}`);
      }
      process.exit(0);
    }

    const mode = rawMode === "backfill" ? "backfill" : "recent";
    const result = await syncService.run(mode);
    if (resultJson) {
      console.log(JSON.stringify(result));
    } else if (result?.skipped) {
      console.log(`[sync] skipped ${mode} ${JSON.stringify({ reason: result.reason || "already-running" })}`);
    } else {
      console.log(`[sync] completed ${mode}`);
    }
    process.exit(0);
  }

  const sendDailyReportIndex = process.argv.indexOf("--send-daily-report");
  if (sendDailyReportIndex !== -1) {
    const dateIndex = process.argv.indexOf("--date");
    const localDate = dateIndex !== -1 ? String(process.argv[dateIndex + 1] || "").trim() : undefined;
    const result = await dailyEmailReport.sendReport({ localDate, force: true });
    console.log(`[sync] completed send-daily-report ${JSON.stringify(result)}`);
    process.exit(0);
  }

  const sendTroDailyRoundupIndex = process.argv.indexOf("--send-tro-daily-roundup");
  if (sendTroDailyRoundupIndex !== -1) {
    const dateIndex = process.argv.indexOf("--date");
    const localDate = dateIndex !== -1 ? String(process.argv[dateIndex + 1] || "").trim() : undefined;
    const result = await troDailyRoundup.sendRoundup({ localDate, force: true });
    console.log(`[sync] completed send-tro-daily-roundup ${JSON.stringify(result)}`);
    process.exit(0);
  }

  server.listen(config.server.port, () => {
    console.log(`TRO Case Watch listening on http://localhost:${config.server.port}`);
  });

  if (config.sync?.webhookEnrichmentEnabled) {
    setTimeout(() => {
      scheduleWebhookEnrichmentWorker({ force: true });
    }, Math.min(Math.max(Number(config.sync?.webhookEnrichmentPollIntervalMs || 60 * 1000), 15 * 1000), 60 * 1000));

    setInterval(() => {
      scheduleWebhookEnrichmentWorker({ force: true });
    }, Math.max(Number(config.sync?.webhookEnrichmentPollIntervalMs || 60 * 1000), 15 * 1000));
  }

  if (!config.sync.internalSchedulersEnabled) {
    console.log("[scheduler] internal background schedulers disabled; use cron or explicit sync commands");
    return;
  }

  if (config.sync.bootstrapSync) {
    setTimeout(() => {
      spawnDetachedTask(["--sync-only", "recent"]);
    }, config.sync.bootstrapSyncDelayMs);
  }

  if (config.sync.enableScheduler) {
    const recentCheckIntervalMs = Math.min(
      Math.max(Number(config.sync.recentSchedulerCheckIntervalMs || 5 * 60 * 1000), 60 * 1000),
      Math.max(Number(config.sync.pollIntervalMs || 30 * 60 * 1000), 60 * 1000)
    );
    setInterval(() => {
      spawnDetachedTaskIfDue("recent", config.sync.pollIntervalMs);
    }, recentCheckIntervalMs);
  }

  if (config.sync.watchdogEnabled) {
    setTimeout(() => {
      reapAndRecoverStaleSyncRuns();
    }, Math.min(Number(config.sync.watchdogIntervalMs || 120000), 60 * 1000));

    setInterval(() => {
      reapAndRecoverStaleSyncRuns();
    }, Math.max(Number(config.sync.watchdogIntervalMs || 120000), 30 * 1000));
  }

  if (config.sync.enableBackfillScheduler) {
    setTimeout(() => {
      if (syncService.getBackfillStatus().pending) {
        spawnDetachedTaskIfDue("backfill", config.sync.backfillIntervalMs);
      }
    }, config.sync.bootstrapBackfillDelayMs);

    const backfillCheckIntervalMs = Math.min(
      Math.max(Number(config.sync.backfillSchedulerCheckIntervalMs || 15 * 60 * 1000), 5 * 60 * 1000),
      Math.max(Number(config.sync.backfillIntervalMs || 60 * 60 * 1000), 5 * 60 * 1000)
    );
    setInterval(() => {
      if (!syncService.getBackfillStatus().pending) {
        return;
      }

      spawnDetachedTaskIfDue("backfill", config.sync.backfillIntervalMs);
    }, backfillCheckIntervalMs);
  }

  if (courtListener.hasDocketAlertAccess()) {
    setTimeout(() => {
      spawnDetachedTask(["--sync-only", "courtlistener-alerts"]);
    }, Math.max(Number(config.courtListener?.docketAlertSyncBootstrapDelayMs || 90 * 1000), 10 * 1000));

    setInterval(() => {
      spawnDetachedTask(["--sync-only", "courtlistener-alerts"]);
    }, Math.max(Number(config.courtListener?.docketAlertSyncIntervalMs || 6 * 60 * 60 * 1000), 60 * 60 * 1000));
  }

  if (config.reports?.dailyEmail?.enabled) {
    setTimeout(() => {
      spawnDetachedTask(["--sync-only", "daily-report"]);
    }, config.reports.dailyEmail.startupDelayMs);

    setInterval(() => {
      spawnDetachedTask(["--sync-only", "daily-report"]);
    }, config.reports.dailyEmail.checkIntervalMs);
  }

  if (config.reports?.troDailyRoundup?.enabled) {
    setTimeout(() => {
      spawnDetachedTask(["--sync-only", "tro-daily-updates"]);
    }, Math.min(config.reports.troDailyRoundup.startupDelayMs, 60 * 1000));

    setTimeout(() => {
      spawnDetachedTask(["--sync-only", "tro-daily-roundup"]);
    }, config.reports.troDailyRoundup.startupDelayMs);

    setInterval(() => {
      spawnDetachedTask(["--sync-only", "tro-daily-roundup"]);
    }, config.reports.troDailyRoundup.checkIntervalMs);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
