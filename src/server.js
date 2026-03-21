import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import { spawn } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import zlib from "node:zlib";
import { URL, fileURLToPath } from "node:url";
import { config } from "./config.js";
import { Store } from "./db.js";
import { CourtListenerClient } from "./providers/courtlistener.js";
import { CourtFeedClient } from "./providers/courtfeed.js";
import { LawFirmClient } from "./providers/lawfirm.js";
import { WorldtroClient } from "./providers/worldtro.js";
import { PacerAdapter } from "./providers/pacer.js";
import { PacerMonitorAdapter } from "./providers/pacermonitor.js";
import { TranslationService } from "./translation.js";
import { CaseSyncService } from "./sync.js";
import { docketLooksLike } from "./insights.js";

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

ensureSeedDatabase();

const store = createStoreWithRecovery();
const courtListener = new CourtListenerClient(config.courtListener);
const courtFeeds = new CourtFeedClient(config.courtFeeds);
const lawFirms = new LawFirmClient(config.lawFirms);
const worldtro = new WorldtroClient(config.worldtro);
const pacerMonitor = new PacerMonitorAdapter(config.pacerMonitor);
const pacer = new PacerAdapter(config.pacer, store);
const translator = new TranslationService(config.translation, store);
const syncService = new CaseSyncService({
  config,
  store,
  courtFeeds,
  lawFirms,
  courtListener,
  worldtro,
  pacerMonitor,
  pacer,
  translator
});
const backgroundCaseHydrations = new Map();
const publicResponseCache = new Map();
const publicRateLimitBuckets = new Map();

function clearPublicResponseCache() {
  publicResponseCache.clear();
}

function spawnDetachedTask(args = []) {
  const child = spawn(process.execPath, [currentScriptPath, ...args], {
    cwd: path.dirname(config.publicDir),
    env: process.env,
    detached: true,
    stdio: "ignore"
  });
  child.unref();
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

function buildApiHeaders(origin = "") {
  const headers = {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "x-robots-tag": "noindex, nofollow, noarchive"
  };

  const allowedOrigins = new Set([
    "https://trotracker.com",
    "https://www.trotracker.com",
    "https://tro-case-watch-production.up.railway.app",
    "http://localhost:4127"
  ]);

  if (allowedOrigins.has(origin)) {
    headers["access-control-allow-origin"] = origin;
    headers["access-control-allow-methods"] = "GET,POST,OPTIONS";
    headers["access-control-allow-headers"] = "content-type,x-admin-token";
    headers["vary"] = "Origin";
  }

  return headers;
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

function authorize(request) {
  if (!config.server.adminToken) {
    return true;
  }

  return request.headers["x-admin-token"] === config.server.adminToken;
}

function getClientIp(request) {
  const forwarded = String(request.headers["x-forwarded-for"] || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)[0];
  return forwarded || request.socket?.remoteAddress || "unknown";
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

function getPublicCacheTtlMs(pathname) {
  if (pathname === "/api/health") {
    return config.server.publicHealthCacheTtlMs;
  }

  if (pathname === "/api/sync/status") {
    return config.server.publicStatusCacheTtlMs;
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
    highlights: Array.isArray(insights.highlights) ? insights.highlights : [],
    narrative: insights.narrative || null,
    badges: Array.isArray(insights.badges) ? insights.badges : []
  };
}

function sanitizeEntryDocumentType(value) {
  const type = String(value || "").trim();
  if (!type) {
    return "Docket Entry";
  }

  if (/worldtro/i.test(type)) {
    return "Docket Entry";
  }

  if (/pacermonitor/i.test(type)) {
    return /document/i.test(type) ? "Docket Document" : "Docket Entry";
  }

  if (/pacer document/i.test(type)) {
    return "Docket Document";
  }

  return type.replace(/worldtro/gi, "Docket").replace(/pacermonitor/gi, "Docket");
}

function normalizeDisplayNumber(value) {
  const text = String(value || "").trim();
  if (!text) {
    return null;
  }

  return text.replace(/\.0+$/g, "");
}

function sanitizeTimelineLabel(entry = {}) {
  if (entry.primary_source === "courtlistener") {
    return "公开文书摘要";
  }

  return "Docket 时间线";
}

function hasWorldtroCoverage(item = {}) {
  if (Number(item.raw?.worldtro?.rowCount || 0) > 0) {
    return true;
  }

  if (Array.isArray(item.source_urls) && item.source_urls.some((url) => String(url || "").includes("worldtro.com"))) {
    return true;
  }

  return Array.isArray(item.entries) && item.entries.some((entry) => entry.primary_source === "worldtro");
}

function shouldHydrateWorldtroOnDemand(item = {}) {
  if (!config.worldtro.enabled || !item.insights?.is_seller_case) {
    return false;
  }

  const entryCount = Number(item.entries?.length || 0);
  const worldtroRowCount = Number(item.raw?.worldtro?.rowCount || 0);
  const minimumExpectedEntries = Math.max(12, Number(item.docket_count || 0), 6);

  if (!hasWorldtroCoverage(item)) {
    return entryCount < minimumExpectedEntries;
  }

  return worldtroRowCount > 0 && entryCount < worldtroRowCount;
}

function shouldForceWorldtroRefresh(item = {}) {
  const worldtroRowCount = Number(item.raw?.worldtro?.rowCount || 0);
  return worldtroRowCount > 0 && Number(item.entries?.length || 0) < worldtroRowCount;
}

function shouldHydrateCourtListenerOnDemand(item = {}) {
  if (!courtListener.hasDocketAccess() || !item?.courtlistener_docket_id) {
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
    Number(item.raw?.worldtro?.rowCount || 0),
    latestNumber
  );

  if (entryCount >= expectedEntries) {
    return false;
  }

  const syncedAt = item.last_docket_sync_at ? Date.parse(item.last_docket_sync_at) : 0;
  if (syncedAt && Date.now() - syncedAt < 2 * 60 * 60 * 1000) {
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
    Number(item.raw?.worldtro?.rowCount || 0),
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

function buildCaseHydrationPlan(item = {}) {
  const courtlistener = shouldHydrateCourtListenerOnDemand(item);
  const worldtro = shouldHydrateWorldtroOnDemand(item);
  const pacermonitor = shouldHydratePacerMonitorOnDemand(item);
  return {
    pending: courtlistener || worldtro || pacermonitor,
    courtlistener,
    worldtro,
    pacermonitor
  };
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

    if (plan.courtlistener && shouldHydrateCourtListenerOnDemand(current)) {
      try {
        await syncService.enrichCaseWithCourtListener(caseId);
        current = store.getCase(caseId) || current;
      } catch {
        current = store.getCase(caseId) || current;
      }
    }

    if (plan.worldtro && shouldHydrateWorldtroOnDemand(current)) {
      try {
        await syncService.enrichCaseWithWorldtro(caseId, {
          force: shouldForceWorldtroRefresh(current)
        });
        current = store.getCase(caseId) || current;
      } catch {
        current = store.getCase(caseId) || current;
      }
    }

    if (shouldHydratePacerMonitorOnDemand(current)) {
      try {
        await syncService.enrichCaseWithPacerMonitor(caseId);
      } catch {
        // Keep the existing detail payload available even when the fallback source misses.
      }
    }
  })()
    .catch(() => {})
    .finally(() => {
      backgroundCaseHydrations.delete(caseId);
      clearPublicResponseCache();
    });

  backgroundCaseHydrations.set(caseId, task);
  return plan;
}

function serializePublicEntry(entry = {}) {
  return {
    id: entry.id,
    filed_at: entry.filed_at || null,
    entry_number: normalizeDisplayNumber(entry.entry_number),
    document_number: normalizeDisplayNumber(entry.document_number),
    document_type: sanitizeEntryDocumentType(entry.document_type),
    description: entry.description || null,
    description_zh: entry.description_zh || null,
    timeline_label: sanitizeTimelineLabel(entry)
  };
}

function serializePublicCaseSummary(item = {}) {
  return {
    id: item.id,
    case_name: item.case_name || null,
    case_name_zh: item.case_name_zh || null,
    court_id: item.court_id || null,
    court_name: item.court_name || null,
    docket_number: item.docket_number || null,
    date_filed: item.date_filed || null,
    date_terminated: item.date_terminated || null,
    status: item.status || null,
    recent_activity_summary: item.recent_activity_summary || null,
    recent_activity_summary_zh: item.recent_activity_summary_zh || null,
    latest_docket_filed_at: item.latest_docket_filed_at || null,
    latest_docket_number: item.latest_docket_number || null,
    docket_count: Number(item.docket_count || 0),
    insights: sanitizeInsights(item.insights)
  };
}

function serializePublicCaseDetail(item = {}) {
  return {
    ...serializePublicCaseSummary(item),
    hydration_pending: item.hydration_pending
      ? {
          pending: Boolean(item.hydration_pending.pending),
          worldtro: Boolean(item.hydration_pending.worldtro),
          pacermonitor: Boolean(item.hydration_pending.pacermonitor)
        }
      : null,
    entries: Array.isArray(item.entries) ? item.entries.map(serializePublicEntry) : []
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
      courtfeeds: status.providers?.courtfeeds || null,
      lawfirms: status.providers?.lawfirms || null,
      worldtro: status.providers?.worldtro || null,
      pacermonitor: status.providers?.pacermonitor || null,
      pacer: status.providers?.pacer || null,
      courtlistener: status.providers?.courtlistener || null,
      translation: status.providers?.translation || null
    }
  };
}

function serializeGapPayload(payload = {}) {
  return {
    summary: {
      total: Number(payload.summary?.total || 0),
      courtlistener: Number(payload.summary?.courtlistener || 0),
      worldtro: Number(payload.summary?.worldtro || 0),
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
          worldtro_row_count: Number(item.worldtro_row_count || 0),
          worldtro_entries: Number(item.worldtro_entries || 0),
          pacermonitor_entries: Number(item.pacermonitor_entries || 0),
          worldtro_synced_at: item.worldtro_synced_at || null,
          pacermonitor_synced_at: item.pacermonitor_synced_at || null,
          pacermonitor_state: item.pacermonitor_state || null,
          is_recent_case: Boolean(item.is_recent_case),
          providers_needed: Array.isArray(item.providers_needed) ? item.providers_needed : [],
          reasons: Array.isArray(item.reasons) ? item.reasons : [],
          source_urls: Array.isArray(item.source_urls) ? item.source_urls : []
        }))
      : []
  };
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

  if (docketLooksLike(search)) {
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

  if (enforcePublicReadRateLimit(request, response, pathname)) {
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
    const cached = getCachedPublicPayload(request, pathname);
    if (cached) {
      return sendJson(response, 200, cached);
    }

    const filters = {
      startDate: config.sync.startDate,
      category: resolveSearchCategory(searchParams.get("search") || "", searchParams.get("category") || ""),
      search: searchParams.get("search") || "",
      court: searchParams.get("court") || "",
      page: Number(searchParams.get("page") || 1),
      pageSize: Math.min(Number(searchParams.get("pageSize") || 25), config.server.publicCasesMaxPageSize)
    };

    let payload = store.listCases(filters);
    const isDirectDocketLookup = docketLooksLike(filters.search);

    if (filters.search && payload.total === 0 && isDirectDocketLookup) {
      const relaxedPayload = findRelaxedPayload(store, filters);
      if (relaxedPayload) {
        payload = relaxedPayload;
      }
    }

    if (filters.search && payload.total === 0 && isDirectDocketLookup) {
      try {
        const imported = await syncService.importLookup(filters.search);
        payload = store.listCases(filters);
        if (payload.total === 0 && isDirectDocketLookup) {
          const relaxedPayload = findRelaxedPayload(store, filters);
          if (relaxedPayload) {
            payload = relaxedPayload;
          }
        }
        payload.liveImported = imported;
      } catch (error) {
        payload.lookupError = error.message;
      }
    }

    if (filters.search && payload.items?.length) {
      const exactDocketLookup = docketLooksLike(filters.search);
      if (exactDocketLookup) {
        payload.items.slice(0, 3).forEach((item) => {
          const detail = store.getCase(item.id);
          if (detail) {
            queueCaseHydration(item.id, detail);
          }
        });
      }
    }

    const serialized = serializePublicCasesPayload(payload);
    setCachedPublicPayload(request, pathname, serialized);
    return sendJson(response, 200, serialized);
  }

  if (request.method === "GET" && pathname.startsWith("/api/cases/")) {
    const cached = getCachedPublicPayload(request, pathname);
    if (cached) {
      return sendJson(response, 200, cached);
    }

    const caseId = Number(pathname.split("/").pop());
    let item = store.getCase(caseId);

    if (!item) {
      return sendJson(response, 404, { error: "Case not found" });
    }

    const hydrationPlan = queueCaseHydration(caseId, item);
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
    clearPublicResponseCache();

    return sendJson(response, 202, {
      accepted: true,
      mode
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/docket-backfill") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "worldtro"]);
    clearPublicResponseCache();

    return sendJson(response, 202, {
      accepted: true,
      mode: "docket-backfill"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/pacermonitor-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "pacermonitor"]);
    clearPublicResponseCache();

    return sendJson(response, 202, {
      accepted: true,
      mode: "pacermonitor-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/courtlistener-docket-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "courtlistener-docket"]);
    clearPublicResponseCache();

    return sendJson(response, 202, {
      accepted: true,
      mode: "courtlistener-docket-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/court-feed-sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    spawnDetachedTask(["--sync-only", "courtfeeds"]);
    clearPublicResponseCache();

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
    clearPublicResponseCache();

    return sendJson(response, 202, {
      accepted: true,
      mode: "law-firm-sync"
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/reconcile-duplicates") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    const body = await readRequestBody(request);
    const limit = Math.min(Math.max(Number(body.limit || 100), 1), 500);
    spawnDetachedTask(["--sync-only", "reconcile-duplicates", "--limit", String(limit)]);
    clearPublicResponseCache();

    return sendJson(response, 202, {
      accepted: true,
      mode: "reconcile-duplicates",
      limit
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/enrich-case") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    const body = await readRequestBody(request);
    const requestedProviders = Array.isArray(body.providers)
      ? body.providers
      : ["courtlistener", "worldtro", "pacermonitor"];
    const providers = [...new Set(
      requestedProviders.filter((item) =>
        item === "courtlistener" || item === "worldtro" || item === "pacermonitor"
      )
    )];

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
    clearPublicResponseCache();

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

function serveStatic(response, pathname) {
  const target = pathname === "/" ? "/index.html" : pathname;
  const filePath = path.join(config.publicDir, target);

  if (!filePath.startsWith(config.publicDir)) {
    response.writeHead(403);
    response.end("Forbidden");
    return;
  }

  if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    const fallback = path.join(config.publicDir, "index.html");
    response.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "x-content-type-options": "nosniff"
    });
    response.end(fs.readFileSync(fallback));
    return;
  }

  const extension = path.extname(filePath);
  const headers = {
    "content-type": mimeTypes[extension] || "application/octet-stream"
  };

  if (target === "/ops.html") {
    headers["cache-control"] = "no-store";
    headers["x-robots-tag"] = "noindex, nofollow, noarchive";
  }

  headers["x-content-type-options"] = "nosniff";
  response.writeHead(200, headers);
  response.end(fs.readFileSync(filePath));
}

const server = http.createServer(async (request, response) => {
  try {
    const hostname = normalizeHostHeader(request.headers.host);
    if (shouldRedirectToWww(hostname)) {
      redirectToWww(request, response);
      return;
    }

    const url = new URL(request.url, `http://${request.headers.host}`);

    if (url.pathname.startsWith("/api/")) {
      response.setHeader("access-control-allow-origin", buildApiHeaders(request.headers.origin || "")["access-control-allow-origin"] || "");
      response.setHeader("access-control-allow-methods", "GET,POST,OPTIONS");
      response.setHeader("access-control-allow-headers", "content-type,x-admin-token");
      await handleApi(request, response, url.pathname, url.searchParams);
      return;
    }

    serveStatic(response, url.pathname);
  } catch (error) {
    console.error("[server]", error);
    sendJson(response, 500, { error: error.message });
  }
});

async function main() {
  const enrichCaseIdIndex = process.argv.indexOf("--enrich-case-id");
  if (enrichCaseIdIndex !== -1) {
    const caseId = Number(process.argv[enrichCaseIdIndex + 1]);
    const providersIndex = process.argv.indexOf("--providers");
    const providers = providersIndex !== -1
      ? String(process.argv[providersIndex + 1] || "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean)
      : ["courtlistener", "worldtro", "pacermonitor"];

    if (providers.includes("courtlistener")) {
      await syncService.enrichCaseWithCourtListener(caseId, { force: true });
    }

    if (providers.includes("worldtro")) {
      await syncService.enrichCaseWithWorldtro(caseId, { force: true });
    }

    if (providers.includes("pacermonitor")) {
      await syncService.enrichCaseWithPacerMonitor(caseId, { force: true });
    }

    console.log(`[sync] enriched case ${caseId} with ${providers.join(",")}`);
    process.exit(0);
  }

  const syncOnlyIndex = process.argv.indexOf("--sync-only");
  if (syncOnlyIndex !== -1) {
    const rawMode = process.argv[syncOnlyIndex + 1];
    if (rawMode === "worldtro") {
      const result = await syncService.syncWorldtroRecent("backfill");
      console.log(`[sync] completed worldtro ${JSON.stringify(result)}`);
      process.exit(0);
    }

    if (rawMode === "courtfeeds") {
      const result = await syncService.syncCourtFeedsRecent("recent");
      console.log(`[sync] completed courtfeeds ${JSON.stringify(result)}`);
      process.exit(0);
    }

    if (rawMode === "lawfirms") {
      const result = await syncService.syncLawFirmRecent("recent");
      console.log(`[sync] completed lawfirms ${JSON.stringify(result)}`);
      process.exit(0);
    }

    if (rawMode === "pacermonitor") {
      const result = await syncService.syncPacerMonitorRecent("backfill");
      console.log(`[sync] completed pacermonitor ${JSON.stringify(result)}`);
      process.exit(0);
    }

    if (rawMode === "courtlistener-docket") {
      const result = await syncService.syncCourtListenerDockets();
      console.log(`[sync] completed courtlistener-docket ${JSON.stringify(result)}`);
      process.exit(0);
    }

    if (rawMode === "reconcile-duplicates") {
      const limitIndex = process.argv.indexOf("--limit");
      const limit = limitIndex !== -1 ? Math.min(Math.max(Number(process.argv[limitIndex + 1] || 100), 1), 500) : 100;
      const result = await store.reconcileDuplicateCases({
        startDate: config.sync.startDate,
        category: "watchlist",
        limit
      });
      console.log(`[sync] completed reconcile-duplicates ${JSON.stringify(result)}`);
      process.exit(0);
    }

    const mode = rawMode === "backfill" ? "backfill" : "recent";
    await syncService.run(mode);
    console.log(`[sync] completed ${mode}`);
    process.exit(0);
  }

  server.listen(config.server.port, () => {
    console.log(`TRO Case Watch listening on http://localhost:${config.server.port}`);
  });

  if (config.sync.bootstrapSync) {
    setTimeout(() => {
      spawnDetachedTask(["--sync-only", "recent"]);
    }, config.sync.bootstrapSyncDelayMs);
  }

  if (config.sync.enableScheduler) {
    setInterval(() => {
      spawnDetachedTask(["--sync-only", "recent"]);
    }, config.sync.pollIntervalMs);
  }

  if (config.sync.enableBackfillScheduler) {
    setTimeout(() => {
      spawnDetachedTask(["--sync-only", "backfill"]);
    }, config.sync.bootstrapBackfillDelayMs);

    setInterval(() => {
      if (!syncService.getBackfillStatus().pending) {
        return;
      }

      spawnDetachedTask(["--sync-only", "backfill"]);
    }, config.sync.backfillIntervalMs);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
