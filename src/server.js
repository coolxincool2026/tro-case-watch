import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import { DatabaseSync } from "node:sqlite";
import zlib from "node:zlib";
import { URL } from "node:url";
import { config } from "./config.js";
import { Store } from "./db.js";
import { CourtListenerClient } from "./providers/courtlistener.js";
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

ensureSeedDatabase();

const store = createStoreWithRecovery();
const courtListener = new CourtListenerClient(config.courtListener);
const worldtro = new WorldtroClient(config.worldtro);
const pacerMonitor = new PacerMonitorAdapter(config.pacerMonitor);
const pacer = new PacerAdapter(config.pacer, store);
const translator = new TranslationService(config.translation, store);
const syncService = new CaseSyncService({
  config,
  store,
  courtListener,
  worldtro,
  pacerMonitor,
  pacer,
  translator
});

function ensureSeedDatabase() {
  if (!config.seedDbArchivePath || !fs.existsSync(config.seedDbArchivePath)) {
    return;
  }

  const shouldRestore = needsSeedRestore();
  if (!shouldRestore.restore) {
    return;
  }

  restoreSeedDatabase(shouldRestore.reason);
}

function createStoreWithRecovery() {
  try {
    return new Store(config.dbPath);
  } catch (error) {
    if (!isRecoverableSqliteError(error) || !config.seedDbArchivePath || !fs.existsSync(config.seedDbArchivePath)) {
      throw error;
    }

    console.error(`[bootstrap-db] store open failed, attempting seed restore (${error.message})`);
    restoreSeedDatabase(`store-open-failed:${error.code || "unknown"}`);
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

function restoreSeedDatabase(reason = "manual") {
  fs.mkdirSync(path.dirname(config.dbPath), { recursive: true });
  cleanupDatabaseFiles(config.dbPath, { includePrimary: true });

  const archive = fs.readFileSync(config.seedDbArchivePath);
  const dbBuffer = zlib.gunzipSync(archive);
  const tempPath = `${config.dbPath}.restore-${Date.now()}.tmp`;
  fs.writeFileSync(tempPath, dbBuffer);
  verifyDatabase(tempPath, config.seedDbMinimumCases);
  fs.renameSync(tempPath, config.dbPath);
  cleanupDatabaseFiles(config.dbPath, { includePrimary: false });
  verifyDatabase(config.dbPath, config.seedDbMinimumCases);
  console.log(`[bootstrap-db] restored seed database from ${config.seedDbArchivePath} (${reason})`);
}

function cleanupDatabaseFiles(dbPath, { includePrimary = false } = {}) {
  const targets = includePrimary ? [dbPath] : [];
  targets.push(`${dbPath}-wal`, `${dbPath}-shm`, `${dbPath}-journal`);

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
    const integrity = db.prepare("PRAGMA integrity_check").get();
    if (String(integrity?.integrity_check || "").toLowerCase() !== "ok") {
      throw new Error(`seed-integrity-failed:${integrity?.integrity_check || "unknown"}`);
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
    const integrity = db.prepare("PRAGMA integrity_check").get();
    if (String(integrity?.integrity_check || "").toLowerCase() !== "ok") {
      db.close();
      return { restore: true, reason: `db-integrity:${integrity?.integrity_check || "unknown"}` };
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
    "cache-control": "no-store"
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

function shouldRedirectCustomApi(hostname, pathname) {
  return hostname === "www.trotracker.com" && pathname.startsWith("/api/");
}

function redirectCustomApi(url, response) {
  const target = `https://tro-case-watch-production.up.railway.app${url.pathname}${url.search}`;
  response.writeHead(307, {
    location: target,
    "cache-control": "no-store"
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
      : config.pacerMonitor.staleAfterHours;

  if (syncedAt && Date.now() - syncedAt < retryHours * 60 * 60 * 1000) {
    return false;
  }

  return true;
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

function looksLikeDocketFragment(value = "") {
  return /^\d{4,6}$/.test(String(value || "").trim());
}

function resolveSearchCategory(search = "") {
  if (docketLooksLike(search) || looksLikeDocketFragment(search)) {
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

  if (request.method === "GET" && pathname === "/api/health") {
    return sendJson(response, 200, {
      ok: true,
      startDate: config.sync.startDate,
      status: serializePublicStatus(syncService.getPublicStatus())
    });
  }

  if (request.method === "GET" && pathname === "/api/cases") {
    const filters = {
      startDate: config.sync.startDate,
      category: resolveSearchCategory(searchParams.get("search") || ""),
      search: searchParams.get("search") || "",
      court: searchParams.get("court") || "",
      page: Number(searchParams.get("page") || 1),
      pageSize: Number(searchParams.get("pageSize") || 25)
    };

    let payload = store.listCases(filters);
    const isDirectDocketLookup = docketLooksLike(filters.search) || looksLikeDocketFragment(filters.search);

    if (filters.search && payload.total === 0 && isDirectDocketLookup) {
      const relaxedPayload = findRelaxedPayload(store, filters);
      if (relaxedPayload) {
        payload = relaxedPayload;
      }
    }

    if (filters.search && payload.total === 0) {
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

    return sendJson(response, 200, serializePublicCasesPayload(payload));
  }

  if (request.method === "GET" && pathname.startsWith("/api/cases/")) {
    const caseId = Number(pathname.split("/").pop());
    let item = store.getCase(caseId);

    if (!item) {
      return sendJson(response, 404, { error: "Case not found" });
    }

    if (shouldHydrateWorldtroOnDemand(item)) {
      try {
        await syncService.enrichCaseWithWorldtro(caseId, {
          force: shouldForceWorldtroRefresh(item)
        });
        item = store.getCase(caseId) || item;
      } catch {
        // Fall back to the best data already in the local store.
      }
    }

    if (shouldHydratePacerMonitorOnDemand(item)) {
      try {
        await syncService.enrichCaseWithPacerMonitor(caseId);
        item = store.getCase(caseId) || item;
      } catch {
        // Fall back to the best data already in the local store.
      }
    }

    return sendJson(response, 200, serializePublicCaseDetail(item));
  }

  if (request.method === "GET" && pathname === "/api/sync/status") {
    return sendJson(response, 200, serializePublicStatus(syncService.getPublicStatus()));
  }

  if (request.method === "POST" && pathname === "/api/admin/sync") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    const body = await readRequestBody(request);
    const mode = body.mode === "backfill" ? "backfill" : "recent";

    syncService
      .run(mode)
      .catch((error) => console.error("[sync]", error.message, error.body || ""));

    return sendJson(response, 202, {
      accepted: true,
      mode
    });
  }

  if (request.method === "POST" && pathname === "/api/admin/docket-backfill") {
    if (!authorize(request)) {
      return sendJson(response, 401, { error: "Unauthorized" });
    }

    syncService
      .syncWorldtroRecent("backfill")
      .catch((error) => console.error("[docket-backfill]", error.message, error.body || ""));

    return sendJson(response, 202, {
      accepted: true,
      mode: "docket-backfill"
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
    response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    response.end(fs.readFileSync(fallback));
    return;
  }

  const extension = path.extname(filePath);
  response.writeHead(200, {
    "content-type": mimeTypes[extension] || "application/octet-stream"
  });
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
    if (shouldRedirectCustomApi(hostname, url.pathname)) {
      redirectCustomApi(url, response);
      return;
    }

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
  const syncOnlyIndex = process.argv.indexOf("--sync-only");
  if (syncOnlyIndex !== -1) {
    const rawMode = process.argv[syncOnlyIndex + 1];
    if (rawMode === "worldtro") {
      const result = await syncService.syncWorldtroRecent("backfill");
      console.log(`[sync] completed worldtro ${JSON.stringify(result)}`);
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
      syncService.run("recent").catch((error) => {
        console.error("[bootstrap-sync]", error.message, error.body || "");
      });
    }, config.sync.bootstrapSyncDelayMs);
  }

  if (config.sync.enableScheduler) {
    setInterval(() => {
      syncService.run("recent").catch((error) => {
        console.error("[scheduled-sync]", error.message, error.body || "");
      });
    }, config.sync.pollIntervalMs);
  }

  if (config.sync.enableBackfillScheduler) {
    setTimeout(() => {
      syncService.run("backfill").catch((error) => {
        console.error("[bootstrap-backfill]", error.message, error.body || "");
      });
    }, config.sync.bootstrapBackfillDelayMs);

    setInterval(() => {
      if (!syncService.getBackfillStatus().pending) {
        return;
      }

      syncService.run("backfill").catch((error) => {
        console.error("[scheduled-backfill]", error.message, error.body || "");
      });
    }, config.sync.backfillIntervalMs);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
