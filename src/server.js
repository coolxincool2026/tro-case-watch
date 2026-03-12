import fs from "node:fs";
import path from "node:path";
import http from "node:http";
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

const store = new Store(config.dbPath);
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

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store"
  });
  response.end(JSON.stringify(payload));
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

  if (/pacer document/i.test(type)) {
    return "Docket Document";
  }

  return type.replace(/worldtro/gi, "Docket");
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
    relaxedCategory: payload.categoryRelaxed ? "tro" : null,
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
        seller_cases: Number(dashboard.totals?.seller_cases || 0)
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

function findRelaxedPayload(store, filters) {
  for (const category of ["seller_watch", "all"]) {
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
      category: "tro",
      search: searchParams.get("search") || "",
      court: searchParams.get("court") || "",
      page: Number(searchParams.get("page") || 1),
      pageSize: Number(searchParams.get("pageSize") || 25)
    };

    let payload = store.listCases(filters);
    const isDirectDocketLookup = docketLooksLike(filters.search);

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

    if (item.insights?.is_seller_case && (item.entries?.length || 0) < 12) {
      try {
        const result = await syncService.enrichCaseWithWorldtro(caseId);
        if (result.enriched) {
          item = store.getCase(caseId);
        }
      } catch {}
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
    const url = new URL(request.url, `http://${request.headers.host}`);
    if (url.pathname.startsWith("/api/")) {
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
    syncService.run("recent").catch((error) => {
      console.error("[bootstrap-sync]", error.message, error.body || "");
    });
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
    }, 20_000);

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
