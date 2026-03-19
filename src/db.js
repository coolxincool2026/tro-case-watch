import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { deriveCaseInsights, normalizeDocket, normalizeText } from "./insights.js";

function nowIso() {
  return new Date().toISOString();
}

function parseJson(value, fallback) {
  if (!value) {
    return fallback;
  }

  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function toJson(value, fallback = null) {
  if (value === undefined) {
    return fallback;
  }

  return JSON.stringify(value ?? null);
}

function normalizeSourceUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return raw;
  }

  try {
    const url = new URL(raw);
    if (
      url.hostname.includes("worldtro.com") ||
      url.hostname.includes("pacermonitor.com") ||
      url.hostname.includes("sriplaw.com") ||
      url.hostname.includes("gbc.law") ||
      url.hostname.includes("whitewoodlaw.com") ||
      url.hostname.includes("jiangip.com") ||
      url.hostname.includes("dropbox.com")
    ) {
      url.search = "";
      url.hash = "";
      return url.toString();
    }
  } catch {
    return raw;
  }

  return raw;
}

function looksLikeDocketSearch(value = "") {
  const text = String(value || "").trim();
  return /^\d{4,6}$/.test(text) || /\b\d{2}-cv-\d{3,6}\b/i.test(text) || /\b\d+:\d{2}-cv-\d{3,6}\b/i.test(text);
}

function compareIsoDesc(left, right) {
  return String(right || "").localeCompare(String(left || ""));
}

function normalizeLookupText(value) {
  return normalizeText(value).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
}

function buildCaseIdentityKeys(caseLike) {
  const docketKey = normalizeDocket(caseLike?.docket_number);
  if (!docketKey) {
    return [];
  }

  const keys = new Set();
  const courtIdKey = normalizeLookupText(caseLike?.court_id);
  const courtNameKey = normalizeLookupText(caseLike?.court_name);

  if (courtIdKey) {
    keys.add(`${courtIdKey}|${docketKey}`);
  }

  if (courtNameKey) {
    keys.add(`${courtNameKey}|${docketKey}`);
  }

  return [...keys];
}

const shanghaiDateFormatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Asia/Shanghai",
  year: "numeric",
  month: "2-digit",
  day: "2-digit"
});

function formatShanghaiDateKey(value) {
  if (!value) {
    return "";
  }

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return shanghaiDateFormatter.format(date);
}

function buildCaseView(row) {
  const hydrated = hydrateCase(row);
  const insights = deriveCaseInsights(hydrated);
  return {
    ...hydrated,
    insights,
    _created_date_shanghai: formatShanghaiDateKey(hydrated.created_at),
    _docket_raw: normalizeText(hydrated.docket_number),
    _docket_normalized: normalizeDocket(hydrated.docket_number),
    _search_blob: normalizeText([
      hydrated.case_name,
      hydrated.case_name_zh,
      hydrated.docket_number,
      normalizeDocket(hydrated.docket_number),
      hydrated.court_name,
      hydrated.recent_activity_summary,
      hydrated.recent_activity_summary_zh,
      insights?.brand_name,
      insights?.lead_law_firm,
      ...(hydrated.plaintiffs || []),
      ...(hydrated.defendants || [])
    ].join(" | ")),
    _label_blob: normalizeText([
      hydrated.case_name,
      insights?.brand_name,
      insights?.lead_law_firm,
      ...(hydrated.plaintiffs || []),
      ...(hydrated.defendants || [])
    ].join(" | "))
  };
}

function hydrateCase(row) {
  if (!row) {
    return null;
  }

  const marker = String(row.tags_marker || "");

  return {
    ...row,
    source_urls: parseJson(row.source_urls_json, []),
    plaintiffs: parseJson(row.plaintiffs_json, []),
    defendants: parseJson(row.defendants_json, []),
    raw: parseJson(row.raw_json, {}),
    tags: marker
      .split("|")
      .map((item) => item.trim())
      .filter(Boolean)
  };
}

function hydrateEntry(row) {
  if (!row) {
    return null;
  }

  return {
    ...row,
    raw: parseJson(row.raw_json, {})
  };
}

function dedupeEntries(entries) {
  const deduped = new Map();

  for (const entry of entries) {
    const orderKey = normalizedEntryOrderKey(entry);
    const fallbackKey = normalizeText(
      [entry.filed_at, String(entry.description || "").replace(/[^\w]+/g, " ").trim().slice(0, 240)].join("|")
    );
    const key = orderKey ? `order:${orderKey}` : fallbackKey;

    if (!key) {
      continue;
    }

    const existing = deduped.get(key);
    if (!existing || compareEntriesForCanonicalRow(entry, existing) < 0) {
      deduped.set(key, entry);
    }
  }

  return [...deduped.values()].sort(compareEntriesForTimeline);
}

function normalizeOrderText(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }

  return text.replace(/,+/g, "").replace(/\.0+$/, "");
}

function normalizedEntryOrderKey(entry) {
  return normalizeOrderText(entry.document_number) || normalizeOrderText(entry.entry_number) || "";
}

function parseEntryOrderValue(entry) {
  const candidates = [entry.document_number, entry.entry_number];

  for (const rawValue of candidates) {
    const text = normalizeOrderText(rawValue);
    if (!text) {
      continue;
    }

    const numeric = Number.parseFloat(text);
    if (Number.isFinite(numeric)) {
      return numeric;
    }
  }

  return null;
}

function entrySourceRank(entry) {
  if (entry.primary_source === "pacermonitor") {
    return 3;
  }

  if (entry.primary_source === "sriplaw") {
    return 3;
  }

  if (entry.primary_source === "worldtro") {
    return 2;
  }

  if (entry.primary_source === "gbc") {
    return 2;
  }

  if (entry.primary_source === "courtlistener") {
    return 1;
  }

  return 0;
}

function entryContentRank(entry) {
  const type = normalizeText(entry.document_type);

  if (type.includes("docket entry")) {
    return 3;
  }

  if (type.includes("docket document")) {
    return 2;
  }

  if (type.includes("entry")) {
    return 1;
  }

  return 0;
}

function compareEntriesForCanonicalRow(left, right) {
  const contentCompare = entryContentRank(right) - entryContentRank(left);
  if (contentCompare !== 0) {
    return contentCompare;
  }

  const descriptionCompare = String(right.description || "").length - String(left.description || "").length;
  if (descriptionCompare !== 0) {
    return descriptionCompare;
  }

  const sourceCompare = entrySourceRank(right) - entrySourceRank(left);
  if (sourceCompare !== 0) {
    return sourceCompare;
  }

  const dateCompare = String(right.filed_at || right.created_at).localeCompare(String(left.filed_at || left.created_at));
  if (dateCompare !== 0) {
    return dateCompare;
  }

  return Number(right.id || 0) - Number(left.id || 0);
}

function compareEntriesForTimeline(left, right) {
  const dateCompare = String(right.filed_at || right.created_at).localeCompare(String(left.filed_at || left.created_at));
  if (dateCompare !== 0) {
    return dateCompare;
  }

  const leftOrder = parseEntryOrderValue(left);
  const rightOrder = parseEntryOrderValue(right);
  if (leftOrder !== null || rightOrder !== null) {
    const numericCompare = (rightOrder ?? -1) - (leftOrder ?? -1);
    if (numericCompare !== 0) {
      return numericCompare;
    }
  }

  const sourceCompare = entrySourceRank(right) - entrySourceRank(left);
  if (sourceCompare !== 0) {
    return sourceCompare;
  }

  return Number(right.id || 0) - Number(left.id || 0);
}

function compareCaseActivityDesc(left, right) {
  return String(right || "").localeCompare(String(left || ""));
}

function shanghaiDayBounds(date = new Date()) {
  const key = formatShanghaiDateKey(date);
  if (!key) {
    return { startIso: "", endIso: "" };
  }

  const startDate = new Date(`${key}T00:00:00+08:00`);
  const endDate = new Date(startDate.getTime() + 24 * 60 * 60 * 1000);
  return {
    startIso: startDate.toISOString(),
    endIso: endDate.toISOString()
  };
}

function cloneListPayload(payload = {}) {
  return {
    ...payload,
    items: Array.isArray(payload.items) ? payload.items.map((item) => ({ ...item })) : [],
    courts: Array.isArray(payload.courts) ? payload.courts.map((court) => ({ ...court })) : []
  };
}

export class Store {
  constructor(dbPath) {
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.caseCacheVersion = 0;
    this.caseViewCache = new Map();
    this.caseDetailCache = new Map();
    this.listPayloadCache = new Map();
    this.dashboardStatsCache = null;
    this.caseIdentityCache = null;
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE IF NOT EXISTS cases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_case_key TEXT NOT NULL UNIQUE,
        primary_source TEXT NOT NULL,
        source_case_id TEXT NOT NULL,
        courtlistener_docket_id INTEGER,
        pacer_case_id TEXT,
        court_id TEXT,
        court_name TEXT,
        case_name TEXT,
        case_name_zh TEXT,
        docket_number TEXT,
        date_filed TEXT,
        date_terminated TEXT,
        cause TEXT,
        nature_of_suit TEXT,
        status TEXT,
        tags_marker TEXT,
        docket_url TEXT,
        source_urls_json TEXT,
        plaintiffs_json TEXT,
        defendants_json TEXT,
        recent_activity_summary TEXT,
        recent_activity_summary_zh TEXT,
        latest_docket_filed_at TEXT,
        latest_docket_number TEXT,
        docket_count INTEGER DEFAULT 0,
        last_seen_at TEXT,
        last_synced_at TEXT,
        last_docket_sync_at TEXT,
        last_translation_at TEXT,
        raw_json TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS docket_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id INTEGER NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
        source_entry_key TEXT NOT NULL UNIQUE,
        primary_source TEXT NOT NULL,
        source_entry_id TEXT,
        document_type TEXT,
        entry_number TEXT,
        document_number TEXT,
        filed_at TEXT,
        description TEXT,
        description_zh TEXT,
        absolute_url TEXT,
        is_available INTEGER,
        page_count INTEGER,
        pacer_doc_id TEXT,
        raw_json TEXT,
        last_synced_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS sync_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider TEXT NOT NULL,
        mode TEXT NOT NULL,
        status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        stats_json TEXT,
        error_text TEXT
      );

      CREATE TABLE IF NOT EXISTS checkpoints (
        checkpoint_key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS translation_cache (
        cache_key TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        source_text TEXT NOT NULL,
        translated_text TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS provider_usage (
        provider TEXT NOT NULL,
        usage_day TEXT NOT NULL,
        request_count INTEGER NOT NULL DEFAULT 0,
        estimated_cost_usd REAL NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (provider, usage_day)
      );

      CREATE INDEX IF NOT EXISTS idx_cases_docket_number ON cases(docket_number);
      CREATE INDEX IF NOT EXISTS idx_cases_date_filed ON cases(date_filed);
      CREATE INDEX IF NOT EXISTS idx_docket_entries_case_id ON docket_entries(case_id);
    `);
  }

  invalidateCaseViews() {
    this.caseCacheVersion += 1;
    this.caseViewCache.clear();
    this.caseDetailCache.clear();
    this.listPayloadCache.clear();
    this.dashboardStatsCache = null;
    this.caseIdentityCache = null;
  }

  getCaseIdentityIndex(startDate = "2025-01-01") {
    const cacheKey = String(startDate || "2025-01-01");
    if (this.caseIdentityCache?.version === this.caseCacheVersion && this.caseIdentityCache?.cacheKey === cacheKey) {
      return this.caseIdentityCache.index;
    }

    const index = new Map();
    for (const row of this.getHydratedCases(cacheKey)) {
      for (const key of buildCaseIdentityKeys(row)) {
        if (!index.has(key)) {
          index.set(key, row);
        }
      }
    }

    this.caseIdentityCache = {
      version: this.caseCacheVersion,
      cacheKey,
      index
    };

    return index;
  }

  findCaseByCourtAndDocket({ courtId = "", courtName = "", docketNumber = "", startDate = "2025-01-01" } = {}) {
    const docketKey = normalizeDocket(docketNumber);
    if (!docketKey) {
      return null;
    }

    const index = this.getCaseIdentityIndex(startDate);
    const candidateKeys = [
      `${normalizeLookupText(courtId)}|${docketKey}`,
      `${normalizeLookupText(courtName)}|${docketKey}`
    ].filter((value) => !value.startsWith("|"));

    for (const key of candidateKeys) {
      const row = index.get(key);
      if (row) {
        return row;
      }
    }

    return null;
  }

  findCaseByDocketNumber(docketNumber, startDate = "2025-01-01") {
    const docketKey = normalizeDocket(docketNumber);
    if (!docketKey) {
      return null;
    }

    const matches = this.getHydratedCases(startDate).filter((row) => normalizeDocket(row.docket_number) === docketKey);
    if (matches.length !== 1) {
      return null;
    }

    return matches[0];
  }

  getHydratedCases(startDate = "2025-01-01") {
    const cacheKey = String(startDate || "2025-01-01");
    const cached = this.caseViewCache.get(cacheKey);
    if (cached && cached.version === this.caseCacheVersion) {
      return cached.rows;
    }

    const rows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date(date_filed) >= date(?)
        ORDER BY COALESCE(latest_docket_filed_at, date_filed, updated_at) DESC, updated_at DESC
      `)
      .all(cacheKey)
      .map(buildCaseView);

    this.caseViewCache.set(cacheKey, {
      version: this.caseCacheVersion,
      rows
    });

    return rows;
  }

  upsertCase(record) {
    const existing = this.db
      .prepare(
        "SELECT id, created_at, tags_marker, source_urls_json, plaintiffs_json, defendants_json FROM cases WHERE source_case_key = ?"
      )
      .get(record.source_case_key);

    const mergedTags = this.mergeTagMarkers(existing?.tags_marker, record.tags_marker);
    const mergedUrls = this.mergeJsonArrays(existing?.source_urls_json, record.source_urls);
    const mergedPlaintiffs = this.mergeJsonArrays(existing?.plaintiffs_json, record.plaintiffs);
    const mergedDefendants = this.mergeJsonArrays(existing?.defendants_json, record.defendants);
    const timestamp = nowIso();

    this.db
      .prepare(`
        INSERT INTO cases (
          source_case_key,
          primary_source,
          source_case_id,
          courtlistener_docket_id,
          pacer_case_id,
          court_id,
          court_name,
          case_name,
          case_name_zh,
          docket_number,
          date_filed,
          date_terminated,
          cause,
          nature_of_suit,
          status,
          tags_marker,
          docket_url,
          source_urls_json,
          plaintiffs_json,
          defendants_json,
          recent_activity_summary,
          recent_activity_summary_zh,
          latest_docket_filed_at,
          latest_docket_number,
          docket_count,
          last_seen_at,
          last_synced_at,
          last_docket_sync_at,
          last_translation_at,
          raw_json,
          created_at,
          updated_at
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(source_case_key) DO UPDATE SET
          primary_source = excluded.primary_source,
          source_case_id = excluded.source_case_id,
          courtlistener_docket_id = COALESCE(excluded.courtlistener_docket_id, cases.courtlistener_docket_id),
          pacer_case_id = COALESCE(excluded.pacer_case_id, cases.pacer_case_id),
          court_id = COALESCE(excluded.court_id, cases.court_id),
          court_name = COALESCE(excluded.court_name, cases.court_name),
          case_name = COALESCE(excluded.case_name, cases.case_name),
          docket_number = COALESCE(excluded.docket_number, cases.docket_number),
          date_filed = COALESCE(excluded.date_filed, cases.date_filed),
          date_terminated = COALESCE(excluded.date_terminated, cases.date_terminated),
          cause = COALESCE(excluded.cause, cases.cause),
          nature_of_suit = COALESCE(excluded.nature_of_suit, cases.nature_of_suit),
          status = COALESCE(excluded.status, cases.status),
          tags_marker = excluded.tags_marker,
          docket_url = COALESCE(excluded.docket_url, cases.docket_url),
          source_urls_json = excluded.source_urls_json,
          plaintiffs_json = excluded.plaintiffs_json,
          defendants_json = excluded.defendants_json,
          recent_activity_summary = COALESCE(excluded.recent_activity_summary, cases.recent_activity_summary),
          recent_activity_summary_zh = CASE
            WHEN excluded.recent_activity_summary IS NOT NULL AND excluded.recent_activity_summary <> cases.recent_activity_summary THEN NULL
            ELSE cases.recent_activity_summary_zh
          END,
          latest_docket_filed_at = COALESCE(excluded.latest_docket_filed_at, cases.latest_docket_filed_at),
          latest_docket_number = COALESCE(excluded.latest_docket_number, cases.latest_docket_number),
          docket_count = CASE
            WHEN excluded.docket_count > cases.docket_count THEN excluded.docket_count
            ELSE cases.docket_count
          END,
          last_seen_at = excluded.last_seen_at,
          last_synced_at = excluded.last_synced_at,
          last_docket_sync_at = COALESCE(excluded.last_docket_sync_at, cases.last_docket_sync_at),
          raw_json = excluded.raw_json,
          updated_at = excluded.updated_at
      `)
      .run(
        record.source_case_key,
        record.primary_source,
        record.source_case_id,
        record.courtlistener_docket_id ?? null,
        record.pacer_case_id ?? null,
        record.court_id ?? null,
        record.court_name ?? null,
        record.case_name ?? null,
        record.case_name_zh ?? null,
        record.docket_number ?? null,
        record.date_filed ?? null,
        record.date_terminated ?? null,
        record.cause ?? null,
        record.nature_of_suit ?? null,
        record.status ?? null,
        mergedTags,
        record.docket_url ?? null,
        toJson(mergedUrls, "[]"),
        toJson(mergedPlaintiffs, "[]"),
        toJson(mergedDefendants, "[]"),
        record.recent_activity_summary ?? null,
        record.recent_activity_summary_zh ?? null,
        record.latest_docket_filed_at ?? null,
        record.latest_docket_number ?? null,
        record.docket_count ?? 0,
        record.last_seen_at ?? timestamp,
        record.last_synced_at ?? timestamp,
        record.last_docket_sync_at ?? null,
        record.last_translation_at ?? null,
        toJson(record.raw, "{}"),
        existing?.created_at ?? timestamp,
        timestamp
      );

    this.invalidateCaseViews();

    return hydrateCase(
      this.db.prepare("SELECT * FROM cases WHERE source_case_key = ?").get(record.source_case_key)
    );
  }

  updateCaseTranslations(caseId, fields) {
    const allowed = ["case_name_zh", "recent_activity_summary_zh"];
    const keys = Object.keys(fields).filter((field) => allowed.includes(field));
    if (!keys.length) {
      return;
    }

    const sets = keys.map((field) => `${field} = ?`);
    const values = keys.map((field) => fields[field]);
    sets.push("last_translation_at = ?", "updated_at = ?");
    values.push(nowIso(), nowIso(), caseId);

    this.db.prepare(`UPDATE cases SET ${sets.join(", ")} WHERE id = ?`).run(...values);
    this.invalidateCaseViews();
  }

  touchCaseDocketSync(caseId) {
    this.db
      .prepare("UPDATE cases SET last_docket_sync_at = ?, updated_at = ? WHERE id = ?")
      .run(nowIso(), nowIso(), caseId);
    this.invalidateCaseViews();
  }

  upsertDocketEntry(record) {
    const existing = this.db
      .prepare("SELECT id, description FROM docket_entries WHERE source_entry_key = ?")
      .get(record.source_entry_key);

    const timestamp = nowIso();

    this.db
      .prepare(`
        INSERT INTO docket_entries (
          case_id,
          source_entry_key,
          primary_source,
          source_entry_id,
          document_type,
          entry_number,
          document_number,
          filed_at,
          description,
          description_zh,
          absolute_url,
          is_available,
          page_count,
          pacer_doc_id,
          raw_json,
          last_synced_at,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_entry_key) DO UPDATE SET
          case_id = excluded.case_id,
          primary_source = excluded.primary_source,
          source_entry_id = COALESCE(excluded.source_entry_id, docket_entries.source_entry_id),
          document_type = COALESCE(excluded.document_type, docket_entries.document_type),
          entry_number = COALESCE(excluded.entry_number, docket_entries.entry_number),
          document_number = COALESCE(excluded.document_number, docket_entries.document_number),
          filed_at = COALESCE(excluded.filed_at, docket_entries.filed_at),
          description = COALESCE(excluded.description, docket_entries.description),
          description_zh = CASE
            WHEN excluded.description IS NOT NULL AND excluded.description <> docket_entries.description THEN NULL
            ELSE docket_entries.description_zh
          END,
          absolute_url = COALESCE(excluded.absolute_url, docket_entries.absolute_url),
          is_available = COALESCE(excluded.is_available, docket_entries.is_available),
          page_count = COALESCE(excluded.page_count, docket_entries.page_count),
          pacer_doc_id = COALESCE(excluded.pacer_doc_id, docket_entries.pacer_doc_id),
          raw_json = excluded.raw_json,
          last_synced_at = excluded.last_synced_at,
          updated_at = excluded.updated_at
      `)
      .run(
        record.case_id,
        record.source_entry_key,
        record.primary_source,
        record.source_entry_id ?? null,
        record.document_type ?? null,
        record.entry_number ?? null,
        record.document_number ?? null,
        record.filed_at ?? null,
        record.description ?? null,
        record.description_zh ?? null,
        record.absolute_url ?? null,
        record.is_available ?? null,
        record.page_count ?? null,
        record.pacer_doc_id ?? null,
        toJson(record.raw, "{}"),
        record.last_synced_at ?? timestamp,
        existing ? existing.created_at ?? timestamp : timestamp,
        timestamp
      );

    this.invalidateCaseViews();

    return hydrateEntry(
      this.db.prepare("SELECT * FROM docket_entries WHERE source_entry_key = ?").get(record.source_entry_key)
    );
  }

  updateEntryTranslation(entryId, translation) {
    this.db
      .prepare("UPDATE docket_entries SET description_zh = ?, updated_at = ? WHERE id = ?")
      .run(translation, nowIso(), entryId);
    this.invalidateCaseViews();
  }

  getFastPathDocketCases(startDate, rawSearch) {
    const rawNeedle = normalizeText(rawSearch || "");
    const docketNeedle = normalizeDocket(rawSearch);
    const numericSuffix = String(rawSearch || "").match(/(\d{4,6})$/)?.[1] || "";
    const exactNeedles = [...new Set([rawNeedle, docketNeedle].filter(Boolean))];
    const suffixNeedles = [...new Set([rawNeedle, docketNeedle, numericSuffix].filter(Boolean))];
    const clauses = [];
    const params = [startDate];

    for (const needle of exactNeedles) {
      clauses.push("lower(docket_number) = ?");
      params.push(needle);
    }

    for (const needle of suffixNeedles) {
      clauses.push("lower(docket_number) LIKE ?");
      params.push(`%${needle}`);
    }

    if (!clauses.length) {
      return [];
    }

    return this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date(date_filed) >= date(?)
          AND (${clauses.join(" OR ")})
        ORDER BY COALESCE(latest_docket_filed_at, date_filed, updated_at) DESC, updated_at DESC
        LIMIT 250
      `)
      .all(...params)
      .map(buildCaseView);
  }

  buildCategoryWhereClause(category) {
    if (category === "watchlist") {
      return "(tags_marker LIKE '%|tro|%' OR tags_marker LIKE '%|schedule_a|%' OR tags_marker LIKE '%|seller_tro|%')";
    }

    if (category === "tro") {
      return "tags_marker LIKE '%|tro|%'";
    }

    if (category === "schedule_a") {
      return "tags_marker LIKE '%|schedule_a|%'";
    }

    if (category === "seller_watch") {
      return "tags_marker LIKE '%|seller_tro|%'";
    }

    return "1 = 1";
  }

  listCasesBySql({ startDate, pageSize, page, category, selectedCourt }) {
    const categoryClause = this.buildCategoryWhereClause(category);
    const baseWhere = [`date(date_filed) >= date(?)`, `(${categoryClause})`];
    const baseParams = [startDate];

    if (selectedCourt) {
      baseWhere.push(`court_id = ?`);
      baseParams.push(selectedCourt);
    }

    const whereSql = baseWhere.join(" AND ");
    const total = Number(
      this.db.prepare(`SELECT COUNT(*) AS total FROM cases WHERE ${whereSql}`).get(...baseParams)?.total || 0
    );
    const offset = (page - 1) * pageSize;
    const items = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE ${whereSql}
        ORDER BY COALESCE(latest_docket_filed_at, date_filed, updated_at) DESC, updated_at DESC
        LIMIT ?
        OFFSET ?
      `)
      .all(...baseParams, pageSize, offset)
      .map(buildCaseView);
    const courts = this.db
      .prepare(`
        SELECT court_id, court_name, COUNT(*) AS total
        FROM cases
        WHERE date(date_filed) >= date(?)
          AND (${categoryClause})
        GROUP BY court_id, court_name
        ORDER BY total DESC, court_name ASC
      `)
      .all(startDate)
      .map((row) => ({
        court_id: row.court_id,
        court_name: row.court_name,
        total: Number(row.total || 0)
      }));

    return {
      items,
      total,
      page,
      pageSize,
      pageCount: Math.max(1, Math.ceil(total / pageSize)),
      courts
    };
  }

  listCases(filters = {}) {
    const startDate = filters.startDate || "2025-01-01";
    const pageSize = Math.min(Number(filters.pageSize || 25), 100);
    const page = Math.max(Number(filters.page || 1), 1);
    const category = filters.category || "seller_watch";
    const rawSearch = String(filters.search || "").trim();
    const searchTerm = normalizeText(rawSearch);
    const selectedCourt = String(filters.court || "");
    const cacheKey = JSON.stringify({
      startDate,
      pageSize,
      page,
      category,
      searchTerm,
      selectedCourt
    });

    const cached = this.listPayloadCache.get(cacheKey);
    if (cached && cached.version === this.caseCacheVersion) {
      return cloneListPayload(cached.value);
    }

    if (!searchTerm) {
      const payload = this.listCasesBySql({
        startDate,
        pageSize,
        page,
        category,
        selectedCourt
      });

      this.listPayloadCache.set(cacheKey, {
        version: this.caseCacheVersion,
        value: payload
      });

      return cloneListPayload(payload);
    }

    const rows = looksLikeDocketSearch(rawSearch)
      ? this.getFastPathDocketCases(startDate, rawSearch)
      : this.getHydratedCases(startDate);

    const categoryFiltered = rows.filter((row) => this.matchesCategory(row, category));
    const searchFiltered = searchTerm
      ? categoryFiltered
          .filter((row) => this.matchesSearch(row, searchTerm))
          .sort((left, right) => this.compareSearchPriority(left, right, searchTerm))
      : categoryFiltered;

    const courtsMap = new Map();
    for (const row of searchFiltered) {
      const key = `${row.court_id || ""}|${row.court_name || ""}`;
      if (!courtsMap.has(key)) {
        courtsMap.set(key, {
          court_id: row.court_id,
          court_name: row.court_name,
          total: 0
        });
      }
      courtsMap.get(key).total += 1;
    }

    const courtFiltered = selectedCourt
      ? searchFiltered.filter((row) => row.court_id === selectedCourt)
      : searchFiltered;

    const total = courtFiltered.length;
    const offset = (page - 1) * pageSize;
    const items = courtFiltered.slice(offset, offset + pageSize);
    const courts = [...courtsMap.values()].sort(
      (left, right) => right.total - left.total || String(left.court_name).localeCompare(String(right.court_name))
    );

    const payload = {
      items,
      total,
      page,
      pageSize,
      pageCount: Math.max(1, Math.ceil(total / pageSize)),
      courts
    };

    this.listPayloadCache.set(cacheKey, {
      version: this.caseCacheVersion,
      value: payload
    });

    return cloneListPayload(payload);
  }

  getCase(id) {
    const cacheKey = Number(id);
    const cached = this.caseDetailCache.get(cacheKey);
    if (cached && cached.version === this.caseCacheVersion) {
      return cached.value;
    }

    const row = hydrateCase(this.db.prepare("SELECT * FROM cases WHERE id = ?").get(id));
    if (!row) {
      return null;
    }

    const entries = this.db
      .prepare(`
        SELECT *
        FROM docket_entries
        WHERE case_id = ?
        ORDER BY COALESCE(filed_at, created_at) DESC, id DESC
      `)
      .all(id)
      .map(hydrateEntry);

    const uniqueEntries = dedupeEntries(entries);

    const detail = {
      ...row,
      entries: uniqueEntries,
      insights: deriveCaseInsights({
        ...row,
        entries: uniqueEntries
      })
    };

    this.caseDetailCache.set(cacheKey, {
      version: this.caseCacheVersion,
      value: detail
    });

    return detail;
  }

  getCasesNeedingDocketSync(limit) {
    return this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE courtlistener_docket_id IS NOT NULL
          AND date(date_filed) >= date('2025-01-01')
        ORDER BY COALESCE(last_docket_sync_at, '1970-01-01T00:00:00.000Z') ASC,
                 COALESCE(latest_docket_filed_at, date_filed, updated_at) DESC
        LIMIT ?
      `)
      .all(limit)
      .map(hydrateCase);
  }

  getEntryCoverageForCaseIds(caseIds = []) {
    const ids = [...new Set(caseIds.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0))];
    if (!ids.length) {
      return new Map();
    }

    const chunkSize = 900;
    const coverage = new Map();

    for (let index = 0; index < ids.length; index += chunkSize) {
      const chunk = ids.slice(index, index + chunkSize);
      const placeholders = chunk.map(() => "?").join(", ");
      const rows = this.db
        .prepare(`
          SELECT
            case_id,
            COUNT(*) AS total_entries,
            SUM(CASE WHEN primary_source = 'worldtro' THEN 1 ELSE 0 END) AS worldtro_entries,
            SUM(CASE WHEN primary_source = 'pacermonitor' THEN 1 ELSE 0 END) AS pacermonitor_entries
          FROM docket_entries
          WHERE case_id IN (${placeholders})
          GROUP BY case_id
        `)
        .all(...chunk);

      for (const row of rows) {
        coverage.set(Number(row.case_id), {
          totalEntries: Number(row.total_entries || 0),
          worldtroEntries: Number(row.worldtro_entries || 0),
          pacermonitorEntries: Number(row.pacermonitor_entries || 0)
        });
      }
    }

    return coverage;
  }

  getCasesNeedingWorldtroSync(limit, staleAfterHours = 12) {
    const staleBefore = Date.now() - staleAfterHours * 60 * 60 * 1000;
    const poolSize = Math.max(limit * 12, 180);
    const fetchCandidateRows = (whereSql, params = [], orderBySql, queryLimit = poolSize) =>
      this.db
        .prepare(`
          SELECT *
          FROM cases
          WHERE date(date_filed) >= date(?)
            AND tags_marker LIKE '%|seller_tro|%'
            AND ${whereSql}
          ORDER BY ${orderBySql}
          LIMIT ?
        `)
        .all("2025-01-01", ...params, queryLimit)
        .map(hydrateCase);

    const recentRows = fetchCandidateRows(
      `date(COALESCE(latest_docket_filed_at, date_filed, updated_at)) >= date('now', '-45 day')`,
      [],
      `COALESCE(latest_docket_filed_at, date_filed, updated_at) DESC, updated_at DESC`,
      Math.max(poolSize, limit * 8)
    );
    const knownWorldtroRows = fetchCandidateRows(
      `(source_urls_json LIKE '%worldtro.com%' OR raw_json LIKE '%"worldtro"%')`,
      [],
      `COALESCE(latest_docket_filed_at, date_filed, updated_at) ASC, id ASC`,
      Math.max(limit * 8, 120)
    );
    const sparseRows = fetchCandidateRows(
      `docket_count <= ?`,
      [4],
      `COALESCE(latest_docket_filed_at, date_filed, updated_at) DESC, updated_at DESC`,
      Math.max(limit * 8, 120)
    );

    const candidateRows = [];
    const seenCandidateIds = new Set();
    for (const row of [...recentRows, ...knownWorldtroRows, ...sparseRows]) {
      if (seenCandidateIds.has(row.id)) {
        continue;
      }

      seenCandidateIds.add(row.id);
      candidateRows.push(row);
    }

    const entryCounts = this.getEntryCoverageForCaseIds(candidateRows.map((row) => row.id));

    const rows = candidateRows
      .map((row) => {
        const coverage = entryCounts.get(Number(row.id)) || {
          totalEntries: 0,
          worldtroEntries: 0
        };
        const hasCivilDocketNumber = /\b\d{2}-cv-\d{3,6}\b/i.test(String(row.docket_number || ""));
        const syncedAt = row.raw?.worldtro?.syncedAt ? Date.parse(row.raw.worldtro.syncedAt) : 0;
        const worldtroRowCount = Number(row.raw?.worldtro?.rowCount || 0);
        const hasWorldtroUrl = row.source_urls?.some((url) => String(url).includes("worldtro.com"));
        const hasWorldtroEntries = worldtroRowCount > 0 || coverage.worldtroEntries > 0;
        const hasKnownWorldtroSource = hasWorldtroUrl || hasWorldtroEntries;
        const minimumExpectedEntries = Math.max(12, Number(row.docket_count || 0), 6);
        const missingMarked = Boolean(row.raw?.worldtro?.missing);
        const isFreshlyMissing = missingMarked && syncedAt && syncedAt >= staleBefore;

        const needsCompletion = worldtroRowCount > 0
          ? coverage.totalEntries < worldtroRowCount
          : hasWorldtroUrl
            ? coverage.worldtroEntries === 0 || coverage.totalEntries < minimumExpectedEntries
            : !hasKnownWorldtroSource && coverage.totalEntries < minimumExpectedEntries;
        const isStale = !syncedAt || syncedAt < staleBefore;
        const shouldSync = hasCivilDocketNumber && (
          needsCompletion ||
          (!hasKnownWorldtroSource && !isFreshlyMissing) ||
          (hasKnownWorldtroSource && isStale)
        );
        const activityAtRaw = row.latest_docket_filed_at || row.date_filed || row.updated_at;

        const priority = needsCompletion
          ? hasWorldtroUrl
            ? 0
            : 1
          : !hasKnownWorldtroSource
            ? 2
            : 3;

        return {
          row,
          priority,
          shouldSync,
          totalEntries: coverage.totalEntries,
          hasWorldtroCoverage: hasKnownWorldtroSource,
          hasWorldtroUrl,
          activityAtRaw
        };
      })
      .filter((item) => item.shouldSync);

    const recentOrdered = rows.slice().sort((left, right) => {
      if (left.priority !== right.priority) {
        return left.priority - right.priority;
      }

      const activityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
      if (activityCompare !== 0) {
        return activityCompare;
      }

      return Number(right.row.id || 0) - Number(left.row.id || 0);
    });

    const backlogOrdered = rows.slice().sort((left, right) => {
      if (left.priority !== right.priority) {
        return left.priority - right.priority;
      }

      if (left.hasWorldtroUrl !== right.hasWorldtroUrl) {
        return left.hasWorldtroUrl ? -1 : 1;
      }

      if (left.hasWorldtroCoverage !== right.hasWorldtroCoverage) {
        return left.hasWorldtroCoverage ? 1 : -1;
      }

      const olderActivityCompare = String(left.activityAtRaw || "").localeCompare(String(right.activityAtRaw || ""));
      if (olderActivityCompare !== 0) {
        return olderActivityCompare;
      }

      if (left.totalEntries !== right.totalEntries) {
        return left.totalEntries - right.totalEntries;
      }

      return Number(left.row.id || 0) - Number(right.row.id || 0);
    });

    const recentSlots = Math.max(1, Math.ceil(limit * 0.65));
    const selected = [];
    const seen = new Set();
    const appendRows = (items, maxItems = limit) => {
      for (const item of items) {
        if (selected.length >= maxItems || seen.has(item.row.id)) {
          continue;
        }

        seen.add(item.row.id);
        selected.push(item.row);
      }
    };

    appendRows(recentOrdered, recentSlots);
    appendRows(backlogOrdered, limit);
    appendRows(recentOrdered, limit);

    return selected.slice(0, limit);
  }

  getCasesNeedingPacerMonitorSync(
    limit,
    {
      staleAfterHours = 24,
      blockedRetryAfterHours = 12,
      notFoundRetryAfterHours = 6,
      recentWindowDays = 45
    } = {}
  ) {
    const staleBefore = Date.now() - staleAfterHours * 60 * 60 * 1000;
    const blockedBefore = Date.now() - blockedRetryAfterHours * 60 * 60 * 1000;
    const notFoundBefore = Date.now() - notFoundRetryAfterHours * 60 * 60 * 1000;
    const recentCutoff = Date.now() - recentWindowDays * 24 * 60 * 60 * 1000;
    const recentCutoffIso = new Date(recentCutoff).toISOString();
    const candidatePoolSize = Math.max(limit * 80, 240);

    const candidateRows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date(date_filed) >= date('2025-01-01')
          AND docket_number IS NOT NULL
          AND TRIM(docket_number) <> ''
          AND (
            tags_marker LIKE '%|seller_tro|%'
            OR COALESCE(latest_docket_filed_at, date_filed, updated_at) >= ?
          )
        ORDER BY COALESCE(latest_docket_filed_at, date_filed, updated_at) DESC
        LIMIT ?
      `)
      .all(recentCutoffIso, candidatePoolSize)
      .map(buildCaseView);
    const entryCounts = this.getEntryCoverageForCaseIds(candidateRows.map((row) => row.id));

    return candidateRows
      .map((row) => {
        const coverage = entryCounts.get(Number(row.id)) || {
          totalEntries: 0,
          worldtroEntries: 0,
          pacermonitorEntries: 0
        };
        const hasCivilDocketNumber = /\b\d{2}-cv-\d{3,6}\b/i.test(String(row.docket_number || ""));
        const activityAtRaw = row.latest_docket_filed_at || row.date_filed || row.updated_at;
        const activityAtMs = Number.isFinite(Date.parse(activityAtRaw || "")) ? Date.parse(activityAtRaw || "") : 0;
        const isRecentCase = activityAtMs >= recentCutoff;
        const worldtroRowCount = Number(row.raw?.worldtro?.rowCount || 0);
        const expectedEntries = Math.max(
          row.insights?.is_seller_case ? 12 : 8,
          isRecentCase ? 10 : 0,
          Number(row.docket_count || 0),
          worldtroRowCount
        );
        const gap = Math.max(0, expectedEntries - coverage.totalEntries);
        const syncedAt = row.raw?.pacermonitor?.syncedAt ? Date.parse(row.raw.pacermonitor.syncedAt) : 0;
        const state = String(row.raw?.pacermonitor?.state || "").toLowerCase();
        const isBlockedFresh =
          (state === "challenge" || state === "rate_limited") &&
          syncedAt &&
          syncedAt >= blockedBefore;
        const freshnessCutoff =
          state === "challenge" || state === "rate_limited"
            ? blockedBefore
            : state === "not_found"
              ? notFoundBefore
              : staleBefore;
        const isFresh = syncedAt && syncedAt >= freshnessCutoff;
        const needsWorldtroLevelCompletion =
          worldtroRowCount > 0 && coverage.totalEntries < worldtroRowCount;
        const needsBasicCompletion = coverage.totalEntries < expectedEntries;
        const shouldSync =
          hasCivilDocketNumber &&
          (row.insights?.is_seller_case || isRecentCase) &&
          (needsWorldtroLevelCompletion || needsBasicCompletion) &&
          !isBlockedFresh &&
          !isFresh;

        const priority = needsWorldtroLevelCompletion
          ? 0
          : row.insights?.is_seller_case
            ? 1
            : 2;

        return {
          row,
          priority,
          gap,
          activityAtRaw,
          totalEntries: coverage.totalEntries,
          shouldSync
        };
      })
      .filter((item) => item.shouldSync)
      .sort((left, right) => {
        if (left.priority !== right.priority) {
          return left.priority - right.priority;
        }

        if (left.gap !== right.gap) {
          return right.gap - left.gap;
        }

        const activityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
        if (activityCompare !== 0) {
          return activityCompare;
        }

        if (left.totalEntries !== right.totalEntries) {
          return left.totalEntries - right.totalEntries;
        }

        return Number(right.row.id || 0) - Number(left.row.id || 0);
      })
      .slice(0, limit)
      .map((item) => item.row);
  }

  getPendingCaseTranslations(limit) {
    return this.db
      .prepare(`
        SELECT id, case_name, recent_activity_summary, case_name_zh, recent_activity_summary_zh
        FROM cases
        WHERE (case_name IS NOT NULL AND TRIM(case_name) <> '' AND case_name_zh IS NULL)
           OR (recent_activity_summary IS NOT NULL AND TRIM(recent_activity_summary) <> '' AND recent_activity_summary_zh IS NULL)
        ORDER BY updated_at DESC
        LIMIT ?
      `)
      .all(limit);
  }

  getPendingEntryTranslations(limit) {
    return this.db
      .prepare(`
        SELECT id, description
        FROM docket_entries
        WHERE description IS NOT NULL
          AND TRIM(description) <> ''
          AND description_zh IS NULL
        ORDER BY COALESCE(filed_at, created_at) DESC
        LIMIT ?
      `)
      .all(limit);
  }

  getTranslation(cacheKey) {
    return this.db
      .prepare("SELECT translated_text FROM translation_cache WHERE cache_key = ?")
      .get(cacheKey)?.translated_text;
  }

  saveTranslation(cacheKey, provider, sourceText, translatedText) {
    this.db
      .prepare(`
        INSERT INTO translation_cache (cache_key, provider, source_text, translated_text, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
          provider = excluded.provider,
          source_text = excluded.source_text,
          translated_text = excluded.translated_text,
          updated_at = excluded.updated_at
      `)
      .run(cacheKey, provider, sourceText, translatedText, nowIso());
  }

  startSyncRun(provider, mode) {
    const result = this.db
      .prepare(`
        INSERT INTO sync_runs (provider, mode, status, started_at)
        VALUES (?, ?, ?, ?)
      `)
      .run(provider, mode, "running", nowIso());

    return Number(result.lastInsertRowid);
  }

  finishSyncRun(id, status, stats, errorText = null) {
    this.db
      .prepare(`
        UPDATE sync_runs
        SET status = ?, finished_at = ?, stats_json = ?, error_text = ?
        WHERE id = ?
      `)
      .run(status, nowIso(), toJson(stats, "{}"), errorText, id);
  }

  getRecentSyncRuns(limit = 10) {
    return this.db
      .prepare(`
        SELECT *
        FROM sync_runs
        ORDER BY started_at DESC
        LIMIT ?
      `)
      .all(limit)
      .map((row) => ({
        ...row,
        stats: parseJson(row.stats_json, {})
      }));
  }

  getCheckpoint(key) {
    const row = this.db.prepare("SELECT value_json FROM checkpoints WHERE checkpoint_key = ?").get(key);
    return row ? parseJson(row.value_json, {}) : null;
  }

  saveCheckpoint(key, value) {
    this.db
      .prepare(`
        INSERT INTO checkpoints (checkpoint_key, value_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(checkpoint_key) DO UPDATE SET
          value_json = excluded.value_json,
          updated_at = excluded.updated_at
      `)
      .run(key, toJson(value, "{}"), nowIso());
  }

  addProviderUsage(provider, requestCount, estimatedCostUsd) {
    const usageDay = new Date().toISOString().slice(0, 10);
    this.db
      .prepare(`
        INSERT INTO provider_usage (provider, usage_day, request_count, estimated_cost_usd, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(provider, usage_day) DO UPDATE SET
          request_count = provider_usage.request_count + excluded.request_count,
          estimated_cost_usd = provider_usage.estimated_cost_usd + excluded.estimated_cost_usd,
          updated_at = excluded.updated_at
      `)
      .run(provider, usageDay, requestCount, estimatedCostUsd, nowIso());
  }

  getProviderUsage(provider) {
    return (
      this.db
        .prepare("SELECT * FROM provider_usage WHERE provider = ? AND usage_day = ?")
        .get(provider, new Date().toISOString().slice(0, 10)) || {
        provider,
        usage_day: new Date().toISOString().slice(0, 10),
        request_count: 0,
        estimated_cost_usd: 0
      }
    );
  }

  getDashboardStats() {
    if (this.dashboardStatsCache && this.dashboardStatsCache.version === this.caseCacheVersion) {
      return this.dashboardStatsCache.value;
    }

    const todayBounds = shanghaiDayBounds(new Date());
    const totalsRow = this.db
      .prepare(`
        SELECT
          COUNT(*) AS total_cases,
          SUM(CASE WHEN tags_marker LIKE '%|tro|%' THEN 1 ELSE 0 END) AS tro_cases,
          SUM(CASE WHEN tags_marker LIKE '%|schedule_a|%' THEN 1 ELSE 0 END) AS schedule_a_cases,
          SUM(CASE WHEN tags_marker LIKE '%|seller_tro|%' THEN 1 ELSE 0 END) AS seller_cases,
          SUM(
            CASE
              WHEN created_at >= ? AND created_at < ?
               AND (
                 tags_marker LIKE '%|tro|%'
                 OR tags_marker LIKE '%|schedule_a|%'
                 OR tags_marker LIKE '%|seller_tro|%'
               )
              THEN 1
              ELSE 0
            END
          ) AS today_added_watchlist
        FROM cases
        WHERE date(date_filed) >= date(?)
      `)
      .get(todayBounds.startIso, todayBounds.endIso, "2025-01-01");
    const totals = {
      total_cases: Number(totalsRow?.total_cases || 0),
      tro_cases: Number(totalsRow?.tro_cases || 0),
      schedule_a_cases: Number(totalsRow?.schedule_a_cases || 0),
      seller_cases: Number(totalsRow?.seller_cases || 0),
      today_added_watchlist: Number(totalsRow?.today_added_watchlist || 0)
    };

    const latestCase = this.db
      .prepare(`
        SELECT updated_at, case_name, docket_number
        FROM cases
        ORDER BY updated_at DESC
        LIMIT 1
      `)
      .get();

    const recentSync = this.db
      .prepare(`
        SELECT *
        FROM sync_runs
        ORDER BY started_at DESC
        LIMIT 1
      `)
      .get();

    const value = {
      totals,
      latestCase,
      recentSync: recentSync
        ? {
            ...recentSync,
            stats: parseJson(recentSync.stats_json, {})
          }
        : null
    };

    this.dashboardStatsCache = {
      version: this.caseCacheVersion,
      value
    };

    return value;
  }

  mergeTagMarkers(existing, incoming) {
    const tags = new Set();

    for (const marker of [existing, incoming]) {
      String(marker || "")
        .split("|")
        .map((item) => item.trim())
        .filter(Boolean)
        .forEach((item) => tags.add(item));
    }

    const values = [...tags].sort();
    return values.length ? `|${values.join("|")}|` : "";
  }

  mergeJsonArrays(existingJson, incoming) {
    const merged = new Set(parseJson(existingJson, []).map(normalizeSourceUrl));
    for (const item of incoming || []) {
      const normalized = normalizeSourceUrl(item);
      if (normalized) {
        merged.add(normalized);
      }
    }
    return [...merged];
  }

  matchesCategory(row, category) {
    if (category === "all") {
      return true;
    }

    if (category === "watchlist") {
      return Boolean(row.insights?.is_tro_case || row.insights?.is_schedule_a_case || row.insights?.is_seller_case);
    }

    if (category === "tro") {
      return Boolean(row.insights?.is_tro_case);
    }

    if (category === "schedule_a") {
      return Boolean(row.insights?.is_schedule_a_case);
    }

    if (category === "seller_watch") {
      return Boolean(row.insights?.is_seller_case);
    }

    return true;
  }

  matchesSearch(row, searchTerm) {
    const docketNeedle = normalizeDocket(searchTerm);
    return row._search_blob.includes(searchTerm) || (docketNeedle && row._search_blob.includes(docketNeedle));
  }

  compareSearchPriority(left, right, searchTerm) {
    const priorityDiff = this.searchPriority(right, searchTerm) - this.searchPriority(left, searchTerm);
    if (priorityDiff !== 0) {
      return priorityDiff;
    }

    if (looksLikeDocketSearch(searchTerm)) {
      const filedAtDiff = compareIsoDesc(left.date_filed, right.date_filed);
      if (filedAtDiff !== 0) {
        return filedAtDiff;
      }
    }

    return (
      compareIsoDesc(
        left.latest_docket_filed_at || left.date_filed || left.updated_at,
        right.latest_docket_filed_at || right.date_filed || right.updated_at
      ) || compareIsoDesc(left.updated_at, right.updated_at)
    );
  }

  searchPriority(row, searchTerm) {
    const rawNeedle = String(searchTerm || "").trim();
    const normalizedNeedle = normalizeDocket(rawNeedle);
    const docketRaw = row._docket_raw;
    const docketNormalized = row._docket_normalized;
    const labelBlob = row._label_blob;

    let score = 0;

    if (rawNeedle && docketRaw === rawNeedle) {
      score += 120;
    } else if (rawNeedle && docketRaw.includes(rawNeedle)) {
      score += 55;
    }

    if (normalizedNeedle && docketNormalized === normalizedNeedle) {
      score += 140;
    } else if (normalizedNeedle && docketNormalized.endsWith(normalizedNeedle)) {
      score += 90;
    } else if (normalizedNeedle && docketNormalized.includes(normalizedNeedle)) {
      score += 45;
    }

    if (labelBlob.includes(searchTerm)) {
      score += 15;
    }

    if (row.insights?.is_seller_case) {
      score += 5;
    }

    return score;
  }
}
