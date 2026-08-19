import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { deriveCaseInsights, docketLooksLike, normalizeDocket, normalizeText } from "./insights.js";
import { buildTagsMarker } from "./queries.js";
import { SIGNAL_FEED_PROVIDER_KEY } from "./providers/signal-feed.js";
import {
  PRIORITY_FEED_ENTRY_SOURCE,
  PRIORITY_FEED_HOST,
  PRIORITY_FEED_LEGACY_RAW_KEY,
  PRIORITY_FEED_MODERN_RAW_KEY,
  PRIORITY_FEED_PROVIDER_KEY,
  caseHasPriorityFeedUrl,
  getPriorityFeedRaw,
  getPriorityFeedRowCount as getPriorityFeedMetadataRowCount,
  getPriorityFeedSyncedAt,
  isPriorityFeedMissing,
  isPriorityFeedPrimarySource,
  sourceUrlUsesPriorityFeed
} from "./priority-feed.js";

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

function toCount(value) {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function toTimestamp(value) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : null;
}

function getTableColumns(db, tableName) {
  return new Set(
    db
      .prepare(`PRAGMA table_info(${tableName})`)
      .all()
      .map((row) => String(row?.name || "").trim())
      .filter(Boolean)
  );
}

function ensureTableColumns(db, tableName, columnDefinitions = {}) {
  const existing = getTableColumns(db, tableName);
  for (const [columnName, definition] of Object.entries(columnDefinitions)) {
    if (!existing.has(columnName)) {
      db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
    }
  }
}

function isSqliteBusyError(error) {
  const message = String(error?.message || "").toLowerCase();
  return (
    error?.code === "ERR_SQLITE_ERROR" &&
    (Number(error?.errcode || 0) === 5 || message.includes("database is locked") || message.includes("database table is locked"))
  );
}

function sqliteStringLiteral(value) {
  return `'${String(value ?? "").replace(/'/g, "''")}'`;
}

const DASHBOARD_STATS_CACHE_TTL_MS = 3 * 60_000;
const RECENT_SYNC_CACHE_TTL_MS = 15_000;
const CASE_VIEW_CACHE_TTL_MS = 30_000;
const LIST_PAYLOAD_CACHE_TTL_MS = 15_000;
const CASE_DETAIL_CACHE_TTL_MS = 30_000;

function syncRunHeartbeatKey(id) {
  return `sync-run-heartbeat:${Number(id || 0)}`;
}

function summarizeSyncRunStats(stats = {}) {
  return {
    casesWritten:
      toCount(stats.casesUpserted) +
      toCount(stats.courtFeedCasesUpserted) +
      toCount(stats.recentFilingsCasesUpserted) +
      toCount(stats.pacerCasesUpserted) +
      toCount(stats.lawFirmCasesUpserted),
    docketEntriesWritten:
      toCount(stats.docketEntriesUpserted) +
      toCount(stats.courtFeedEntriesUpserted) +
      toCount(stats.lawFirmEntriesUpserted),
    lookupsTriggered:
      toCount(stats.courtFeedLookups) +
      toCount(stats.recentFilingsLookups) +
      toCount(stats.lawFirmLookups),
    priorityFeedCasesSynced: toCount(stats.priorityFeedCasesSynced),
    docketCasesSynced: toCount(stats.docketCasesSynced),
    docketAlarmCasesSynced: toCount(stats.docketAlarmCasesSynced),
    uniCourtCasesSynced: toCount(stats.uniCourtCasesSynced),
    translationsApplied: toCount(stats.translationsApplied)
  };
}

function emptySyncRunBreakdown(label) {
  return {
    label,
    runCount: 0,
    casesWritten: 0,
    docketEntriesWritten: 0,
    lookupsTriggered: 0,
    priorityFeedCasesSynced: 0,
    docketCasesSynced: 0,
    docketAlarmCasesSynced: 0,
    uniCourtCasesSynced: 0,
    translationsApplied: 0
  };
}

function normalizeSourceUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return raw;
  }

  try {
    const url = new URL(raw);
    if (
      sourceUrlUsesPriorityFeed(url.hostname) ||
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

function looksLikeShortNumericFragment(value = "") {
  return /^\d{4,6}$/.test(String(value || "").trim());
}

function parseDocketNumber(value) {
  const match = String(value || "").trim().match(/^(\d+)/);
  if (!match) {
    return 0;
  }

  const parsed = Number.parseInt(match[1], 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function isBankruptcyLike(caseLike = {}) {
  const docketNumber = String(caseLike.docket_number || "").toLowerCase();
  const courtName = String(caseLike.court_name || "").toLowerCase();
  return docketNumber.includes("-bk-") || courtName.includes("bankruptcy");
}

function isCivilLike(caseLike = {}) {
  return /\b\d{2}-cv-\d{3,6}\b/i.test(String(caseLike.docket_number || "")) ||
    /\b\d+:\d{2}-cv-\d{3,6}\b/i.test(String(caseLike.docket_number || ""));
}

const CRIMINAL_CASE_TERMS = [
  "criminal",
  "indictment",
  "grand jury",
  "sentencing",
  "supervised release",
  "probation violation",
  "search warrant",
  "arrest warrant",
  "united states v.",
  "usa v."
];

const HABEAS_CASE_TERMS = [
  "habeas",
  "2254",
  "2255",
  "2241",
  "prisoner petition",
  "warden"
];

const IMMIGRATION_CASE_TERMS = [
  "immigration",
  "asylum",
  "removal proceeding",
  "deport",
  "deportation",
  "uscis",
  "ice detention"
];

const INJURY_CASE_TERMS = [
  "personal injury",
  "wrongful death",
  "bodily injury",
  "medical malpractice",
  "product liability",
  "premises liability",
  "slip and fall",
  "motor vehicle",
  "auto accident"
];

const IP_ENFORCEMENT_TERMS = [
  "trademark",
  "copyright",
  "patent",
  "lanham act",
  "counterfeit",
  "infringement",
  "trade dress",
  "temporary restraining order"
];

const IP_CAUSE_PATTERNS = [
  /\b15:1114\b/i,
  /\b15:1116\b/i,
  /\b15:1117\b/i,
  /\b15:1125\b/i,
  /\b17:501\b/i,
  /\b35:271\b/i
];

function includesOneOf(text, patterns = []) {
  return patterns.some((pattern) => text.includes(pattern));
}

function normalizeRetentionText(caseLike = {}) {
  return normalizeText([
    caseLike.case_name,
    caseLike.court_name,
    caseLike.docket_number,
    caseLike.cause,
    caseLike.nature_of_suit,
    caseLike.recent_activity_summary,
    caseLike.recent_activity_summary_zh,
    ...(Array.isArray(caseLike.plaintiffs) ? caseLike.plaintiffs : []),
    ...(Array.isArray(caseLike.defendants) ? caseLike.defendants : []),
    JSON.stringify(caseLike.raw || {})
  ].filter(Boolean).join(" | "));
}

function hasAuthoritativeTargetSignal(caseLike = {}) {
  const caseName = normalizeText(caseLike.case_name || "");
  const primarySource = normalizeText(caseLike.primary_source || "");
  const sourceCaseKey = normalizeText(caseLike.source_case_key || "");
  const sourceUrls = Array.isArray(caseLike.source_urls)
    ? caseLike.source_urls
    : parseJson(caseLike.source_urls_json, []);
  return Boolean(
    primarySource === PRIORITY_FEED_PROVIDER_KEY ||
    sourceCaseKey.startsWith(`${PRIORITY_FEED_PROVIDER_KEY}:`) ||
    sourceUrls.some((value) => sourceUrlUsesPriorityFeed(normalizeText(value))) ||
    caseHasPriorityFeedUrl(caseLike) ||
    caseName.includes("schedule a") ||
    caseName.includes("unincorporated associations")
  );
}

function hasIpEnforcementSignal(caseLike = {}, insights = null) {
  const derived = insights || deriveCaseInsights(caseLike);
  const text = normalizeRetentionText({
    ...caseLike,
    insights: derived
  });
  const cause = normalizeText(caseLike.cause || "");
  const natureOfSuit = normalizeText(caseLike.nature_of_suit || "");
  const tags = Array.isArray(caseLike.tags)
    ? caseLike.tags
    : String(caseLike.tags_marker || "")
      .split("|")
      .map((item) => item.trim())
      .filter(Boolean);

  return Boolean(
    tags.includes("tro") ||
    tags.includes("seller_tro") ||
    tags.includes("schedule_a") ||
    derived?.is_tro_case ||
    derived?.is_schedule_a_case ||
    derived?.is_seller_case ||
    /\b(820|830|840)\b/.test(natureOfSuit) ||
    IP_CAUSE_PATTERNS.some((pattern) => pattern.test(cause)) ||
    includesOneOf(text, IP_ENFORCEMENT_TERMS)
  );
}

function hasTargetWatchlistSignals(caseLike = {}, insights = null) {
  const derived = insights || deriveCaseInsights(caseLike);
  const tags = Array.isArray(caseLike.tags)
    ? caseLike.tags
    : String(caseLike.tags_marker || "")
      .split("|")
      .map((item) => item.trim())
      .filter(Boolean);

  return Boolean(
    hasAuthoritativeTargetSignal(caseLike) ||
    hasIpEnforcementSignal(caseLike, derived) ||
    tags.includes("seller_tro") ||
    tags.includes("schedule_a") ||
    derived?.is_schedule_a_case ||
    derived?.is_seller_case ||
    (derived?.is_tro_case && Number(derived?.seller_relevance_score || 0) >= 4)
  );
}

function getCaseRetentionDecision(caseLike = {}) {
  const insights = caseLike.insights || deriveCaseInsights(caseLike);
  const docketNumber = String(caseLike.docket_number || "").toLowerCase();
  const text = normalizeRetentionText({
    ...caseLike,
    insights
  });
  const authoritativeTargetSignal = hasAuthoritativeTargetSignal(caseLike);
  const reasons = [];

  if (!isCivilLike(caseLike)) {
    reasons.push("non-cv-docket");
  }

  if (isBankruptcyLike(caseLike) || insights?.is_bankruptcy_case) {
    reasons.push("bankruptcy");
  }

  if (!authoritativeTargetSignal) {
    if (
      /\b\d{2}-(cr|mj|mc|po)-\d{1,6}\b/i.test(docketNumber) ||
      /\b\d+:\d{2}-(cr|mj|mc|po)-\d{1,6}\b/i.test(docketNumber) ||
      includesOneOf(text, CRIMINAL_CASE_TERMS)
    ) {
      reasons.push("criminal");
    }

    if (includesOneOf(text, HABEAS_CASE_TERMS)) {
      reasons.push("habeas");
    }

    if (includesOneOf(text, IMMIGRATION_CASE_TERMS)) {
      reasons.push("immigration");
    }

    if (includesOneOf(text, INJURY_CASE_TERMS)) {
      reasons.push("injury");
    }
  }

  if (!reasons.length && isCivilLike(caseLike) && !hasTargetWatchlistSignals(caseLike, insights)) {
    reasons.push("general-civil");
  }

  return {
    retain: reasons.length === 0,
    reasons,
    insights
  };
}

function hasSparsePublicCoverage(caseLike = {}) {
  const latestNumber = parseDocketNumber(caseLike.latest_docket_number);
  const knownEntries = Number(caseLike.docket_count || 0);
  return latestNumber >= 5 && knownEntries <= 1;
}

const GAP_PRIORITY_START_DATE = "2025-08-01";
const CURRENT_PRIORITY_YEAR_START_DATE = `${new Date().getUTCFullYear()}-01-01`;

function isGapPriorityCase(caseLike = {}) {
  return String(caseLike.date_filed || "") >= GAP_PRIORITY_START_DATE;
}

function isCurrentPriorityYearCase(caseLike = {}) {
  return String(caseLike.date_filed || "") >= CURRENT_PRIORITY_YEAR_START_DATE;
}

function getCoverageGapPressure(coverage = {}) {
  const totalEntries = Number(coverage.totalEntries || 0);
  const firstNumber = Number(coverage.firstNumber || 0);
  const lastNumber = Number(coverage.lastNumber || 0);
  const numberedEntries = Number(coverage.numberedEntries || 0);
  const leadingGap = firstNumber > 1 ? firstNumber - 1 : 0;
  const sequenceGap = firstNumber > 0 && lastNumber >= firstNumber && numberedEntries >= 2
    ? Math.max(0, lastNumber - firstNumber + 1 - numberedEntries)
    : 0;
  const smallDocketGap = totalEntries < 6 ? 6 - totalEntries : 0;
  return {
    leadingGap,
    sequenceGap,
    smallDocketGap,
    gapPriorityScore: leadingGap + sequenceGap + smallDocketGap,
    hasContinuityGap: leadingGap > 0 || sequenceGap > 0
  };
}

function getCurrentYearGapPriorityWeight(caseLike = {}, coverage = {}, expectedEntries = 0) {
  if (!isCurrentPriorityYearCase(caseLike)) {
    return 0;
  }

  const totalEntries = Number(coverage.totalEntries || 0);
  const gapPriorityScore = Number(coverage.gapPriorityScore || 0);
  const docketCount = Number(caseLike.docket_count || 0);
  const latestGap = Math.max(0, parseDocketNumber(caseLike.latest_docket_number) - Math.max(totalEntries, docketCount));
  const completionGap = Math.max(0, Math.max(Number(expectedEntries || 0), docketCount, 6) - totalEntries);
  return gapPriorityScore + latestGap + completionGap + 10;
}

function compareIsoDesc(left, right) {
  return String(right || "").localeCompare(String(left || ""));
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

function getCasePriorityActivityAt(caseLike = {}) {
  return laterIso(laterIso(caseLike.updated_at, caseLike.latest_docket_filed_at), caseLike.date_filed);
}

const CASE_PRIORITY_ACTIVITY_SQL = `MAX(COALESCE(updated_at, ''), COALESCE(latest_docket_filed_at, ''), COALESCE(date_filed, ''))`;
const CASE_SEARCH_TEXT_SQL = `lower(trim(
  COALESCE(case_name, '') || ' ' ||
  COALESCE(case_name_zh, '') || ' ' ||
  COALESCE(docket_number, '') || ' ' ||
  lower(replace(replace(replace(replace(COALESCE(docket_number, ''), ':', ' '), '-', ' '), '.', ' '), '/', ' ')) || ' ' ||
  COALESCE(court_name, '') || ' ' ||
  COALESCE(recent_activity_summary, '') || ' ' ||
  COALESCE(recent_activity_summary_zh, '') || ' ' ||
  COALESCE(plaintiffs_json, '') || ' ' ||
  COALESCE(defendants_json, '') || ' ' ||
  COALESCE(source_urls_json, '') || ' ' ||
  replace(COALESCE(tags_marker, ''), '|', ' ')
))`;
const CASE_FAST_PATH_SET_SQL = `
  is_tro = CASE WHEN instr(COALESCE(tags_marker, ''), '|tro|') > 0 THEN 1 ELSE 0 END,
  is_schedule_a = CASE WHEN instr(COALESCE(tags_marker, ''), '|schedule_a|') > 0 THEN 1 ELSE 0 END,
  is_seller_watch = CASE WHEN instr(COALESCE(tags_marker, ''), '|seller_tro|') > 0 THEN 1 ELSE 0 END,
  is_watchlist = CASE
    WHEN instr(COALESCE(tags_marker, ''), '|tro|') > 0
      OR instr(COALESCE(tags_marker, ''), '|schedule_a|') > 0
      OR instr(COALESCE(tags_marker, ''), '|seller_tro|') > 0
    THEN 1
    ELSE 0
  END,
  priority_feed_row_count = COALESCE(priority_feed_row_count, 0),
  priority_activity_at = ${CASE_PRIORITY_ACTIVITY_SQL},
  search_text = ${CASE_SEARCH_TEXT_SQL}
`;
const CASE_FAST_PATH_MISSING_WHERE_SQL = `
  priority_activity_at IS NULL
  OR search_text IS NULL
  OR search_text = ''
  OR is_watchlist IS NULL
  OR is_tro IS NULL
  OR is_schedule_a IS NULL
  OR is_seller_watch IS NULL
  OR priority_feed_row_count IS NULL
`;

function chunkArray(values = [], size = 500) {
  const chunks = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function higherOrderValue(left, right) {
  const leftNumber = parseDocketNumber(left);
  const rightNumber = parseDocketNumber(right);
  if (!leftNumber) {
    return right || null;
  }

  if (!rightNumber) {
    return left || null;
  }

  return rightNumber > leftNumber ? right : left;
}

function normalizeLookupText(value) {
  return normalizeText(value).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
}

const DISTRICT_DIRECTION_MAP = {
  N: "Northern",
  S: "Southern",
  E: "Eastern",
  W: "Western",
  C: "Central",
  M: "Middle"
};

function expandCourtLabel(value) {
  return String(value || "")
    .replace(/\b([NSEWCM])\s*\.?\s*D\.?\s+([A-Za-z][A-Za-z .-]+)/gi, (_, direction, place) => {
      const prefix = DISTRICT_DIRECTION_MAP[String(direction || "").toUpperCase()] || direction;
      return `${prefix} District of ${String(place || "").trim()}`;
    })
    .replace(/\bD\.?\s+([A-Za-z][A-Za-z .-]+)/gi, (_, place) => `District of ${String(place || "").trim()}`)
    .replace(/\bU\.?S\.?\b/gi, "United States")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeCourtLookupText(value) {
  return normalizeLookupText(
    expandCourtLabel(value)
      .replace(/\bDistrict Court,?\s*/gi, "")
      .replace(/\bDistrict Court for the\b/gi, "")
      .replace(/\bUnited States\b/gi, "")
  );
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function mergeNestedObjects(...objects) {
  const merged = {};

  for (const object of objects) {
    if (!isPlainObject(object)) {
      continue;
    }

    for (const [key, value] of Object.entries(object)) {
      if (Array.isArray(value) && Array.isArray(merged[key])) {
        merged[key] = mergeArraysNormalized(merged[key], value);
        continue;
      }

      if (isPlainObject(value) && isPlainObject(merged[key])) {
        merged[key] = mergeNestedObjects(merged[key], value);
        continue;
      }

      if (value !== undefined) {
        merged[key] = value;
      }
    }
  }

  return merged;
}

function buildCanonicalCourtKey(caseLike) {
  return normalizeCourtLookupText(caseLike?.court_name) || normalizeLookupText(caseLike?.court_id);
}

function buildCaseIdentityKeys(caseLike) {
  const docketKey = normalizeDocket(caseLike?.docket_number);
  if (!docketKey) {
    return [];
  }

  const keys = new Set();
  const courtIdKey = normalizeLookupText(caseLike?.court_id);
  const courtNameKey = normalizeCourtLookupText(caseLike?.court_name);

  if (courtIdKey) {
    keys.add(`${courtIdKey}|${docketKey}`);
  }

  if (courtNameKey) {
    keys.add(`${courtNameKey}|${docketKey}`);
  }

  return [...keys];
}

function buildCanonicalCaseGroupKey(caseLike) {
  const docketKey = normalizeDocket(caseLike?.docket_number);
  const courtKey = buildCanonicalCourtKey(caseLike);
  if (!docketKey || !courtKey) {
    return "";
  }

  return `${courtKey}|${docketKey}`;
}

function buildPriorityFeedTagClause() {
  return "(tags_marker LIKE '%|seller_tro|%' OR tags_marker LIKE '%|tro|%' OR tags_marker LIKE '%|schedule_a|%')";
}

const TRACKED_LAW_FIRM_SOURCE_PATTERNS = [
  "sriplaw.com",
  "gbc.law",
  "whitewoodlaw.com",
  "jiangip.com"
];

const PRIORITY_FEED_URL_CLAUSE = `source_urls_json LIKE '%${PRIORITY_FEED_HOST}%'`;
const PRIORITY_FEED_RAW_CLAUSE =
  `(raw_json LIKE '%"${PRIORITY_FEED_MODERN_RAW_KEY}"%' OR raw_json LIKE '%"${PRIORITY_FEED_LEGACY_RAW_KEY}"%')`;

function buildTrackedLawFirmSourceClause(columnName = "source_urls_json") {
  return `(${TRACKED_LAW_FIRM_SOURCE_PATTERNS.map((pattern) => `${columnName} LIKE '%${pattern}%'`).join(" OR ")})`;
}

function sourceUrlsContain(caseLike, needle) {
  return (caseLike?.source_urls || []).some((url) => String(url || "").includes(needle));
}

function hasTrackedLawFirmSource(caseLike) {
  return TRACKED_LAW_FIRM_SOURCE_PATTERNS.some((pattern) => sourceUrlsContain(caseLike, pattern));
}

function hasConcretePriorityFeedLink(caseLike = {}) {
  return caseHasPriorityFeedUrl(caseLike) || Boolean(getPriorityFeedRaw(caseLike)?.url);
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

function hasTagMarker(marker = "", tag = "") {
  const normalizedTag = String(tag || "").trim();
  if (!normalizedTag) {
    return false;
  }

  return String(marker || "").includes(`|${normalizedTag}|`);
}

function deriveCategoryFlags(tagsMarker = "") {
  const isTro = hasTagMarker(tagsMarker, "tro");
  const isScheduleA = hasTagMarker(tagsMarker, "schedule_a");
  const isSellerWatch = hasTagMarker(tagsMarker, "seller_tro");
  return {
    is_watchlist: isTro || isScheduleA || isSellerWatch ? 1 : 0,
    is_tro: isTro ? 1 : 0,
    is_schedule_a: isScheduleA ? 1 : 0,
    is_seller_watch: isSellerWatch ? 1 : 0
  };
}

function buildCaseSearchText(caseLike, insights = null) {
  const derived = insights || deriveCaseInsights(caseLike);
  return normalizeText([
    caseLike.case_name,
    caseLike.case_name_zh,
    caseLike.docket_number,
    normalizeDocket(caseLike.docket_number),
    caseLike.court_name,
    caseLike.recent_activity_summary,
    caseLike.recent_activity_summary_zh,
    derived?.brand_name,
    derived?.plaintiff_name,
    derived?.lead_law_firm,
    ...(caseLike.plaintiffs || []),
    ...(caseLike.defendants || []),
    ...(caseLike.source_urls || [])
  ].join(" | "));
}

function buildCaseListMetadata(caseLike, {
  insights = null,
  updatedAt = null
} = {}) {
  const derived = insights || deriveCaseInsights(caseLike);
  const effectiveRow = {
    ...caseLike,
    updated_at: updatedAt || caseLike.updated_at || null
  };
  return {
    ...deriveCategoryFlags(caseLike.tags_marker || buildTagsMarker(caseLike.tags || [])),
    priority_feed_row_count: Math.max(0, Number(getPriorityFeedMetadataRowCount(caseLike) || 0)),
    priority_activity_at: getCasePriorityActivityAt(effectiveRow) || null,
    search_text: buildCaseSearchText(caseLike, derived)
  };
}

function buildCaseView(row) {
  const hydrated = applyAuthoritativeDocketPreference(hydrateCase(row));
  const insights = deriveCaseInsights(hydrated);
  return {
    ...hydrated,
    insights,
    _created_date_shanghai: formatShanghaiDateKey(hydrated.created_at),
    _docket_raw: normalizeText(hydrated.docket_number),
    _docket_normalized: normalizeDocket(hydrated.docket_number),
    _search_blob: hydrated.search_text || buildCaseSearchText(hydrated, insights),
    _label_blob: normalizeText([
      hydrated.case_name,
      insights?.brand_name,
      insights?.lead_law_firm,
      ...(hydrated.plaintiffs || []),
      ...(hydrated.defendants || [])
    ].join(" | "))
  };
}

function caseSourceRank(caseLike = {}) {
  switch (String(caseLike.primary_source || "").toLowerCase()) {
    case "sriplaw":
    case "gbc":
      return 6;
    case SIGNAL_FEED_PROVIDER_KEY:
      return 5;
    case PRIORITY_FEED_ENTRY_SOURCE:
      return 5;
    case "courtfeed":
      return 4;
    case "courtlistener":
      return 3;
    case "pacermonitor":
      return 2;
    default:
      return 1;
  }
}

const CASE_NAME_STOP_WORDS = new Set([
  "a",
  "an",
  "and",
  "associations",
  "company",
  "corporation",
  "corp",
  "defendant",
  "defendants",
  "identified",
  "inc",
  "incorporated",
  "liability",
  "limited",
  "llc",
  "listed",
  "on",
  "partnerships",
  "schedule",
  "the",
  "unincorporated",
  "v",
  "vs"
]);

function preferLongerNormalizedValue(existingValue, incomingValue, normalize = normalizeLookupText) {
  const left = String(existingValue || "").trim();
  const right = String(incomingValue || "").trim();

  if (!left) {
    return right || null;
  }

  if (!right) {
    return left || null;
  }

  if (normalize(left) === normalize(right)) {
    return right.length > left.length ? right : left;
  }

  return right.length > left.length ? right : left;
}

function docketNumberQualityScore(value) {
  const text = String(value || "").trim();
  if (!text || !docketLooksLike(text)) {
    return -1;
  }

  let score = text.length;
  if (/\b\d+:\d{2}-cv-\d{3,6}\b/i.test(text)) {
    score += 40;
  }
  if (/\b20\d{2}-cv-\d{3,6}\b/i.test(text)) {
    score -= 4;
  }

  return score;
}

function pickPreferredDocketNumber(existingValue, incomingValue) {
  const left = String(existingValue || "").trim();
  const right = String(incomingValue || "").trim();
  if (!left) {
    return right || null;
  }

  if (!right) {
    return left || null;
  }

  const leftNormalized = normalizeDocket(left);
  const rightNormalized = normalizeDocket(right);
  if (leftNormalized && rightNormalized && leftNormalized === rightNormalized) {
    const leftScore = docketNumberQualityScore(left);
    const rightScore = docketNumberQualityScore(right);
    if (rightScore !== leftScore) {
      return rightScore > leftScore ? right : left;
    }
    return right.length > left.length ? right : left;
  }

  return docketNumberQualityScore(right) > docketNumberQualityScore(left) ? right : left;
}

function courtNameQualityScore(value) {
  const text = String(value || "").trim();
  if (!text) {
    return -1;
  }

  let score = text.length;
  if (/\b(?:northern|southern|eastern|western|central|middle)\b/i.test(text)) {
    score += 12;
  }
  if (/\bdistrict of\b/i.test(text)) {
    score += 10;
  }
  if (/\b[a-z]\.?\s*d\.\b/i.test(text) || /\b[a-z]\.?\s*d\.?\s+[a-z]/i.test(text)) {
    score -= 8;
  }
  return score;
}

function pickPreferredCourtName(existingValue, incomingValue) {
  const left = String(existingValue || "").trim();
  const right = String(incomingValue || "").trim();
  if (!left) {
    return right || null;
  }

  if (!right) {
    return left || null;
  }

  const leftNormalized = normalizeCourtLookupText(left);
  const rightNormalized = normalizeCourtLookupText(right);
  if (
    leftNormalized &&
    rightNormalized &&
    (leftNormalized === rightNormalized || leftNormalized.includes(rightNormalized) || rightNormalized.includes(leftNormalized))
  ) {
    const leftScore = courtNameQualityScore(left);
    const rightScore = courtNameQualityScore(right);
    if (rightScore !== leftScore) {
      return rightScore > leftScore ? right : left;
    }
    return right.length > left.length ? right : left;
  }

  return courtNameQualityScore(right) > courtNameQualityScore(left) ? right : left;
}

function buildPartyNeedles(values = []) {
  const needles = new Set();

  for (const value of values) {
    const normalized = normalizeLookupText(value);
    if (!normalized) {
      continue;
    }

    needles.add(normalized);
    for (const token of normalized.split(" ").filter((item) => item.length >= 3 && !CASE_NAME_STOP_WORDS.has(item))) {
      needles.add(token);
    }
  }

  return [...needles.values()];
}

function countNeedleHits(text, needles = []) {
  const haystack = normalizeLookupText(text);
  if (!haystack) {
    return 0;
  }

  let hits = 0;
  for (const needle of needles) {
    if (needle && haystack.includes(needle)) {
      hits += needle.includes(" ") ? 3 : 1;
    }
  }

  return hits;
}

function caseNameQualityScore(value, { plaintiffs = [], defendants = [], primarySource = "" } = {}) {
  const text = String(value || "").trim();
  if (!text) {
    return -1;
  }

  let score = Math.min(text.length, 220);
  if (/\b(v\.|vs\.)\b/i.test(text)) {
    score += 18;
  }

  score += countNeedleHits(text, buildPartyNeedles(plaintiffs)) * 14;
  score += countNeedleHits(text, buildPartyNeedles(defendants)) * 8;
  score += caseSourceRank({ primary_source: primarySource }) * 2;

  return score;
}

function pickPreferredCaseName(
  existingValue,
  incomingValue,
  {
    plaintiffs = [],
    defendants = [],
    existingSource = "",
    incomingSource = ""
  } = {}
) {
  const left = String(existingValue || "").trim();
  const right = String(incomingValue || "").trim();
  if (!left) {
    return right || null;
  }

  if (!right) {
    return left || null;
  }

  const leftNormalized = normalizeLookupText(left);
  const rightNormalized = normalizeLookupText(right);
  if (leftNormalized && rightNormalized && leftNormalized === rightNormalized) {
    return right.length > left.length ? right : left;
  }

  const leftScore = caseNameQualityScore(left, {
    plaintiffs,
    defendants,
    primarySource: existingSource
  });
  const rightScore = caseNameQualityScore(right, {
    plaintiffs,
    defendants,
    primarySource: incomingSource
  });

  if (rightScore !== leftScore) {
    return rightScore > leftScore ? right : left;
  }

  return right.length > left.length ? right : left;
}

function pickPreferredPrimarySource(existingValue, incomingValue) {
  const left = String(existingValue || "").trim();
  const right = String(incomingValue || "").trim();
  if (!left) {
    return right || null;
  }
  if (!right) {
    return left || null;
  }

  return caseSourceRank({ primary_source: right }) > caseSourceRank({ primary_source: left }) ? right : left;
}

function compareCaseRowsForCanonicalChoice(left, right) {
  const leftPriorityFeed = getPriorityFeedRowCount(left) > 0 ? 1 : 0;
  const rightPriorityFeed = getPriorityFeedRowCount(right) > 0 ? 1 : 0;
  if (leftPriorityFeed !== rightPriorityFeed) {
    return rightPriorityFeed - leftPriorityFeed;
  }

  const leftTroScore =
    (left.insights?.is_tro_case ? 4 : 0) +
    (left.insights?.is_schedule_a_case ? 3 : 0) +
    (left.insights?.is_seller_case ? 2 : 0);
  const rightTroScore =
    (right.insights?.is_tro_case ? 4 : 0) +
    (right.insights?.is_schedule_a_case ? 3 : 0) +
    (right.insights?.is_seller_case ? 2 : 0);
  if (leftTroScore !== rightTroScore) {
    return rightTroScore - leftTroScore;
  }

  const leftEntries = Number(left.entries?.length || 0);
  const rightEntries = Number(right.entries?.length || 0);
  if (leftEntries !== rightEntries) {
    return rightEntries - leftEntries;
  }

  const leftDocketCount = Number(left.docket_count || 0);
  const rightDocketCount = Number(right.docket_count || 0);
  if (leftDocketCount !== rightDocketCount) {
    return rightDocketCount - leftDocketCount;
  }

  const sourceRankDiff = caseSourceRank(right) - caseSourceRank(left);
  if (sourceRankDiff !== 0) {
    return sourceRankDiff;
  }

  return (
    compareIsoDesc(
      getCasePriorityActivityAt(left),
      getCasePriorityActivityAt(right)
    ) ||
    compareIsoDesc(left.updated_at, right.updated_at)
  );
}

function compareCaseListFreshness(left, right) {
  const activityDiff = compareIsoDesc(getCasePriorityActivityAt(left), getCasePriorityActivityAt(right));
  if (activityDiff !== 0) {
    return activityDiff;
  }

  const filedAtDiff = compareIsoDesc(left?.date_filed, right?.date_filed);
  if (filedAtDiff !== 0) {
    return filedAtDiff;
  }

  const docketCountDiff = Number(right?.docket_count || 0) - Number(left?.docket_count || 0);
  if (docketCountDiff !== 0) {
    return docketCountDiff;
  }

  return Number(right?.id || 0) - Number(left?.id || 0);
}

function mergeArraysNormalized(...lists) {
  const merged = new Map();
  for (const list of lists) {
    for (const item of Array.isArray(list) ? list : []) {
      const value = String(item || "").trim();
      const key = normalizeText(value).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
      if (!value || !key || merged.has(key)) {
        continue;
      }
      merged.set(key, value);
    }
  }
  return [...merged.values()];
}

function mergeCaseRaw(group) {
  return mergeNestedObjects(...group.map((item) => item?.raw || {}));
}

function mergeDuplicateCaseGroup(group) {
  if (group.length === 1) {
    return group[0];
  }

  const ordered = [...group].sort(compareCaseRowsForCanonicalChoice);
  const canonical = ordered[0];
  const mergedSourceUrls = mergeArraysNormalized(...ordered.map((item) => item.source_urls));
  const mergedPlaintiffs = mergeArraysNormalized(...ordered.map((item) => item.plaintiffs));
  const mergedDefendants = mergeArraysNormalized(...ordered.map((item) => item.defendants));
  const preferredCaseName = ordered.reduce(
    (best, item) => pickPreferredCaseName(best, item.case_name, {
      plaintiffs: mergedPlaintiffs,
      defendants: mergedDefendants,
      existingSource: canonical.primary_source,
      incomingSource: item.primary_source
    }),
    canonical.case_name || null
  );
  const preferredCourtName = ordered.reduce(
    (best, item) => pickPreferredCourtName(best, item.court_name),
    canonical.court_name || null
  );
  const preferredDocketNumber = ordered.reduce(
    (best, item) => pickPreferredDocketNumber(best, item.docket_number),
    canonical.docket_number || null
  );
  const preferredPrimarySource = ordered.reduce(
    (best, item) => pickPreferredPrimarySource(best, item.primary_source),
    canonical.primary_source || null
  );
  const merged = {
    ...canonical,
    courtlistener_docket_id:
      canonical.courtlistener_docket_id || ordered.find((item) => item.courtlistener_docket_id)?.courtlistener_docket_id || null,
    pacer_case_id: canonical.pacer_case_id || ordered.find((item) => item.pacer_case_id)?.pacer_case_id || null,
    court_id: canonical.court_id || ordered.find((item) => item.court_id)?.court_id || null,
    court_name: preferredCourtName,
    case_name: preferredCaseName,
    docket_number: preferredDocketNumber,
    primary_source: preferredPrimarySource,
    source_urls: mergedSourceUrls,
    plaintiffs: mergedPlaintiffs,
    defendants: mergedDefendants,
    entries: dedupeEntries(ordered.flatMap((item) => item.entries || [])),
    raw: mergeCaseRaw(ordered),
    latest_docket_filed_at: ordered.reduce(
      (acc, item) => laterIso(acc, item.latest_docket_filed_at),
      canonical.latest_docket_filed_at || null
    ),
    latest_docket_number: ordered.reduce(
      (acc, item) => higherOrderValue(acc, item.latest_docket_number),
      canonical.latest_docket_number || null
    ),
    docket_count: Math.max(...ordered.map((item) => Number(item.docket_count || 0)), Number(canonical.docket_count || 0))
  };

  const displayMerged = applyAuthoritativeDocketPreference(merged, merged.entries);
  displayMerged.insights = deriveCaseInsights(displayMerged);
  displayMerged._search_blob = normalizeText([
    displayMerged.case_name,
    displayMerged.case_name_zh,
    displayMerged.docket_number,
    normalizeDocket(displayMerged.docket_number),
    displayMerged.court_name,
    displayMerged.recent_activity_summary,
    displayMerged.recent_activity_summary_zh,
    displayMerged.insights?.brand_name,
    displayMerged.insights?.lead_law_firm,
    ...(displayMerged.plaintiffs || []),
    ...(displayMerged.defendants || [])
  ].join(" | "));
  displayMerged._label_blob = normalizeText([
    displayMerged.case_name,
    displayMerged.insights?.brand_name,
    displayMerged.insights?.lead_law_firm,
    ...(displayMerged.plaintiffs || []),
    ...(displayMerged.defendants || [])
  ].join(" | "));
  return displayMerged;
}

function collapseDuplicateCases(rows) {
  const grouped = new Map();
  const passthrough = [];

  for (const row of rows) {
    const key = buildCanonicalCaseGroupKey(row);
    if (!key) {
      passthrough.push(row);
      continue;
    }

    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key).push(row);
  }

  const merged = [...grouped.values()].map(mergeDuplicateCaseGroup);
  const combined = [...passthrough, ...merged];
  combined.sort(compareCaseListFreshness);
  return combined;
}

function buildDocketSearchNeedles(rawSearch) {
  const rawNeedle = normalizeText(rawSearch || "");
  const docketNeedle = normalizeDocket(rawSearch);
  const numericSuffix = String(rawSearch || "").match(/(\d{4,6})$/)?.[1] || "";
  const exactNeedles = [...new Set([rawNeedle, docketNeedle].filter(Boolean))];
  const suffixNeedles = [...new Set([rawNeedle, docketNeedle, numericSuffix].filter(Boolean))];
  return {
    exactNeedles,
    suffixNeedles
  };
}

function buildFtsMatchQuery(rawSearch) {
  const tokens = [...new Set(
    normalizeLookupText(rawSearch)
      .split(/\s+/)
      .map((item) => item.trim())
      .filter(Boolean)
      .filter((item) => item.length > 1)
  )];
  if (!tokens.length) {
    return "";
  }

  return tokens
    .map((token) => `"${token.replace(/"/g, "\"\"")}"*`)
    .join(" AND ");
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
  const orderCollapsed = new Map();
  const orderlessEntries = [];

  for (const entry of entries) {
    const orderKey = normalizedEntryOrderKey(entry);
    if (!orderKey) {
      orderlessEntries.push(entry);
      continue;
    }

    const existing = orderCollapsed.get(orderKey);
    if (!existing || compareEntriesForCanonicalRow(entry, existing) < 0) {
      orderCollapsed.set(orderKey, entry);
    }
  }

  const candidates = [...orderCollapsed.values(), ...orderlessEntries];
  const familyCounts = new Map();
  for (const entry of candidates) {
    const familyKey = buildEntryFamilyKey(entry);
    if (!familyKey) {
      continue;
    }

    familyCounts.set(familyKey, (familyCounts.get(familyKey) || 0) + 1);
  }

  const deduped = [];
  const dateBuckets = new Map();

  for (const entry of candidates.sort(compareEntriesForTimeline)) {
    const bucketKey = String(entry.filed_at || entry.created_at || "");
    const bucket = dateBuckets.get(bucketKey) || [];
    let matchedIndex = -1;

    for (const index of bucket) {
      if (areSemanticallyDuplicateEntries(entry, deduped[index], familyCounts)) {
        matchedIndex = index;
        break;
      }
    }

    if (matchedIndex === -1) {
      const nextIndex = deduped.push(entry) - 1;
      bucket.push(nextIndex);
      dateBuckets.set(bucketKey, bucket);
      continue;
    }

    deduped[matchedIndex] = mergeDisplayEntries(deduped[matchedIndex], entry);
  }

  return deduped.filter(Boolean).sort(compareEntriesForTimeline);
}

function normalizeOrderText(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }

  const compact = text.replace(/,+/g, "");

  if (/^\d+(?:\.\d+)?$/.test(compact)) {
    const numeric = Number.parseFloat(compact);
    if (Number.isFinite(numeric)) {
      if (Number.isInteger(numeric)) {
        return String(numeric);
      }

      return String(numeric).replace(/\.0+$/, "");
    }
  }

  if (/^\d+(?:[-/]\d+)+$/.test(compact)) {
    return compact
      .split(/[-/]/)
      .map((segment) => String(Number.parseInt(segment, 10)))
      .join("-");
  }

  return compact.replace(/^0+(?=\d)/, "");
}

const ENTRY_GENERIC_TOKENS = new Set([
  "above",
  "accordance",
  "action",
  "addressed",
  "affidavit",
  "against",
  "application",
  "approved",
  "as",
  "be",
  "by",
  "case",
  "certificate",
  "chambers",
  "civil",
  "clerk",
  "compliance",
  "conference",
  "counsel",
  "court",
  "day",
  "declaration",
  "defendant",
  "defendants",
  "dismissal",
  "document",
  "entry",
  "federal",
  "filed",
  "filing",
  "form",
  "for",
  "further",
  "granted",
  "hearing",
  "held",
  "hereby",
  "in",
  "injunction",
  "law",
  "letter",
  "materials",
  "memorandum",
  "minute",
  "motion",
  "notice",
  "of",
  "on",
  "order",
  "other",
  "plaintiff",
  "plaintiffs",
  "prejudice",
  "preliminary",
  "procedure",
  "proceedings",
  "proposed",
  "pursuant",
  "regarding",
  "re",
  "related",
  "reply",
  "request",
  "response",
  "reviewed",
  "rule",
  "service",
  "signed",
  "so",
  "staff",
  "submission",
  "submitted",
  "support",
  "temporary",
  "text",
  "that",
  "the",
  "to",
  "transcript",
  "voluntary",
  "with",
  "without"
]);

function tokenizeEntryDescription(value) {
  return String(value || "")
    .replace(/\b([A-Za-z]+)'s\b/g, "$1s")
    .toLowerCase()
    .replace(/[\u4e00-\u9fff]+/g, " ")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter((token) => token && token.length > 1);
}

function normalizeEntryDescription(value) {
  return tokenizeEntryDescription(value).join(" ");
}

function extractEntryReferenceNumbers(value) {
  const references = new Set();
  const text = String(value || "");
  for (const match of text.matchAll(/\[(\d+(?:\.\d+)?)\]/g)) {
    references.add(normalizeOrderText(match[1]));
  }

  for (const match of text.matchAll(/\b(?:ecf|document)\s+no\.?\s*(\d+(?:\.\d+)?)\b/gi)) {
    references.add(normalizeOrderText(match[1]));
  }

  return [...references].filter(Boolean);
}

function extractDistinctiveEntryTokens(value) {
  const tokens = tokenizeEntryDescription(value);
  return [...new Set(tokens.filter((token) => {
    if (!token) {
      return false;
    }

    if (ENTRY_GENERIC_TOKENS.has(token)) {
      return false;
    }

    if (/^\d{4}$/.test(token)) {
      return false;
    }

    if (/^\d+$/.test(token)) {
      return token.length >= 2;
    }

    return token.length >= 4 || /\d/.test(token);
  }))];
}

function buildEntryFamilyKey(entry) {
  const dateKey = String(entry?.filed_at || entry?.created_at || "");
  const lead = tokenizeEntryDescription(entry?.description).slice(0, 12).join(" ");
  if (!dateKey || lead.split(" ").length < 6) {
    return "";
  }

  return `${dateKey}|${lead}`;
}

function intersectSets(left = [], right = []) {
  const rightSet = new Set(right);
  return left.filter((item) => rightSet.has(item));
}

function countFuzzySharedTokens(left = [], right = []) {
  const rightTokens = [...right];
  const used = new Set();
  let matches = 0;

  for (const token of left) {
    if (!token) {
      continue;
    }

    const matchIndex = rightTokens.findIndex((candidate, index) => {
      if (!candidate || used.has(index)) {
        return false;
      }

      if (candidate === token) {
        return true;
      }

      const short = token.length <= candidate.length ? token : candidate;
      const long = token.length > candidate.length ? token : candidate;
      return short.length >= 8 && long.startsWith(short);
    });

    if (matchIndex !== -1) {
      used.add(matchIndex);
      matches += 1;
    }
  }

  return matches;
}

function isStrongDescriptionPrefixMatch(left, right) {
  const first = normalizeEntryDescription(left);
  const second = normalizeEntryDescription(right);
  if (!first || !second) {
    return false;
  }

  const short = first.length <= second.length ? first : second;
  const long = first.length > second.length ? first : second;
  return short.length >= 60 && long.startsWith(short);
}

function entrySourcesDiffer(left, right) {
  return String(left?.primary_source || "") !== String(right?.primary_source || "") ||
    String(left?.source || "") !== String(right?.source || "");
}

function isCrossSourceDocketDedupSource(source = "") {
  const normalized = String(source || "").trim().toLowerCase();
  if (!normalized) {
    return false;
  }

  return (
    normalized === "courtlistener" ||
    normalized === "courtfeed" ||
    normalized === "61tro" ||
    normalized === "gbc" ||
    normalized === "sriplaw" ||
    normalized === "pacermonitor" ||
    normalized === "docketalarm" ||
    normalized === "unicourt" ||
    isPriorityFeedPrimarySource(normalized)
  );
}

function areSemanticallyDuplicateEntries(left, right, familyCounts = new Map()) {
  if (!left || !right) {
    return false;
  }

  const leftDate = String(left.filed_at || left.created_at || "");
  const rightDate = String(right.filed_at || right.created_at || "");
  if (!leftDate || leftDate !== rightDate) {
    return false;
  }

  const leftOrder = normalizedEntryOrderKey(left);
  const rightOrder = normalizedEntryOrderKey(right);
  if (leftOrder && rightOrder && leftOrder === rightOrder) {
    return true;
  }

  const leftDescription = normalizeEntryDescription(left.description);
  const rightDescription = normalizeEntryDescription(right.description);
  if (!leftDescription || !rightDescription) {
    return false;
  }

  const crossSource = entrySourcesDiffer(left, right);
  const semanticEligible = crossSource &&
    isCrossSourceDocketDedupSource(left?.primary_source) &&
    isCrossSourceDocketDedupSource(right?.primary_source);
  if (!semanticEligible) {
    return false;
  }

  const priorityFeedEligible =
    isPriorityFeedPrimarySource(left?.primary_source) ||
    isPriorityFeedPrimarySource(right?.primary_source);

  if (leftDescription === rightDescription) {
    return true;
  }

  const leftRefs = extractEntryReferenceNumbers(left.description);
  const rightRefs = extractEntryReferenceNumbers(right.description);
  const sharedRefs = intersectSets(leftRefs, rightRefs);
  const leftTokens = extractDistinctiveEntryTokens(left.description);
  const rightTokens = extractDistinctiveEntryTokens(right.description);
  const sharedTokens = intersectSets(leftTokens, rightTokens);
  const fuzzySharedTokenCount = Math.max(
    countFuzzySharedTokens(leftTokens, rightTokens),
    countFuzzySharedTokens(rightTokens, leftTokens)
  );
  const leftFamily = buildEntryFamilyKey(left);
  const rightFamily = buildEntryFamilyKey(right);
  const sameFamily = Boolean(leftFamily && leftFamily === rightFamily);

  if (sharedRefs.length && sharedTokens.length >= 2) {
    return true;
  }

  if (sharedRefs.length && fuzzySharedTokenCount >= 2) {
    return true;
  }

  if (sameFamily &&
      isStrongDescriptionPrefixMatch(left.description, right.description) &&
      sharedTokens.length >= 2) {
    return true;
  }

  if (isStrongDescriptionPrefixMatch(left.description, right.description) &&
      fuzzySharedTokenCount >= 2 &&
      sharedTokens.length >= 2) {
    return true;
  }

  if (sameFamily &&
      sharedTokens.length >= 3 &&
      fuzzySharedTokenCount >= 3) {
    return true;
  }

  if (sameFamily &&
      familyCounts.get(leftFamily) === 2 &&
      priorityFeedEligible &&
      fuzzySharedTokenCount >= 2) {
    return true;
  }

  return false;
}

function normalizedEntryOrderKey(entry) {
  const normalized = normalizeOrderText(entry.document_number) || normalizeOrderText(entry.entry_number) || "";
  if (!normalized) {
    return "";
  }

  if (isPriorityFeedPrimarySource(entry?.primary_source)) {
    return `priority:${normalized}`;
  }

  return normalized;
}

function normalizeEntryUrlKey(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }

  try {
    const url = new URL(raw);
    url.hash = "";
    return url.toString().toLowerCase();
  } catch {
    return raw.toLowerCase();
  }
}

function buildStoredEntryDeduplicationKey(entry) {
  const caseId = Number(entry?.case_id || 0);
  const primarySource = String(entry?.primary_source || "").trim().toLowerCase();
  if (!caseId || !primarySource) {
    return "";
  }

  const sourceEntryId = normalizeLookupText(entry?.source_entry_id);
  if (sourceEntryId) {
    return `${caseId}|${primarySource}|id:${sourceEntryId}`;
  }

  const absoluteUrl = normalizeEntryUrlKey(entry?.absolute_url);
  if (absoluteUrl) {
    return `${caseId}|${primarySource}|url:${absoluteUrl}`;
  }

  const orderKey = normalizedEntryOrderKey(entry);
  const filedAt = String(entry?.filed_at || "").trim();
  const description = normalizeEntryDescription(entry?.description).slice(0, 320);
  if (!orderKey && !filedAt && !description) {
    return "";
  }

  return `${caseId}|${primarySource}|fallback:${orderKey}|${filedAt}|${description}`;
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
  if (entry.primary_source === SIGNAL_FEED_PROVIDER_KEY) {
    return 4;
  }

  if (entry.primary_source === "pacermonitor") {
    return 3;
  }

  if (entry.primary_source === "sriplaw") {
    return 3;
  }

  if (entry.primary_source === "61tro") {
    return 2;
  }

  if (isPriorityFeedPrimarySource(entry.primary_source)) {
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

function entryAuthorityRank(source = "") {
  const normalized = String(source || "").trim().toLowerCase();
  if (!normalized) {
    return 0;
  }

  if (normalized === "courtfeed") {
    return 7;
  }

  if (normalized === SIGNAL_FEED_PROVIDER_KEY) {
    return 6;
  }

  if (normalized === "courtlistener") {
    return 5;
  }

  if (normalized === "docketalarm" || normalized === "pacermonitor") {
    return 4;
  }

  if (normalized === "unicourt") {
    return 3;
  }

  if (normalized === "61tro" || normalized === "gbc" || normalized === "sriplaw") {
    return 2;
  }

  if (isPriorityFeedPrimarySource(normalized)) {
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

function pickPreferredEntryDescription(existingValue, incomingValue) {
  return preferLongerNormalizedValue(existingValue, incomingValue, normalizeEntryDescription);
}

function pickPreferredEntryType(existingValue, incomingValue) {
  const left = String(existingValue || "").trim();
  const right = String(incomingValue || "").trim();
  if (!left) {
    return right || null;
  }
  if (!right) {
    return left || null;
  }

  const leftScore = entryContentRank({ document_type: left });
  const rightScore = entryContentRank({ document_type: right });
  if (rightScore !== leftScore) {
    return rightScore > leftScore ? right : left;
  }

  return right.length > left.length ? right : left;
}

function pickPreferredEntryNumber(existingValue, incomingValue) {
  const left = normalizeOrderText(existingValue);
  const right = normalizeOrderText(incomingValue);
  if (!left) {
    return right || incomingValue || null;
  }
  if (!right) {
    return left || existingValue || null;
  }
  return right.length > left.length ? incomingValue : existingValue;
}

function pickPreferredEntryUrl(existingValue, incomingValue) {
  const left = normalizeEntryUrlKey(existingValue);
  const right = normalizeEntryUrlKey(incomingValue);
  if (!left) {
    return incomingValue || null;
  }
  if (!right) {
    return existingValue || null;
  }
  return String(incomingValue || "").length > String(existingValue || "").length ? incomingValue : existingValue;
}

function pickPreferredNumericValue(existingValue, incomingValue) {
  const numbers = [existingValue, incomingValue]
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value));
  if (!numbers.length) {
    return null;
  }

  return Math.max(...numbers);
}

function pickPreferredDisplayPrimarySource(existingValue, incomingValue) {
  const left = String(existingValue || "").trim();
  const right = String(incomingValue || "").trim();
  if (!left) {
    return right || null;
  }
  if (!right) {
    return left || null;
  }

  const leftRank = entryAuthorityRank(left);
  const rightRank = entryAuthorityRank(right);
  if (rightRank !== leftRank) {
    return rightRank > leftRank ? right : left;
  }

  return right.length > left.length ? right : left;
}

function mergeDisplayEntries(existing = null, incoming = null) {
  if (!existing) {
    return incoming;
  }
  if (!incoming) {
    return existing;
  }

  const canonical = compareEntriesForCanonicalRow(existing, incoming) <= 0 ? existing : incoming;
  const mergedSources = mergeArraysNormalized(
    Array.isArray(existing?.raw?.merged_sources) ? existing.raw.merged_sources : [],
    Array.isArray(incoming?.raw?.merged_sources) ? incoming.raw.merged_sources : [],
    [existing?.primary_source, incoming?.primary_source].filter(Boolean)
  );

  return {
    ...canonical,
    primary_source: pickPreferredDisplayPrimarySource(existing?.primary_source, incoming?.primary_source),
    source_entry_id: preferLongerNormalizedValue(existing?.source_entry_id, incoming?.source_entry_id) || null,
    document_type: pickPreferredEntryType(existing?.document_type, incoming?.document_type),
    entry_number: pickPreferredEntryNumber(existing?.entry_number, incoming?.entry_number),
    document_number: pickPreferredEntryNumber(existing?.document_number, incoming?.document_number),
    filed_at: laterIso(existing?.filed_at, incoming?.filed_at) || existing?.filed_at || incoming?.filed_at || null,
    description: pickPreferredEntryDescription(existing?.description, incoming?.description),
    description_zh: preferLongerNormalizedValue(existing?.description_zh, incoming?.description_zh) || null,
    absolute_url: pickPreferredEntryUrl(existing?.absolute_url, incoming?.absolute_url),
    is_available: pickPreferredNumericValue(existing?.is_available, incoming?.is_available),
    page_count: pickPreferredNumericValue(existing?.page_count, incoming?.page_count),
    pacer_doc_id: preferLongerNormalizedValue(existing?.pacer_doc_id, incoming?.pacer_doc_id) || null,
    raw: mergeNestedObjects(existing?.raw || {}, incoming?.raw || {}, {
      merged_sources: mergedSources
    })
  };
}

function buildMergedDocketEntryRecord(existing = null, record = {}, timestamp = nowIso()) {
  const mergedRaw = mergeNestedObjects(existing?.raw || {}, record.raw || {});
  return {
    case_id: Number(record.case_id || existing?.case_id || 0),
    source_entry_key: record.source_entry_key || existing?.source_entry_key,
    primary_source: record.primary_source || existing?.primary_source || null,
    source_entry_id: preferLongerNormalizedValue(existing?.source_entry_id, record.source_entry_id) || null,
    document_type: pickPreferredEntryType(existing?.document_type, record.document_type),
    entry_number: pickPreferredEntryNumber(existing?.entry_number, record.entry_number),
    document_number: pickPreferredEntryNumber(existing?.document_number, record.document_number),
    filed_at: laterIso(existing?.filed_at, record.filed_at) || existing?.filed_at || record.filed_at || null,
    description: pickPreferredEntryDescription(existing?.description, record.description),
    description_zh: existing?.description_zh || record.description_zh || null,
    absolute_url: pickPreferredEntryUrl(existing?.absolute_url, record.absolute_url),
    is_available: pickPreferredNumericValue(existing?.is_available, record.is_available),
    page_count: pickPreferredNumericValue(existing?.page_count, record.page_count),
    pacer_doc_id: preferLongerNormalizedValue(existing?.pacer_doc_id, record.pacer_doc_id) || null,
    raw: mergedRaw,
    last_synced_at: laterIso(existing?.last_synced_at, record.last_synced_at) || timestamp,
    created_at: existing?.created_at || timestamp,
    updated_at: timestamp
  };
}

function compareEntriesForCanonicalRow(left, right) {
  const descriptionCompare = String(right.description || "").length - String(left.description || "").length;
  if (descriptionCompare !== 0) {
    return descriptionCompare;
  }

  const contentCompare = entryContentRank(right) - entryContentRank(left);
  if (contentCompare !== 0) {
    return contentCompare;
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

function getPriorityFeedRowCount(caseLike = {}) {
  return Math.max(
    Number(caseLike?.priority_feed_row_count || 0),
    Number(getPriorityFeedMetadataRowCount(caseLike) || 0)
  );
}

function getPriorityFeedEntries(entries = []) {
  const deduped = new Map();

  for (const entry of entries
    .filter((entry) => isPriorityFeedPrimarySource(entry?.primary_source))
    .sort(compareEntriesForTimeline)) {
    const signature = [
      normalizeOrderText(entry?.document_number) || normalizeOrderText(entry?.entry_number) || "",
      String(entry?.filed_at || ""),
      normalizeEntryDescription(entry?.description)
    ].join("|");

    if (!deduped.has(signature)) {
      deduped.set(signature, entry);
    }
  }

  return [...deduped.values()].sort(compareEntriesForTimeline);
}

function hasPriorityFeedAuthority(caseLike, entries = []) {
  return getPriorityFeedRowCount(caseLike) > 0 || getPriorityFeedEntries(entries).length > 0;
}

function applyAuthoritativeDocketPreference(caseLike, entries = null) {
  if (!caseLike) {
    return caseLike;
  }

  const priorityFeedRowCount = getPriorityFeedRowCount(caseLike);
  const priorityFeedEntries = Array.isArray(entries) ? getPriorityFeedEntries(entries) : null;
  const hasPriorityFeedRecord = Array.isArray(entries)
    ? hasPriorityFeedAuthority(caseLike, entries)
    : priorityFeedRowCount > 0;

  if (!hasPriorityFeedRecord) {
    if (Array.isArray(entries)) {
      return {
        ...caseLike,
        entries
      };
    }
    return caseLike;
  }

  const newestEntry = priorityFeedEntries?.[0] || null;
  const preferred = {
    ...caseLike,
    primary_source: PRIORITY_FEED_ENTRY_SOURCE,
    source_urls: mergeArraysNormalized(
      ...(caseLike.source_urls ? [caseLike.source_urls] : []),
      getPriorityFeedRaw(caseLike)?.url ? [[getPriorityFeedRaw(caseLike).url]] : []
    ),
    recent_activity_summary: caseLike.recent_activity_summary || newestEntry?.description || null,
    latest_docket_filed_at: laterIso(caseLike.latest_docket_filed_at, newestEntry?.filed_at || null),
    latest_docket_number: higherOrderValue(
      caseLike.latest_docket_number,
      newestEntry?.row_number ||
        newestEntry?.document_number ||
        newestEntry?.entry_number ||
        (priorityFeedRowCount > 0 ? String(priorityFeedRowCount) : null)
    ),
    docket_count: Math.max(
      Number(caseLike.docket_count || 0),
      Number(priorityFeedEntries?.length || 0),
      Number(priorityFeedRowCount || 0)
    )
  };

  if (Array.isArray(entries)) {
    preferred.entries = entries;
  }

  return preferred;
}

function getLawFirmMetadataEntryCount(caseLike = {}) {
  const sites = caseLike?.raw?.law_firm_sites;
  if (!isPlainObject(sites)) {
    return 0;
  }

  return Object.values(sites).reduce((maxCount, site) => {
    const count = Number(site?.entryCount || 0);
    return Number.isFinite(count) ? Math.max(maxCount, count) : maxCount;
  }, 0);
}

function getKnownExternalEntryCount(caseLike = {}) {
  return Math.max(
    getPriorityFeedRowCount(caseLike),
    getLawFirmMetadataEntryCount(caseLike)
  );
}

function compareCaseActivityDesc(left, right) {
  return String(right || "").localeCompare(String(left || ""));
}

function isActivityAheadOfTimeline(activityAt, latestEntryFiledAt) {
  if (!activityAt) {
    return false;
  }

  if (!latestEntryFiledAt) {
    return true;
  }

  return String(activityAt).localeCompare(String(latestEntryFiledAt)) > 0;
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
    this.dbPath = path.resolve(dbPath);
    this.db = new DatabaseSync(dbPath);
    this.caseCacheVersion = 0;
    this.caseViewCache = new Map();
    this.caseDetailCache = new Map();
    this.listPayloadCache = new Map();
    this.dashboardStatsCache = null;
    this.recentSyncCache = null;
    this.caseIdentityCache = null;
    this.deferInvalidationDepth = 0;
    this.pendingInvalidation = false;
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;
      PRAGMA busy_timeout = 5000;

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
        is_watchlist INTEGER DEFAULT 0,
        is_tro INTEGER DEFAULT 0,
        is_schedule_a INTEGER DEFAULT 0,
        is_seller_watch INTEGER DEFAULT 0,
        priority_feed_row_count INTEGER DEFAULT 0,
        priority_activity_at TEXT,
        docket_url TEXT,
        source_urls_json TEXT,
        plaintiffs_json TEXT,
        defendants_json TEXT,
        recent_activity_summary TEXT,
        recent_activity_summary_zh TEXT,
        search_text TEXT,
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
      CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON sync_runs(started_at DESC);
      CREATE INDEX IF NOT EXISTS idx_sync_runs_provider_status_started_at ON sync_runs(provider, status, started_at DESC);
      CREATE INDEX IF NOT EXISTS idx_sync_runs_provider_mode_status_started_at ON sync_runs(provider, mode, status, started_at DESC);
    `);
    ensureTableColumns(this.db, "cases", {
      is_watchlist: "INTEGER DEFAULT 0",
      is_tro: "INTEGER DEFAULT 0",
      is_schedule_a: "INTEGER DEFAULT 0",
      is_seller_watch: "INTEGER DEFAULT 0",
      priority_feed_row_count: "INTEGER DEFAULT 0",
      priority_activity_at: "TEXT",
      search_text: "TEXT"
    });
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_cases_courtlistener_docket_id ON cases(courtlistener_docket_id);
      CREATE INDEX IF NOT EXISTS idx_cases_watchlist_activity ON cases(is_watchlist, priority_activity_at DESC, updated_at DESC, id DESC);
      CREATE INDEX IF NOT EXISTS idx_cases_tro_activity ON cases(is_tro, priority_activity_at DESC, updated_at DESC, id DESC);
      CREATE INDEX IF NOT EXISTS idx_cases_schedule_a_activity ON cases(is_schedule_a, priority_activity_at DESC, updated_at DESC, id DESC);
      CREATE INDEX IF NOT EXISTS idx_cases_seller_watch_activity ON cases(is_seller_watch, priority_activity_at DESC, updated_at DESC, id DESC);
      CREATE INDEX IF NOT EXISTS idx_cases_court_activity ON cases(court_id, priority_activity_at DESC, updated_at DESC, id DESC);
      CREATE INDEX IF NOT EXISTS idx_cases_dashboard_rollup ON cases(date_filed, created_at, is_watchlist, is_tro, is_schedule_a, is_seller_watch);
      CREATE INDEX IF NOT EXISTS idx_cases_watchlist_court_facets ON cases(is_watchlist, date_filed, court_id, court_name);
      CREATE INDEX IF NOT EXISTS idx_cases_updated_at_latest ON cases(updated_at DESC, docket_number, case_name);

      CREATE VIRTUAL TABLE IF NOT EXISTS cases_search_fts
      USING fts5(search_text, content='cases', content_rowid='id', tokenize='unicode61 remove_diacritics 2');

      CREATE TRIGGER IF NOT EXISTS cases_search_fts_ai AFTER INSERT ON cases BEGIN
        INSERT INTO cases_search_fts(rowid, search_text)
        VALUES (new.id, COALESCE(new.search_text, ''));
      END;

      CREATE TRIGGER IF NOT EXISTS cases_search_fts_ad AFTER DELETE ON cases BEGIN
        INSERT INTO cases_search_fts(cases_search_fts, rowid, search_text)
        VALUES ('delete', old.id, COALESCE(old.search_text, ''));
      END;

      CREATE TRIGGER IF NOT EXISTS cases_search_fts_au AFTER UPDATE OF search_text ON cases BEGIN
        INSERT INTO cases_search_fts(cases_search_fts, rowid, search_text)
        VALUES ('delete', old.id, COALESCE(old.search_text, ''));
        INSERT INTO cases_search_fts(rowid, search_text)
        VALUES (new.id, COALESCE(new.search_text, ''));
      END;
    `);

    try {
      this.ensurePublicReadFastPath();
    } catch (error) {
      console.warn(
        `[public-read-fast-path] initialization skipped: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  ensurePublicReadFastPath() {
    const missingCases = Number(
      this.db
        .prepare(`SELECT COUNT(*) AS total FROM cases WHERE ${CASE_FAST_PATH_MISSING_WHERE_SQL}`)
        .get()?.total || 0
    );

    if (!missingCases) {
      const fts = this.rebuildCaseSearchFtsIfNeeded();
      if (fts.rebuilt) {
        this.invalidateCaseViews();
      }
      return {
        missingCases: 0,
        updatedCases: 0,
        rebuiltFts: Boolean(fts.rebuilt)
      };
    }

    try {
      this.backfillCaseFastPathColumns();
      const fts = this.rebuildCaseSearchFtsIfNeeded();
      this.invalidateCaseViews();
      console.log(
        `[public-read-fast-path] backfilled ${missingCases} case rows${fts.rebuilt ? " and rebuilt FTS" : ""}`
      );
      return {
        missingCases,
        updatedCases: missingCases,
        rebuiltFts: Boolean(fts.rebuilt),
        mode: "bulk"
      };
    } catch (error) {
      console.warn(
        `[public-read-fast-path] bulk backfill failed, retrying in batches: ${error instanceof Error ? error.message : String(error)}`
      );
      return this.rebuildCaseFastPathColumns({
        batchSize: 500,
        limit: 0
      });
    }
  }

  backfillCaseFastPathColumns() {
    this.db
      .prepare(`
        UPDATE cases
        SET ${CASE_FAST_PATH_SET_SQL}
        WHERE ${CASE_FAST_PATH_MISSING_WHERE_SQL}
      `)
      .run();
  }

  rebuildCaseSearchFtsIfNeeded() {
    const caseCount = Number(this.db.prepare("SELECT COUNT(*) AS n FROM cases").get()?.n || 0);
    const ftsCount = Number(this.db.prepare("SELECT COUNT(*) AS n FROM cases_search_fts").get()?.n || 0);
    if (caseCount === ftsCount) {
      return {
        caseCount,
        rebuilt: false
      };
    }

    this.db.exec(`INSERT INTO cases_search_fts(cases_search_fts) VALUES('rebuild');`);
    return {
      caseCount,
      rebuilt: true
    };
  }

  rebuildCaseFastPathColumns({
    batchSize = 1000,
    limit = 0
  } = {}) {
    const normalizedBatchSize = Math.max(1, Number(batchSize || 1000));
    const normalizedLimit = Math.max(0, Number(limit || 0));
    let updated = 0;
    let skipped = 0;
    const skippedCaseIds = [];
    let skippedOverflowLogged = false;
    let processed = 0;
    let cursor = Number.MAX_SAFE_INTEGER;
    const safeUpdateStatement = this.db.prepare(`
      UPDATE cases
      SET ${CASE_FAST_PATH_SET_SQL}
      WHERE id = ?
    `);
    const richUpdateStatement = this.db.prepare(`
      UPDATE cases
      SET is_watchlist = ?,
          is_tro = ?,
          is_schedule_a = ?,
          is_seller_watch = ?,
          priority_feed_row_count = ?,
          priority_activity_at = ?,
          search_text = ?
      WHERE id = ?
    `);

    while (true) {
      const remaining = normalizedLimit > 0 ? Math.max(normalizedLimit - processed, 0) : normalizedBatchSize;
      if (normalizedLimit > 0 && remaining <= 0) {
        break;
      }

      const chunk = this.db
        .prepare("SELECT id FROM cases WHERE id < ? ORDER BY id DESC LIMIT ?")
        .all(cursor, Math.min(normalizedBatchSize, remaining))
        .map((row) => Number(row.id || 0))
        .filter(Boolean);

      if (!chunk.length) {
        break;
      }

      cursor = Math.min(...chunk);
      processed += chunk.length;
      for (const caseId of chunk) {
        try {
          const row = this.db.prepare("SELECT * FROM cases WHERE id = ? LIMIT 1").get(caseId);
          const hydrated = hydrateCase(row);
          const metadata = buildCaseListMetadata(hydrated, {
            updatedAt: hydrated.updated_at
          });
          updated += Number(
            richUpdateStatement.run(
              metadata.is_watchlist,
              metadata.is_tro,
              metadata.is_schedule_a,
              metadata.is_seller_watch,
              metadata.priority_feed_row_count,
              metadata.priority_activity_at,
              metadata.search_text,
              caseId
            )?.changes || 0
          );
        } catch (error) {
          try {
            updated += Number(safeUpdateStatement.run(caseId)?.changes || 0);
          } catch (fallbackError) {
            skipped += 1;
            if (skippedCaseIds.length < 25) {
              skippedCaseIds.push(caseId);
              console.warn(
                `[rebuild-case-fast-path] skipping case ${caseId}: ${fallbackError instanceof Error ? fallbackError.message : String(fallbackError)}`
              );
            } else if (!skippedOverflowLogged) {
              skippedOverflowLogged = true;
              console.warn("[rebuild-case-fast-path] additional malformed cases omitted from logs");
            }
          }
        }
      }
    }

    const fts = this.rebuildCaseSearchFtsIfNeeded();
    this.invalidateCaseViews();
    return {
      updatedCases: updated,
      skippedCases: skipped,
      skippedCaseIds,
      batchSize: normalizedBatchSize,
      rebuiltFts: Boolean(fts.rebuilt)
    };
  }

  cleanupWindowEmailArtifacts({ vacuum = false, terminalRetentionDays = 30 } = {}) {
    const retentionDays = Math.min(Math.max(Number(terminalRetentionDays || 30), 7), 365);
    const reportCutoffIso = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000).toISOString();
    let checkpointsDeleted = 0;
    let syncRunsDeleted = 0;
    let reportSyncRunsDeleted = 0;
    let reportCheckpointsDeleted = 0;
    let terminalSyncRunsDeleted = 0;

    this.db.exec("BEGIN IMMEDIATE");
    try {
      checkpointsDeleted = Number(
        this.db
          .prepare(`DELETE FROM checkpoints WHERE checkpoint_key LIKE 'window-email-report:%'`)
          .run()?.changes || 0
      );
      syncRunsDeleted = Number(
        this.db
          .prepare(`DELETE FROM sync_runs WHERE provider = 'window-email'`)
          .run()?.changes || 0
      );
      reportSyncRunsDeleted = Number(
        this.db
          .prepare(`
            DELETE FROM sync_runs
            WHERE provider IN ('daily-email', 'tro-daily-roundup')
              AND mode = 'report'
              AND started_at < ?
          `)
          .run(reportCutoffIso)?.changes || 0
      );
      reportCheckpointsDeleted = Number(
        this.db
          .prepare(`
            DELETE FROM checkpoints
            WHERE (
                checkpoint_key LIKE 'daily-email-report:%'
                OR checkpoint_key LIKE 'tro-daily-roundup:%'
              )
              AND updated_at < ?
          `)
          .run(reportCutoffIso)?.changes || 0
      );
      terminalSyncRunsDeleted = Number(
        this.db
          .prepare(`
            DELETE FROM sync_runs
            WHERE status IN ('succeeded', 'failed', 'completed', 'skipped')
              AND COALESCE(finished_at, started_at) < ?
          `)
          .run(reportCutoffIso)?.changes || 0
      );
      this.db.exec("COMMIT");
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {}
      throw error;
    }

    const totalDeleted = checkpointsDeleted + syncRunsDeleted + reportSyncRunsDeleted + reportCheckpointsDeleted + terminalSyncRunsDeleted;
    if (vacuum && totalDeleted > 0) {
      this.db.exec("VACUUM;");
      this.db.exec("PRAGMA optimize;");
    }

    return {
      checkpointsDeleted,
      syncRunsDeleted,
      reportSyncRunsDeleted,
      reportCheckpointsDeleted,
      terminalSyncRunsDeleted,
      reportRetentionDays: retentionDays,
      terminalSyncRunRetentionDays: retentionDays,
      vacuumed: Boolean(vacuum && totalDeleted > 0)
    };
  }

  async restoreMissingFromBackup({
    sourceDbPath,
    dryRun = false,
    limit = 0,
    retainedOnly = false
  } = {}) {
    const resolvedSourceDbPath = path.resolve(String(sourceDbPath || "").trim());
    if (!resolvedSourceDbPath) {
      throw new Error("sourceDbPath is required");
    }
    if (!fs.existsSync(resolvedSourceDbPath)) {
      throw new Error(`backup database not found: ${resolvedSourceDbPath}`);
    }
    if (resolvedSourceDbPath === this.dbPath) {
      throw new Error("backup database must differ from the live database");
    }

    const alias = "restore_source";
    const attachSql = `ATTACH DATABASE ${sqliteStringLiteral(resolvedSourceDbPath)} AS ${alias};`;
    let attached = false;

    try {
      this.db.exec(attachSql);
      attached = true;
      const missingCaseCount = Number(
        this.db
          .prepare(`
            SELECT COUNT(*) AS n
            FROM ${alias}.cases old_case
            LEFT JOIN cases live_case ON live_case.source_case_key = old_case.source_case_key
            WHERE live_case.id IS NULL
          `)
          .get()?.n || 0
      );
      const missingRows = this.db
        .prepare(`
          SELECT old_case.*
          FROM ${alias}.cases old_case
          LEFT JOIN cases live_case ON live_case.source_case_key = old_case.source_case_key
          WHERE live_case.id IS NULL
          ORDER BY old_case.id ASC
        `)
        .all()
        .map((row) => buildCaseView(row));

      let retainedByCurrentRules = 0;
      let selectedRows = missingRows;
      if (retainedOnly) {
        selectedRows = [];
        for (const row of missingRows) {
          const retention = getCaseRetentionDecision(row);
          if (retention.retain) {
            retainedByCurrentRules += 1;
            selectedRows.push(row);
          }
        }
      }
      if (Number(limit || 0) > 0) {
        selectedRows = selectedRows.slice(0, Math.max(Number(limit || 0), 1));
      }

      this.db.exec(`
        DROP TABLE IF EXISTS temp.restore_case_keys;
        CREATE TEMP TABLE restore_case_keys (
          source_case_key TEXT PRIMARY KEY
        );
      `);
      const insertRestoreCaseKey = this.db.prepare(`
        INSERT OR IGNORE INTO temp.restore_case_keys(source_case_key)
        VALUES (?)
      `);
      for (const key of selectedRows.map((row) => String(row.source_case_key || "").trim()).filter(Boolean)) {
        insertRestoreCaseKey.run(key);
      }

      const selectedCaseCount = Number(
        this.db.prepare(`SELECT COUNT(*) AS n FROM temp.restore_case_keys`).get()?.n || 0
      );
      const missingEntryCount = Number(
        this.db
          .prepare(`
            SELECT COUNT(*) AS n
            FROM ${alias}.docket_entries old_entry
            JOIN ${alias}.cases old_case ON old_case.id = old_entry.case_id
            JOIN temp.restore_case_keys restore_case ON restore_case.source_case_key = old_case.source_case_key
            LEFT JOIN docket_entries live_entry ON live_entry.source_entry_key = old_entry.source_entry_key
            WHERE live_entry.id IS NULL
          `)
          .get()?.n || 0
      );
      const sample = selectedRows.slice(0, 25).map((row) => ({
        id: Number(row.id || 0),
        docket_number: row.docket_number || null,
        case_name: row.case_name || null,
        court_id: row.court_id || null,
        date_filed: row.date_filed || null
      }));

      if (dryRun || selectedCaseCount === 0) {
        return {
          dryRun: true,
          sourceDbPath: resolvedSourceDbPath,
          missingCaseCount,
          selectedCaseCount,
          retainedOnly: Boolean(retainedOnly),
          retainedByCurrentRules,
          missingEntryCount,
          restoredCases: 0,
          restoredEntries: 0,
          affectedCases: 0,
          sample
        };
      }

      const restoreCasesStatement = this.db.prepare(`
        INSERT OR IGNORE INTO cases (
          id,
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
          is_watchlist,
          is_tro,
          is_schedule_a,
          is_seller_watch,
          priority_feed_row_count,
          priority_activity_at,
          docket_url,
          source_urls_json,
          plaintiffs_json,
          defendants_json,
          recent_activity_summary,
          recent_activity_summary_zh,
          search_text,
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
        )
        SELECT
          old_case.id,
          old_case.source_case_key,
          old_case.primary_source,
          old_case.source_case_id,
          old_case.courtlistener_docket_id,
          old_case.pacer_case_id,
          old_case.court_id,
          old_case.court_name,
          old_case.case_name,
          old_case.case_name_zh,
          old_case.docket_number,
          old_case.date_filed,
          old_case.date_terminated,
          old_case.cause,
          old_case.nature_of_suit,
          old_case.status,
          old_case.tags_marker,
          old_case.is_watchlist,
          old_case.is_tro,
          old_case.is_schedule_a,
          old_case.is_seller_watch,
          old_case.priority_feed_row_count,
          old_case.priority_activity_at,
          old_case.docket_url,
          old_case.source_urls_json,
          old_case.plaintiffs_json,
          old_case.defendants_json,
          old_case.recent_activity_summary,
          old_case.recent_activity_summary_zh,
          old_case.search_text,
          old_case.latest_docket_filed_at,
          old_case.latest_docket_number,
          old_case.docket_count,
          old_case.last_seen_at,
          old_case.last_synced_at,
          old_case.last_docket_sync_at,
          old_case.last_translation_at,
          old_case.raw_json,
          old_case.created_at,
          old_case.updated_at
        FROM ${alias}.cases old_case
        JOIN temp.restore_case_keys restore_case ON restore_case.source_case_key = old_case.source_case_key
        LEFT JOIN cases live_case ON live_case.source_case_key = old_case.source_case_key
        WHERE live_case.id IS NULL
      `);
      const restoreEntriesStatement = this.db.prepare(`
        INSERT OR IGNORE INTO docket_entries (
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
        )
        SELECT
          live_case.id,
          old_entry.source_entry_key,
          old_entry.primary_source,
          old_entry.source_entry_id,
          old_entry.document_type,
          old_entry.entry_number,
          old_entry.document_number,
          old_entry.filed_at,
          old_entry.description,
          old_entry.description_zh,
          old_entry.absolute_url,
          old_entry.is_available,
          old_entry.page_count,
          old_entry.pacer_doc_id,
          old_entry.raw_json,
          old_entry.last_synced_at,
          old_entry.created_at,
          old_entry.updated_at
        FROM ${alias}.docket_entries old_entry
        JOIN ${alias}.cases old_case ON old_case.id = old_entry.case_id
        JOIN temp.restore_case_keys restore_case ON restore_case.source_case_key = old_case.source_case_key
        JOIN cases live_case ON live_case.source_case_key = old_case.source_case_key
        LEFT JOIN docket_entries live_entry ON live_entry.source_entry_key = old_entry.source_entry_key
        WHERE live_entry.id IS NULL
      `);

      const safeFastPathUpdateStatement = this.db.prepare(`
        UPDATE cases
        SET ${CASE_FAST_PATH_SET_SQL}
        WHERE id = ?
      `);

      let restoredCases = 0;
      let restoredEntries = 0;
      await this.batchMutations(async () => {
        restoredCases = Number(restoreCasesStatement.run()?.changes || 0);
        restoredEntries = Number(restoreEntriesStatement.run()?.changes || 0);
      });

      const affectedCaseIds = this.db
        .prepare(`
          SELECT live_case.id
          FROM cases live_case
          JOIN temp.restore_case_keys restore_case ON restore_case.source_case_key = live_case.source_case_key
          ORDER BY live_case.id ASC
        `)
        .all()
        .map((row) => Number(row.id || 0))
        .filter(Boolean);

      for (const caseId of affectedCaseIds) {
        this.refreshCaseDocketSummary(caseId, { touchUpdatedAt: false });
        safeFastPathUpdateStatement.run(caseId);
      }
      const fts = this.rebuildCaseSearchFtsIfNeeded();
      this.invalidateCaseViews();

      return {
        dryRun: false,
        sourceDbPath: resolvedSourceDbPath,
        missingCaseCount,
        selectedCaseCount,
        retainedOnly: Boolean(retainedOnly),
        retainedByCurrentRules,
        missingEntryCount,
        restoredCases,
        restoredEntries,
        affectedCases: affectedCaseIds.length,
        rebuiltFts: Boolean(fts.rebuilt),
        sample
      };
    } finally {
      try {
        this.db.exec("DROP TABLE IF EXISTS temp.restore_case_keys;");
      } catch {
        // ignore temp table cleanup failures
      }
      if (attached) {
        this.db.exec(`DETACH DATABASE ${alias};`);
      }
    }
  }

  inspectMissingFromBackup({
    sourceDbPath,
    limit = 0,
    retainedOnly = false
  } = {}) {
    const resolvedSourceDbPath = path.resolve(String(sourceDbPath || "").trim());
    if (!resolvedSourceDbPath) {
      throw new Error("sourceDbPath is required");
    }
    if (!fs.existsSync(resolvedSourceDbPath)) {
      throw new Error(`backup database not found: ${resolvedSourceDbPath}`);
    }
    if (resolvedSourceDbPath === this.dbPath) {
      throw new Error("backup database must differ from the live database");
    }

    const alias = "inspect_source";
    const attachSql = `ATTACH DATABASE ${sqliteStringLiteral(resolvedSourceDbPath)} AS ${alias};`;
    const limitClause = Number(limit || 0) > 0 ? ` LIMIT ${Math.max(Number(limit || 0), 1)}` : "";
    let attached = false;

    try {
      this.db.exec(attachSql);
      attached = true;
      const missingCaseCount = Number(
        this.db
          .prepare(`
            SELECT COUNT(*) AS n
            FROM ${alias}.cases old_case
            LEFT JOIN cases live_case ON live_case.source_case_key = old_case.source_case_key
            WHERE live_case.id IS NULL
          `)
          .get()?.n || 0
      );

      let rows = this.db
        .prepare(`
          SELECT old_case.*
          FROM ${alias}.cases old_case
          LEFT JOIN cases live_case ON live_case.source_case_key = old_case.source_case_key
          WHERE live_case.id IS NULL
          ORDER BY old_case.id ASC${limitClause}
        `)
        .all()
        .map((row) => buildCaseView(row));

      const reasonCounts = {};
      const primarySourceCounts = {};
      let watchlistFlagCount = 0;
      let troFlagCount = 0;
      let scheduleACount = 0;
      let sellerWatchCount = 0;
      let retainedByCurrentRules = 0;
      if (retainedOnly) {
        rows = rows.filter((row) => getCaseRetentionDecision(row).retain);
      }
      const sample = [];

      for (const row of rows) {
        const retention = getCaseRetentionDecision(row);
        const primarySource = String(row.primary_source || "").trim() || "unknown";
        primarySourceCounts[primarySource] = (primarySourceCounts[primarySource] || 0) + 1;

        if (row.is_watchlist) {
          watchlistFlagCount += 1;
        }
        if (row.is_tro) {
          troFlagCount += 1;
        }
        if (row.is_schedule_a) {
          scheduleACount += 1;
        }
        if (row.is_seller_watch) {
          sellerWatchCount += 1;
        }

        if (retention.retain) {
          retainedByCurrentRules += 1;
        } else {
          for (const reason of retention.reasons) {
            reasonCounts[reason] = (reasonCounts[reason] || 0) + 1;
          }
        }

        if (sample.length < 25) {
          sample.push({
            id: row.id,
            docket_number: row.docket_number,
            case_name: row.case_name,
            court_id: row.court_id,
            primary_source: row.primary_source,
            is_watchlist: row.is_watchlist,
            is_tro: row.is_tro,
            is_schedule_a: row.is_schedule_a,
            is_seller_watch: row.is_seller_watch,
            reasons: retention.reasons
          });
        }
      }

      return {
        sourceDbPath: resolvedSourceDbPath,
        missingCaseCount,
        inspectedCaseCount: rows.length,
        retainedOnly: Boolean(retainedOnly),
        retainedByCurrentRules,
        reasonCounts,
        primarySourceCounts,
        watchlistFlagCount,
        troFlagCount,
        scheduleACount,
        sellerWatchCount,
        sample
      };
    } finally {
      if (attached) {
        this.db.exec(`DETACH DATABASE ${alias};`);
      }
    }
  }

  invalidateCaseViews() {
    if (this.deferInvalidationDepth > 0) {
      this.pendingInvalidation = true;
      return;
    }

    this.caseCacheVersion += 1;
    this.caseViewCache.clear();
    this.caseDetailCache.clear();
    this.listPayloadCache.clear();
    this.caseIdentityCache = null;
  }

  async batchMutations(task) {
    this.deferInvalidationDepth += 1;
    try {
      return await task();
    } finally {
      this.deferInvalidationDepth = Math.max(0, this.deferInvalidationDepth - 1);
      if (this.deferInvalidationDepth === 0 && this.pendingInvalidation) {
        this.pendingInvalidation = false;
        this.invalidateCaseViews();
      }
    }
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
      `${normalizeCourtLookupText(courtName)}|${docketKey}`
    ].filter((value) => !value.startsWith("|"));

    for (const key of candidateKeys) {
      const row = index.get(key);
      if (row) {
        return row;
      }
    }

    return null;
  }

  getCaseByCourtListenerDocketId(docketId) {
    const normalized = Number(docketId || 0);
    if (!normalized) {
      return null;
    }

    const row = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE courtlistener_docket_id = ?
        ORDER BY COALESCE(priority_activity_at, ${CASE_PRIORITY_ACTIVITY_SQL}) DESC, updated_at DESC
        LIMIT 1
      `)
      .get(normalized);

    return row ? buildCaseView(row) : null;
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
    if (cached && cached.version === this.caseCacheVersion && cached.expiresAt > Date.now()) {
      return cached.rows;
    }

    const rows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date_filed >= ?
        ORDER BY COALESCE(priority_activity_at, ${CASE_PRIORITY_ACTIVITY_SQL}) DESC, updated_at DESC
      `)
      .all(cacheKey)
      .map(buildCaseView);

    const collapsedRows = collapseDuplicateCases(rows);

    this.caseViewCache.set(cacheKey, {
      version: this.caseCacheVersion,
      expiresAt: Date.now() + CASE_VIEW_CACHE_TTL_MS,
      rows: collapsedRows
    });

    return collapsedRows;
  }

  getRawCaseViews(startDate = "2025-01-01") {
    return this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date_filed >= ?
        ORDER BY COALESCE(priority_activity_at, ${CASE_PRIORITY_ACTIVITY_SQL}) DESC, updated_at DESC
      `)
      .all(String(startDate || "2025-01-01"))
      .map(buildCaseView);
  }

  getDuplicateCaseGroups(limit = 25, {
    startDate = "2025-01-01",
    category = "watchlist",
    civilOnly = true,
    excludeBankruptcy = true
  } = {}) {
    const groups = new Map();

    for (const row of this.getRawCaseViews(startDate)) {
      const key = buildCanonicalCaseGroupKey(row);
      if (!key) {
        continue;
      }

      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(row);
    }

    return [...groups.values()]
      .filter((group) => group.length > 1)
      .map((group) => {
        const ordered = [...group].sort(compareCaseRowsForCanonicalChoice);
        const canonical = mergeDuplicateCaseGroup(ordered);
        return {
          canonical,
          rows: ordered,
          duplicateCount: ordered.length - 1
        };
      })
      .filter((group) => !category || this.matchesCategory(group.canonical, category))
      .filter((group) => !civilOnly || isCivilLike(group.canonical))
      .filter((group) => !excludeBankruptcy || !isBankruptcyLike(group.canonical))
      .sort((left, right) => {
        const troDiff =
          (Number(Boolean(right.canonical.insights?.is_tro_case)) + Number(Boolean(right.canonical.insights?.is_schedule_a_case)) + Number(Boolean(right.canonical.insights?.is_seller_case))) -
          (Number(Boolean(left.canonical.insights?.is_tro_case)) + Number(Boolean(left.canonical.insights?.is_schedule_a_case)) + Number(Boolean(left.canonical.insights?.is_seller_case)));
        if (troDiff !== 0) {
          return troDiff;
        }

        if (left.duplicateCount !== right.duplicateCount) {
          return right.duplicateCount - left.duplicateCount;
        }

        if (Number(left.canonical.docket_count || 0) !== Number(right.canonical.docket_count || 0)) {
          return Number(right.canonical.docket_count || 0) - Number(left.canonical.docket_count || 0);
        }

        return compareIsoDesc(
          left.canonical.latest_docket_filed_at || left.canonical.date_filed || left.canonical.updated_at,
          right.canonical.latest_docket_filed_at || right.canonical.date_filed || right.canonical.updated_at
        );
      })
      .slice(0, Math.max(1, Number(limit || 25)));
  }

  async reconcileDuplicateCases({
    startDate = "2025-01-01",
    category = "watchlist",
    limit = 100
  } = {}) {
    const groups = this.getDuplicateCaseGroups(limit, {
      startDate,
      category,
      civilOnly: true,
      excludeBankruptcy: true
    });

    let groupsProcessed = 0;
    let casesMerged = 0;
    let entriesMoved = 0;

    await this.batchMutations(async () => {
      for (const group of groups) {
        const ordered = group.rows;
        const canonicalSeed = ordered[0];
        const canonical = mergeDuplicateCaseGroup(ordered);
        const duplicateIds = ordered
          .slice(1)
          .map((row) => Number(row.id))
          .filter((value) => Number.isFinite(value) && value > 0);

        if (!duplicateIds.length) {
          continue;
        }

        const savedCase = this.upsertCase({
          source_case_key: canonicalSeed.source_case_key,
          primary_source: canonical.primary_source,
          source_case_id: canonicalSeed.source_case_id,
          courtlistener_docket_id: canonical.courtlistener_docket_id,
          pacer_case_id: canonical.pacer_case_id,
          court_id: canonical.court_id,
          court_name: canonical.court_name,
          case_name: canonical.case_name,
          case_name_zh: canonical.case_name_zh,
          docket_number: canonical.docket_number,
          date_filed: canonical.date_filed,
          date_terminated: canonical.date_terminated,
          cause: canonical.cause,
          nature_of_suit: canonical.nature_of_suit,
          status: canonical.status,
          tags_marker: buildTagsMarker(canonical.tags || []),
          docket_url: canonical.docket_url,
          source_urls: canonical.source_urls,
          plaintiffs: canonical.plaintiffs,
          defendants: canonical.defendants,
          recent_activity_summary: canonical.recent_activity_summary,
          recent_activity_summary_zh: canonical.recent_activity_summary_zh,
          latest_docket_filed_at: canonical.latest_docket_filed_at,
          latest_docket_number: canonical.latest_docket_number,
          docket_count: Number(canonical.docket_count || 0),
          last_seen_at: canonical.last_seen_at,
          last_synced_at: canonical.last_synced_at,
          last_docket_sync_at: canonical.last_docket_sync_at,
          last_translation_at: canonical.last_translation_at,
          raw: {
            ...(canonical.raw || {}),
            duplicate_reconcile: {
              reconciledAt: nowIso(),
              mergedCaseIds: duplicateIds,
              mergedSourceCaseKeys: ordered.slice(1).map((row) => row.source_case_key).filter(Boolean)
            }
          }
        });

        if (!savedCase) {
          const allIds = ordered
            .map((row) => Number(row.id))
            .filter((value) => Number.isFinite(value) && value > 0);
          if (allIds.length) {
            const allPlaceholders = allIds.map(() => "?").join(", ");
            this.db.prepare(`DELETE FROM cases WHERE id IN (${allPlaceholders})`).run(...allIds);
          }
          groupsProcessed += 1;
          casesMerged += ordered.length;
          continue;
        }

        const placeholders = duplicateIds.map(() => "?").join(", ");
        const movedResult = this.db
          .prepare(`UPDATE docket_entries SET case_id = ? WHERE case_id IN (${placeholders})`)
          .run(savedCase.id, ...duplicateIds);
        this.db.prepare(`DELETE FROM cases WHERE id IN (${placeholders})`).run(...duplicateIds);
        this.refreshCaseDocketSummary(savedCase.id, { touchUpdatedAt: false });

        groupsProcessed += 1;
        casesMerged += duplicateIds.length;
        entriesMoved += Number(movedResult?.changes || 0);
      }
    });

    return {
      groupsProcessed,
      casesMerged,
      entriesMoved
    };
  }

  upsertCase(record) {
    const existing = hydrateCase(
      this.db
        .prepare("SELECT * FROM cases WHERE source_case_key = ?")
        .get(record.source_case_key)
    );

    const mergedTags = this.mergeTagMarkers(existing?.tags_marker, record.tags_marker);
    const mergedUrls = this.mergeJsonArrays(existing?.source_urls, record.source_urls);
    const mergedPlaintiffs = this.mergeJsonArrays(existing?.plaintiffs, record.plaintiffs);
    const mergedDefendants = this.mergeJsonArrays(existing?.defendants, record.defendants);
    const timestamp = nowIso();
    const candidate = {
      ...(existing || {}),
      ...(record || {}),
      primary_source: pickPreferredPrimarySource(existing?.primary_source, record.primary_source),
      source_case_id:
        preferLongerNormalizedValue(existing?.source_case_id, record.source_case_id) ||
        existing?.source_case_id ||
        record.source_case_id ||
        null,
      courtlistener_docket_id: record.courtlistener_docket_id ?? existing?.courtlistener_docket_id ?? null,
      pacer_case_id: record.pacer_case_id ?? existing?.pacer_case_id ?? null,
      court_id: record.court_id || existing?.court_id || null,
      court_name: pickPreferredCourtName(existing?.court_name, record.court_name),
      case_name: pickPreferredCaseName(existing?.case_name, record.case_name, {
        plaintiffs: mergedPlaintiffs,
        defendants: mergedDefendants,
        existingSource: existing?.primary_source,
        incomingSource: record.primary_source
      }),
      case_name_zh: preferLongerNormalizedValue(existing?.case_name_zh, record.case_name_zh, normalizeText),
      docket_number: pickPreferredDocketNumber(existing?.docket_number, record.docket_number),
      docket_url: record.docket_url || existing?.docket_url || null,
      date_filed: record.date_filed || existing?.date_filed || null,
      date_terminated: record.date_terminated || existing?.date_terminated || null,
      cause: record.cause || existing?.cause || null,
      nature_of_suit: record.nature_of_suit || existing?.nature_of_suit || null,
      status: record.status || existing?.status || null,
      tags_marker: mergedTags,
      tags: mergedTags
        .split("|")
        .map((item) => item.trim())
        .filter(Boolean),
      source_urls: mergedUrls,
      plaintiffs: mergedPlaintiffs,
      defendants: mergedDefendants,
      recent_activity_summary: pickPreferredEntryDescription(existing?.recent_activity_summary, record.recent_activity_summary),
      recent_activity_summary_zh: preferLongerNormalizedValue(
        existing?.recent_activity_summary_zh,
        record.recent_activity_summary_zh,
        normalizeText
      ),
      latest_docket_filed_at: laterIso(record.latest_docket_filed_at, existing?.latest_docket_filed_at),
      latest_docket_number: higherOrderValue(record.latest_docket_number, existing?.latest_docket_number),
      docket_count: Math.max(Number(record.docket_count ?? 0), Number(existing?.docket_count ?? 0)),
      last_seen_at: laterIso(record.last_seen_at, existing?.last_seen_at),
      last_synced_at: laterIso(record.last_synced_at, existing?.last_synced_at),
      last_docket_sync_at: laterIso(record.last_docket_sync_at, existing?.last_docket_sync_at),
      last_translation_at: laterIso(record.last_translation_at, existing?.last_translation_at),
      raw: {
        ...mergeNestedObjects(existing?.raw || {}, record.raw || {})
      }
    };
    const retention = getCaseRetentionDecision(candidate);

    if (!retention.retain) {
      if (existing?.id) {
        this.db.prepare("DELETE FROM cases WHERE id = ?").run(existing.id);
        this.invalidateCaseViews();
      }
      return null;
    }

    const storedMetadata = buildCaseListMetadata(candidate, {
      insights: retention.insights,
      updatedAt: timestamp
    });

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
          is_watchlist,
          is_tro,
          is_schedule_a,
          is_seller_watch,
          priority_feed_row_count,
          priority_activity_at,
          docket_url,
          source_urls_json,
          plaintiffs_json,
          defendants_json,
          recent_activity_summary,
          recent_activity_summary_zh,
          search_text,
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
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(source_case_key) DO UPDATE SET
          primary_source = excluded.primary_source,
          source_case_id = excluded.source_case_id,
          courtlistener_docket_id = COALESCE(excluded.courtlistener_docket_id, cases.courtlistener_docket_id),
          pacer_case_id = COALESCE(excluded.pacer_case_id, cases.pacer_case_id),
          court_id = COALESCE(excluded.court_id, cases.court_id),
          court_name = COALESCE(excluded.court_name, cases.court_name),
          case_name = COALESCE(excluded.case_name, cases.case_name),
          case_name_zh = COALESCE(excluded.case_name_zh, cases.case_name_zh),
          docket_number = COALESCE(excluded.docket_number, cases.docket_number),
          date_filed = COALESCE(excluded.date_filed, cases.date_filed),
          date_terminated = COALESCE(excluded.date_terminated, cases.date_terminated),
          cause = COALESCE(excluded.cause, cases.cause),
          nature_of_suit = COALESCE(excluded.nature_of_suit, cases.nature_of_suit),
          status = COALESCE(excluded.status, cases.status),
          tags_marker = excluded.tags_marker,
          is_watchlist = excluded.is_watchlist,
          is_tro = excluded.is_tro,
          is_schedule_a = excluded.is_schedule_a,
          is_seller_watch = excluded.is_seller_watch,
          priority_feed_row_count = excluded.priority_feed_row_count,
          priority_activity_at = excluded.priority_activity_at,
          docket_url = COALESCE(excluded.docket_url, cases.docket_url),
          source_urls_json = excluded.source_urls_json,
          plaintiffs_json = excluded.plaintiffs_json,
          defendants_json = excluded.defendants_json,
          recent_activity_summary = COALESCE(excluded.recent_activity_summary, cases.recent_activity_summary),
          recent_activity_summary_zh = CASE
            WHEN excluded.recent_activity_summary IS NOT NULL AND excluded.recent_activity_summary <> cases.recent_activity_summary THEN NULL
            ELSE cases.recent_activity_summary_zh
          END,
          search_text = excluded.search_text,
          latest_docket_filed_at = COALESCE(excluded.latest_docket_filed_at, cases.latest_docket_filed_at),
          latest_docket_number = COALESCE(excluded.latest_docket_number, cases.latest_docket_number),
          docket_count = CASE
            WHEN excluded.docket_count > cases.docket_count THEN excluded.docket_count
            ELSE cases.docket_count
          END,
          last_seen_at = excluded.last_seen_at,
          last_synced_at = excluded.last_synced_at,
          last_docket_sync_at = COALESCE(excluded.last_docket_sync_at, cases.last_docket_sync_at),
          last_translation_at = COALESCE(excluded.last_translation_at, cases.last_translation_at),
          raw_json = excluded.raw_json,
          updated_at = excluded.updated_at
      `)
      .run(
        record.source_case_key,
        candidate.primary_source,
        candidate.source_case_id,
        candidate.courtlistener_docket_id ?? null,
        candidate.pacer_case_id ?? null,
        candidate.court_id ?? null,
        candidate.court_name ?? null,
        candidate.case_name ?? null,
        candidate.case_name_zh ?? null,
        candidate.docket_number ?? null,
        candidate.date_filed ?? null,
        candidate.date_terminated ?? null,
        candidate.cause ?? null,
        candidate.nature_of_suit ?? null,
        candidate.status ?? null,
        mergedTags,
        storedMetadata.is_watchlist,
        storedMetadata.is_tro,
        storedMetadata.is_schedule_a,
        storedMetadata.is_seller_watch,
        storedMetadata.priority_feed_row_count,
        storedMetadata.priority_activity_at,
        candidate.docket_url ?? null,
        toJson(mergedUrls, "[]"),
        toJson(mergedPlaintiffs, "[]"),
        toJson(mergedDefendants, "[]"),
        candidate.recent_activity_summary ?? null,
        candidate.recent_activity_summary_zh ?? null,
        storedMetadata.search_text,
        candidate.latest_docket_filed_at ?? null,
        candidate.latest_docket_number ?? null,
        candidate.docket_count ?? 0,
        candidate.last_seen_at ?? timestamp,
        candidate.last_synced_at ?? timestamp,
        candidate.last_docket_sync_at ?? null,
        candidate.last_translation_at ?? null,
        toJson(candidate.raw, "{}"),
        existing?.created_at ?? timestamp,
        timestamp
      );

    if (record.docket_count_exact === true) {
      this.db
        .prepare("UPDATE cases SET docket_count = ?, updated_at = ?, priority_activity_at = ? WHERE source_case_key = ?")
        .run(record.docket_count ?? 0, timestamp, timestamp, record.source_case_key);
    }

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
    const row = hydrateCase(this.db.prepare("SELECT * FROM cases WHERE id = ?").get(caseId));
    if (row) {
      const metadata = buildCaseListMetadata(row, { updatedAt: row.updated_at });
      this.db
        .prepare(`
          UPDATE cases
          SET is_watchlist = ?,
              is_tro = ?,
              is_schedule_a = ?,
              is_seller_watch = ?,
              priority_feed_row_count = ?,
              priority_activity_at = ?,
              search_text = ?
          WHERE id = ?
        `)
        .run(
          metadata.is_watchlist,
          metadata.is_tro,
          metadata.is_schedule_a,
          metadata.is_seller_watch,
          metadata.priority_feed_row_count,
          metadata.priority_activity_at,
          metadata.search_text,
          caseId
        );
    }
    this.invalidateCaseViews();
  }

  touchCaseDocketSync(caseId) {
    const timestamp = nowIso();
    this.db
      .prepare("UPDATE cases SET last_docket_sync_at = ?, updated_at = ?, priority_activity_at = ? WHERE id = ?")
      .run(timestamp, timestamp, timestamp, caseId);
    this.invalidateCaseViews();
  }

  listNonWatchlistCases({
    startDate = null,
    limit = 50
  } = {}) {
    const rows = this.db
      .prepare(`
        SELECT *
        FROM cases
        ORDER BY ${CASE_PRIORITY_ACTIVITY_SQL} DESC, updated_at DESC
      `)
      .all()
      .map(buildCaseView)
      .filter((row) => !startDate || !row.date_filed || String(row.date_filed) >= String(startDate))
      .filter((row) => !getCaseRetentionDecision(row).retain)
      .map((row) => ({
        id: row.id,
        docket_number: row.docket_number,
        case_name: row.case_name,
        court_id: row.court_id,
        court_name: row.court_name,
        date_filed: row.date_filed,
        reasons: getCaseRetentionDecision(row).reasons
      }));

    return limit > 0 ? rows.slice(0, limit) : rows;
  }

  async purgeNonWatchlistCases({
    startDate = null,
    limit = 0,
    dryRun = false
  } = {}) {
    const candidates = this.db
      .prepare(`
        SELECT *
        FROM cases
        ORDER BY ${CASE_PRIORITY_ACTIVITY_SQL} DESC, updated_at DESC
      `)
      .all()
      .map(buildCaseView)
      .filter((row) => !startDate || !row.date_filed || String(row.date_filed) >= String(startDate))
      .map((row) => ({
        row,
        retention: getCaseRetentionDecision(row)
      }))
      .filter(({ retention }) => !retention.retain);

    const selected = limit > 0 ? candidates.slice(0, limit) : candidates;
    const ids = selected
      .map(({ row }) => Number(row.id))
      .filter((value) => Number.isFinite(value) && value > 0);
    const reasonCounts = {};

    for (const { retention } of selected) {
      for (const reason of retention.reasons) {
        reasonCounts[reason] = (reasonCounts[reason] || 0) + 1;
      }
    }

    if (!ids.length) {
      return {
        dryRun,
        deletedCases: 0,
        deletedEntries: 0,
        reasonCounts,
        sample: []
      };
    }

    let deletedEntries = 0;
    for (const chunk of chunkArray(ids, 400)) {
      const placeholders = chunk.map(() => "?").join(", ");
      deletedEntries += Number(
        this.db.prepare(`SELECT COUNT(*) AS n FROM docket_entries WHERE case_id IN (${placeholders})`).get(...chunk)?.n || 0
      );
    }
    const sample = selected.slice(0, 25).map(({ row, retention }) => ({
      id: row.id,
      docket_number: row.docket_number,
      case_name: row.case_name,
      court_id: row.court_id,
      date_filed: row.date_filed,
      reasons: retention.reasons
    }));

    if (dryRun) {
      return {
        dryRun: true,
        deletedCases: ids.length,
        deletedEntries,
        reasonCounts,
        sample
      };
    }

    await this.batchMutations(async () => {
      for (const chunk of chunkArray(ids, 400)) {
        const placeholders = chunk.map(() => "?").join(", ");
        this.db.prepare(`DELETE FROM cases WHERE id IN (${placeholders})`).run(...chunk);
      }
    });

    return {
      dryRun: false,
      deletedCases: ids.length,
      deletedEntries,
      reasonCounts,
      sample
    };
  }

  deleteDocketEntriesBySource(caseId, primarySource) {
    const result = this.db
      .prepare("DELETE FROM docket_entries WHERE case_id = ? AND primary_source = ?")
      .run(caseId, primarySource);
    this.refreshCaseDocketSummary(caseId, { touchUpdatedAt: false });
    this.invalidateCaseViews();
    return Number(result?.changes || 0);
  }

  getRelatedCaseIds(caseLike) {
    if (!caseLike) {
      return [];
    }

    const canonicalGroupKey = buildCanonicalCaseGroupKey(caseLike);
    const { exactNeedles, suffixNeedles } = buildDocketSearchNeedles(caseLike.docket_number);
    const clauses = [];
    const params = [];

    for (const needle of exactNeedles) {
      clauses.push("lower(docket_number) = ?");
      params.push(needle);
    }

    for (const needle of suffixNeedles) {
      clauses.push("lower(docket_number) LIKE ?");
      params.push(`%${needle}`);
    }

    if (!clauses.length) {
      return [Number(caseLike.id)].filter((value) => Number.isFinite(value) && value > 0);
    }

    const matched = this.db
      .prepare(`
        SELECT id, docket_number, court_id, court_name
        FROM cases
        WHERE ${clauses.join(" OR ")}
        LIMIT 250
      `)
      .all(...params)
      .filter((candidate) => buildCanonicalCaseGroupKey(candidate) === canonicalGroupKey)
      .map((candidate) => Number(candidate.id))
      .filter((value) => Number.isFinite(value) && value > 0);

    return matched.length ? [...new Set(matched)] : [Number(caseLike.id)].filter((value) => Number.isFinite(value) && value > 0);
  }

  deleteDocketEntriesBySourceForRelatedCases(caseLike, primarySource) {
    const ids = this.getRelatedCaseIds(caseLike);
    if (!ids.length) {
      return 0;
    }

    const placeholders = ids.map(() => "?").join(", ");
    const result = this.db
      .prepare(`DELETE FROM docket_entries WHERE primary_source = ? AND case_id IN (${placeholders})`)
      .run(primarySource, ...ids);
    this.refreshCaseDocketSummaries(ids, { touchUpdatedAt: false });
    this.invalidateCaseViews();
    return Number(result?.changes || 0);
  }

  deleteDocketEntriesNotFromSourceForRelatedCases(caseLike, primarySource) {
    const ids = this.getRelatedCaseIds(caseLike);
    if (!ids.length) {
      return 0;
    }

    const placeholders = ids.map(() => "?").join(", ");
    const result = this.db
      .prepare(`DELETE FROM docket_entries WHERE primary_source <> ? AND case_id IN (${placeholders})`)
      .run(primarySource, ...ids);
    this.refreshCaseDocketSummaries(ids, { touchUpdatedAt: false });
    this.invalidateCaseViews();
    return Number(result?.changes || 0);
  }

  caseGroupHasPriorityFeedAuthority(caseLike) {
    const ids = this.getRelatedCaseIds(caseLike);
    if (!ids.length) {
      return false;
    }

    const placeholders = ids.map(() => "?").join(", ");
    const caseRows = this.db
      .prepare(`
        SELECT raw_json, source_urls_json
        FROM cases
        WHERE id IN (${placeholders})
      `)
      .all(...ids)
      .map((row) => hydrateCase(row));

    if (caseRows.some((row) => getPriorityFeedRowCount(row) > 0 || caseHasPriorityFeedUrl(row))) {
      return true;
    }

    const coverage = this.db
      .prepare(`
        SELECT COUNT(*) AS priority_entries
        FROM docket_entries
        WHERE primary_source = '${PRIORITY_FEED_ENTRY_SOURCE}'
          AND case_id IN (${placeholders})
      `)
      .get(...ids);

    return Number(coverage?.priority_entries || 0) > 0;
  }

  refreshCaseDocketSummaries(caseIds = [], { touchUpdatedAt = true, exactDocketCount = false } = {}) {
    const ids = [...new Set(
      caseIds
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value) && value > 0)
    )];
    if (!ids.length) {
      return { requested: 0, refreshed: 0 };
    }

    let refreshed = 0;
    for (const chunk of chunkArray(ids, 400)) {
      const placeholders = chunk.map(() => "?").join(", ");
      const caseRows = this.db
        .prepare(`SELECT * FROM cases WHERE id IN (${placeholders})`)
        .all(...chunk)
        .map((row) => hydrateCase(row));
      const coverageMap = this.getEntryCoverageForCaseIds(chunk);

      for (const caseRow of caseRows) {
        const coverage = coverageMap.get(Number(caseRow.id)) || null;
        if (!coverage) {
          continue;
        }

        const nextLatestFiledAt = laterIso(caseRow.latest_docket_filed_at, coverage.latestEntryFiledAt);
        const nextLatestNumber = higherOrderValue(
          caseRow.latest_docket_number,
          coverage.lastNumber ? String(coverage.lastNumber) : null
        );
        const knownExternalEntryCount = getKnownExternalEntryCount(caseRow);
        const computedDocketCount = Math.max(
          Number(coverage.totalEntries || 0),
          Number(coverage.lastNumber || 0),
          Number(knownExternalEntryCount || 0)
        );
        const nextDocketCount = exactDocketCount
          ? computedDocketCount
          : Math.max(Number(caseRow.docket_count || 0), computedDocketCount);
        const hasChanged =
          String(nextLatestFiledAt || "") !== String(caseRow.latest_docket_filed_at || "") ||
          String(nextLatestNumber || "") !== String(caseRow.latest_docket_number || "") ||
          nextDocketCount !== Number(caseRow.docket_count || 0);

        if (!hasChanged && !touchUpdatedAt) {
          continue;
        }

        const timestamp = nowIso();
        const nextPriorityActivityAt = getCasePriorityActivityAt({
          ...caseRow,
          updated_at: touchUpdatedAt || hasChanged ? timestamp : caseRow.updated_at,
          latest_docket_filed_at: nextLatestFiledAt ?? null
        });
        this.db
          .prepare(`
            UPDATE cases
            SET latest_docket_filed_at = ?,
                latest_docket_number = ?,
                docket_count = ?,
                priority_activity_at = ?,
                updated_at = CASE
                  WHEN ? = 1 THEN ?
                  ELSE updated_at
                END
            WHERE id = ?
          `)
          .run(
            nextLatestFiledAt ?? null,
            nextLatestNumber ?? null,
            nextDocketCount,
            nextPriorityActivityAt ?? null,
            touchUpdatedAt || hasChanged ? 1 : 0,
            timestamp,
            Number(caseRow.id)
          );
        refreshed += 1;
      }
    }

    if (refreshed > 0) {
      this.invalidateCaseViews();
    }

    return {
      requested: ids.length,
      refreshed
    };
  }

  refreshCaseDocketSummary(caseId, { touchUpdatedAt = true, exactDocketCount = false } = {}) {
    const normalizedCaseId = Number(caseId || 0);
    if (!Number.isFinite(normalizedCaseId) || normalizedCaseId <= 0) {
      return null;
    }

    this.refreshCaseDocketSummaries([normalizedCaseId], { touchUpdatedAt, exactDocketCount });
    return hydrateCase(this.db.prepare("SELECT * FROM cases WHERE id = ?").get(normalizedCaseId));
  }

  recomputeAllCaseDocketSummaries({
    batchSize = 500,
    limit = 0,
    touchUpdatedAt = false
  } = {}) {
    const normalizedBatchSize = Math.max(1, Number(batchSize || 500));
    const normalizedLimit = Math.max(0, Number(limit || 0));
    const rows = normalizedLimit > 0
      ? this.db.prepare("SELECT id FROM cases ORDER BY id ASC LIMIT ?").all(normalizedLimit)
      : this.db.prepare("SELECT id FROM cases ORDER BY id ASC").all();
    const ids = rows
      .map((row) => Number(row.id))
      .filter((value) => Number.isFinite(value) && value > 0);

    let refreshed = 0;
    for (let index = 0; index < ids.length; index += normalizedBatchSize) {
      const chunk = ids.slice(index, index + normalizedBatchSize);
        const result = this.refreshCaseDocketSummaries(chunk, { touchUpdatedAt });
      refreshed += Number(result.refreshed || 0);
    }

    return {
      totalCases: ids.length,
      refreshedCases: refreshed,
      batchSize: normalizedBatchSize,
      touchUpdatedAt: Boolean(touchUpdatedAt)
    };
  }

  findEquivalentDocketEntryCandidates(record) {
    const normalizedCaseId = Number(record?.case_id || 0);
    const primarySource = String(record?.primary_source || "").trim();
    if (!normalizedCaseId || !primarySource) {
      return [];
    }

    const rowsById = new Map();
    const pushRows = (rows = []) => {
      for (const row of rows) {
        const hydrated = hydrateEntry(row);
        if (hydrated?.id) {
          rowsById.set(Number(hydrated.id), hydrated);
        }
      }
    };

    const sourceEntryId = String(record?.source_entry_id || "").trim();
    if (sourceEntryId) {
      pushRows(
        this.db
          .prepare(`
            SELECT *
            FROM docket_entries
            WHERE case_id = ?
              AND primary_source = ?
              AND source_entry_id = ?
          `)
          .all(normalizedCaseId, primarySource, sourceEntryId)
      );
    }

    if (!rowsById.size && record?.filed_at) {
      pushRows(
        this.db
          .prepare(`
            SELECT *
            FROM docket_entries
            WHERE case_id = ?
              AND primary_source = ?
              AND filed_at = ?
          `)
          .all(normalizedCaseId, primarySource, record.filed_at)
      );
    }

    const targetKey = buildStoredEntryDeduplicationKey(record);
    return [...rowsById.values()]
      .filter((row) => !targetKey || buildStoredEntryDeduplicationKey(row) === targetKey)
      .sort(compareEntriesForCanonicalRow);
  }

  async dedupeStoredDocketEntries({
    startDate = "2025-01-01",
    caseId = 0,
    limit = 0,
    primarySource = null
  } = {}) {
    const normalizedCaseId = Number(caseId || 0);
    const normalizedLimit = Math.max(0, Number(limit || 0));
    const clauses = [];
    const params = [];

    if (normalizedCaseId > 0) {
      clauses.push("de.case_id = ?");
      params.push(normalizedCaseId);
    } else if (startDate) {
      clauses.push("date(c.date_filed) >= date(?)");
      params.push(String(startDate));
    }

    if (primarySource) {
      clauses.push("de.primary_source = ?");
      params.push(String(primarySource));
    }

    const whereClause = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const rows = this.db
      .prepare(`
        SELECT de.*
        FROM docket_entries de
        JOIN cases c ON c.id = de.case_id
        ${whereClause}
        ORDER BY de.case_id ASC, de.primary_source ASC, de.id ASC
      `)
      .all(...params)
      .map((row) => hydrateEntry(row));

    const grouped = new Map();
    for (const row of rows) {
      const key = buildStoredEntryDeduplicationKey(row);
      if (!key) {
        continue;
      }

      if (!grouped.has(key)) {
        grouped.set(key, []);
      }
      grouped.get(key).push(row);
    }

    const duplicateGroups = [...grouped.values()].filter((group) => group.length > 1);
    const selectedGroups = normalizedLimit > 0 ? duplicateGroups.slice(0, normalizedLimit) : duplicateGroups;
    let groupsProcessed = 0;
    let entriesDeleted = 0;
    const affectedCaseIds = new Set();

    await this.batchMutations(async () => {
      for (const group of selectedGroups) {
        const ordered = [...group].sort(compareEntriesForCanonicalRow);
        const duplicateIds = ordered
          .slice(1)
          .map((row) => Number(row.id))
          .filter((value) => Number.isFinite(value) && value > 0);

        if (!duplicateIds.length) {
          continue;
        }

        const placeholders = duplicateIds.map(() => "?").join(", ");
        this.db.prepare(`DELETE FROM docket_entries WHERE id IN (${placeholders})`).run(...duplicateIds);
        groupsProcessed += 1;
        entriesDeleted += duplicateIds.length;
        affectedCaseIds.add(Number(ordered[0].case_id));
      }

      if (affectedCaseIds.size) {
        this.refreshCaseDocketSummaries([...affectedCaseIds], {
          touchUpdatedAt: false,
          exactDocketCount: true
        });
      }
    });

    return {
      groupsProcessed,
      entriesDeleted,
      affectedCases: affectedCaseIds.size
    };
  }

  upsertDocketEntry(record) {
    const timestamp = nowIso();
    const exactExisting = hydrateEntry(
      this.db.prepare("SELECT * FROM docket_entries WHERE source_entry_key = ?").get(record.source_entry_key)
    );
    const candidatesById = new Map();
    for (const row of [exactExisting, ...this.findEquivalentDocketEntryCandidates(record)].filter(Boolean)) {
      candidatesById.set(Number(row.id), row);
    }

    const candidates = [...candidatesById.values()].sort(compareEntriesForCanonicalRow);
    const existing = candidates[0] || null;
    const duplicateIds = candidates
      .slice(1)
      .map((row) => Number(row.id))
      .filter((value) => Number.isFinite(value) && value > 0);
    const mergedRecord = buildMergedDocketEntryRecord(existing, record, timestamp);

    if (duplicateIds.length) {
      const placeholders = duplicateIds.map(() => "?").join(", ");
      this.db.prepare(`DELETE FROM docket_entries WHERE id IN (${placeholders})`).run(...duplicateIds);
    }

    if (existing?.id) {
      this.db
        .prepare(`
          UPDATE docket_entries
          SET case_id = ?,
              source_entry_key = ?,
              primary_source = ?,
              source_entry_id = ?,
              document_type = ?,
              entry_number = ?,
              document_number = ?,
              filed_at = ?,
              description = ?,
              description_zh = ?,
              absolute_url = ?,
              is_available = ?,
              page_count = ?,
              pacer_doc_id = ?,
              raw_json = ?,
              last_synced_at = ?,
              updated_at = ?
          WHERE id = ?
        `)
        .run(
          mergedRecord.case_id,
          mergedRecord.source_entry_key,
          mergedRecord.primary_source,
          mergedRecord.source_entry_id ?? null,
          mergedRecord.document_type ?? null,
          mergedRecord.entry_number ?? null,
          mergedRecord.document_number ?? null,
          mergedRecord.filed_at ?? null,
          mergedRecord.description ?? null,
          mergedRecord.description_zh ?? null,
          mergedRecord.absolute_url ?? null,
          mergedRecord.is_available ?? null,
          mergedRecord.page_count ?? null,
          mergedRecord.pacer_doc_id ?? null,
          toJson(mergedRecord.raw, "{}"),
          mergedRecord.last_synced_at ?? timestamp,
          timestamp,
          Number(existing.id)
        );
    } else {
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
        `)
        .run(
          mergedRecord.case_id,
          mergedRecord.source_entry_key,
          mergedRecord.primary_source,
          mergedRecord.source_entry_id ?? null,
          mergedRecord.document_type ?? null,
          mergedRecord.entry_number ?? null,
          mergedRecord.document_number ?? null,
          mergedRecord.filed_at ?? null,
          mergedRecord.description ?? null,
          mergedRecord.description_zh ?? null,
          mergedRecord.absolute_url ?? null,
          mergedRecord.is_available ?? null,
          mergedRecord.page_count ?? null,
          mergedRecord.pacer_doc_id ?? null,
          toJson(mergedRecord.raw, "{}"),
          mergedRecord.last_synced_at ?? timestamp,
          mergedRecord.created_at ?? timestamp,
          timestamp
        );
    }

    const savedEntry = hydrateEntry(
      existing?.id
        ? this.db.prepare("SELECT * FROM docket_entries WHERE id = ?").get(Number(existing.id))
        : this.db.prepare("SELECT * FROM docket_entries WHERE source_entry_key = ?").get(mergedRecord.source_entry_key)
    );

    const affectedCaseIds = new Set(
      [Number(record.case_id), Number(existing?.case_id)]
        .filter((value) => Number.isFinite(value) && value > 0)
    );
    if (affectedCaseIds.size) {
      this.refreshCaseDocketSummaries([...affectedCaseIds], {
        touchUpdatedAt: true,
        exactDocketCount: duplicateIds.length > 0
      });
    }

    this.invalidateCaseViews();

    return savedEntry;
  }

  updateEntryTranslation(entryId, translation) {
    this.db
      .prepare("UPDATE docket_entries SET description_zh = ?, updated_at = ? WHERE id = ?")
      .run(translation, nowIso(), entryId);
    this.invalidateCaseViews();
  }

  getFastPathDocketCases(startDate, rawSearch) {
    const { exactNeedles, suffixNeedles } = buildDocketSearchNeedles(rawSearch);
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

    const rows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date_filed >= ?
          AND (${clauses.join(" OR ")})
        ORDER BY docket_number DESC, COALESCE(priority_activity_at, ${CASE_PRIORITY_ACTIVITY_SQL}) DESC, updated_at DESC
        LIMIT 250
      `)
      .all(...params)
      .map(buildCaseView);

    return collapseDuplicateCases(rows);
  }

  getSlowPathTextSearchCases(startDate, rawSearch, { category = "all", selectedCourt = "", limit = 400 } = {}) {
    const rawNeedle = String(rawSearch || "").trim().toLowerCase();
    const searchNeedle = normalizeText(rawSearch);
    const docketNeedle = normalizeDocket(rawSearch);
    const needles = [...new Set([rawNeedle, searchNeedle, docketNeedle].filter(Boolean))];
    if (!needles.length) {
      return [];
    }

    const whereClauses = [`date_filed >= ?`];
    const params = [startDate];
    const categoryClause = this.buildCategoryWhereClause(category);
    if (categoryClause !== "1 = 1") {
      whereClauses.push(`(${categoryClause})`);
    }
    if (selectedCourt) {
      whereClauses.push(`court_id = ?`);
      params.push(selectedCourt);
    }

    const searchExpression = `
      lower(
        COALESCE(case_name, '') || ' ' ||
        COALESCE(case_name_zh, '') || ' ' ||
        COALESCE(docket_number, '') || ' ' ||
        COALESCE(court_name, '') || ' ' ||
        COALESCE(recent_activity_summary, '') || ' ' ||
        COALESCE(recent_activity_summary_zh, '') || ' ' ||
        COALESCE(plaintiffs_json, '') || ' ' ||
        COALESCE(defendants_json, '') || ' ' ||
        COALESCE(source_urls_json, '') || ' ' ||
        COALESCE(tags_marker, '')
      )
    `;
    whereClauses.push(`(${needles.map(() => `${searchExpression} LIKE ?`).join(" OR ")})`);
    params.push(...needles.map((needle) => `%${needle}%`));

    const rows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE ${whereClauses.join("\n          AND ")}
        ORDER BY docket_number DESC, COALESCE(priority_activity_at, ${CASE_PRIORITY_ACTIVITY_SQL}) DESC, updated_at DESC
        LIMIT ?
      `)
      .all(...params, Math.max(limit, 120))
      .map(buildCaseView);

    return collapseDuplicateCases(rows);
  }

  getFastPathTextSearchCases(startDate, rawSearch, { category = "all", selectedCourt = "", limit = 400 } = {}) {
    const matchQuery = buildFtsMatchQuery(rawSearch);
    if (!matchQuery) {
      return this.getSlowPathTextSearchCases(startDate, rawSearch, {
        category,
        selectedCourt,
        limit
      });
    }

    try {
      const whereClauses = [`c.date_filed >= ?`];
      const params = [matchQuery, startDate];
      const categoryClause = this.buildCategoryWhereClause(category, "c.");
      if (categoryClause !== "1 = 1") {
        whereClauses.push(`(${categoryClause})`);
      }
      if (selectedCourt) {
        whereClauses.push(`c.court_id = ?`);
        params.push(selectedCourt);
      }

      const rows = this.db
        .prepare(`
          SELECT c.*
          FROM cases_search_fts
          JOIN cases c ON c.id = cases_search_fts.rowid
          WHERE cases_search_fts MATCH ?
            AND ${whereClauses.join("\n            AND ")}
          ORDER BY c.docket_number DESC, bm25(cases_search_fts), COALESCE(c.priority_activity_at, ${CASE_PRIORITY_ACTIVITY_SQL.replaceAll("updated_at", "c.updated_at").replaceAll("latest_docket_filed_at", "c.latest_docket_filed_at").replaceAll("date_filed", "c.date_filed")}) DESC, c.updated_at DESC
          LIMIT ?
        `)
        .all(...params, Math.max(limit, 120))
        .map(buildCaseView);

      if (rows.length) {
        return collapseDuplicateCases(rows);
      }
    } catch {
      // fall through to the legacy slow scan for compatibility if FTS is unavailable or rebuilding.
    }

    return this.getSlowPathTextSearchCases(startDate, rawSearch, {
      category,
      selectedCourt,
      limit
    });
  }

  buildCategoryWhereClause(category, columnPrefix = "") {
    const column = (name) => `${columnPrefix}${name}`;
    if (category === "watchlist") {
      return `${column("is_watchlist")} = 1`;
    }

    if (category === "tro") {
      return `${column("is_tro")} = 1`;
    }

    if (category === "schedule_a") {
      return `${column("is_schedule_a")} = 1`;
    }

    if (category === "seller_watch") {
      return `${column("is_seller_watch")} = 1`;
    }

    return "1 = 1";
  }

  listCasesBySql({ startDate, pageSize, page, category, selectedCourt }) {
    const categoryClause = this.buildCategoryWhereClause(category);
    const baseWhere = [`date_filed >= ?`, `(${categoryClause})`];
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
        ORDER BY
          docket_number DESC,
          COALESCE(priority_activity_at, ${CASE_PRIORITY_ACTIVITY_SQL}) DESC,
          latest_docket_filed_at DESC,
          updated_at DESC,
          date_filed DESC,
          docket_count DESC,
          id DESC
        LIMIT ?
        OFFSET ?
      `)
      .all(...baseParams, pageSize, offset)
      .map(buildCaseView);
    const courts = this.db
      .prepare(`
        SELECT court_id, court_name, COUNT(*) AS total
        FROM cases
        WHERE date_filed >= ?
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
    if (cached && cached.version === this.caseCacheVersion && cached.expiresAt > Date.now()) {
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
        expiresAt: Date.now() + LIST_PAYLOAD_CACHE_TTL_MS,
        value: payload
      });

      return cloneListPayload(payload);
    }

    const isDocketSearch = looksLikeDocketSearch(rawSearch);
    const fastRows = isDocketSearch
      ? this.getFastPathDocketCases(startDate, rawSearch)
      : this.getFastPathTextSearchCases(startDate, rawSearch, {
          category,
          selectedCourt
        });
    const rows = fastRows.length
      ? fastRows
      : isDocketSearch
        ? this.getHydratedCases(startDate)
        : [];

    const categoryFiltered = fastRows.length ? rows : rows.filter((row) => this.matchesCategory(row, category));
    const searchFiltered = searchTerm
      ? (fastRows.length
          ? categoryFiltered
          : categoryFiltered.filter((row) => this.matchesSearch(row, searchTerm)))
          .sort((left, right) => this.compareSearchPriority(left, right, searchTerm))
      : categoryFiltered;

    const courtFacetRows =
      searchFiltered.length > 200 && !selectedCourt
        ? searchFiltered.slice(0, 200)
        : searchFiltered;
    const courtsMap = new Map();
    for (const row of courtFacetRows) {
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
      expiresAt: Date.now() + LIST_PAYLOAD_CACHE_TTL_MS,
      value: payload
    });

    return cloneListPayload(payload);
  }

  getCase(id, { recentEntriesLimit = 0 } = {}) {
    const normalizedLimit = Math.max(0, Number(recentEntriesLimit || 0));
    const cacheKey = `${Number(id)}:${normalizedLimit || "full"}`;
    const cached = this.caseDetailCache.get(cacheKey);
    if (cached && cached.version === this.caseCacheVersion && cached.expiresAt > Date.now()) {
      return cached.value;
    }

    const row = hydrateCase(this.db.prepare("SELECT * FROM cases WHERE id = ?").get(id));
    if (!row) {
      return null;
    }

    const canonicalGroupKey = buildCanonicalCaseGroupKey(row);
    const { exactNeedles, suffixNeedles } = buildDocketSearchNeedles(row.docket_number);
    const clauses = [];
    const params = [];

    for (const needle of exactNeedles) {
      clauses.push("lower(docket_number) = ?");
      params.push(needle);
    }

    for (const needle of suffixNeedles) {
      clauses.push("lower(docket_number) LIKE ?");
      params.push(`%${needle}`);
    }

    const matchedPeerRows = clauses.length
      ? this.db
          .prepare(`
            SELECT *
            FROM cases
            WHERE ${clauses.join(" OR ")}
            LIMIT 250
          `)
          .all(...params)
          .map(hydrateCase)
          .filter((candidate) => buildCanonicalCaseGroupKey(candidate) === canonicalGroupKey)
      : [row];
    const peerRows = matchedPeerRows.length ? matchedPeerRows : [row];

    const canonicalRow = mergeDuplicateCaseGroup(peerRows.map(buildCaseView));
    const peerIds = [...new Set(peerRows.map((candidate) => Number(candidate.id)).filter((value) => Number.isFinite(value) && value > 0))];
    const placeholders = peerIds.map(() => "?").join(", ");
    const rawEntries = this.db
      .prepare(`
        SELECT *
        FROM docket_entries
        WHERE case_id IN (${placeholders})
        ORDER BY COALESCE(filed_at, created_at) DESC, id DESC
        ${normalizedLimit > 0 ? "LIMIT ?" : ""}
      `)
      .all(...peerIds, ...(normalizedLimit > 0 ? [normalizedLimit + 1] : []))
      .map(hydrateEntry);
    const entriesTruncated = normalizedLimit > 0 && rawEntries.length > normalizedLimit;
    const entries = entriesTruncated ? rawEntries.slice(0, normalizedLimit) : rawEntries;

    const displayEntries = dedupeEntries(entries);

    const detail = applyAuthoritativeDocketPreference({
      ...canonicalRow,
      entries: displayEntries,
      entries_truncated: entriesTruncated,
      insights: deriveCaseInsights({
        ...canonicalRow,
        entries: displayEntries
      })
    }, displayEntries);
    detail.insights = deriveCaseInsights(detail);

    this.caseDetailCache.set(cacheKey, {
      version: this.caseCacheVersion,
      expiresAt: Date.now() + CASE_DETAIL_CACHE_TTL_MS,
      value: detail
    });

    return detail;
  }

  getCasesNeedingDocketSync(limit) {
    const candidateRows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE courtlistener_docket_id IS NOT NULL
          AND date(date_filed) >= date('2025-01-01')
        ORDER BY COALESCE(last_docket_sync_at, '1970-01-01T00:00:00.000Z') ASC,
                 CASE
                   WHEN tags_marker LIKE '%|seller_tro|%'
                     OR tags_marker LIKE '%|tro|%'
                     OR tags_marker LIKE '%|schedule_a|%'
                   THEN 1
                   ELSE 0
                 END DESC,
                 CASE
                   WHEN lower(court_name) LIKE '%bankruptcy%'
                     OR lower(docket_number) LIKE '%-bk-%'
                   THEN 1
                   ELSE 0
                 END ASC,
                 MAX(
                   0,
                   CAST(REPLACE(COALESCE(latest_docket_number, '0'), '.0', '') AS INTEGER) - COALESCE(docket_count, 0)
                 ) DESC,
                 ${CASE_PRIORITY_ACTIVITY_SQL} DESC
        LIMIT ?
      `)
      .all(Math.max(limit * 12, 120))
      .map(hydrateCase);

    const entryCounts = this.getEntryCoverageForCaseIds(candidateRows.map((row) => row.id));
    const prioritizedRows = candidateRows.slice().sort((left, right) => {
      const leftCoverage = entryCounts.get(Number(left.id)) || {};
      const rightCoverage = entryCounts.get(Number(right.id)) || {};
      const leftGapPriority = isGapPriorityCase(left) ? Number(leftCoverage.gapPriorityScore || 0) : 0;
      const rightGapPriority = isGapPriorityCase(right) ? Number(rightCoverage.gapPriorityScore || 0) : 0;
      const leftIsCurrentYear = isCurrentPriorityYearCase(left);
      const rightIsCurrentYear = isCurrentPriorityYearCase(right);
      const leftCurrentYearWeight = getCurrentYearGapPriorityWeight(left, leftCoverage, Number(left.docket_count || 0));
      const rightCurrentYearWeight = getCurrentYearGapPriorityWeight(right, rightCoverage, Number(right.docket_count || 0));

      if (leftIsCurrentYear !== rightIsCurrentYear) {
        return leftIsCurrentYear ? -1 : 1;
      }

      if (leftIsCurrentYear && rightIsCurrentYear) {
        const currentYearActivityCompare = compareCaseActivityDesc(
          getCasePriorityActivityAt(left),
          getCasePriorityActivityAt(right)
        );
        if (currentYearActivityCompare !== 0) {
          return currentYearActivityCompare;
        }
      }

      if (leftCurrentYearWeight !== rightCurrentYearWeight) {
        return rightCurrentYearWeight - leftCurrentYearWeight;
      }

      if (leftGapPriority !== rightGapPriority) {
        return rightGapPriority - leftGapPriority;
      }

      const leftSyncAt = String(left.last_docket_sync_at || "1970-01-01T00:00:00.000Z");
      const rightSyncAt = String(right.last_docket_sync_at || "1970-01-01T00:00:00.000Z");
      if (leftSyncAt !== rightSyncAt) {
        return leftSyncAt.localeCompare(rightSyncAt);
      }

      const leftTagged = left.tags_marker?.includes("|seller_tro|") || left.tags_marker?.includes("|tro|") || left.tags_marker?.includes("|schedule_a|");
      const rightTagged = right.tags_marker?.includes("|seller_tro|") || right.tags_marker?.includes("|tro|") || right.tags_marker?.includes("|schedule_a|");
      if (leftTagged !== rightTagged) {
        return leftTagged ? -1 : 1;
      }

      if (isBankruptcyLike(left) !== isBankruptcyLike(right)) {
        return isBankruptcyLike(left) ? 1 : -1;
      }

      const leftLatestGap = Math.max(0, parseDocketNumber(left.latest_docket_number) - Number(left.docket_count || 0));
      const rightLatestGap = Math.max(0, parseDocketNumber(right.latest_docket_number) - Number(right.docket_count || 0));
      if (leftLatestGap !== rightLatestGap) {
        return rightLatestGap - leftLatestGap;
      }

      return compareCaseActivityDesc(
        getCasePriorityActivityAt(left),
        getCasePriorityActivityAt(right)
      );
    });

    const selected = [];
    const groupAuthorityCache = new Map();

    for (const row of prioritizedRows) {
      if (selected.length >= limit) {
        break;
      }

      const groupKey = buildCanonicalCaseGroupKey(row) || `id:${row.id}`;
      let hasPriorityFeedAuthority = groupAuthorityCache.get(groupKey);
      if (hasPriorityFeedAuthority === undefined) {
        hasPriorityFeedAuthority = this.caseGroupHasPriorityFeedAuthority(row);
        groupAuthorityCache.set(groupKey, hasPriorityFeedAuthority);
      }

      if (!hasPriorityFeedAuthority) {
        selected.push(row);
      }
    }

    return selected;
  }

  getCasesNeedingCourtListenerFollowUp(limit, { recentFiledWindowDays = 2, startDate = "2025-01-01" } = {}) {
    const recentFiledCutoff = Date.now() - recentFiledWindowDays * 24 * 60 * 60 * 1000;
    const recentFiledCutoffIso = new Date(recentFiledCutoff).toISOString();
    const candidatePoolSize = Math.max(limit * 40, 240);

    const candidateRows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date(date_filed) >= date(?)
          AND docket_number IS NOT NULL
          AND TRIM(docket_number) <> ''
          AND ${CASE_PRIORITY_ACTIVITY_SQL} >= ?
        ORDER BY ${CASE_PRIORITY_ACTIVITY_SQL} DESC
        LIMIT ?
      `)
      .all(String(startDate || "2025-01-01"), recentFiledCutoffIso, candidatePoolSize)
      .map(hydrateCase);

    const entryCounts = this.getEntryCoverageForCaseIds(candidateRows.map((row) => row.id));

    return candidateRows
      .map((row) => {
        const coverage = entryCounts.get(Number(row.id)) || {
          totalEntries: 0,
          courtlistenerEntries: 0,
          latestEntryFiledAt: null,
          gapPriorityScore: 0,
          hasContinuityGap: false
        };
        const activityAtRaw = getCasePriorityActivityAt(row);
        const activityAtMs = Number.isFinite(Date.parse(activityAtRaw || "")) ? Date.parse(activityAtRaw || "") : 0;
        const createdAtMs = Number.isFinite(Date.parse(row.created_at || "")) ? Date.parse(row.created_at || "") : 0;
        const isRecentFiledFollowUp = createdAtMs > 0 && createdAtMs < recentFiledCutoff && activityAtMs >= recentFiledCutoff;
        const hasCivilDocketNumber = /\b(?:\d+:)?\d{2}-cv-\d{3,6}\b/i.test(String(row.docket_number || ""));
        const hasTargetTags =
          row.tags_marker?.includes("|seller_tro|") ||
          row.tags_marker?.includes("|tro|") ||
          row.tags_marker?.includes("|schedule_a|") ||
          row.insights?.is_seller_case ||
          row.insights?.is_tro_case ||
          row.insights?.is_schedule_a_case;
        const latestNumber = parseDocketNumber(row.latest_docket_number);
        const expectedEntries = Math.max(
          Number(row.docket_count || 0),
          latestNumber,
          row.insights?.is_seller_case ? 12 : 6
        );
        const totalEntries = Number(coverage.totalEntries || 0);
        const gap = Math.max(0, expectedEntries - totalEntries);
        const hasContinuityGap = Boolean(coverage.hasContinuityGap);
        const hasActivityAheadOfTimeline = isActivityAheadOfTimeline(activityAtRaw, coverage.latestEntryFiledAt);
        const hasCourtListenerDocket = Number(row.courtlistener_docket_id || 0) > 0;
        const shouldSync =
          hasCivilDocketNumber &&
          isRecentFiledFollowUp &&
          hasTargetTags &&
          (hasActivityAheadOfTimeline || hasCourtListenerDocket || gap > 0 || hasContinuityGap);

        return {
          row,
          shouldSync,
          hasTargetTags: Boolean(hasTargetTags),
          hasActivityAheadOfTimeline,
          hasCourtListenerDocket,
          hasContinuityGap,
          isCurrentYearCase: isCurrentPriorityYearCase(row),
          gap,
          gapPriorityScore: Number(coverage.gapPriorityScore || 0),
          totalEntries,
          activityAtRaw
        };
      })
      .filter((item) => item.shouldSync)
      .sort((left, right) => {
        if (left.isCurrentYearCase !== right.isCurrentYearCase) {
          return left.isCurrentYearCase ? -1 : 1;
        }

        if (left.hasTargetTags !== right.hasTargetTags) {
          return left.hasTargetTags ? -1 : 1;
        }

        if (left.hasCourtListenerDocket !== right.hasCourtListenerDocket) {
          return left.hasCourtListenerDocket ? -1 : 1;
        }

        if (left.hasActivityAheadOfTimeline !== right.hasActivityAheadOfTimeline) {
          return left.hasActivityAheadOfTimeline ? -1 : 1;
        }

        if (left.hasContinuityGap !== right.hasContinuityGap) {
          return left.hasContinuityGap ? -1 : 1;
        }

        if (left.gapPriorityScore !== right.gapPriorityScore) {
          return right.gapPriorityScore - left.gapPriorityScore;
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

  getCasesNeedingCourtListenerAlertSync(limit, { startDate = "2025-01-01", staleAfterHours = 24, force = false } = {}) {
    const staleBeforeMs = Date.now() - Math.max(Number(staleAfterHours || 0), 1) * 60 * 60 * 1000;
    const poolSize = Math.max(Number(limit || 0) * 40, 400);

    return this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE courtlistener_docket_id IS NOT NULL
          AND date(date_filed) >= date(?)
        ORDER BY ${CASE_PRIORITY_ACTIVITY_SQL} DESC, updated_at DESC
        LIMIT ?
      `)
      .all(String(startDate || "2025-01-01"), Math.max(poolSize, Math.max(Number(limit || 0), 1)))
      .map(hydrateCase)
      .filter((row) => Number(row.courtlistener_docket_id || 0) > 0)
      .filter((row) => {
        if (force) {
          return true;
        }

        const alertState = row.raw?.courtlistener?.docketAlert || {};
        const syncedAtMs = Date.parse(String(alertState.syncedAt || alertState.updatedAt || alertState.dateModified || ""));
        const alertType = Number(alertState.alertType ?? alertState.alert_type ?? 0);
        const hasRemoteAlert = Number(alertState.id || 0) > 0;
        if (!hasRemoteAlert || alertType !== 1) {
          return true;
        }

        return !Number.isFinite(syncedAtMs) || syncedAtMs <= staleBeforeMs;
      })
      .sort((left, right) => {
        const leftAlert = left.raw?.courtlistener?.docketAlert || {};
        const rightAlert = right.raw?.courtlistener?.docketAlert || {};
        const leftActive = Number(leftAlert.alertType ?? leftAlert.alert_type ?? 0) === 1;
        const rightActive = Number(rightAlert.alertType ?? rightAlert.alert_type ?? 0) === 1;
        if (leftActive !== rightActive) {
          return leftActive ? 1 : -1;
        }

        const leftSyncedAt = String(leftAlert.syncedAt || leftAlert.updatedAt || leftAlert.dateModified || "1970-01-01T00:00:00.000Z");
        const rightSyncedAt = String(rightAlert.syncedAt || rightAlert.updatedAt || rightAlert.dateModified || "1970-01-01T00:00:00.000Z");
        if (leftSyncedAt !== rightSyncedAt) {
          return leftSyncedAt.localeCompare(rightSyncedAt);
        }

        return compareCaseActivityDesc(
          getCasePriorityActivityAt(left),
          getCasePriorityActivityAt(right)
        );
      })
      .slice(0, Math.max(Number(limit || 0), 1));
  }

  getCasesWithActivityAheadOfTimeline(limit, { recentFiledWindowDays = 2, startDate = "2025-01-01" } = {}) {
    const recentFiledCutoff = Date.now() - recentFiledWindowDays * 24 * 60 * 60 * 1000;
    const recentFiledCutoffIso = new Date(recentFiledCutoff).toISOString();
    const candidatePoolSize = Math.max(limit * 80, 360);

    const candidateRows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date(date_filed) >= date(?)
          AND docket_number IS NOT NULL
          AND TRIM(docket_number) <> ''
          AND ${CASE_PRIORITY_ACTIVITY_SQL} >= ?
        ORDER BY ${CASE_PRIORITY_ACTIVITY_SQL} DESC
        LIMIT ?
      `)
      .all(String(startDate || "2025-01-01"), recentFiledCutoffIso, candidatePoolSize)
      .map(hydrateCase);

    const entryCounts = this.getEntryCoverageForCaseIds(candidateRows.map((row) => row.id));

    return candidateRows
      .map((row) => {
        const coverage = entryCounts.get(Number(row.id)) || {
          totalEntries: 0,
          latestEntryFiledAt: null
        };
        const activityAtRaw = getCasePriorityActivityAt(row);
        const activityAtMs = Number.isFinite(Date.parse(activityAtRaw || "")) ? Date.parse(activityAtRaw || "") : 0;
        const createdAtMs = Number.isFinite(Date.parse(row.created_at || "")) ? Date.parse(row.created_at || "") : 0;
        const isRecentFiledFollowUp = createdAtMs > 0 && createdAtMs < recentFiledCutoff && activityAtMs >= recentFiledCutoff;
        const hasCivilDocketNumber = isCivilLike(row);
        const hasTargetSignals = hasTargetWatchlistSignals(row, row.insights);
        const hasActivityAheadOfTimeline = isActivityAheadOfTimeline(activityAtRaw, coverage.latestEntryFiledAt);
        const hasPriorityFeedSignals =
          hasConcretePriorityFeedLink(row) ||
          getPriorityFeedRowCount(row) > 0 ||
          hasTrackedLawFirmSource(row) ||
          hasTargetSignals;
        const hasCourtListenerPath = Boolean(row.courtlistener_docket_id || row.docket_number || row.case_name);
        const activityLeadMs =
          Number.isFinite(activityAtMs) && coverage.latestEntryFiledAt
            ? Math.max(0, activityAtMs - Date.parse(String(coverage.latestEntryFiledAt)))
            : hasActivityAheadOfTimeline
              ? recentFiledWindowDays * 24 * 60 * 60 * 1000
              : 0;

        return {
          row,
          shouldSync:
            hasCivilDocketNumber &&
            isRecentFiledFollowUp &&
            hasTargetSignals &&
            hasActivityAheadOfTimeline &&
            (hasPriorityFeedSignals || hasCourtListenerPath),
          hasPriorityFeedSignals,
          hasCourtListenerPath,
          isCurrentYearCase: isCurrentPriorityYearCase(row),
          hasAuthoritativeSignal: hasAuthoritativeTargetSignal(row),
          activityLeadMs,
          totalEntries: Number(coverage.totalEntries || 0),
          activityAtRaw
        };
      })
      .filter((item) => item.shouldSync)
      .sort((left, right) => {
        if (left.isCurrentYearCase !== right.isCurrentYearCase) {
          return left.isCurrentYearCase ? -1 : 1;
        }

        if (left.hasAuthoritativeSignal !== right.hasAuthoritativeSignal) {
          return left.hasAuthoritativeSignal ? -1 : 1;
        }

        if (left.hasPriorityFeedSignals !== right.hasPriorityFeedSignals) {
          return left.hasPriorityFeedSignals ? -1 : 1;
        }

        if (left.hasCourtListenerPath !== right.hasCourtListenerPath) {
          return left.hasCourtListenerPath ? -1 : 1;
        }

        if (left.activityLeadMs !== right.activityLeadMs) {
          return right.activityLeadMs - left.activityLeadMs;
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

  getEntryCoverageForCaseIds(caseIds = []) {
    const ids = [...new Set(caseIds.map((value) => Number(value)).filter((value) => Number.isFinite(value) && value > 0))];
    if (!ids.length) {
      return new Map();
    }

    const chunkSize = 900;
    const coverage = new Map();
    const numericOrderSql = `
      CASE
        WHEN TRIM(COALESCE(NULLIF(document_number, ''), NULLIF(entry_number, ''), '')) GLOB '[0-9]*'
        THEN CAST(COALESCE(NULLIF(document_number, ''), NULLIF(entry_number, '')) AS INTEGER)
        ELSE NULL
      END
    `;

    for (let index = 0; index < ids.length; index += chunkSize) {
      const chunk = ids.slice(index, index + chunkSize);
      const placeholders = chunk.map(() => "?").join(", ");
      const rows = this.db
        .prepare(`
          SELECT
            case_id,
            COUNT(*) AS total_entries,
            SUM(CASE WHEN primary_source = 'courtlistener' THEN 1 ELSE 0 END) AS courtlistener_entries,
            SUM(CASE WHEN primary_source = '${PRIORITY_FEED_ENTRY_SOURCE}' THEN 1 ELSE 0 END) AS priority_entries,
            SUM(CASE WHEN primary_source = 'pacermonitor' THEN 1 ELSE 0 END) AS pacermonitor_entries,
            MAX(filed_at) AS latest_entry_filed_at,
            COUNT(DISTINCT ${numericOrderSql}) AS numbered_entries,
            MIN(${numericOrderSql}) AS first_number,
            MAX(${numericOrderSql}) AS last_number
          FROM docket_entries
          WHERE case_id IN (${placeholders})
          GROUP BY case_id
        `)
        .all(...chunk);

      for (const row of rows) {
        const baseCoverage = {
          totalEntries: Number(row.total_entries || 0),
          courtlistenerEntries: Number(row.courtlistener_entries || 0),
          priorityFeedEntries: Number(row.priority_entries || 0),
          pacermonitorEntries: Number(row.pacermonitor_entries || 0),
          latestEntryFiledAt: row.latest_entry_filed_at || null,
          numberedEntries: Number(row.numbered_entries || 0),
          firstNumber: Number(row.first_number || 0),
          lastNumber: Number(row.last_number || 0)
        };
        coverage.set(Number(row.case_id), {
          ...baseCoverage,
          ...getCoverageGapPressure(baseCoverage)
        });
      }
    }

    return coverage;
  }

  getCasesNeedingPriorityFeedSync(limit, staleAfterHours = 12, { preferKnownPriorityFeed = false, recentFiledWindowDays = 2 } = {}) {
    const staleBefore = Date.now() - staleAfterHours * 60 * 60 * 1000;
    const recentFiledFollowUpBefore = Date.now() - recentFiledWindowDays * 24 * 60 * 60 * 1000;
    const poolSize = Math.max(limit * 40, 400);
    const knownPriorityFeedPoolSize = preferKnownPriorityFeed
      ? Math.max(limit * 500, 8000)
      : Math.max(limit * 12, 240);
    const fetchCandidateRows = (
      whereSql,
      params = [],
      orderBySql,
      queryLimit = poolSize,
      { startDate = "2025-01-01", requirePriorityTags = true } = {}
    ) => {
      const clauses = [whereSql];
      const queryParams = [];

      if (startDate) {
        clauses.unshift("date(date_filed) >= date(?)");
        queryParams.push(startDate);
      }

      if (requirePriorityTags) {
        clauses.unshift(buildPriorityFeedTagClause());
      }

      return this.db
        .prepare(`
          SELECT *
          FROM cases
          WHERE ${clauses.join("\n            AND ")}
          ORDER BY ${orderBySql}
          LIMIT ?
        `)
        .all(...queryParams, ...params, queryLimit)
        .map(hydrateCase);
    };

    const recentRows = fetchCandidateRows(
      `date(${CASE_PRIORITY_ACTIVITY_SQL}) >= date('now', '-45 day')`,
      [],
      `${CASE_PRIORITY_ACTIVITY_SQL} DESC, updated_at DESC`,
      Math.max(poolSize, limit * 8)
    );
    const knownPriorityFeedRows = fetchCandidateRows(
      `(${PRIORITY_FEED_URL_CLAUSE} OR ${PRIORITY_FEED_RAW_CLAUSE})`,
      [],
      `${CASE_PRIORITY_ACTIVITY_SQL} DESC, docket_count DESC, id ASC`,
      knownPriorityFeedPoolSize,
      { startDate: null, requirePriorityTags: false }
    ).filter((row) => hasConcretePriorityFeedLink(row) || getPriorityFeedRowCount(row) > 0);

    if (preferKnownPriorityFeed) {
      const gapRetryBefore = Date.now() - Math.min(staleAfterHours, 4) * 60 * 60 * 1000;
      const entryCounts = this.getEntryCoverageForCaseIds(knownPriorityFeedRows.map((row) => row.id));
      return knownPriorityFeedRows
        .map((row) => {
          const coverage = entryCounts.get(Number(row.id)) || {
            totalEntries: 0,
            priorityFeedEntries: 0,
            gapPriorityScore: 0,
            hasContinuityGap: false
          };
          const syncedAt = getPriorityFeedSyncedAt(row) ? Date.parse(getPriorityFeedSyncedAt(row)) : 0;
          const priorityFeedRowCount = getPriorityFeedRowCount(row);
          const hasPriorityFeedUrl = hasConcretePriorityFeedLink(row);
          const minimumExpectedEntries = Math.max(12, Number(row.docket_count || 0), priorityFeedRowCount, 6);
          const missingMarked = isPriorityFeedMissing(row);
          const isCurrentYearCase = isCurrentPriorityYearCase(row);
          const hasRecentGapIssue = isGapPriorityCase(row) && Number(coverage.gapPriorityScore || 0) > 0;
          const hasContinuityGap = Boolean(coverage.hasContinuityGap);
          const currentYearGapWeight = getCurrentYearGapPriorityWeight(row, coverage, minimumExpectedEntries);
          const activityAtRaw = getCasePriorityActivityAt(row);
          const activityAtMs = Number.isFinite(Date.parse(activityAtRaw || "")) ? Date.parse(activityAtRaw || "") : 0;
          const createdAtMs = Number.isFinite(Date.parse(row.created_at || "")) ? Date.parse(row.created_at || "") : 0;
          const isRecentFiledFollowUp = createdAtMs > 0 && createdAtMs < recentFiledFollowUpBefore && activityAtMs >= recentFiledFollowUpBefore;
          const freshnessCutoff = hasRecentGapIssue || isRecentFiledFollowUp ? gapRetryBefore : staleBefore;
          const isFreshlyMissing = missingMarked && syncedAt && syncedAt >= freshnessCutoff;
          const needsCompletion = priorityFeedRowCount > 0
            ? coverage.totalEntries < priorityFeedRowCount
            : hasPriorityFeedUrl
              ? coverage.priorityFeedEntries === 0 || coverage.totalEntries < minimumExpectedEntries || hasContinuityGap
              : false;
          const isStale = !syncedAt || syncedAt < freshnessCutoff;
          const shouldSync = !isFreshlyMissing && (needsCompletion || isStale || hasContinuityGap);
          return {
            row,
            needsCompletion,
            isCurrentYearCase,
            isRecentFiledFollowUp,
            hasRecentGapIssue,
            hasContinuityGap,
            currentYearGapWeight,
            gapPriorityScore: Number(coverage.gapPriorityScore || 0),
            neverSynced: !syncedAt,
            isStale,
            isFreshlyMissing,
            shouldSync,
            activityAtRaw,
            priorityFeedRowCount,
            totalEntries: coverage.totalEntries
          };
        })
        .filter((item) => item.shouldSync)
        .sort((left, right) => {
          if (left.needsCompletion !== right.needsCompletion) {
            return left.needsCompletion ? -1 : 1;
          }

          if (left.isCurrentYearCase !== right.isCurrentYearCase) {
            return left.isCurrentYearCase ? -1 : 1;
          }

          if (left.isCurrentYearCase && right.isCurrentYearCase) {
            const currentYearActivityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
            if (currentYearActivityCompare !== 0) {
              return currentYearActivityCompare;
            }
          }

          if (left.isRecentFiledFollowUp !== right.isRecentFiledFollowUp) {
            return left.isRecentFiledFollowUp ? -1 : 1;
          }

          if (left.currentYearGapWeight !== right.currentYearGapWeight) {
            return right.currentYearGapWeight - left.currentYearGapWeight;
          }

          if (left.hasRecentGapIssue !== right.hasRecentGapIssue) {
            return left.hasRecentGapIssue ? -1 : 1;
          }

          if (left.hasContinuityGap !== right.hasContinuityGap) {
            return left.hasContinuityGap ? -1 : 1;
          }

          if (left.gapPriorityScore !== right.gapPriorityScore) {
            return right.gapPriorityScore - left.gapPriorityScore;
          }

          if (left.neverSynced !== right.neverSynced) {
            return left.neverSynced ? -1 : 1;
          }

          if (left.priorityFeedRowCount !== right.priorityFeedRowCount) {
            return right.priorityFeedRowCount - left.priorityFeedRowCount;
          }

          if (left.totalEntries !== right.totalEntries) {
            return left.totalEntries - right.totalEntries;
          }

          const activityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
          if (activityCompare !== 0) {
            return activityCompare;
          }

          return Number(left.row.id || 0) - Number(right.row.id || 0);
        })
        .slice(0, limit)
        .map((item) => item.row);
    }
    const lawFirmBackedRows = fetchCandidateRows(
      `(${buildTrackedLawFirmSourceClause("source_urls_json")})
       AND NOT (${PRIORITY_FEED_RAW_CLAUSE})
       AND NOT (${PRIORITY_FEED_URL_CLAUSE})`,
      [],
      `docket_count DESC,
       ${CASE_PRIORITY_ACTIVITY_SQL} DESC,
       COALESCE(last_synced_at, '1970-01-01T00:00:00.000Z') ASC,
       id ASC`,
      Math.max(limit * 20, 240)
    );
    const sparseRows = fetchCandidateRows(
      `docket_count <= ?`,
      [4],
      `${CASE_PRIORITY_ACTIVITY_SQL} DESC, updated_at DESC`,
      Math.max(limit * 8, 120)
    );
    const legacyUnsyncedRows = fetchCandidateRows(
      `NOT (${PRIORITY_FEED_RAW_CLAUSE})
       AND NOT (${PRIORITY_FEED_URL_CLAUSE})
       AND docket_count >= ?`,
      [8],
      `docket_count DESC,
       ${CASE_PRIORITY_ACTIVITY_SQL} DESC,
       COALESCE(last_synced_at, '1970-01-01T00:00:00.000Z') ASC,
       id ASC`,
      Math.max(limit * 20, 240)
    );

    const candidateRows = [];
    const seenCandidateIds = new Set();
    for (const row of [...knownPriorityFeedRows, ...lawFirmBackedRows, ...recentRows, ...sparseRows, ...legacyUnsyncedRows]) {
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
          priorityFeedEntries: 0,
          gapPriorityScore: 0,
          hasContinuityGap: false
        };
        const hasCivilDocketNumber = /\b\d{2}-cv-\d{3,6}\b/i.test(String(row.docket_number || ""));
        const syncedAt = getPriorityFeedSyncedAt(row) ? Date.parse(getPriorityFeedSyncedAt(row)) : 0;
        const priorityFeedRowCount = getPriorityFeedRowCount(row);
        const hasPriorityFeedUrl = hasConcretePriorityFeedLink(row);
        const hasPriorityFeedEntries = priorityFeedRowCount > 0 || coverage.priorityFeedEntries > 0;
        const hasKnownPriorityFeedSource = hasPriorityFeedUrl || hasPriorityFeedEntries;
        const hasLawFirmSource = hasTrackedLawFirmSource(row);
        const minimumExpectedEntries = Math.max(12, Number(row.docket_count || 0), 6);
        const missingMarked = isPriorityFeedMissing(row);
        const isCurrentYearCase = isCurrentPriorityYearCase(row);
        const hasRecentGapIssue = isGapPriorityCase(row) && Number(coverage.gapPriorityScore || 0) > 0;
        const hasContinuityGap = Boolean(coverage.hasContinuityGap);
        const currentYearGapWeight = getCurrentYearGapPriorityWeight(row, coverage, minimumExpectedEntries);
        const activityAtRaw = getCasePriorityActivityAt(row);
        const activityAtMs = Number.isFinite(Date.parse(activityAtRaw || "")) ? Date.parse(activityAtRaw || "") : 0;
        const createdAtMs = Number.isFinite(Date.parse(row.created_at || "")) ? Date.parse(row.created_at || "") : 0;
        const isRecentFiledFollowUp = createdAtMs > 0 && createdAtMs < recentFiledFollowUpBefore && activityAtMs >= recentFiledFollowUpBefore;
        const freshnessCutoff = hasRecentGapIssue
          ? Date.now() - Math.min(staleAfterHours, 4) * 60 * 60 * 1000
          : isRecentFiledFollowUp
            ? Date.now() - Math.min(staleAfterHours, 2) * 60 * 60 * 1000
          : staleBefore;
        const isFreshlyMissing = missingMarked && syncedAt && syncedAt >= freshnessCutoff;
        const isPriorityFeedBacklog = !hasKnownPriorityFeedSource && Number(row.docket_count || 0) >= 8;

        const needsCompletion = priorityFeedRowCount > 0
          ? coverage.totalEntries < priorityFeedRowCount
          : hasPriorityFeedUrl
            ? coverage.priorityFeedEntries === 0 || coverage.totalEntries < minimumExpectedEntries || hasContinuityGap
            : !hasKnownPriorityFeedSource && (coverage.totalEntries < minimumExpectedEntries || hasContinuityGap);
        const isStale = !syncedAt || syncedAt < freshnessCutoff;
        const shouldSync = hasCivilDocketNumber && !isFreshlyMissing && (
          needsCompletion ||
          hasContinuityGap ||
          !hasKnownPriorityFeedSource ||
          (hasKnownPriorityFeedSource && isStale) ||
          isPriorityFeedBacklog
        );
        let priority = needsCompletion
          ? hasPriorityFeedUrl
            ? 0
            : hasLawFirmSource
              ? 1
              : 2
          : hasLawFirmSource
            ? 3
            : isPriorityFeedBacklog
              ? Number(row.docket_count || 0) >= 20
                ? 4
                : 5
            : !hasKnownPriorityFeedSource
              ? 6
              : 7;
        if (currentYearGapWeight > 0) {
          priority -= 2;
        }
        if (hasRecentGapIssue) {
          priority -= 2;
        }
        if (isRecentFiledFollowUp) {
          priority -= 2;
        }
        if (hasContinuityGap) {
          priority -= 3;
        }

        return {
          row,
          priority,
          shouldSync,
          isCurrentYearCase,
          isRecentFiledFollowUp,
          hasRecentGapIssue,
          hasContinuityGap,
          currentYearGapWeight,
          gapPriorityScore: Number(coverage.gapPriorityScore || 0),
          totalEntries: coverage.totalEntries,
          hasPriorityFeedCoverage: hasKnownPriorityFeedSource,
          hasPriorityFeedUrl,
          hasLawFirmSource,
          expectedEntries: minimumExpectedEntries,
          isPriorityFeedBacklog,
          activityAtRaw
        };
      })
      .filter((item) => item.shouldSync);

    const recentOrdered = rows.slice().sort((left, right) => {
      if (left.priority !== right.priority) {
        return left.priority - right.priority;
      }

      if (left.isCurrentYearCase !== right.isCurrentYearCase) {
        return left.isCurrentYearCase ? -1 : 1;
      }

      if (left.isCurrentYearCase && right.isCurrentYearCase) {
        const currentYearActivityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
        if (currentYearActivityCompare !== 0) {
          return currentYearActivityCompare;
        }
      }

      if (left.isRecentFiledFollowUp !== right.isRecentFiledFollowUp) {
        return left.isRecentFiledFollowUp ? -1 : 1;
      }

      if (left.currentYearGapWeight !== right.currentYearGapWeight) {
        return right.currentYearGapWeight - left.currentYearGapWeight;
      }

      if (left.gapPriorityScore !== right.gapPriorityScore) {
        return right.gapPriorityScore - left.gapPriorityScore;
      }

      if (left.hasContinuityGap !== right.hasContinuityGap) {
        return left.hasContinuityGap ? -1 : 1;
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

      if (left.isCurrentYearCase !== right.isCurrentYearCase) {
        return left.isCurrentYearCase ? -1 : 1;
      }

      if (left.isCurrentYearCase && right.isCurrentYearCase) {
        const currentYearActivityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
        if (currentYearActivityCompare !== 0) {
          return currentYearActivityCompare;
        }
      }

      if (left.isRecentFiledFollowUp !== right.isRecentFiledFollowUp) {
        return left.isRecentFiledFollowUp ? -1 : 1;
      }

      if (left.currentYearGapWeight !== right.currentYearGapWeight) {
        return right.currentYearGapWeight - left.currentYearGapWeight;
      }

      if (left.gapPriorityScore !== right.gapPriorityScore) {
        return right.gapPriorityScore - left.gapPriorityScore;
      }

      if (left.hasContinuityGap !== right.hasContinuityGap) {
        return left.hasContinuityGap ? -1 : 1;
      }

      if (left.hasPriorityFeedUrl !== right.hasPriorityFeedUrl) {
        return left.hasPriorityFeedUrl ? -1 : 1;
      }

      if (left.hasPriorityFeedCoverage !== right.hasPriorityFeedCoverage) {
        return left.hasPriorityFeedCoverage ? 1 : -1;
      }

      if (Number(left.row.docket_count || 0) !== Number(right.row.docket_count || 0)) {
        return Number(right.row.docket_count || 0) - Number(left.row.docket_count || 0);
      }

      if (left.expectedEntries !== right.expectedEntries) {
        return right.expectedEntries - left.expectedEntries;
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

    const knownPriorityFeedOrdered = rows
      .filter((item) => item.hasPriorityFeedUrl || item.hasPriorityFeedCoverage)
      .sort((left, right) => {
        if (left.priority !== right.priority) {
          return left.priority - right.priority;
        }

        if (left.isCurrentYearCase !== right.isCurrentYearCase) {
          return left.isCurrentYearCase ? -1 : 1;
        }

        if (left.isCurrentYearCase && right.isCurrentYearCase) {
          const currentYearActivityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
          if (currentYearActivityCompare !== 0) {
            return currentYearActivityCompare;
          }
        }

        if (left.isRecentFiledFollowUp !== right.isRecentFiledFollowUp) {
          return left.isRecentFiledFollowUp ? -1 : 1;
        }

        if (left.currentYearGapWeight !== right.currentYearGapWeight) {
          return right.currentYearGapWeight - left.currentYearGapWeight;
        }

        if (left.gapPriorityScore !== right.gapPriorityScore) {
          return right.gapPriorityScore - left.gapPriorityScore;
        }

        if (left.hasPriorityFeedUrl !== right.hasPriorityFeedUrl) {
          return left.hasPriorityFeedUrl ? -1 : 1;
        }

        if (left.expectedEntries !== right.expectedEntries) {
          return right.expectedEntries - left.expectedEntries;
        }

        return compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
      });

    const currentYearGapOrdered = rows
      .filter((item) => item.isCurrentYearCase && item.currentYearGapWeight > 0)
      .sort((left, right) => {
        const currentYearActivityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
        if (currentYearActivityCompare !== 0) {
          return currentYearActivityCompare;
        }

        if (left.isRecentFiledFollowUp !== right.isRecentFiledFollowUp) {
          return left.isRecentFiledFollowUp ? -1 : 1;
        }

        if (left.currentYearGapWeight !== right.currentYearGapWeight) {
          return right.currentYearGapWeight - left.currentYearGapWeight;
        }

        if (left.gapPriorityScore !== right.gapPriorityScore) {
          return right.gapPriorityScore - left.gapPriorityScore;
        }

        if (left.hasContinuityGap !== right.hasContinuityGap) {
          return left.hasContinuityGap ? -1 : 1;
        }

        if (left.totalEntries !== right.totalEntries) {
          return left.totalEntries - right.totalEntries;
        }

        return Number(right.row.id || 0) - Number(left.row.id || 0);
      });

    const lawFirmBackedOrdered = rows
      .filter((item) => item.hasLawFirmSource && !item.hasPriorityFeedUrl && !item.hasPriorityFeedCoverage)
      .sort((left, right) => {
        if (left.priority !== right.priority) {
          return left.priority - right.priority;
        }

        if (left.isCurrentYearCase !== right.isCurrentYearCase) {
          return left.isCurrentYearCase ? -1 : 1;
        }

        if (left.isCurrentYearCase && right.isCurrentYearCase) {
          const currentYearActivityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
          if (currentYearActivityCompare !== 0) {
            return currentYearActivityCompare;
          }
        }

        if (left.isRecentFiledFollowUp !== right.isRecentFiledFollowUp) {
          return left.isRecentFiledFollowUp ? -1 : 1;
        }

        if (left.currentYearGapWeight !== right.currentYearGapWeight) {
          return right.currentYearGapWeight - left.currentYearGapWeight;
        }

        if (left.gapPriorityScore !== right.gapPriorityScore) {
          return right.gapPriorityScore - left.gapPriorityScore;
        }

        if (left.hasContinuityGap !== right.hasContinuityGap) {
          return left.hasContinuityGap ? -1 : 1;
        }

        if (Number(left.row.docket_count || 0) !== Number(right.row.docket_count || 0)) {
          return Number(right.row.docket_count || 0) - Number(left.row.docket_count || 0);
        }

        if (left.expectedEntries !== right.expectedEntries) {
          return right.expectedEntries - left.expectedEntries;
        }

        return compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
      });

    const legacyOrdered = rows
      .filter((item) => item.isPriorityFeedBacklog && !item.hasLawFirmSource)
      .sort((left, right) => {
        if (left.isCurrentYearCase !== right.isCurrentYearCase) {
          return left.isCurrentYearCase ? -1 : 1;
        }

        if (left.isCurrentYearCase && right.isCurrentYearCase) {
          const currentYearActivityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
          if (currentYearActivityCompare !== 0) {
            return currentYearActivityCompare;
          }
        }

        if (left.currentYearGapWeight !== right.currentYearGapWeight) {
          return right.currentYearGapWeight - left.currentYearGapWeight;
        }

        if (left.gapPriorityScore !== right.gapPriorityScore) {
          return right.gapPriorityScore - left.gapPriorityScore;
        }

        if (left.hasContinuityGap !== right.hasContinuityGap) {
          return left.hasContinuityGap ? -1 : 1;
        }

        if (Number(left.row.docket_count || 0) !== Number(right.row.docket_count || 0)) {
          return Number(right.row.docket_count || 0) - Number(left.row.docket_count || 0);
        }

        if (left.expectedEntries !== right.expectedEntries) {
          return right.expectedEntries - left.expectedEntries;
        }

        return compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
      });

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

    if (preferKnownPriorityFeed) {
      appendRows(currentYearGapOrdered, limit);
      appendRows(knownPriorityFeedOrdered, limit);
      appendRows(lawFirmBackedOrdered, limit);
      appendRows(legacyOrdered, limit);
      appendRows(backlogOrdered, limit);
      appendRows(recentOrdered, limit);
    } else {
      const currentYearGapSlots = Math.max(1, Math.min(limit, Math.ceil(limit * 0.5)));
      const knownPriorityFeedSlots = Math.max(1, Math.min(limit, Math.ceil(limit * 0.4)));
      const lawFirmSlots = Math.max(1, Math.min(limit, Math.ceil(limit * 0.35)));
      const recentSlots = Math.max(1, limit - currentYearGapSlots - knownPriorityFeedSlots - lawFirmSlots);

      appendRows(currentYearGapOrdered, currentYearGapSlots);
      appendRows(knownPriorityFeedOrdered, knownPriorityFeedSlots);
      appendRows(lawFirmBackedOrdered, currentYearGapSlots + knownPriorityFeedSlots + lawFirmSlots);
      appendRows(recentOrdered, currentYearGapSlots + knownPriorityFeedSlots + lawFirmSlots + recentSlots);
      appendRows(legacyOrdered, limit);
      appendRows(backlogOrdered, limit);
      appendRows(recentOrdered, limit);
    }

    return selected.slice(0, limit);
  }

  getCasesNeedingSupplementalDocketSync(limit, { recentWindowDays = 120, startDate = "2025-01-01" } = {}) {
    const recentCutoff = Date.now() - recentWindowDays * 24 * 60 * 60 * 1000;
    const recentCutoffIso = new Date(recentCutoff).toISOString();
    const candidatePoolSize = Math.max(limit * 120, 360);

    const candidateRows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date(date_filed) >= date(?)
          AND docket_number IS NOT NULL
          AND TRIM(docket_number) <> ''
          AND (
            tags_marker LIKE '%|seller_tro|%'
            OR tags_marker LIKE '%|tro|%'
            OR tags_marker LIKE '%|schedule_a|%'
            OR ${CASE_PRIORITY_ACTIVITY_SQL} >= ?
          )
        ORDER BY ${CASE_PRIORITY_ACTIVITY_SQL} DESC
        LIMIT ?
      `)
      .all(String(startDate || "2025-01-01"), recentCutoffIso, candidatePoolSize)
      .map(buildCaseView);
    const entryCounts = this.getEntryCoverageForCaseIds(candidateRows.map((row) => row.id));

    return candidateRows
      .map((row) => {
        const coverage = entryCounts.get(Number(row.id)) || {
          totalEntries: 0,
          courtlistenerEntries: 0,
          priorityFeedEntries: 0,
          pacermonitorEntries: 0,
          gapPriorityScore: 0,
          hasContinuityGap: false
        };
        const hasCivilDocketNumber = /\b\d{2}-cv-\d{3,6}\b/i.test(String(row.docket_number || ""));
        const activityAtRaw = getCasePriorityActivityAt(row);
        const activityAtMs = Number.isFinite(Date.parse(activityAtRaw || "")) ? Date.parse(activityAtRaw || "") : 0;
        const isRecentCase = activityAtMs >= recentCutoff;
        const isCurrentYearCase = isCurrentPriorityYearCase(row);
        const hasRecentGapIssue = isGapPriorityCase(row) && Number(coverage.gapPriorityScore || 0) > 0;
        const hasContinuityGap = Boolean(coverage.hasContinuityGap);
        const priorityFeedRowCount = getPriorityFeedRowCount(row);
        const latestNumber = parseDocketNumber(row.latest_docket_number);
        const expectedEntries = Math.max(
          row.insights?.is_seller_case ? 12 : 8,
          isRecentCase ? 10 : 0,
          Number(row.docket_count || 0),
          priorityFeedRowCount,
          latestNumber
        );
        const currentYearGapWeight = getCurrentYearGapPriorityWeight(row, coverage, expectedEntries);
        const totalEntries = Number(coverage.totalEntries || 0);
        const gap = Math.max(0, expectedEntries - totalEntries);
        const missingPriorityFeedCoverage = priorityFeedRowCount > 0 && totalEntries < priorityFeedRowCount;
        const shouldSync = hasCivilDocketNumber && (gap > 0 || missingPriorityFeedCoverage || hasContinuityGap);

        let priority = missingPriorityFeedCoverage
          ? 0
          : row.insights?.is_seller_case
            ? 1
            : 2;
        if (currentYearGapWeight > 0) {
          priority -= 2;
        }
        if (hasRecentGapIssue) {
          priority -= 1;
        }
        if (hasContinuityGap) {
          priority -= 2;
        }

        return {
          row,
          priority,
          gap,
          totalEntries,
          isCurrentYearCase,
          currentYearGapWeight,
          gapPriorityScore: Number(coverage.gapPriorityScore || 0),
          hasContinuityGap,
          missingPriorityFeedCoverage,
          activityAtRaw,
          shouldSync
        };
      })
      .filter((item) => item.shouldSync)
      .sort((left, right) => {
        if (left.priority !== right.priority) {
          return left.priority - right.priority;
        }

        if (left.isCurrentYearCase !== right.isCurrentYearCase) {
          return left.isCurrentYearCase ? -1 : 1;
        }

        if (left.isCurrentYearCase && right.isCurrentYearCase) {
          const currentYearActivityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
          if (currentYearActivityCompare !== 0) {
            return currentYearActivityCompare;
          }
        }

        if (left.currentYearGapWeight !== right.currentYearGapWeight) {
          return right.currentYearGapWeight - left.currentYearGapWeight;
        }

        if (left.gapPriorityScore !== right.gapPriorityScore) {
          return right.gapPriorityScore - left.gapPriorityScore;
        }

        if (left.hasContinuityGap !== right.hasContinuityGap) {
          return left.hasContinuityGap ? -1 : 1;
        }

        if (left.missingPriorityFeedCoverage !== right.missingPriorityFeedCoverage) {
          return left.missingPriorityFeedCoverage ? -1 : 1;
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

  getCasesNeedingPacerMonitorSync(
    limit,
    {
      staleAfterHours = 24,
      blockedRetryAfterHours = 12,
      notFoundRetryAfterHours = 6,
      recentWindowDays = 45,
      startDate = "2025-01-01"
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
        WHERE date(date_filed) >= date(?)
          AND docket_number IS NOT NULL
          AND TRIM(docket_number) <> ''
          AND (
            tags_marker LIKE '%|seller_tro|%'
            OR ${CASE_PRIORITY_ACTIVITY_SQL} >= ?
          )
        ORDER BY ${CASE_PRIORITY_ACTIVITY_SQL} DESC
        LIMIT ?
      `)
      .all(String(startDate || "2025-01-01"), recentCutoffIso, candidatePoolSize)
      .map(buildCaseView);
    const entryCounts = this.getEntryCoverageForCaseIds(candidateRows.map((row) => row.id));

    return candidateRows
      .map((row) => {
        const coverage = entryCounts.get(Number(row.id)) || {
          totalEntries: 0,
          courtlistenerEntries: 0,
          priorityFeedEntries: 0,
          pacermonitorEntries: 0,
          gapPriorityScore: 0,
          hasContinuityGap: false
        };
        const hasCivilDocketNumber = /\b\d{2}-cv-\d{3,6}\b/i.test(String(row.docket_number || ""));
        const activityAtRaw = getCasePriorityActivityAt(row);
        const activityAtMs = Number.isFinite(Date.parse(activityAtRaw || "")) ? Date.parse(activityAtRaw || "") : 0;
        const isRecentCase = activityAtMs >= recentCutoff;
        const isCurrentYearCase = isCurrentPriorityYearCase(row);
        const hasRecentGapIssue = isGapPriorityCase(row) && Number(coverage.gapPriorityScore || 0) > 0;
        const hasContinuityGap = Boolean(coverage.hasContinuityGap);
        const priorityFeedRowCount = getPriorityFeedRowCount(row);
        const expectedEntries = Math.max(
          row.insights?.is_seller_case ? 12 : 8,
          isRecentCase ? 10 : 0,
          Number(row.docket_count || 0),
          priorityFeedRowCount
        );
        const currentYearGapWeight = getCurrentYearGapPriorityWeight(row, coverage, expectedEntries);
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
        const needsPriorityFeedLevelCompletion =
          priorityFeedRowCount > 0 && coverage.totalEntries < priorityFeedRowCount;
        const needsBasicCompletion = coverage.totalEntries < expectedEntries || hasContinuityGap;
        const shouldSync =
          hasCivilDocketNumber &&
          (row.insights?.is_seller_case || isRecentCase) &&
          (needsPriorityFeedLevelCompletion || needsBasicCompletion) &&
          !isBlockedFresh &&
          !isFresh;

        let priority = needsPriorityFeedLevelCompletion
          ? 0
          : row.insights?.is_seller_case
            ? 1
            : 2;
        if (currentYearGapWeight > 0) {
          priority -= 2;
        }
        if (hasRecentGapIssue) {
          priority -= 1;
        }
        if (hasContinuityGap) {
          priority -= 2;
        }

        return {
          row,
          priority,
          gap,
          isCurrentYearCase,
          currentYearGapWeight,
          gapPriorityScore: Number(coverage.gapPriorityScore || 0),
          hasContinuityGap,
          activityAtRaw,
          totalEntries: coverage.totalEntries,
          shouldSync
        };
      })
      .filter((item) => !hasPriorityFeedAuthority(item.row))
      .filter((item) => item.shouldSync)
      .sort((left, right) => {
        if (left.priority !== right.priority) {
          return left.priority - right.priority;
        }

        if (left.isCurrentYearCase !== right.isCurrentYearCase) {
          return left.isCurrentYearCase ? -1 : 1;
        }

        if (left.isCurrentYearCase && right.isCurrentYearCase) {
          const currentYearActivityCompare = compareCaseActivityDesc(left.activityAtRaw, right.activityAtRaw);
          if (currentYearActivityCompare !== 0) {
            return currentYearActivityCompare;
          }
        }

        if (left.currentYearGapWeight !== right.currentYearGapWeight) {
          return right.currentYearGapWeight - left.currentYearGapWeight;
        }

        if (left.gapPriorityScore !== right.gapPriorityScore) {
          return right.gapPriorityScore - left.gapPriorityScore;
        }

        if (left.hasContinuityGap !== right.hasContinuityGap) {
          return left.hasContinuityGap ? -1 : 1;
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

  getCoverageGapCases(limit = 25, { recentWindowDays = 90, startDate = "2025-01-01" } = {}) {
    const recentCutoff = Date.now() - recentWindowDays * 24 * 60 * 60 * 1000;
    const recentCutoffIso = new Date(recentCutoff).toISOString();
    const candidatePoolSize = Math.max(limit * 80, 300);

    const candidateRows = this.db
      .prepare(`
        SELECT *
        FROM cases
        WHERE date(date_filed) >= date(?)
          AND (
            tags_marker LIKE '%|tro|%'
            OR tags_marker LIKE '%|schedule_a|%'
            OR tags_marker LIKE '%|seller_tro|%'
            OR ${CASE_PRIORITY_ACTIVITY_SQL} >= ?
          )
        ORDER BY ${CASE_PRIORITY_ACTIVITY_SQL} DESC
        LIMIT ?
      `)
      .all(String(startDate || "2025-01-01"), recentCutoffIso, candidatePoolSize)
      .map(buildCaseView);

    const entryCounts = this.getEntryCoverageForCaseIds(candidateRows.map((row) => row.id));
    const snapshots = candidateRows
      .map((row) => {
        const coverage = entryCounts.get(Number(row.id)) || {
          totalEntries: 0,
          priorityFeedEntries: 0,
          pacermonitorEntries: 0
        };
        const activityAtRaw = getCasePriorityActivityAt(row);
        const activityAtMs = Number.isFinite(Date.parse(activityAtRaw || "")) ? Date.parse(activityAtRaw || "") : 0;
        const isRecentCase = activityAtMs >= recentCutoff;
        const priorityFeedRowCount = getPriorityFeedRowCount(row);
        const latestNumber = parseDocketNumber(row.latest_docket_number);
        const expectedEntries = Math.max(
          row.insights?.is_seller_case ? 12 : 8,
          isRecentCase ? 10 : 0,
          Number(row.docket_count || 0),
          priorityFeedRowCount,
          latestNumber
        );
        const totalEntries = Number(coverage.totalEntries || 0);
        const gap = Math.max(0, expectedEntries - totalEntries);
        const courtListenerEntries = Number(coverage.courtlistenerEntries || 0);
        const pacerMonitorState = String(row.raw?.pacermonitor?.state || "").toLowerCase() || null;
        const priorityFeedSyncedAt = getPriorityFeedSyncedAt(row);
        const pacerMonitorSyncedAt = row.raw?.pacermonitor?.syncedAt || null;
        const missingPriorityFeedCoverage = priorityFeedRowCount > 0 && totalEntries < priorityFeedRowCount;
        const courtListenerGap = Math.max(0, latestNumber - totalEntries);
        const hasCivilDocketNumber = /\b\d{2}-cv-\d{3,6}\b/i.test(String(row.docket_number || ""));
        const hasCourtListenerDocket = Number(row.courtlistener_docket_id || 0) > 0;
        const priorityScore =
          (row.insights?.is_seller_case ? 40 : 0) +
          (row.insights?.is_tro_case ? 25 : 0) +
          (row.insights?.is_schedule_a_case ? 20 : 0) +
          (isRecentCase ? 5 : 0);
        const reasons = [];
        const providersNeeded = [];

        if (hasCourtListenerDocket && priorityScore > 0 && courtListenerGap >= 3) {
          reasons.push(`CourtListener 最新公开号约到 ${latestNumber}，本站现有 ${totalEntries} 条`);
          providersNeeded.push("courtlistener");
        }

        if (missingPriorityFeedCoverage) {
          reasons.push(`优先目录 公开时间线应有 ${priorityFeedRowCount} 条，当前只有 ${totalEntries} 条`);
          if (!providersNeeded.includes(PRIORITY_FEED_PROVIDER_KEY)) {
            providersNeeded.push(PRIORITY_FEED_PROVIDER_KEY);
          }
        }

        if (gap > 0 && hasCivilDocketNumber && !providersNeeded.includes("pacermonitor")) {
          reasons.push(`当前预期至少 ${expectedEntries} 条，本站已有 ${totalEntries} 条`);
          providersNeeded.push("pacermonitor");
        }

        if ((pacerMonitorState === "challenge" || pacerMonitorState === "rate_limited") && !providersNeeded.includes("pacermonitor")) {
          reasons.push(`PACERMonitor 当前返回 ${pacerMonitorState}`);
          providersNeeded.push("pacermonitor");
        }

        return {
          id: row.id,
          docket_number: row.docket_number,
          case_name: row.case_name,
          court_id: row.court_id,
          court_name: row.court_name,
          date_filed: row.date_filed || null,
          primary_source: row.primary_source || null,
          latest_docket_filed_at: row.latest_docket_filed_at || row.date_filed || null,
          lead_law_firm: row.insights?.lead_law_firm || null,
          defendant_count: Number(row.insights?.defendant_count || 0),
          docket_count: Number(row.docket_count || 0),
          total_entries: totalEntries,
          courtlistener_entries: courtListenerEntries,
          expected_entries: expectedEntries,
          gap,
          courtlistener_gap: courtListenerGap,
          priority_row_count: priorityFeedRowCount,
          priority_entries: Number(coverage.priorityFeedEntries || 0),
          pacermonitor_entries: Number(coverage.pacermonitorEntries || 0),
          priority_synced_at: priorityFeedSyncedAt,
          pacermonitor_synced_at: pacerMonitorSyncedAt,
          pacermonitor_state: pacerMonitorState,
          is_recent_case: isRecentCase,
          priority_score: priorityScore,
          providers_needed: providersNeeded,
          reasons,
          source_urls: Array.isArray(row.source_urls) ? row.source_urls : []
        };
      })
      .filter((item) => item.providers_needed.length > 0)
      .sort((left, right) => {
        if ((left.priority_score || 0) !== (right.priority_score || 0)) {
          return (right.priority_score || 0) - (left.priority_score || 0);
        }

        const leftNeedsPriorityFeed = left.providers_needed.includes(PRIORITY_FEED_PROVIDER_KEY);
        const rightNeedsPriorityFeed = right.providers_needed.includes(PRIORITY_FEED_PROVIDER_KEY);
        if (leftNeedsPriorityFeed !== rightNeedsPriorityFeed) {
          return leftNeedsPriorityFeed ? -1 : 1;
        }

        const leftNeedsCourtListener = left.providers_needed.includes("courtlistener");
        const rightNeedsCourtListener = right.providers_needed.includes("courtlistener");
        if (leftNeedsCourtListener !== rightNeedsCourtListener) {
          return leftNeedsCourtListener ? -1 : 1;
        }

        if (left.gap !== right.gap) {
          return right.gap - left.gap;
        }

        const activityCompare = compareCaseActivityDesc(left.latest_docket_filed_at, right.latest_docket_filed_at);
        if (activityCompare !== 0) {
          return activityCompare;
        }

        return Number(right.id || 0) - Number(left.id || 0);
      });

    const items = snapshots.slice(0, limit);
    const summary = snapshots.reduce(
      (acc, item) => {
        acc.total += 1;
        if (item.providers_needed.includes("courtlistener")) {
          acc.courtlistener += 1;
        }
        if (item.providers_needed.includes(PRIORITY_FEED_PROVIDER_KEY)) {
          acc.priority += 1;
        }
        if (item.providers_needed.includes("pacermonitor")) {
          acc.pacermonitor += 1;
        }
        if (item.pacermonitor_state === "challenge" || item.pacermonitor_state === "rate_limited") {
          acc.challenge += 1;
        }
        return acc;
      },
      { total: 0, courtlistener: 0, priority: 0, pacermonitor: 0, challenge: 0 }
    );

    return {
      summary,
      items
    };
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

  claimSyncRun(provider, mode, maxAgeMinutes = 180) {
    const cutoff = new Date(Date.now() - maxAgeMinutes * 60 * 1000).toISOString();

    this.db.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.db
        .prepare(`
          SELECT id
          FROM sync_runs
          WHERE provider = ?
            AND status = 'running'
            AND started_at >= ?
          ORDER BY started_at DESC
          LIMIT 1
        `)
        .get(provider, cutoff);

      if (existing?.id) {
        this.db.exec("COMMIT");
        return null;
      }

      const result = this.db
        .prepare(`
          INSERT INTO sync_runs (provider, mode, status, started_at)
          VALUES (?, ?, ?, ?)
        `)
        .run(provider, mode, "running", nowIso());

      this.db.exec("COMMIT");
      this.recentSyncCache = null;
      return Number(result.lastInsertRowid);
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {}
      throw error;
    }
  }

  finishSyncRun(id, status, stats, errorText = null) {
    this.db
      .prepare(`
        UPDATE sync_runs
        SET status = ?, finished_at = ?, stats_json = ?, error_text = ?
        WHERE id = ?
      `)
      .run(status, nowIso(), toJson(stats, "{}"), errorText, id);
    this.recentSyncCache = null;
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

  getRecentSyncSummary() {
    if (this.recentSyncCache?.expiresAt > Date.now()) {
      return this.recentSyncCache.value;
    }

    const recentSync = this.db
      .prepare(`
        SELECT *
        FROM sync_runs
        ORDER BY started_at DESC
        LIMIT 1
      `)
      .get();

    const value = recentSync
      ? {
          ...recentSync,
          stats: parseJson(recentSync.stats_json, {})
        }
      : null;

    this.recentSyncCache = {
      expiresAt: Date.now() + RECENT_SYNC_CACHE_TTL_MS,
      value
    };

    return value;
  }

  getCheckpoint(key) {
    const row = this.db.prepare("SELECT value_json FROM checkpoints WHERE checkpoint_key = ?").get(key);
    return row ? parseJson(row.value_json, {}) : null;
  }

  updateCheckpointAtomically(key, updater) {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const row = this.db.prepare("SELECT value_json FROM checkpoints WHERE checkpoint_key = ?").get(key);
      const currentValue = row ? parseJson(row.value_json, {}) : null;
      const outcome = updater(currentValue);
      const hasStructuredOutcome =
        outcome &&
        typeof outcome === "object" &&
        Object.prototype.hasOwnProperty.call(outcome, "value");
      const nextValue = hasStructuredOutcome ? outcome.value : outcome;

      if (nextValue === null) {
        this.db.prepare("DELETE FROM checkpoints WHERE checkpoint_key = ?").run(key);
      } else {
        this.db
          .prepare(`
            INSERT INTO checkpoints (checkpoint_key, value_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(checkpoint_key) DO UPDATE SET
              value_json = excluded.value_json,
              updated_at = excluded.updated_at
          `)
          .run(key, toJson(nextValue, "{}"), nowIso());
      }

      this.db.exec("COMMIT");
      return hasStructuredOutcome ? outcome.result : nextValue;
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch {}
      throw error;
    }
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

  deleteCheckpoint(key) {
    this.db.prepare("DELETE FROM checkpoints WHERE checkpoint_key = ?").run(key);
  }

  touchSyncRun(id, payload = {}) {
    const heartbeatAt = nowIso();
    this.saveCheckpoint(syncRunHeartbeatKey(id), {
      ...payload,
      heartbeatAt
    });
    return heartbeatAt;
  }

  clearSyncRunHeartbeat(id) {
    this.deleteCheckpoint(syncRunHeartbeatKey(id));
  }

  getSyncRun(id) {
    const row = this.db.prepare("SELECT * FROM sync_runs WHERE id = ?").get(id);
    return row
      ? {
          ...row,
          stats: parseJson(row.stats_json, {})
        }
      : null;
  }

  getRunningSyncRuns(provider, { mode = null } = {}) {
    const rows = mode
      ? this.db
          .prepare(`
            SELECT *
            FROM sync_runs
            WHERE provider = ?
              AND mode = ?
              AND status = 'running'
            ORDER BY started_at ASC
          `)
          .all(provider, mode)
      : this.db
          .prepare(`
            SELECT *
            FROM sync_runs
            WHERE provider = ?
              AND status = 'running'
            ORDER BY started_at ASC
          `)
          .all(provider);

    return rows.map((row) => ({
      ...row,
      stats: parseJson(row.stats_json, {})
    }));
  }

  reapStaleSyncRuns(provider, {
    mode = null,
    heartbeatTimeoutMs = 0,
    maxRuntimeMs = 0,
    reasonPrefix = "sync watchdog"
  } = {}) {
    const now = Date.now();
    const rows = this.getRunningSyncRuns(provider, { mode });
    const reaped = [];

    for (const row of rows) {
      let heartbeat = null;
      try {
        heartbeat = this.getCheckpoint(syncRunHeartbeatKey(row.id));
      } catch (error) {
        if (isSqliteBusyError(error)) {
          continue;
        }
        throw error;
      }
      const startedAtMs = toTimestamp(row.started_at);
      const heartbeatAtMs = toTimestamp(heartbeat?.heartbeatAt || row.started_at);
      const reasons = [];

      if (
        heartbeatTimeoutMs > 0 &&
        heartbeatAtMs &&
        now - heartbeatAtMs > heartbeatTimeoutMs
      ) {
        reasons.push(`heartbeat stalled for ${Math.round((now - heartbeatAtMs) / 1000)}s`);
      }

      if (
        maxRuntimeMs > 0 &&
        startedAtMs &&
        now - startedAtMs > maxRuntimeMs
      ) {
        reasons.push(`runtime exceeded ${Math.round(maxRuntimeMs / 60000)}m`);
      }

      if (!reasons.length) {
        continue;
      }

      const errorText = `${reasonPrefix}: ${reasons.join("; ")}`;
      try {
        this.finishSyncRun(row.id, "failed", row.stats || {}, errorText);
        this.clearSyncRunHeartbeat(row.id);
      } catch (error) {
        if (isSqliteBusyError(error)) {
          continue;
        }
        throw error;
      }
      reaped.push({
        ...row,
        error_text: errorText
      });
    }

    return reaped;
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
    if (this.dashboardStatsCache?.expiresAt > Date.now()) {
      return {
        ...this.dashboardStatsCache.value,
        recentSync: this.getRecentSyncSummary()
      };
    }

    const todayBounds = shanghaiDayBounds(new Date());
    const totalsRow = this.db
      .prepare(`
        SELECT
          COUNT(*) AS total_cases,
          SUM(CASE WHEN is_watchlist = 1 THEN 1 ELSE 0 END) AS watchlist_cases,
          SUM(CASE WHEN is_tro = 1 THEN 1 ELSE 0 END) AS tro_cases,
          SUM(CASE WHEN is_schedule_a = 1 THEN 1 ELSE 0 END) AS schedule_a_cases,
          SUM(CASE WHEN is_seller_watch = 1 THEN 1 ELSE 0 END) AS seller_cases,
          SUM(
            CASE
              WHEN created_at >= ? AND created_at < ?
               AND is_watchlist = 1
              THEN 1
              ELSE 0
            END
          ) AS today_added_watchlist
        FROM cases
        WHERE date_filed >= ?
      `)
      .get(todayBounds.startIso, todayBounds.endIso, "2025-01-01");
    const totals = {
      total_cases: Number(totalsRow?.total_cases || 0),
      watchlist_cases: Number(totalsRow?.watchlist_cases || 0),
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

    const value = {
      totals,
      latestCase,
      recentSync: this.getRecentSyncSummary()
    };

    this.dashboardStatsCache = {
      expiresAt: Date.now() + DASHBOARD_STATS_CACHE_TTL_MS,
      value
    };

    return value;
  }

  getSyncIngestBreakdown({ startIso, endIso, provider = "system" } = {}) {
    const recent = emptySyncRunBreakdown("recent");
    const backfill = emptySyncRunBreakdown("backfill");
    const total = emptySyncRunBreakdown("total");
    const rows = this.db
      .prepare(`
        SELECT mode, started_at, finished_at, stats_json
        FROM sync_runs
        WHERE provider = ?
          AND status = 'succeeded'
          AND started_at < ?
          AND COALESCE(finished_at, started_at) >= ?
          AND mode IN ('recent', 'backfill')
        ORDER BY started_at ASC
      `)
      .all(provider, endIso, startIso);

    for (const row of rows) {
      const target = row.mode === "backfill" ? backfill : recent;
      const summary = summarizeSyncRunStats(parseJson(row.stats_json, {}));

      for (const bucket of [target, total]) {
        bucket.runCount += 1;
        bucket.casesWritten += summary.casesWritten;
        bucket.docketEntriesWritten += summary.docketEntriesWritten;
        bucket.lookupsTriggered += summary.lookupsTriggered;
        bucket.priorityFeedCasesSynced += summary.priorityFeedCasesSynced;
        bucket.docketCasesSynced += summary.docketCasesSynced;
        bucket.docketAlarmCasesSynced += summary.docketAlarmCasesSynced;
        bucket.uniCourtCasesSynced += summary.uniCourtCasesSynced;
        bucket.translationsApplied += summary.translationsApplied;
      }
    }

    return { recent, backfill, total };
  }

  getDailyEmailReport({ startIso, endIso, caseLimit = 12 } = {}) {
    const newCasesCount = Number(
      this.db
        .prepare(`
          SELECT COUNT(*) AS n
          FROM cases
          WHERE created_at >= ?
            AND created_at < ?
        `)
        .get(startIso, endIso)?.n || 0
    );

    const newDocketEntriesCount = Number(
      this.db
        .prepare(`
          SELECT COUNT(*) AS n
          FROM docket_entries
          WHERE created_at >= ?
            AND created_at < ?
        `)
        .get(startIso, endIso)?.n || 0
    );

    const docketSources = this.db
      .prepare(`
        SELECT
          COALESCE(primary_source, 'unknown') AS source,
          COUNT(*) AS count
        FROM docket_entries
        WHERE created_at >= ?
          AND created_at < ?
        GROUP BY COALESCE(primary_source, 'unknown')
        ORDER BY count DESC, source ASC
      `)
      .all(startIso, endIso)
      .map((row) => ({
        source: row.source || "unknown",
        count: Number(row.count || 0)
      }));

    const rssDocketEntriesCount = Number(
      this.db
        .prepare(`
          SELECT COUNT(*) AS n
          FROM docket_entries
          WHERE created_at >= ?
            AND created_at < ?
            AND primary_source = 'courtfeed'
        `)
        .get(startIso, endIso)?.n || 0
    );

    const rssItems = this.db
      .prepare(`
        SELECT
          c.id,
          c.docket_number,
          c.case_name,
          c.court_id,
          c.court_name,
          COUNT(de.id) AS new_entry_count,
          MAX(de.created_at) AS last_entry_created_at
        FROM docket_entries de
        JOIN cases c
          ON c.id = de.case_id
        WHERE de.created_at >= ?
          AND de.created_at < ?
          AND de.primary_source = 'courtfeed'
        GROUP BY c.id
        ORDER BY
          new_entry_count DESC,
          MAX(de.created_at) DESC,
          c.id DESC
        LIMIT ?
      `)
      .all(startIso, endIso, Math.max(1, Math.min(Number(caseLimit || 12), 20)))
      .map((row) => ({
        id: Number(row.id),
        docket_number: row.docket_number || null,
        case_name: row.case_name || null,
        court_id: row.court_id || null,
        court_name: row.court_name || null,
        new_entry_count: Number(row.new_entry_count || 0),
        last_entry_created_at: row.last_entry_created_at || null
      }));

    const items = this.db
      .prepare(`
        SELECT
          c.id,
          c.docket_number,
          c.case_name,
          c.court_id,
          c.court_name,
          c.date_filed,
          c.created_at,
          c.recent_activity_summary,
          c.recent_activity_summary_zh,
          CASE
            WHEN c.created_at >= ? AND c.created_at < ? THEN 1
            ELSE 0
          END AS is_new_case,
          COUNT(DISTINCT de.id) AS new_entry_count,
          MAX(de.created_at) AS last_entry_created_at
        FROM cases c
        LEFT JOIN docket_entries de
          ON de.case_id = c.id
         AND de.created_at >= ?
         AND de.created_at < ?
        WHERE (
          c.created_at >= ?
          AND c.created_at < ?
        )
        OR EXISTS (
          SELECT 1
          FROM docket_entries de2
          WHERE de2.case_id = c.id
            AND de2.created_at >= ?
            AND de2.created_at < ?
        )
        GROUP BY c.id
        ORDER BY
          is_new_case DESC,
          new_entry_count DESC,
          COALESCE(MAX(de.created_at), c.created_at) DESC,
          c.id DESC
        LIMIT ?
      `)
      .all(
        startIso,
        endIso,
        startIso,
        endIso,
        startIso,
        endIso,
        startIso,
        endIso,
        Math.max(1, Number(caseLimit || 12))
      )
      .map((row) => ({
        id: Number(row.id),
        docket_number: row.docket_number || null,
        case_name: row.case_name || null,
        court_id: row.court_id || null,
        court_name: row.court_name || null,
        date_filed: row.date_filed || null,
        created_at: row.created_at || null,
        is_new_case: Boolean(Number(row.is_new_case || 0)),
        new_entry_count: Number(row.new_entry_count || 0),
        last_entry_created_at: row.last_entry_created_at || null,
        summary: row.recent_activity_summary_zh || row.recent_activity_summary || null
      }));

    return {
      newCasesCount,
      newDocketEntriesCount,
      syncBreakdown: this.getSyncIngestBreakdown({ startIso, endIso }),
      docketSources,
      rssDocketEntriesCount,
      rssItems,
      items
    };
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
    const base = Array.isArray(existingJson) ? existingJson : parseJson(existingJson, []);
    const merged = new Set(base.map(normalizeSourceUrl));
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

    return compareCaseListFreshness(left, right);
  }

  searchPriority(row, searchTerm) {
    const rawNeedle = String(searchTerm || "").trim();
    const normalizedNeedle = normalizeDocket(rawNeedle);
    const docketRaw = row._docket_raw;
    const docketNormalized = row._docket_normalized;
    const labelBlob = row._label_blob;
    const shortNumericFragment = looksLikeShortNumericFragment(rawNeedle);

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

    if (row.insights?.is_tro_case) {
      score += 8;
    }

    if (row.insights?.is_schedule_a_case) {
      score += 6;
    }

    if (row.insights?.is_seller_case) {
      score += 5;
    }

    if (shortNumericFragment) {
      if (isCivilLike(row)) {
        score += 24;
      }

      if (isBankruptcyLike(row)) {
        score -= 45;
      }

      if (hasSparsePublicCoverage(row)) {
        score -= 35;
      }
    }

    return score;
  }
}
