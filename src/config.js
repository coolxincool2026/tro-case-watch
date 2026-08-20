import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PRIORITY_FEED_BASE_URL, buildLegacyPriorityFeedEnvKey } from "./priority-feed.js";

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

function envList(name, fallback = []) {
  const value = env(name, "");
  if (!String(value || "").trim()) {
    return [...fallback];
  }

  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function envAny(names = [], fallback) {
  for (const name of names) {
    const value = process.env[name];
    if (value !== undefined && value !== "") {
      return value;
    }
  }

  return fallback;
}

function envAnyBool(names, fallback) {
  const value = envAny(names, String(fallback));
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function envAnyInt(names, fallback) {
  const value = Number.parseInt(envAny(names, String(fallback)), 10);
  return Number.isFinite(value) ? value : fallback;
}

function envAnyList(names, fallback = []) {
  const value = envAny(names, "");
  if (!String(value || "").trim()) {
    return [...fallback];
  }

  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export const config = {
  projectRoot,
  dataDir,
  publicDir: path.join(projectRoot, "public"),
  dbPath: env("DB_PATH", path.join(dataDir, "tro-watch.sqlite")),
  fallbackDbPath: env("FALLBACK_DB_PATH", path.join(env("TMPDIR", "/tmp"), "tro-watch.sqlite")),
  seedDbArchivePath: env("SEED_DB_ARCHIVE_PATH", path.join(projectRoot, "seed", "tro-watch.sqlite.gz")),
  seedDbMinimumCases: envInt("SEED_DB_MIN_CASES", 40000),
  server: {
    port: envInt("PORT", 4127),
    adminToken: env("ADMIN_TOKEN", ""),
    enablePurgeNonWatchlist: envBool("ENABLE_PURGE_NON_WATCHLIST", false),
    publicCasesMaxPageSize: envInt("PUBLIC_CASES_MAX_PAGE_SIZE", 15),
    publicHealthCacheTtlMs: envInt("PUBLIC_HEALTH_CACHE_TTL_MS", 10_000),
    publicStatusCacheTtlMs: envInt("PUBLIC_STATUS_CACHE_TTL_MS", 3 * 60_000),
    publicTroDailyUpdatesCacheTtlMs: envInt("PUBLIC_TRO_DAILY_UPDATES_CACHE_TTL_MS", 15 * 60_000),
    publicCasesCacheTtlMs: envInt("PUBLIC_CASES_CACHE_TTL_MS", 60_000),
    publicCaseDetailCacheTtlMs: envInt("PUBLIC_CASE_DETAIL_CACHE_TTL_MS", 90_000),
    publicCaseDetailInitialEntries: envInt("PUBLIC_CASE_DETAIL_INITIAL_ENTRIES", 120),
    publicApiCacheMaxEntries: envInt("PUBLIC_API_CACHE_MAX_ENTRIES", 300),
    publicRateLimitWindowMs: envInt("PUBLIC_RATE_LIMIT_WINDOW_MS", 60_000),
    publicRateLimitCasesPerWindow: envInt("PUBLIC_RATE_LIMIT_CASES_PER_WINDOW", 36),
    publicRateLimitCaseDetailPerWindow: envInt("PUBLIC_RATE_LIMIT_CASE_DETAIL_PER_WINDOW", 72),
    publicRateLimitStatusPerWindow: envInt("PUBLIC_RATE_LIMIT_STATUS_PER_WINDOW", 24),
    publicRateLimitHealthPerWindow: envInt("PUBLIC_RATE_LIMIT_HEALTH_PER_WINDOW", 24),
    suspiciousRateLimitPerWindow: envInt("SUSPICIOUS_RATE_LIMIT_PER_WINDOW", 6),
    publicFullDetailGrantTtlMs: envInt("PUBLIC_FULL_DETAIL_GRANT_TTL_MS", 2 * 60_000),
    publicBehaviorDistinctSearchesPerWindow: envInt("PUBLIC_BEHAVIOR_DISTINCT_SEARCHES_PER_WINDOW", 8),
    publicBehaviorDistinctCaseDetailsPerWindow: envInt("PUBLIC_BEHAVIOR_DISTINCT_CASE_DETAILS_PER_WINDOW", 18),
    publicBehaviorDistinctFullCaseDetailsPerWindow: envInt("PUBLIC_BEHAVIOR_DISTINCT_FULL_CASE_DETAILS_PER_WINDOW", 8),
    publicBehaviorStrikeLimit: envInt("PUBLIC_BEHAVIOR_STRIKE_LIMIT", 2),
    publicTemporaryBlockMs: envInt("PUBLIC_TEMPORARY_BLOCK_MS", 10 * 60_000),
    publicScannerBlockMs: envInt("PUBLIC_SCANNER_BLOCK_MS", 30 * 60_000),
    publicCaseDetailBurstWindowMs: envInt("PUBLIC_CASE_DETAIL_BURST_WINDOW_MS", 10_000),
    publicCaseDetailBurstMaxRequests: envInt("PUBLIC_CASE_DETAIL_BURST_MAX_REQUESTS", 10),
    publicFullCaseDetailBurstWindowMs: envInt("PUBLIC_FULL_CASE_DETAIL_BURST_WINDOW_MS", 15_000),
    publicFullCaseDetailBurstMaxRequests: envInt("PUBLIC_FULL_CASE_DETAIL_BURST_MAX_REQUESTS", 4),
    suspiciousPenaltyDelayMs: envInt("SUSPICIOUS_PENALTY_DELAY_MS", 1200),
    suspiciousUserAgentPatterns: envList("SUSPICIOUS_USER_AGENT_PATTERNS", [
      "python-requests",
      "curl/",
      "wget/",
      "scrapy",
      "aiohttp",
      "go-http-client",
      "okhttp",
      "httpclient",
      "node-fetch",
      "axios",
      "java/",
      "libwww-perl",
      "mechanize",
      "phantomjs",
      "headlesschrome",
      "playwright",
      "puppeteer",
      "selenium",
      "postmanruntime",
      "insomnia",
      "python-urllib",
      "httpx",
      "undici"
    ])
  },
  email: {
    host: env("SMTP_HOST", ""),
    port: envInt("SMTP_PORT", 465),
    secure: envBool("SMTP_SECURE", true),
    user: env("SMTP_USER", ""),
    pass: env("SMTP_PASS", ""),
    from: env("SMTP_FROM", "")
  },
  reports: {
    windowEmail: {
      enabled: envBool("WINDOW_EMAIL_REPORT_ENABLED", false)
    },
    dailyEmail: {
      enabled: envBool("DAILY_EMAIL_REPORT_ENABLED", false),
      to: env("DAILY_EMAIL_REPORT_TO", "599214243@qq.com"),
      timeZone: env("DAILY_EMAIL_REPORT_TIME_ZONE", "Asia/Shanghai"),
      hour: envInt("DAILY_EMAIL_REPORT_HOUR", 23),
      minute: envInt("DAILY_EMAIL_REPORT_MINUTE", 59),
      caseLimit: envInt("DAILY_EMAIL_REPORT_CASE_LIMIT", 12),
      checkIntervalMs: envInt("DAILY_EMAIL_REPORT_CHECK_INTERVAL_MS", 60 * 1000),
      startupDelayMs: envInt("DAILY_EMAIL_REPORT_STARTUP_DELAY_MS", 90 * 1000)
    },
    troDailyRoundup: {
      enabled: envBool("TRO_DAILY_ROUNDUP_ENABLED", true),
      to: env("TRO_DAILY_ROUNDUP_TO", env("DAILY_EMAIL_REPORT_TO", "599214243@qq.com")),
      timeZone: env("TRO_DAILY_ROUNDUP_TIME_ZONE", "Asia/Shanghai"),
      hour: envInt("TRO_DAILY_ROUNDUP_HOUR", 20),
      minute: envInt("TRO_DAILY_ROUNDUP_MINUTE", 0),
      itemLimit: envInt("TRO_DAILY_ROUNDUP_ITEM_LIMIT", 3),
      timeoutMs: envInt("TRO_DAILY_ROUNDUP_TIMEOUT_MS", 12_000),
      candidateLimit: envInt("TRO_DAILY_ROUNDUP_CANDIDATE_LIMIT", 24),
      perSourceFetchLimit: envInt("TRO_DAILY_ROUNDUP_PER_SOURCE_FETCH_LIMIT", 8),
      checkIntervalMs: envInt("TRO_DAILY_ROUNDUP_CHECK_INTERVAL_MS", 60 * 1000),
      startupDelayMs: envInt("TRO_DAILY_ROUNDUP_STARTUP_DELAY_MS", 120 * 1000),
      sourcesPath: env("TRO_DAILY_ROUNDUP_SOURCES_PATH", path.join(dataDir, "tro-daily-roundup-sources.json"))
    },
    troDailyUpdates: {
      path: env("TRO_DAILY_UPDATES_PATH", path.join(dataDir, "tro-daily-updates.json")),
      maxItems: envInt("TRO_DAILY_UPDATES_MAX_ITEMS", 3),
      timeZone: env("TRO_DAILY_UPDATES_TIME_ZONE", "Asia/Shanghai")
    }
  },
  sync: {
    internalSchedulersEnabled: envBool("ENABLE_INTERNAL_SCHEDULERS", false),
    enableScheduler: envBool("ENABLE_SCHEDULER", true),
    enableBackfillScheduler: envBool("ENABLE_BACKFILL_SCHEDULER", true),
    watchdogEnabled: envBool("SYNC_WATCHDOG_ENABLED", true),
    bootstrapSync: envBool("BOOTSTRAP_SYNC", true),
    bootstrapSyncDelayMs: envInt("BOOTSTRAP_SYNC_DELAY_MS", 2 * 60 * 1000),
    bootstrapBackfillDelayMs: envInt("BOOTSTRAP_BACKFILL_DELAY_MS", 5 * 60 * 1000),
    startDate: env("START_DATE", "2025-01-01"),
    discoveryStartDate: env("SYNC_DISCOVERY_START_DATE", "2025-01-01"),
    pollIntervalMs: envInt("POLL_INTERVAL_MS", 30 * 60 * 1000),
    recentSchedulerCheckIntervalMs: envInt("RECENT_SCHEDULER_CHECK_INTERVAL_MS", 5 * 60 * 1000),
    backfillIntervalMs: envInt("BACKFILL_INTERVAL_MS", 60 * 60 * 1000),
    backfillSchedulerCheckIntervalMs: envInt("BACKFILL_SCHEDULER_CHECK_INTERVAL_MS", 15 * 60 * 1000),
    watchdogIntervalMs: envInt("SYNC_WATCHDOG_INTERVAL_MS", 2 * 60 * 1000),
    runHeartbeatIntervalMs: envInt("SYNC_RUN_HEARTBEAT_INTERVAL_MS", 30 * 1000),
    runHeartbeatTimeoutMs: envInt("SYNC_RUN_HEARTBEAT_TIMEOUT_MS", 3 * 60 * 1000),
    recentMaxRuntimeMs: envInt("RECENT_SYNC_MAX_RUNTIME_MS", 25 * 60 * 1000),
    backfillMaxRuntimeMs: envInt("BACKFILL_SYNC_MAX_RUNTIME_MS", 2 * 60 * 60 * 1000),
    discoveryMaxPagesPerRun: envInt("DISCOVERY_MAX_PAGES_PER_RUN", 3),
    backfillMaxPagesPerRun: envInt("BACKFILL_MAX_PAGES_PER_RUN", 50),
    recentDocketFollowUpDays: envInt("RECENT_DOCKET_FOLLOW_UP_DAYS", 2),
    recentExistingCaseFollowUpMaxCasesPerRun: envInt("RECENT_EXISTING_CASE_FOLLOW_UP_MAX_CASES_PER_RUN", 6),
    workerCycleSleepMs: envInt("WORKER_CYCLE_SLEEP_MS", 30 * 1000),
    workerErrorBackoffMs: envInt("WORKER_ERROR_BACKOFF_MS", 15 * 1000),
    workerBackfillEveryMs: envInt("WORKER_BACKFILL_EVERY_MS", 60 * 60 * 1000),
    workerCatalogMaxRounds: envInt("WORKER_CATALOG_MAX_ROUNDS", 100),
    workerCatalogIdleRounds: envInt("WORKER_CATALOG_IDLE_ROUNDS", 5),
    workerCatalogSleepMs: envInt("WORKER_CATALOG_SLEEP_MS", 2000),
    workerCatalogBatchSize: envInt("WORKER_CATALOG_BATCH_SIZE", 4),
    webhookEnrichmentEnabled: envBool("WEBHOOK_ENRICHMENT_ENABLED", true),
    webhookEnrichmentProviders: envList("WEBHOOK_ENRICHMENT_PROVIDERS", ["signal-feed", "worldtro", "61tro", "recentfilings"]),
    webhookEnrichmentSettleDelayMs: envInt("WEBHOOK_ENRICHMENT_SETTLE_DELAY_MS", 90 * 1000),
    webhookEnrichmentLeaseTtlMs: envInt("WEBHOOK_ENRICHMENT_LEASE_TTL_MS", 15 * 60 * 1000),
    webhookEnrichmentProviderDelayMs: envInt("WEBHOOK_ENRICHMENT_PROVIDER_DELAY_MS", 2000),
    webhookEnrichmentRetryDelayMs: envInt("WEBHOOK_ENRICHMENT_RETRY_DELAY_MS", 20 * 60 * 1000),
    webhookEnrichmentMemoryRetryDelayMs: envInt("WEBHOOK_ENRICHMENT_MEMORY_RETRY_DELAY_MS", 10 * 60 * 1000),
    webhookEnrichmentMaxOldSpaceMb: envInt("WEBHOOK_ENRICHMENT_MAX_OLD_SPACE_MB", 256),
    webhookEnrichmentMaxRssMb: envInt("WEBHOOK_ENRICHMENT_MAX_RSS_MB", 384),
    webhookEnrichmentMinFreeSystemMb: envInt("WEBHOOK_ENRICHMENT_MIN_FREE_SYSTEM_MB", 768),
    webhookEnrichmentPollIntervalMs: envInt("WEBHOOK_ENRICHMENT_POLL_INTERVAL_MS", 60 * 1000),
    webhookEnrichmentSpawnDebounceMs: envInt("WEBHOOK_ENRICHMENT_SPAWN_DEBOUNCE_MS", 5000),
    webhookEnrichmentMaxPendingJobs: envInt("WEBHOOK_ENRICHMENT_MAX_PENDING_JOBS", 500)
  },
  courtListener: {
    baseUrl: env("COURTLISTENER_BASE_URL", "https://www.courtlistener.com/api/rest/v4"),
    apiToken: env("COURTLISTENER_API_TOKEN", ""),
    enableDocketSync: envBool("COURTLISTENER_ENABLE_DOCKET_SYNC", false),
    enableDocketAlerts: envBool("COURTLISTENER_ENABLE_DOCKET_ALERTS", true),
    recapFetchEnabled: envBool("COURTLISTENER_RECAP_FETCH_ENABLED", envBool("PACER_ENABLED", false)),
    recapFetchPollIntervalMs: envInt("COURTLISTENER_RECAP_FETCH_POLL_INTERVAL_MS", 2000),
    recapFetchMaxPollMs: envInt("COURTLISTENER_RECAP_FETCH_MAX_POLL_MS", 12000),
    recapFetchShowPartiesAndCounsel: envBool("COURTLISTENER_RECAP_FETCH_SHOW_PARTIES_AND_COUNSEL", true),
    docketMaxCasesPerRun: envInt("COURTLISTENER_DOCKET_MAX_CASES_PER_RUN", 8),
    docketBackfillMaxCasesPerRun: envInt("COURTLISTENER_DOCKET_BACKFILL_MAX_CASES_PER_RUN", 40),
    docketAlertSyncMaxCasesPerRun: envInt("COURTLISTENER_DOCKET_ALERT_SYNC_MAX_CASES_PER_RUN", 200),
    docketAlertSyncStaleAfterHours: envInt("COURTLISTENER_DOCKET_ALERT_SYNC_STALE_AFTER_HOURS", 24),
    docketAlertSyncBootstrapDelayMs: envInt("COURTLISTENER_DOCKET_ALERT_SYNC_BOOTSTRAP_DELAY_MS", 90 * 1000),
    docketAlertSyncIntervalMs: envInt("COURTLISTENER_DOCKET_ALERT_SYNC_INTERVAL_MS", 6 * 60 * 60 * 1000),
    autoReupAlerts: envBool("COURTLISTENER_AUTO_REUP_ALERTS", true),
    webhookSecret: env("COURTLISTENER_WEBHOOK_SECRET", "")
  },
  signalFeed: {
    enabled: envBool("SIGNAL_FEED_ENABLED", false),
    publicCasesUrl: env("SIGNAL_FEED_PUBLIC_CASES_URL", ""),
    apiBaseUrl: env("SIGNAL_FEED_API_BASE_URL", "https://api.trohub.com/v1"),
    apiKey: env("SIGNAL_FEED_API_KEY", ""),
    timeoutMs: envInt("SIGNAL_FEED_TIMEOUT_MS", 15_000),
    minIntervalMs: envInt("SIGNAL_FEED_MIN_INTERVAL_MS", 1500),
    recentDays: envInt("SIGNAL_FEED_RECENT_DAYS", 7),
    recentLimit: envInt("SIGNAL_FEED_RECENT_LIMIT", 100),
    maxCasesPerRun: envInt("SIGNAL_FEED_MAX_CASES_PER_RUN", 100)
  },
  priorityFeed: {
    enabled: envAnyBool(["PRIORITY_FEED_ENABLED", buildLegacyPriorityFeedEnvKey("ENABLED")], true),
    baseUrl: envAny(["PRIORITY_FEED_BASE_URL", buildLegacyPriorityFeedEnvKey("BASE_URL")], PRIORITY_FEED_BASE_URL),
    minIntervalMs: envAnyInt(["PRIORITY_FEED_MIN_INTERVAL_MS", buildLegacyPriorityFeedEnvKey("MIN_INTERVAL_MS")], 1500),
    timeoutMs: envAnyInt(["PRIORITY_FEED_TIMEOUT_MS", buildLegacyPriorityFeedEnvKey("TIMEOUT_MS")], 30000),
    maxCasesPerRun: envAnyInt(["PRIORITY_FEED_MAX_CASES_PER_RUN", buildLegacyPriorityFeedEnvKey("MAX_CASES_PER_RUN")], 8),
    backfillMaxCasesPerRun: envAnyInt(
      ["PRIORITY_FEED_BACKFILL_MAX_CASES_PER_RUN", buildLegacyPriorityFeedEnvKey("BACKFILL_MAX_CASES_PER_RUN")],
      160
    ),
    staleAfterHours: envAnyInt(["PRIORITY_FEED_STALE_AFTER_HOURS", buildLegacyPriorityFeedEnvKey("STALE_AFTER_HOURS")], 12),
    discoveryStaleAfterHours: envAnyInt(
      ["PRIORITY_FEED_DISCOVERY_STALE_AFTER_HOURS", buildLegacyPriorityFeedEnvKey("DISCOVERY_STALE_AFTER_HOURS")],
      6
    ),
    discoveryPages: envAnyList(["PRIORITY_FEED_DISCOVERY_PAGES", buildLegacyPriorityFeedEnvKey("DISCOVERY_PAGES")], [
      "/2026/",
      "/2025nianzuixintroanjian/"
    ])
  },
  courtFeeds: {
    enabled: envBool("COURT_FEEDS_ENABLED", true),
    lookupUrl: env("COURT_FEEDS_LOOKUP_URL", "https://pacer.uscourts.gov/file-case/court-cmecf-lookup/data.json"),
    timeoutMs: envInt("COURT_FEEDS_TIMEOUT_MS", 15000),
    minIntervalMs: envInt("COURT_FEEDS_MIN_INTERVAL_MS", 1000),
    maxItemsPerFeed: envInt("COURT_FEEDS_MAX_ITEMS_PER_FEED", 80),
    maxLookupsPerRun: envInt("COURT_FEEDS_MAX_LOOKUPS_PER_RUN", 12),
    crossSourceFollowUpMaxCasesPerRun: envInt("COURT_FEEDS_CROSS_SOURCE_FOLLOW_UP_MAX_CASES_PER_RUN", 4),
    requireKeywordHit: envBool("COURT_FEEDS_REQUIRE_KEYWORD_HIT", true),
    watchKeywords: envList("COURT_FEEDS_WATCH_KEYWORDS", [
      "temporary restraining order",
      "tro",
      "schedule a",
      "identified on schedule a",
      "unincorporated associations",
      "lanham act",
      "counterfeit",
      "seller"
    ]),
    watchLawFirms: envList("COURT_FEEDS_WATCH_LAW_FIRMS", [
      "gbc",
      "greer burns",
      "ams",
      "eps",
      "keith",
      "jiang"
    ]),
    courts: envList("COURT_FEEDS_TARGETS", [
      "ilnd",
      "flsd",
      "nysd"
    ])
  },
  recentFilings: {
    enabled: envBool("RECENT_FILINGS_ENABLED", true),
    timeoutMs: envInt("RECENT_FILINGS_TIMEOUT_MS", 20000),
    minIntervalMs: envInt("RECENT_FILINGS_MIN_INTERVAL_MS", 1200),
    maxItemsPerCourt: envInt("RECENT_FILINGS_MAX_ITEMS_PER_COURT", 120),
    maxPagesPerCourt: envInt("RECENT_FILINGS_MAX_PAGES_PER_COURT", 3),
    maxLookupsPerRun: envInt("RECENT_FILINGS_MAX_LOOKUPS_PER_RUN", 12),
    courts: envList("RECENT_FILINGS_TARGETS", ["ilnd", "cand", "flsd"])
  },
  lawFirms: {
    enabled: envBool("LAW_FIRM_SITES_ENABLED", true),
    timeoutMs: envInt("LAW_FIRM_SITES_TIMEOUT_MS", 15000),
    minIntervalMs: envInt("LAW_FIRM_SITES_MIN_INTERVAL_MS", 1000),
    maxCasesPerSource: envInt("LAW_FIRM_SITES_MAX_CASES_PER_SOURCE", 8),
    maxLookupsPerRun: envInt("LAW_FIRM_SITES_MAX_LOOKUPS_PER_RUN", 8),
    sources: envList("LAW_FIRM_SITES_TARGETS", ["sriplaw", "gbc", "61tro"])
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
    authUrl: env("PACER_AUTH_URL", "https://pacer.login.uscourts.gov/services/cso-auth"),
    baseUrl: env("PACER_PCL_BASE_URL", "https://pcl.uscourts.gov/pcl-public-api/rest"),
    courtLookupUrl: env("PACER_COURT_LOOKUP_URL", "https://pacer.uscourts.gov/file-case/court-cmecf-lookup/data.json"),
    loginId: envAny(["PACER_LOGIN_ID", "PACER_USERNAME"], ""),
    password: envAny(["PACER_PASSWORD", "PACER_PASS"], ""),
    clientCode: env("PACER_CLIENT_CODE", ""),
    otpCode: envAny(["PACER_OTP_CODE", "PACER_OTP_TOKEN"], ""),
    requestSource: env("PACER_REQUEST_SOURCE", "tro-case-watch"),
    timeoutMs: envInt("PACER_TIMEOUT_MS", 20000),
    minIntervalMs: envInt("PACER_MIN_INTERVAL_MS", 1500),
    recentWindowDays: envInt("PACER_RECENT_WINDOW_DAYS", 3),
    backfillWindowDays: envInt("PACER_BACKFILL_WINDOW_DAYS", 14),
    maxPagesPerRun: envInt("PACER_MAX_PAGES_PER_RUN", 2),
    backfillMaxPagesPerRun: envInt("PACER_BACKFILL_MAX_PAGES_PER_RUN", 6),
    caseTypes: envList("PACER_CASE_TYPES", ["cv"]),
    natureOfSuit: envList("PACER_NATURE_OF_SUIT", ["820", "830", "840"]),
    courtIds: envList("PACER_COURT_IDS", []),
    estimatedCostUsdPerRequest: envFloat("PACER_ESTIMATED_COST_USD_PER_REQUEST", 0.10),
    dailyBudgetUsd: envFloat("PACER_DAILY_BUDGET_USD", 15),
    perRunBudgetUsd: envFloat("PACER_PER_RUN_BUDGET_USD", 3),
    maxCasesPerRun: envInt("PACER_MAX_CASES_PER_RUN", 2)
  },
  pacerMonitor: {
    enabled: envBool("PACERMONITOR_PUBLIC_ENABLED", true) || envBool("PACERMONITOR_ENABLED", false),
    baseUrl: env("PACERMONITOR_BASE_URL", "https://www.pacermonitor.com"),
    publicSearchBaseUrl: env("PACERMONITOR_PUBLIC_SEARCH_BASE_URL", "https://html.duckduckgo.com/html/"),
    apiKey: env("PACERMONITOR_API_KEY", ""),
    minIntervalMs: envInt("PACERMONITOR_MIN_INTERVAL_MS", 2000),
    timeoutMs: envInt("PACERMONITOR_TIMEOUT_MS", 15000),
    maxCasesPerRun: envInt("PACERMONITOR_MAX_CASES_PER_RUN", 2),
    backfillMaxCasesPerRun: envInt("PACERMONITOR_BACKFILL_MAX_CASES_PER_RUN", 10),
    staleAfterHours: envInt("PACERMONITOR_STALE_AFTER_HOURS", 24),
    blockedRetryAfterHours: envInt("PACERMONITOR_BLOCKED_RETRY_AFTER_HOURS", 12),
    notFoundRetryAfterHours: envInt("PACERMONITOR_NOT_FOUND_RETRY_AFTER_HOURS", 6),
    recentWindowDays: envInt("PACERMONITOR_RECENT_WINDOW_DAYS", 45),
    maxSearchQueries: envInt("PACERMONITOR_MAX_SEARCH_QUERIES", 5)
  },
  docketAlarm: {
    enabled: envBool("DOCKETALARM_ENABLED", false),
    baseUrl: env("DOCKETALARM_BASE_URL", "https://www.docketalarm.com"),
    username: env("DOCKETALARM_USERNAME", ""),
    password: env("DOCKETALARM_PASSWORD", ""),
    clientMatter: env("DOCKETALARM_CLIENT_MATTER", "tro-case-watch"),
    minIntervalMs: envInt("DOCKETALARM_MIN_INTERVAL_MS", 2000),
    timeoutMs: envInt("DOCKETALARM_TIMEOUT_MS", 20000),
    maxCasesPerRun: envInt("DOCKETALARM_MAX_CASES_PER_RUN", 2),
    backfillMaxCasesPerRun: envInt("DOCKETALARM_BACKFILL_MAX_CASES_PER_RUN", 8),
    staleAfterHours: envInt("DOCKETALARM_STALE_AFTER_HOURS", 24),
    notFoundRetryAfterHours: envInt("DOCKETALARM_NOT_FOUND_RETRY_AFTER_HOURS", 6),
    useCachedDockets: envBool("DOCKETALARM_USE_CACHED_DOCKETS", true),
    testMode: envBool("DOCKETALARM_TEST_MODE", false)
  },
  uniCourt: {
    enabled: envBool("UNICOURT_ENABLED", false),
    baseUrl: env("UNICOURT_BASE_URL", "https://enterpriseapi.unicourt.com"),
    username: env("UNICOURT_USERNAME", ""),
    password: env("UNICOURT_PASSWORD", ""),
    apiToken: env("UNICOURT_API_TOKEN", ""),
    tokenPath: env("UNICOURT_TOKEN_PATH", "/generateNewToken"),
    tokenMethod: env("UNICOURT_TOKEN_METHOD", "POST"),
    caseSearchPath: env("UNICOURT_CASE_SEARCH_PATH", "/caseSearch"),
    caseSearchMethod: env("UNICOURT_CASE_SEARCH_METHOD", "POST"),
    caseDetailPath: env("UNICOURT_CASE_DETAIL_PATH", "/case"),
    caseDetailMethod: env("UNICOURT_CASE_DETAIL_METHOD", "GET"),
    authHeader: env("UNICOURT_AUTH_HEADER", "Authorization"),
    authScheme: env("UNICOURT_AUTH_SCHEME", "Bearer"),
    minIntervalMs: envInt("UNICOURT_MIN_INTERVAL_MS", 2000),
    timeoutMs: envInt("UNICOURT_TIMEOUT_MS", 20000),
    maxCasesPerRun: envInt("UNICOURT_MAX_CASES_PER_RUN", 2),
    backfillMaxCasesPerRun: envInt("UNICOURT_BACKFILL_MAX_CASES_PER_RUN", 8),
    staleAfterHours: envInt("UNICOURT_STALE_AFTER_HOURS", 24),
    notFoundRetryAfterHours: envInt("UNICOURT_NOT_FOUND_RETRY_AFTER_HOURS", 6),
    testMode: envBool("UNICOURT_TEST_MODE", false)
  }
};
