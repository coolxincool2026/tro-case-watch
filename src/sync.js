import crypto from "node:crypto";
import { buildTagsMarker, classifyCase, discoveryPresets } from "./queries.js";
import { docketLooksLike, normalizeDocket, normalizeDocketIdentity, normalizeText } from "./insights.js";
import {
  PRIORITY_FEED_DISCOVERY_CHECKPOINT,
  PRIORITY_FEED_ENTRY_SOURCE,
  getPriorityFeedRaw,
  getPriorityFeedSyncedAt,
  mergePriorityFeedRaw,
  sourceUrlUsesPriorityFeed,
  isPriorityFeedPrimarySource
} from "./priority-feed.js";
import { SIGNAL_FEED_PROVIDER_KEY } from "./providers/signal-feed.js";

function valueOf(...values) {
  return values.find((value) => value !== undefined && value !== null && value !== "") ?? null;
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function uniqueByNormalized(values) {
  const unique = new Map();
  for (const rawValue of values) {
    const value = String(rawValue || "").trim();
    if (!value) {
      continue;
    }

    const key = normalizeText(value).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
    if (!key || unique.has(key)) {
      continue;
    }

    unique.set(key, value);
  }

  return [...unique.values()];
}

function waitForMs(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function parseComparableDocketNumber(value) {
  const normalized = String(value || "").trim().replace(/\.0$/, "");
  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function hasCivilCaseDocket(caseRow = {}) {
  return /\b\d{2}-cv-\d{3,6}\b/i.test(String(caseRow.docket_number || ""));
}

function getDiscoveryStartDate(config = {}) {
  return config?.sync?.discoveryStartDate || config?.sync?.startDate || "2026-01-01";
}

function shouldAttemptPriorityFeedForCase(caseRow = {}, { force = false } = {}) {
  if (!caseRow || !hasCivilCaseDocket(caseRow)) {
    return false;
  }

  if (force) {
    return true;
  }

  const caseName = normalizeText(caseRow.case_name || "");
  const cause = normalizeText(caseRow.cause || "");
  const natureOfSuit = normalizeText(caseRow.nature_of_suit || "");
  const latestGap = Math.max(
    0,
    parseComparableDocketNumber(caseRow.latest_docket_number) - Number(caseRow.docket_count || 0)
  );

  return Boolean(
    caseRow.insights?.is_seller_case ||
    caseRow.insights?.is_tro_case ||
    caseRow.insights?.is_schedule_a_case ||
    /\b(820|830|840)\b/.test(natureOfSuit) ||
    /\b15:1114\b/.test(cause) ||
    /\b15:1125\b/.test(cause) ||
    /\b17:501\b/.test(cause) ||
    /\b35:271\b/.test(cause) ||
    caseName.includes("schedule a") ||
    caseName.includes("unincorporated associations") ||
    caseName.includes("trademark") ||
    caseName.includes("copyright") ||
    caseName.includes("patent") ||
    caseName.includes("counterfeit") ||
    caseName.includes("infringement") ||
    latestGap >= 3 ||
    Number(caseRow.docket_count || 0) <= 3
  );
}

function buildCourtFeedWatchText(item = {}) {
  return normalizeText([
    item.title,
    item.caseName,
    item.documentType,
    item.description,
    item.rawDescription,
    item.courtName,
    item.docketNumber
  ].filter(Boolean).join(" | "));
}

const DISTRICT_DIRECTION_MAP = {
  N: "Northern",
  S: "Southern",
  E: "Eastern",
  W: "Western",
  C: "Central",
  M: "Middle"
};

const STATE_CODE_TO_NAME = new Map([
  ["AL", "alabama"],
  ["AK", "alaska"],
  ["AZ", "arizona"],
  ["AR", "arkansas"],
  ["CA", "california"],
  ["CO", "colorado"],
  ["CT", "connecticut"],
  ["DC", "district of columbia"],
  ["DE", "delaware"],
  ["FL", "florida"],
  ["GA", "georgia"],
  ["HI", "hawaii"],
  ["IA", "iowa"],
  ["ID", "idaho"],
  ["IL", "illinois"],
  ["IN", "indiana"],
  ["KS", "kansas"],
  ["KY", "kentucky"],
  ["LA", "louisiana"],
  ["MA", "massachusetts"],
  ["MD", "maryland"],
  ["ME", "maine"],
  ["MI", "michigan"],
  ["MN", "minnesota"],
  ["MO", "missouri"],
  ["MS", "mississippi"],
  ["MT", "montana"],
  ["NC", "north carolina"],
  ["ND", "north dakota"],
  ["NE", "nebraska"],
  ["NH", "new hampshire"],
  ["NJ", "new jersey"],
  ["NM", "new mexico"],
  ["NV", "nevada"],
  ["NY", "new york"],
  ["OH", "ohio"],
  ["OK", "oklahoma"],
  ["OR", "oregon"],
  ["PA", "pennsylvania"],
  ["RI", "rhode island"],
  ["SC", "south carolina"],
  ["SD", "south dakota"],
  ["TN", "tennessee"],
  ["TX", "texas"],
  ["UT", "utah"],
  ["VA", "virginia"],
  ["VT", "vermont"],
  ["WA", "washington"],
  ["WI", "wisconsin"],
  ["WV", "west virginia"],
  ["WY", "wyoming"]
]);

const PRIORITY_FEED_LAW_FIRM_PATTERNS = [
  { needle: /sriplaw/i, pattern: "sriplaw.com" },
  { needle: /\bgbc\b|greer\s*burns/i, pattern: "gbc.law" },
  { needle: /whitewood/i, pattern: "whitewoodlaw.com" },
  { needle: /jiang|keith/i, pattern: "jiangip.com" }
];

const PRIORITY_FEED_DISCOVERY_STOP_WORDS = new Set([
  "and",
  "the",
  "llc",
  "inc",
  "ltd",
  "co",
  "company",
  "corp",
  "corporation",
  "plaintiff",
  "plaintiffs",
  "defendant",
  "defendants",
  "et",
  "al"
]);

const FULL_CATALOG_START_DATE = "1900-01-01";
const COURTLISTENER_NEGATIVE_TERMS = {
  bankruptcy: [
    "bankruptcy",
    "debtor",
    "chapter 7",
    "chapter 12",
    "chapter 11",
    "chapter 13",
    "trustee",
    "schedule a/b"
  ],
  habeas: [
    "habeas",
    "2254",
    "2255",
    "2241",
    "prisoner petition",
    "warden"
  ],
  immigration: [
    "immigration",
    "asylum",
    "removal proceeding",
    "deport",
    "deportation",
    "naturalization",
    "citizenship",
    "uscis",
    "ice detention",
    "alien detainee"
  ],
  injury: [
    "personal injury",
    "wrongful death",
    "bodily injury",
    "medical malpractice",
    "product liability",
    "premises liability",
    "slip and fall",
    "motor vehicle",
    "auto accident",
    "negligence",
    "tort"
  ]
};
const COURTFEED_NEGATIVE_TERMS = {
  bankruptcy: COURTLISTENER_NEGATIVE_TERMS.bankruptcy,
  habeas: COURTLISTENER_NEGATIVE_TERMS.habeas,
  immigration: COURTLISTENER_NEGATIVE_TERMS.immigration,
  injury: COURTLISTENER_NEGATIVE_TERMS.injury,
  criminal: [
    "united states of america v",
    "usa v",
    "indictment",
    "criminal complaint",
    "arraignment",
    "sentencing",
    "supervised release",
    "probation violation",
    "search warrant",
    "forfeiture"
  ],
  socialSecurity: [
    "social security",
    "supplemental security income",
    "disability insurance benefits",
    "commissioner of social security",
    "ssi"
  ],
  labor: [
    "fair labor standards act",
    "flsa",
    "employment discrimination",
    "wrongful termination",
    "overtime wages",
    "wage and hour"
  ],
  prisoner: [
    "civil rights complaint",
    "prison conditions",
    "detention center",
    "correctional officer",
    "jail"
  ]
};
const COURTLISTENER_IP_CAUSE_PATTERNS = [/\b15:1114\b/i, /\b15:1125\b/i, /\b17:501\b/i, /\b35:271\b/i];

function normalizeLookupText(value) {
  return normalizeText(value).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
}

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

function normalizePriorityFeedCaseUrl(value) {
  const raw = String(value || "").trim();
  if (!raw || !sourceUrlUsesPriorityFeed(raw)) {
    return "";
  }

  try {
    const url = new URL(raw);
    url.search = "";
    url.hash = "";
    return url.toString();
  } catch {
    return raw;
  }
}

function deriveStateCodeFromCase(caseLike = {}) {
  const courtId = String(caseLike.court_id || "").trim().toLowerCase();
  if (courtId.length >= 2 && /^[a-z]{2}/.test(courtId)) {
    return courtId.slice(0, 2).toUpperCase();
  }

  const normalizedCourt = normalizeLookupText(caseLike.court_name);
  for (const [code, stateName] of STATE_CODE_TO_NAME) {
    if (normalizedCourt.includes(stateName)) {
      return code;
    }
  }

  return "";
}

function detectPriorityFeedLawFirmPattern(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }

  const match = PRIORITY_FEED_LAW_FIRM_PATTERNS.find((item) => item.needle.test(text));
  return match?.pattern || "";
}

function extractDistinctiveTokens(value, minLength = 3) {
  return [...new Set(
    normalizeLookupText(value)
      .split(" ")
      .map((item) => item.trim())
      .filter((item) => item.length >= minLength && !PRIORITY_FEED_DISCOVERY_STOP_WORDS.has(item))
  )];
}

function countTokenHits(tokens, haystack) {
  const text = normalizeLookupText(haystack);
  if (!text) {
    return 0;
  }

  return tokens.filter((token) => text.includes(token)).length;
}

function scorePriorityFeedDiscoveryCandidate(item, row) {
  let score = 0;
  const normalizedPriorityUrl = normalizePriorityFeedCaseUrl(item.caseUrl);
  const candidateUrls = asArray(row.source_urls)
    .map(normalizePriorityFeedCaseUrl)
    .filter(Boolean);

  if (normalizedPriorityUrl && candidateUrls.includes(normalizedPriorityUrl)) {
    score += 1000;
  }

  if (normalizePriorityFeedCaseUrl(getPriorityFeedRaw(row)?.url) === normalizedPriorityUrl) {
    score += 1000;
  }

  const candidateStateCode = deriveStateCodeFromCase(row);
  if (candidateStateCode && item.stateCode) {
    score += candidateStateCode === item.stateCode ? 260 : -260;
  }

  if (courtNamesLikelyMatch(item.courtName, row.court_name)) {
    score += 80;
  }

  const candidateText = [
    row.case_name,
    ...(row.plaintiffs || []),
    ...(row.defendants || []),
    row.insights?.brand_name,
    row.insights?.lead_law_firm
  ].join(" ");
  const plaintiffTokens = extractDistinctiveTokens(item.plaintiff, 3);
  const brandTokens = extractDistinctiveTokens(item.brand, 4);
  const plaintiffHitCount = countTokenHits(plaintiffTokens, candidateText);
  const brandHitCount = countTokenHits(brandTokens, candidateText);
  score += plaintiffHitCount * 45;
  score += brandHitCount * 35;

  const lawFirmPattern = detectPriorityFeedLawFirmPattern(item.lawFirm);
  if (lawFirmPattern && asArray(row.source_urls).some((url) => String(url || "").includes(lawFirmPattern))) {
    score += 200;
  }

  if (item.lawFirm && normalizeLookupText(row.insights?.lead_law_firm).includes(normalizeLookupText(item.lawFirm))) {
    score += 140;
  }

  if (row.insights?.is_tro_case || row.insights?.is_schedule_a_case || row.insights?.is_seller_case) {
    score += 40;
  }

  if (isPriorityFeedPrimarySource(row.primary_source)) {
    score += 120;
  }

  if (Number(row.docket_count || 0) > 0) {
    score += Math.min(30, Number(row.docket_count || 0));
  }

  return score;
}

function buildPriorityFeedDiscoveryIndex(rows = []) {
  const byDocket = new Map();
  const byPriorityUrl = new Map();

  for (const row of rows) {
    const docketKey = normalizeDocketIdentity(row.docket_number);
    if (docketKey) {
      if (!byDocket.has(docketKey)) {
        byDocket.set(docketKey, []);
      }
      byDocket.get(docketKey).push(row);
    }

    const urls = [
      ...asArray(row.source_urls),
      getPriorityFeedRaw(row)?.url
    ]
      .map(normalizePriorityFeedCaseUrl)
      .filter(Boolean);

    for (const url of urls) {
      if (!byPriorityUrl.has(url)) {
        byPriorityUrl.set(url, row);
      }
    }
  }

  return {
    byDocket,
    byPriorityUrl
  };
}

function findBestPriorityFeedDiscoveryCase(item, index) {
  const normalizedUrl = normalizePriorityFeedCaseUrl(item.caseUrl);
  if (normalizedUrl && index.byPriorityUrl.has(normalizedUrl)) {
    return index.byPriorityUrl.get(normalizedUrl);
  }

  const docketKey = normalizeDocketIdentity(item.docketNumber);
  if (!docketKey) {
    return null;
  }

  const candidates = index.byDocket.get(docketKey) || [];
  if (!candidates.length) {
    return null;
  }

  const scored = candidates
    .map((row) => ({ row, score: scorePriorityFeedDiscoveryCandidate(item, row) }))
    .sort((left, right) => {
      if (left.score !== right.score) {
        return right.score - left.score;
      }

      return Number(right.row.docket_count || 0) - Number(left.row.docket_count || 0);
    });

  const best = scored[0];
  if (!best || best.score < 220) {
    return null;
  }

  return best.row;
}

function courtNamesLikelyMatch(expected, actual) {
  const left = normalizeCourtLookupText(expected);
  const right = normalizeCourtLookupText(actual);
  if (!left || !right) {
    return false;
  }

  return left === right || left.includes(right) || right.includes(left);
}

function parseNumericLike(value) {
  const numeric = Number.parseFloat(String(value || "").replace(/,+/g, "").trim());
  return Number.isFinite(numeric) ? numeric : null;
}

function compareCaseActivityDesc(left, right) {
  return String(right || "").localeCompare(String(left || ""));
}

function getSyncModeMaxRuntimeMs(config, mode = "recent") {
  return mode === "backfill"
    ? Number(config.sync?.backfillMaxRuntimeMs || 0)
    : Number(config.sync?.recentMaxRuntimeMs || 0);
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

function getCasePriorityActivityAt(caseRow = {}) {
  return laterIso(laterIso(caseRow.updated_at, caseRow.latest_docket_filed_at), caseRow.date_filed);
}

function higherOrderValue(left, right) {
  const leftNumber = parseNumericLike(left);
  const rightNumber = parseNumericLike(right);
  if (leftNumber === null) {
    return right || null;
  }

  if (rightNumber === null) {
    return left || null;
  }

  return rightNumber > leftNumber ? right : left;
}

function parseCourtListenerResourceId(value) {
  const match = String(value || "").match(/\/(\d+)\/?$/);
  if (!match) {
    return 0;
  }

  const parsed = Number.parseInt(match[1], 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeCourtListenerAlertType(value, fallback = null) {
  const parsed = Number(value ?? fallback ?? 0);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function compareCourtListenerAlerts(left = {}, right = {}) {
  const leftActive = normalizeCourtListenerAlertType(left.alert_type, left.alertType) === 1;
  const rightActive = normalizeCourtListenerAlertType(right.alert_type, right.alertType) === 1;
  if (leftActive !== rightActive) {
    return leftActive ? -1 : 1;
  }

  const leftModified = Date.parse(String(left.date_modified || left.dateModified || left.date_created || left.dateCreated || "")) || 0;
  const rightModified = Date.parse(String(right.date_modified || right.dateModified || right.date_created || right.dateCreated || "")) || 0;
  if (leftModified !== rightModified) {
    return rightModified - leftModified;
  }

  return Number(right.id || 0) - Number(left.id || 0);
}

function getLatestKnownEntryFiledAt(store, caseId) {
  if (!Number(caseId)) {
    return null;
  }

  return store.getEntryCoverageForCaseIds([Number(caseId)]).get(Number(caseId))?.latestEntryFiledAt || null;
}

function hasCaseLevelActivityLead(store, caseRow = {}) {
  const latestActivityAt = String(getCasePriorityActivityAt(caseRow) || "").trim();
  if (!latestActivityAt || !Number(caseRow.id)) {
    return false;
  }

  const latestEntryFiledAt = getLatestKnownEntryFiledAt(store, caseRow.id);
  if (!latestEntryFiledAt) {
    return true;
  }

  return latestActivityAt.localeCompare(String(latestEntryFiledAt)) > 0;
}

function buildCourtDocketKey(courtId, docketNumber) {
  const normalizedDocket = normalizeDocketIdentity(docketNumber);
  if (!normalizedDocket) {
    return "";
  }

  const normalizedCourt = normalizeCourtLookupText(courtId);
  if (!normalizedCourt) {
    return "";
  }

  return `${normalizedCourt}|${normalizedDocket}`;
}

function buildLawFirmEntryIdentity(entry = {}) {
  const sourceEntryId = normalizeLookupText(entry.sourceEntryId);
  if (sourceEntryId) {
    return `id:${sourceEntryId}`;
  }

  const absoluteUrl = String(entry.absoluteUrl || "").trim().toLowerCase();
  if (absoluteUrl) {
    return `url:${absoluteUrl}`;
  }

  const fallbackDigest = crypto
    .createHash("sha1")
    .update(
      [
        String(entry.filedAt || "").trim(),
        String(entry.entryNumber || "").trim(),
        String(entry.documentNumber || "").trim(),
        normalizeLookupText(entry.description),
        String(entry.documentType || "").trim()
      ].join("|")
    )
    .digest("hex")
    .slice(0, 20);

  return `fallback:${fallbackDigest}`;
}

function buildLawFirmEntryKey(item, savedCase, entry) {
  const caseIdentity =
    buildCourtDocketKey(item.courtId || savedCase?.court_id, item.docketNumber || savedCase?.docket_number) ||
    buildCourtDocketKey(item.courtName || savedCase?.court_name, item.docketNumber || savedCase?.docket_number) ||
    `${normalizeLookupText(item.sourceCaseId || item.caseUrl || savedCase?.source_case_key || "")}|${normalizeDocket(item.docketNumber || savedCase?.docket_number) || "unknown"}` ||
    `case:${Number(savedCase?.id || 0) || "unknown"}`;

  return `${item.sourceId}:${caseIdentity}:${buildLawFirmEntryIdentity(entry)}`;
}

function deriveParties(result) {
  const parties = uniqueByNormalized(asArray(result.party));
  const caseName = String(valueOf(result.caseName, result.case_name_full) || "").trim();
  const captionPieces = caseName.split(/\s(?:v\.|vs\.)\s/i);
  const plaintiffFromCaption = captionPieces[0]?.trim() || "";
  const defendantFromCaption = captionPieces.slice(1).join(" v. ").trim();
  const plaintiffKey = normalizeText(plaintiffFromCaption).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
  const defendantKey = normalizeText(defendantFromCaption).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();

  let plaintiffs = [plaintiffFromCaption || parties[0]].filter(Boolean);
  let defendants = [defendantFromCaption].filter(Boolean);

  if (defendantKey && parties.length) {
    const defendantIndex = parties.findIndex((party) => {
      const partyKey = normalizeText(party).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
      return partyKey === defendantKey;
    });

    if (defendantIndex >= 0) {
      plaintiffs = [
        ...parties.slice(0, defendantIndex),
        plaintiffFromCaption
      ].filter(Boolean);
      defendants = [
        defendantFromCaption,
        ...parties.slice(defendantIndex)
      ].filter(Boolean);
    }
  }

  return {
    plaintiffs: uniqueByNormalized(plaintiffs),
    defendants: uniqueByNormalized([
      ...defendants,
      ...parties.filter((party) => {
        const partyKey = normalizeText(party).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
        return partyKey && partyKey !== plaintiffKey;
      })
    ])
  };
}

function includesAny(text, values = []) {
  return values.some((value) => text.includes(value));
}

function hasTargetSignals({ text = "", tags = [], cause = "", natureOfSuit = "", existingCase = null } = {}) {
  const existingTags = new Set(Array.isArray(existingCase?.tags) ? existingCase.tags : []);
  return Boolean(
    tags.length > 0 ||
    existingTags.has("tro") ||
    existingTags.has("schedule_a") ||
    existingTags.has("seller_tro") ||
    /\b(820|830|840)\b/.test(natureOfSuit) ||
    COURTLISTENER_IP_CAUSE_PATTERNS.some((pattern) => pattern.test(cause)) ||
    text.includes("schedule a") ||
    text.includes("unincorporated associations") ||
    text.includes("temporary restraining order") ||
    text.includes("trademark") ||
    text.includes("copyright") ||
    text.includes("patent") ||
    text.includes("lanham act") ||
    text.includes("counterfeit") ||
    text.includes("infringement")
  );
}

function hasHighConfidenceTargetCase(caseLike = {}) {
  const insights = caseLike?.insights || {};
  const tags = new Set(Array.isArray(caseLike?.tags) ? caseLike.tags : []);
  const caseName = normalizeText(caseLike?.case_name || "");
  const cause = normalizeText(caseLike?.cause || "");
  const natureOfSuit = normalizeText(caseLike?.nature_of_suit || "");
  const sourceUrls = asArray(caseLike?.source_urls);

  return Boolean(
    tags.has("seller_tro") ||
    tags.has("schedule_a") ||
    insights?.is_seller_case ||
    insights?.is_schedule_a_case ||
    (insights?.is_tro_case && Number(insights?.seller_relevance_score || 0) >= 4) ||
    /\b(820|830|840)\b/.test(natureOfSuit) ||
    COURTLISTENER_IP_CAUSE_PATTERNS.some((pattern) => pattern.test(cause)) ||
    caseName.includes("schedule a") ||
    caseName.includes("unincorporated associations") ||
    sourceUrls.some((value) => sourceUrlUsesPriorityFeed(value))
  );
}

function shouldTrackCourtListenerResult(result, existingCase, tags) {
  const docketNumber = String(valueOf(result.docketNumber, existingCase?.docket_number) || "");
  if (!/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber) && !/\b\d+:\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
    return false;
  }

  const cause = normalizeText(valueOf(result.cause, existingCase?.cause) || "");
  const natureOfSuit = normalizeText(valueOf(result.nature_of_suit, result.suitNature, existingCase?.nature_of_suit) || "");
  const court = normalizeText(valueOf(result.court, existingCase?.court_name) || "");
  const text = normalizeText([
    result.caseName,
    result.case_name_full,
    result.court,
    result.cause,
    result.nature_of_suit,
    result.suitNature,
    ...(result.party || []),
    ...((result.recap_documents || []).map((item) => item.short_description || item.description || "")),
    existingCase?.case_name,
    existingCase?.cause,
    existingCase?.nature_of_suit
  ].join(" | "));

  const hasNegativeSignal =
    docketNumber.toLowerCase().includes("-bk-") ||
    court.includes("bankruptcy") ||
    includesAny(text, COURTLISTENER_NEGATIVE_TERMS.bankruptcy) ||
    includesAny(text, COURTLISTENER_NEGATIVE_TERMS.habeas) ||
    includesAny(text, COURTLISTENER_NEGATIVE_TERMS.immigration) ||
    includesAny(text, COURTLISTENER_NEGATIVE_TERMS.injury);

  if (hasNegativeSignal) {
    return false;
  }

  if (existingCase) {
    return hasTargetSignals({ text, tags, cause, natureOfSuit, existingCase });
  }

  return hasTargetSignals({ text, tags, cause, natureOfSuit });
}

function buildCourtFeedTrackingContext(item = {}, existingCase = null) {
  return {
    docketNumber: String(valueOf(item.docketNumber, existingCase?.docket_number) || ""),
    cause: normalizeText(existingCase?.cause || ""),
    natureOfSuit: normalizeText(existingCase?.nature_of_suit || ""),
    court: normalizeText(valueOf(item.courtName, existingCase?.court_name) || ""),
    text: normalizeText([
      item.title,
      item.caseName,
      item.documentType,
      item.description,
      item.rawDescription,
      item.courtName,
      item.docketNumber,
      existingCase?.case_name,
      existingCase?.cause,
      existingCase?.nature_of_suit
    ].filter(Boolean).join(" | "))
  };
}

function hasCourtFeedNegativeSignals(item, existingCase = null) {
  const { docketNumber, court, text } = buildCourtFeedTrackingContext(item, existingCase);
  return (
    docketNumber.toLowerCase().includes("-bk-") ||
    court.includes("bankruptcy") ||
    includesAny(text, COURTFEED_NEGATIVE_TERMS.bankruptcy) ||
    includesAny(text, COURTFEED_NEGATIVE_TERMS.habeas) ||
    includesAny(text, COURTFEED_NEGATIVE_TERMS.immigration) ||
    includesAny(text, COURTFEED_NEGATIVE_TERMS.injury) ||
    includesAny(text, COURTFEED_NEGATIVE_TERMS.criminal) ||
    includesAny(text, COURTFEED_NEGATIVE_TERMS.socialSecurity) ||
    includesAny(text, COURTFEED_NEGATIVE_TERMS.labor) ||
    includesAny(text, COURTFEED_NEGATIVE_TERMS.prisoner)
  );
}

export class CaseSyncService {
  constructor({ config, store, courtFeeds, recentFilings, lawFirms, courtListener, signalFeed, priorityFeed, pacerMonitor, docketAlarm, uniCourt, pacer, translator }) {
    this.config = config;
    this.store = store;
    this.courtFeeds = courtFeeds;
    this.recentFilings = recentFilings || {
      enabled: false,
      maxLookupsPerRun: 0,
      getStatus() {
        return { enabled: false, state: "disabled", trackedCourts: 0, sources: [] };
      },
      listSources() {
        return [];
      },
      async lookupByDocket() {
        return null;
      }
    };
    this.lawFirms = lawFirms;
    this.courtListener = courtListener;
    this.signalFeed = signalFeed || {
      enabled: false,
      getStatus() {
        return { enabled: false, state: "disabled" };
      },
      async fetchRecent() {
        return { items: [], source: "disabled" };
      },
      async lookupByDocket() {
        return null;
      }
    };
    this.priorityFeed = priorityFeed;
    this.pacerMonitor = pacerMonitor;
    this.docketAlarm = docketAlarm;
    this.uniCourt = uniCourt || {
      enabled: false,
      hasCredentials() {
        return false;
      },
      getStatus() {
        return { enabled: false, state: "disabled" };
      }
    };
    this.pacer = pacer;
    this.translator = translator;
    this.state = {
      isRunning: false,
      currentMode: null,
      lastStartedAt: null,
      lastFinishedAt: null,
      lastError: null,
      lastStats: null
    };
  }

  getPublicStatus() {
    const dashboard = this.store.getDashboardStats();
    const lastNotes = dashboard.recentSync?.stats?.notes || [];
    const knownNoDocketEntries = lastNotes.some((note) => note.includes("无权访问 docket-entries"));
    const recentSyncRunning = dashboard.recentSync?.status === "running";

    return {
      ...this.state,
      isRunning: this.state.isRunning || recentSyncRunning,
      currentMode: this.state.currentMode || (recentSyncRunning ? dashboard.recentSync?.mode || null : null),
      lastStartedAt: this.state.lastStartedAt || dashboard.recentSync?.started_at || null,
      lastFinishedAt: this.state.lastFinishedAt || dashboard.recentSync?.finished_at || null,
      dashboard,
      backfill: this.getBackfillStatus(),
      providers: {
        courtfeeds: this.courtFeeds.getStatus(),
        recentfilings: this.recentFilings.getStatus(),
        lawfirms: this.lawFirms.getStatus(),
        courtlistener: {
          searchEnabled: true,
          docketEnabled: this.courtListener.hasDocketAccess(),
          docketEntriesEnabled: this.courtListener.hasDocketEntriesAccess() && !knownNoDocketEntries,
          docketAlertsEnabled: this.courtListener.hasDocketAlertAccess(),
          recapFetchEnabled: this.courtListener.hasRecapFetchAccess()
        },
        priorityFeed: this.priorityFeed.getStatus(),
        pacermonitor: this.pacerMonitor.getStatus(),
        docketalarm: this.docketAlarm.getStatus(),
        unicourt: this.uniCourt.getStatus(),
        pacer: this.pacer.getStatus(),
        translation: {
          enabled: this.translator.isEnabled(),
          provider: this.translator.provider
        }
      }
    };
  }

  getBackfillStatus() {
    const checkpoints = discoveryPresets.map((preset) => {
      const checkpoint = this.store.getCheckpoint(`courtlistener:${preset.key}:backfill`);
      return {
        key: preset.key,
        label: preset.label,
        completed: Boolean(checkpoint?.completed),
        hasStarted: Boolean(checkpoint),
        updatedAt: checkpoint?.updatedAt || null
      };
    });

    return {
      pending: checkpoints.some((item) => !item.completed),
      checkpoints
    };
  }

  async run(mode = "recent") {
    if (this.state.isRunning) {
      return {
        ...this.getPublicStatus(),
        skipped: true,
        reason: "already-running"
      };
    }

    this.state.isRunning = true;
    this.state.currentMode = mode;
    this.state.lastStartedAt = new Date().toISOString();
    this.state.lastError = null;

    const heartbeatIntervalMs = Math.max(Number(this.config.sync?.runHeartbeatIntervalMs || 30 * 1000), 5 * 1000);
    const heartbeatTimeoutMs = Math.max(Number(this.config.sync?.runHeartbeatTimeoutMs || 3 * 60 * 1000), heartbeatIntervalMs);
    const maxRuntimeMs = Math.max(getSyncModeMaxRuntimeMs(this.config, mode), 60 * 1000);
    const staleRuns = this.store.reapStaleSyncRuns("system", {
      mode,
      heartbeatTimeoutMs,
      maxRuntimeMs,
      reasonPrefix: `${mode} watchdog auto-cleared stale run`
    });
    const claimWindowMinutes = Math.max(
      Math.ceil(Math.max(maxRuntimeMs, heartbeatTimeoutMs) / 60000) + 5,
      10
    );
    const runId = this.store.claimSyncRun("system", mode, claimWindowMinutes);
    if (!runId) {
      this.state.isRunning = false;
      this.state.currentMode = null;
      return {
        ...this.getPublicStatus(),
        skipped: true,
        reason: "already-running"
      };
    }
    const stats = {
      courtFeedCasesUpserted: 0,
      courtFeedEntriesUpserted: 0,
      courtFeedLookups: 0,
      recentFilingsCasesUpserted: 0,
      recentFilingsLookups: 0,
      signalFeedCasesUpserted: 0,
      signalFeedEntriesUpserted: 0,
      pacerCasesUpserted: 0,
      pacerPagesFetched: 0,
      lawFirmCasesUpserted: 0,
      lawFirmEntriesUpserted: 0,
      lawFirmLookups: 0,
      pagesFetched: 0,
      casesUpserted: 0,
      docketEntriesUpserted: 0,
      docketCasesSynced: 0,
      priorityFeedCasesSynced: 0,
      docketAlarmCasesSynced: 0,
      uniCourtCasesSynced: 0,
      translationsApplied: 0,
      notes: []
    };
    if (staleRuns.length) {
      stats.notes.push(`watchdog 自动回收 ${staleRuns.length} 条卡住的 ${mode} 任务：${staleRuns.map((row) => `#${row.id}`).join(", ")}`);
    }

    let runClosed = false;
    const closeRun = (status, errorText = null) => {
      if (runClosed) {
        return false;
      }
      runClosed = true;
      this.store.finishSyncRun(runId, status, stats, errorText);
      this.store.clearSyncRunHeartbeat(runId);
      return true;
    };
    const touchHeartbeat = () => {
      this.store.touchSyncRun(runId, {
        provider: "system",
        mode,
        pid: process.pid,
        startedAt: this.state.lastStartedAt
      });
    };
    touchHeartbeat();
    const heartbeatTimer = setInterval(() => {
      try {
        touchHeartbeat();
      } catch (error) {
        stats.notes.push(`任务心跳写入失败：${error.message}`);
      }
    }, heartbeatIntervalMs);
    heartbeatTimer.unref?.();
    const timeoutTimer = setTimeout(() => {
      const message = `${mode} sync exceeded ${Math.round(maxRuntimeMs / 60000)} minutes and was restarted by watchdog`;
      stats.notes.push(`watchdog 超时重启：${message}`);
      this.state.lastError = message;
      this.state.lastFinishedAt = new Date().toISOString();
      this.state.isRunning = false;
      this.state.currentMode = null;
      try {
        closeRun("failed", message);
      } catch {}
      console.error(`[sync] ${message}`);
      process.exit(124);
    }, maxRuntimeMs);
    timeoutTimer.unref?.();

    try {
      return await this.store.batchMutations(async () => {
      let discoverySourceAvailable = false;
      try {
        const signalFeedResult = await this.syncSignalFeedRecent(mode);
        stats.signalFeedCasesUpserted += signalFeedResult.casesUpserted || 0;
        stats.signalFeedEntriesUpserted += signalFeedResult.docketEntriesUpserted || 0;
        discoverySourceAvailable = discoverySourceAvailable || (signalFeedResult.itemsFetched || 0) > 0;
        if (signalFeedResult.note) {
          stats.notes.push(signalFeedResult.note);
        }
      } catch (error) {
        stats.notes.push(`优先信号源补抓跳过：${error.message}`);
      }

      try {
        const courtFeedResult = await this.syncCourtFeedsRecent(mode);
        stats.courtFeedCasesUpserted += courtFeedResult.casesUpserted || 0;
        stats.courtFeedEntriesUpserted += courtFeedResult.docketEntriesUpserted || 0;
        stats.courtFeedLookups += courtFeedResult.lookupsTriggered || 0;
        discoverySourceAvailable = discoverySourceAvailable || (courtFeedResult.successfulFeeds || 0) > 0;
        if (courtFeedResult.note) {
          stats.notes.push(courtFeedResult.note);
        }
      } catch (error) {
        stats.notes.push(`官方法院 RSS 补源跳过：${error.message}`);
      }

      try {
        const recentFilingsResult = await this.syncRecentFilingsRecent(mode);
        stats.recentFilingsCasesUpserted += recentFilingsResult.casesUpserted || 0;
        stats.recentFilingsLookups += recentFilingsResult.lookupsTriggered || 0;
        discoverySourceAvailable = discoverySourceAvailable || (recentFilingsResult.successfulSources || 0) > 0;
        if (recentFilingsResult.note) {
          stats.notes.push(recentFilingsResult.note);
        }
      } catch (error) {
        stats.notes.push(`官方近期立案页补源跳过：${error.message}`);
      }

      try {
        const lawFirmResult = await this.syncLawFirmRecent(mode);
        stats.lawFirmCasesUpserted += lawFirmResult.casesUpserted || 0;
        stats.lawFirmEntriesUpserted += lawFirmResult.docketEntriesUpserted || 0;
        stats.lawFirmLookups += lawFirmResult.lookupsTriggered || 0;
        discoverySourceAvailable = discoverySourceAvailable || (lawFirmResult.successfulSources || 0) > 0;
        if (lawFirmResult.note) {
          stats.notes.push(lawFirmResult.note);
        }
      } catch (error) {
        stats.notes.push(`律所官网补源跳过：${error.message}`);
      }

      try {
        const pacerResult = await this.syncPacerRecent(mode);
        stats.pacerCasesUpserted += pacerResult.casesUpserted || 0;
        stats.pacerPagesFetched += pacerResult.pagesFetched || 0;
        discoverySourceAvailable = discoverySourceAvailable ||
          Number(pacerResult.casesUpserted || 0) > 0 ||
          Number(pacerResult.pagesFetched || 0) > 0;
        if (pacerResult.note) {
          stats.notes.push(pacerResult.note);
        }
      } catch (error) {
        stats.notes.push(`PACER 官方发现跳过：${error.message}`);
      }

      let successfulPresets = 0;
      let courtListenerRateLimited = false;
      for (const preset of discoveryPresets) {
        try {
          const result = await this.syncPreset(preset, mode);
          stats.pagesFetched += result.pagesFetched;
          stats.casesUpserted += result.casesUpserted;
          stats.docketEntriesUpserted += result.docketEntriesUpserted;
          successfulPresets += 1;
        } catch (error) {
          stats.notes.push(`CourtListener 搜索预设 ${preset.label} 失败：${error.message}`);
          if (error.status === 429) {
            courtListenerRateLimited = true;
            stats.notes.push("CourtListener 已触发限流，本轮停止其余搜索和 docket 请求，等待自动冷却。");
            break;
          }
        }
      }

      if (!successfulPresets && !discoverySourceAvailable && !courtListenerRateLimited) {
        throw new Error("CourtListener 搜索预设全部失败");
      }

      try {
        const recentFollowUpResult = await this.syncRecentExistingCaseFollowUps(mode);
        if (recentFollowUpResult.note) {
          stats.notes.push(recentFollowUpResult.note);
        }
      } catch (error) {
        stats.notes.push(`旧案 recent-filed 跟进跳过：${error.message}`);
      }

      try {
        const priorityFeedResult = await this.syncPriorityFeedRecent(mode);
        stats.priorityFeedCasesSynced += priorityFeedResult.syncedCases;
        if (priorityFeedResult.note) {
          stats.notes.push(priorityFeedResult.note);
        }
      } catch (error) {
        stats.notes.push(`优先目录 补源跳过：${error.message}`);
      }

      if (!courtListenerRateLimited) {
        try {
          const docketResult = await this.syncCourtListenerDockets(mode);
          stats.docketCasesSynced += docketResult.syncedCases;
          if (docketResult.note) {
            stats.notes.push(docketResult.note);
          }
        } catch (error) {
          stats.notes.push(`CourtListener docket 补抓跳过：${error.message}`);
        }
      }

      try {
        const pacerMonitorResult = await this.syncPacerMonitorRecent(mode);
        if (pacerMonitorResult.note) {
          stats.notes.push(pacerMonitorResult.note);
        }
      } catch (error) {
        stats.notes.push(`PACERMonitor 补源跳过：${error.message}`);
      }

      try {
        const docketAlarmResult = await this.syncDocketAlarmRecent(mode);
        stats.docketAlarmCasesSynced += docketAlarmResult.syncedCases || 0;
        if (docketAlarmResult.note) {
          stats.notes.push(docketAlarmResult.note);
        }
      } catch (error) {
        stats.notes.push(`Docket Alarm 补源跳过：${error.message}`);
      }

      try {
        const uniCourtResult = await this.syncUniCourtRecent(mode);
        stats.uniCourtCasesSynced += uniCourtResult.syncedCases || 0;
        if (uniCourtResult.note) {
          stats.notes.push(uniCourtResult.note);
        }
      } catch (error) {
        stats.notes.push(`UniCourt 补源跳过：${error.message}`);
      }

      try {
        const translationResult = await this.translator.translatePending();
        stats.translationsApplied += translationResult.translated || 0;
      } catch (error) {
        stats.notes.push(`翻译链路跳过：${error.message}`);
      }

      closeRun("succeeded");
      try {
        this.store.getDashboardStats();
      } catch (error) {
        stats.notes.push(`缓存预热跳过：${error.message}`);
      }
      this.state.lastFinishedAt = new Date().toISOString();
      this.state.lastStats = stats;
      return this.getPublicStatus();
      });
    } catch (error) {
      this.state.lastError = error.message;
      closeRun("failed", `${error.message}\n${error.body || ""}`.trim());
      throw error;
    } finally {
      clearInterval(heartbeatTimer);
      clearTimeout(timeoutTimer);
      if (!runClosed) {
        this.store.clearSyncRunHeartbeat(runId);
      }
      this.state.isRunning = false;
      this.state.currentMode = null;
    }
  }

  async importLookup(term, { courtName = "", caseName = "", sources = [] } = {}) {
    const rawTerm = String(term || "").trim();
    if (!rawTerm) {
      return {
        imported: 0,
        matched: 0,
        sourceResults: {}
      };
    }

    const requestedSources = new Set(
      (Array.isArray(sources) && sources.length ? sources : ["courtlistener", "recentfilings", "61tro"])
        .map((value) => String(value || "").trim().toLowerCase())
        .filter(Boolean)
    );
    const includeCourtListener = requestedSources.has("courtlistener");
    const includeRecentFilings = requestedSources.has("recentfilings");
    const include61tro = requestedSources.has("61tro");
    const candidates = [];
    const queries = this.buildLookupQueries(rawTerm, { courtName, caseName });
    const sourceResults = {
      courtlistener: {
        attempted: includeCourtListener,
        matched: 0,
        imported: 0
      },
      recentfilings: {
        attempted: includeRecentFilings && docketLooksLike(rawTerm),
        matched: 0,
        imported: 0
      },
      "61tro": {
        attempted: include61tro && docketLooksLike(rawTerm),
        matched: 0,
        imported: 0
      }
    };

    if (includeCourtListener) {
      for (const query of queries) {
        const payload = await this.courtListener.search({
          query,
          startDate: getDiscoveryStartDate(this.config),
          pageSize: 20
        });

        const matches = (payload.results || []).filter((result) =>
          this.lookupMatches(rawTerm, result, { courtName, caseName })
        );
        for (const match of matches) {
          if (!candidates.some((item) => item.docket_id === match.docket_id)) {
            candidates.push(match);
          }
        }

        if (candidates.length >= 10) {
          break;
        }
      }

      sourceResults.courtlistener.matched = candidates.length;
    }

    const ingest = includeCourtListener
      ? this.ingestSearchResults(candidates, {
          tags: []
        })
      : { casesUpserted: 0 };
    let imported = ingest.casesUpserted;
    let matched = candidates.length;
    sourceResults.courtlistener.imported = Number(ingest.casesUpserted || 0);

    if (docketLooksLike(rawTerm)) {
      const caseIndex = this.buildCourtFeedCaseIndex();

      const recentFilingsMatch = includeRecentFilings
        ? await this.recentFilings.lookupByDocket(rawTerm, { courtName }).catch(() => null)
        : null;
      if (recentFilingsMatch?.item) {
        const recentIngest = this.ingestRecentFilingsItems(
          {
            sourceId: recentFilingsMatch.source?.id || "recentfilings",
            note: recentFilingsMatch.note || null,
            items: [recentFilingsMatch.item]
          },
          caseIndex
        );
        imported += Number(recentIngest.casesUpserted || 0);
        matched += 1;
        sourceResults.recentfilings.matched = 1;
        sourceResults.recentfilings.imported = Number(recentIngest.casesUpserted || 0);
      }

      const supplementalMatch = include61tro
        ? await this.lawFirms.lookupByDocket(rawTerm, {
            sourceIds: ["61tro"],
            courtName,
            caseName
          }).catch(() => null)
        : null;
      if (supplementalMatch?.item) {
        const supplementalIngest = this.ingestLawFirmItems(
          {
            source: supplementalMatch.source || null,
            note: supplementalMatch.note || null,
            items: [supplementalMatch.item]
          },
          caseIndex
        );
        imported += Number(supplementalIngest.docketEntriesUpserted || supplementalIngest.casesUpserted || 0);
        matched += 1;
        sourceResults["61tro"].matched = 1;
        sourceResults["61tro"].imported = Number(
          supplementalIngest.docketEntriesUpserted || supplementalIngest.casesUpserted || 0
        );
      }
    }

    return {
      imported,
      matched,
      sourceResults
    };
  }

  buildLookupQueries(term, { courtName = "", caseName = "" } = {}) {
    const queries = [];
    const docketTerm = normalizeDocket(term);
    const shortCaseName = String(caseName || "").trim().slice(0, 120);

    if (docketLooksLike(term)) {
      queries.push(`docketNumber:${docketTerm}`);
      queries.push(`"${docketTerm}"`);
      if (courtName) {
        queries.push(`"${docketTerm}" "${courtName}"`);
      }
      if (shortCaseName) {
        queries.push(`"${docketTerm}" "${shortCaseName}"`);
      }
    }

    queries.push(`"${term}"`);
    if (courtName) {
      queries.push(`"${term}" "${courtName}"`);
    }
    if (shortCaseName) {
      queries.push(`"${term}" "${shortCaseName}"`);
    }
    queries.push(term);

    return [...new Set(queries)];
  }

  lookupMatches(term, result, { courtName = "", caseName = "" } = {}) {
    const docketTerm = normalizeDocket(term);
    const resultDocket = normalizeDocket(result.docketNumber);
    const text = normalizeText([result.caseName, result.docketNumber, result.court].join(" | "));
    const caseNameMatch =
      caseName && normalizeLookupText(result.caseName).includes(normalizeLookupText(String(caseName).slice(0, 120)));
    const courtMatch = !courtName || courtNamesLikelyMatch(courtName, result.court);

    if (docketTerm) {
      if ((resultDocket === docketTerm || resultDocket.endsWith(docketTerm)) && (courtMatch || caseNameMatch)) {
        return true;
      }
    }

    if (caseNameMatch && courtMatch) {
      return true;
    }

    return text.includes(normalizeText(term));
  }

  classifyPacerDiscoveryCase(item) {
    return classifyCase(
      {
        caseName: item.caseTitle,
        case_name_full: item.caseTitle,
        court: item.courtName,
        cause: item.natureOfSuit,
        party: [],
        recap_documents: [
          {
            short_description: item.natureOfSuit
          }
        ]
      },
      []
    );
  }

  shouldTrackPacerDiscoveryCase(item, existingCase, tags) {
    if (!/\b\d{2}-cv-\d{3,6}\b/i.test(String(item.caseNumberFull || ""))) {
      return false;
    }

    if (existingCase) {
      return true;
    }

    const text = normalizeText([item.caseTitle, item.natureOfSuit].join(" | "));
    return Boolean(
      tags.length > 0 ||
      /\b(820|830|840)\b/.test(String(item.natureOfSuit || "")) ||
      text.includes("schedule a") ||
      text.includes("unincorporated associations") ||
      text.includes("trademark") ||
      text.includes("copyright") ||
      text.includes("patent") ||
      text.includes("infringement")
    );
  }

  ingestPacerDiscoveryResults(items = []) {
    let casesUpserted = 0;
    const timestamp = new Date().toISOString();

    for (const item of items) {
      const existingCase = this.store.findCaseByCourtAndDocket({
        courtId: valueOf(item.courtId),
        courtName: valueOf(item.courtName),
        docketNumber: valueOf(item.caseNumberFull),
        startDate: getDiscoveryStartDate(this.config)
      });
      const tags = this.classifyPacerDiscoveryCase(item);
      if (!this.shouldTrackPacerDiscoveryCase(item, existingCase, tags)) {
        continue;
      }

      const parties = deriveParties({
        caseName: item.caseTitle,
        party: []
      });
      const mergedRaw = {
        ...(existingCase?.raw || {}),
        pacer: {
          ...(existingCase?.raw?.pacer || {}),
          ...item.raw,
          caseLink: item.caseLink || null,
          courtId: item.pacerCourtId || item.courtId || null,
          syncedAt: timestamp
        }
      };

      const savedCase = this.store.upsertCase({
        source_case_key: existingCase?.source_case_key || `pacer:${item.pacerCourtId || item.courtId}:${item.caseId || item.caseNumberFull}`,
        primary_source: existingCase?.primary_source || "pacer",
        source_case_id: existingCase?.source_case_id || item.caseId || item.caseNumberFull,
        courtlistener_docket_id: existingCase?.courtlistener_docket_id ?? null,
        pacer_case_id: valueOf(item.caseId, existingCase?.pacer_case_id),
        court_id: valueOf(existingCase?.court_id, item.courtId),
        court_name: valueOf(existingCase?.court_name, item.courtName),
        case_name: valueOf(item.caseTitle, existingCase?.case_name),
        docket_number: valueOf(item.caseNumberFull, existingCase?.docket_number),
        date_filed: valueOf(item.dateFiled, existingCase?.date_filed),
        date_terminated: valueOf(item.dateTermed, existingCase?.date_terminated),
        cause: valueOf(existingCase?.cause),
        nature_of_suit: valueOf(item.natureOfSuit, existingCase?.nature_of_suit),
        status: item.dateTermed ? "terminated" : existingCase?.status || "open",
        tags_marker: buildTagsMarker([...(existingCase?.tags || []), ...tags]),
        docket_url: valueOf(item.caseLink, existingCase?.docket_url),
        source_urls: [...(existingCase?.source_urls || []), item.caseLink].filter(Boolean),
        plaintiffs: parties.plaintiffs.length ? parties.plaintiffs : existingCase?.plaintiffs || [],
        defendants: parties.defendants.length ? parties.defendants : existingCase?.defendants || [],
        recent_activity_summary: existingCase?.recent_activity_summary || item.natureOfSuit || null,
        latest_docket_filed_at: valueOf(existingCase?.latest_docket_filed_at, item.dateFiled),
        latest_docket_number: existingCase?.latest_docket_number || null,
        docket_count: Math.max(existingCase?.docket_count || 0, 0),
        last_seen_at: timestamp,
        last_synced_at: timestamp,
        raw: mergedRaw
      });

      if (savedCase) {
        casesUpserted += 1;
      }
    }

    return {
      casesUpserted
    };
  }

  buildCourtFeedCaseIndex() {
    const rows = this.store.getHydratedCases(getDiscoveryStartDate(this.config));
    const index = new Map();

    for (const row of rows) {
      const keys = [
        buildCourtDocketKey(row.court_id, row.docket_number),
        buildCourtDocketKey(row.court_name, row.docket_number)
      ].filter(Boolean);

      for (const key of keys) {
        if (!index.has(key)) {
          index.set(key, row);
        }
      }
    }

    return index;
  }

  classifySignalFeedItem(item) {
    const tags = classifyCase(
      {
        caseName: item.caseName,
        case_name_full: item.caseName,
        court: item.courtName,
        party: [],
        recap_documents: (item.entries || []).map((entry) => ({
          short_description: "Docket Entry",
          description: entry.title
        }))
      },
      []
    );

    return [...new Set([
      ...tags,
      "tro",
      normalizeText(item.caseName || "").includes("schedule a") ? "schedule_a" : null
    ].filter(Boolean))];
  }

  ingestSignalFeedItems(items = []) {
    const caseIndex = this.buildCourtFeedCaseIndex();
    const timestamp = new Date().toISOString();
    let casesUpserted = 0;
    let docketEntriesUpserted = 0;

    for (const item of items) {
      const docketNumber = String(item.docketNumber || "").trim();
      if (!docketLooksLike(docketNumber)) {
        continue;
      }

      const primaryKey = buildCourtDocketKey(item.courtId, docketNumber);
      const fallbackKey = buildCourtDocketKey(item.courtName, docketNumber);
      const existingCase =
        caseIndex.get(primaryKey) ||
        caseIndex.get(fallbackKey) ||
        this.store.findCaseByCourtAndDocket({
          courtId: item.courtId,
          courtName: item.courtName,
          docketNumber,
          startDate: getDiscoveryStartDate(this.config)
        });
      const entries = Array.isArray(item.entries) ? item.entries : [];
      const latestEntry = entries.reduce((latest, entry) => {
        if (!latest) {
          return entry;
        }
        return String(entry.filedAt || "").localeCompare(String(latest.filedAt || "")) > 0 ? entry : latest;
      }, null);
      const tags = this.classifySignalFeedItem(item);
      const derivedParties = deriveParties({
        caseName: item.caseName || `${item.plaintiff || "Unknown Plaintiff"} v. Schedule A Defendants`,
        party: []
      });
      const plaintiffs = uniqueByNormalized([
        ...(Array.isArray(item.plaintiffs) ? item.plaintiffs : []),
        item.plaintiff,
        ...derivedParties.plaintiffs
      ]);
      const defendants = uniqueByNormalized([
        ...(Array.isArray(item.defendants) ? item.defendants : []),
        item.defendant,
        ...derivedParties.defendants
      ]);
      const mergedRaw = {
        ...(existingCase?.raw || {}),
        signal_feed: {
          ...(existingCase?.raw?.signal_feed || {}),
          docketId: item.docketId || null,
          detailUrl: item.detailUrl || null,
          officialCaseUrl: item.caseUrl || null,
          judge: item.judge || null,
          counsel: item.counsel || null,
          counselFirm: item.counselFirm || null,
          entryCount: entries.length,
          syncedAt: timestamp
        }
      };

      const savedCase = this.store.upsertCase({
        source_case_key: existingCase?.source_case_key || `${SIGNAL_FEED_PROVIDER_KEY}:${item.docketId || normalizeDocketIdentity(docketNumber)}`,
        // Let Store apply source authority instead of pinning an older source.
        primary_source: SIGNAL_FEED_PROVIDER_KEY,
        source_case_id: existingCase?.source_case_id || item.docketId || docketNumber,
        courtlistener_docket_id: existingCase?.courtlistener_docket_id ?? null,
        pacer_case_id: existingCase?.pacer_case_id ?? null,
        court_id: item.courtId || existingCase?.court_id || null,
        court_name: item.courtName || existingCase?.court_name || null,
        case_name: item.caseName || existingCase?.case_name || `${item.plaintiff || "Unknown Plaintiff"} v. Schedule A Defendants`,
        docket_number: docketNumber || existingCase?.docket_number || null,
        date_filed: item.dateFiled || existingCase?.date_filed || null,
        date_terminated: existingCase?.date_terminated || null,
        cause: existingCase?.cause || null,
        nature_of_suit: existingCase?.nature_of_suit || null,
        status: item.status || existingCase?.status || "open",
        tags_marker: buildTagsMarker([...(existingCase?.tags || []), ...tags]),
        docket_url: item.caseUrl || existingCase?.docket_url || null,
        source_urls: [...(existingCase?.source_urls || []), item.caseUrl].filter(Boolean),
        plaintiffs: plaintiffs.length ? plaintiffs : existingCase?.plaintiffs || [],
        defendants: defendants.length ? defendants : existingCase?.defendants || [],
        recent_activity_summary: latestEntry?.title || existingCase?.recent_activity_summary || null,
        latest_docket_filed_at: laterIso(existingCase?.latest_docket_filed_at, latestEntry?.filedAt || item.dateFiled) || null,
        latest_docket_number: higherOrderValue(existingCase?.latest_docket_number, latestEntry?.documentNumber) || null,
        docket_count: Math.max(Number(existingCase?.docket_count || 0), entries.length),
        last_seen_at: latestEntry?.filedAt || item.dateFiled || timestamp,
        last_synced_at: timestamp,
        last_docket_sync_at: entries.length ? timestamp : existingCase?.last_docket_sync_at || null,
        raw: mergedRaw
      });

      if (!savedCase) {
        continue;
      }
      casesUpserted += 1;

      for (const entry of entries) {
        const documentNumber = entry.documentNumber ?? null;
        const sourceEntryId = entry.entryId || `${item.docketId || normalizeDocketIdentity(docketNumber)}:${documentNumber || entry.filedAt || crypto.randomUUID()}`;
        const savedEntry = this.store.upsertDocketEntry({
          case_id: savedCase.id,
          source_entry_key: `${SIGNAL_FEED_PROVIDER_KEY}:${sourceEntryId}`,
          primary_source: SIGNAL_FEED_PROVIDER_KEY,
          source_entry_id: sourceEntryId,
          document_type: "Docket Entry",
          entry_number: documentNumber,
          document_number: documentNumber,
          filed_at: entry.filedAt || item.dateFiled || null,
          description: entry.title || "Docket entry",
          absolute_url: entry.documentUrl || null,
          is_available: entry.documentUrl && !entry.restricted ? 1 : 0,
          page_count: null,
          pacer_doc_id: null,
          raw: {
            signal_feed: {
              ...entry,
              docketId: item.docketId || null
            }
          },
          last_synced_at: timestamp
        });
        if (savedEntry) {
          docketEntriesUpserted += 1;
        }
      }

      for (const key of [primaryKey, fallbackKey].filter(Boolean)) {
        caseIndex.set(key, savedCase);
      }
    }

    return { casesUpserted, docketEntriesUpserted };
  }

  async syncSignalFeedRecent(mode = "recent") {
    if (!this.signalFeed.enabled || mode !== "recent") {
      return {
        itemsFetched: 0,
        casesUpserted: 0,
        docketEntriesUpserted: 0,
        note: this.signalFeed.enabled ? null : "优先信号源已关闭。"
      };
    }

    return this.store.batchMutations(async () => {
      const result = await this.signalFeed.fetchRecent({ hydrate: true });
      const recentItems = result.items || [];
      const ingest = this.ingestSignalFeedItems(recentItems);
      const recentDocketIds = new Set(recentItems.map((item) => String(item.docketId || "")).filter(Boolean));
      const candidates = this.store.listSignalFeedRefreshCandidates({
        limit: this.config.signalFeed.refreshMaxCasesPerRun,
        staleAfterMinutes: this.config.signalFeed.staleAfterMinutes
      });
      let refreshedCases = 0;
      let refreshErrors = 0;

      for (const caseRow of candidates) {
        const signalRaw = caseRow.raw?.signal_feed || {};
        if (signalRaw.docketId && recentDocketIds.has(String(signalRaw.docketId))) {
          continue;
        }
        try {
          const item = await this.signalFeed.fetchKnownCase({
            docketId: signalRaw.docketId,
            docketNumber: caseRow.docket_number,
            detailUrl: signalRaw.detailUrl,
            caseName: caseRow.case_name,
            courtId: caseRow.court_id,
            courtName: caseRow.court_name,
            dateFiled: caseRow.date_filed
          });
          if (!item) {
            continue;
          }
          const refreshed = this.ingestSignalFeedItems([item]);
          ingest.casesUpserted += refreshed.casesUpserted;
          ingest.docketEntriesUpserted += refreshed.docketEntriesUpserted;
          refreshedCases += 1;
        } catch {
          refreshErrors += 1;
        }
      }
      return {
        itemsFetched: Number(recentItems.length),
        refreshedCases,
        refreshErrors,
        casesUpserted: ingest.casesUpserted,
        docketEntriesUpserted: ingest.docketEntriesUpserted,
        note: `优先信号源本轮检查 ${Number(recentItems.length)} 个最新案件、复查 ${refreshedCases} 个已知案件，写入 ${ingest.casesUpserted} 个案件、${ingest.docketEntriesUpserted} 条 docket${refreshErrors ? `，${refreshErrors} 个复查暂时失败` : ""}。`
      };
    });
  }

  async enrichCaseWithSignalFeed(caseId, { force = false } = {}) {
    return this.store.batchMutations(async () => {
      const caseRow = this.store.getCase(caseId);
      if (!caseRow || !this.signalFeed.enabled || !docketLooksLike(caseRow.docket_number)) {
        return { enriched: false, reason: "not-applicable" };
      }

      const syncedAt = caseRow.raw?.signal_feed?.syncedAt
        ? Date.parse(caseRow.raw.signal_feed.syncedAt)
        : 0;
      if (!force && syncedAt && Date.now() - syncedAt < 2 * 60 * 60 * 1000) {
        return { enriched: false, reason: "fresh" };
      }

      const item = await this.signalFeed.lookupByDocket(caseRow.docket_number);
      if (!item) {
        return { enriched: false, reason: "not-found" };
      }

      const ingest = this.ingestSignalFeedItems([item]);
      return {
        enriched: ingest.casesUpserted > 0 || ingest.docketEntriesUpserted > 0,
        casesUpserted: ingest.casesUpserted,
        docketEntriesUpserted: ingest.docketEntriesUpserted
      };
    });
  }

  classifyRecentFilingsItem(item) {
    return classifyCase(
      {
        caseName: item.caseName,
        case_name_full: item.caseName,
        court: item.courtName,
        cause: item.cause,
        nature_of_suit: item.natureOfSuit,
        party: [],
        recap_documents: [
          {
            short_description: item.category || "Official Recent Filing",
            description: [item.natureOfSuit, item.cause, item.judge].filter(Boolean).join(" | ")
          }
        ]
      },
      []
    );
  }

  shouldTrackRecentFilingsItem(item, existingCase, tags) {
    const docketNumber = String(item.docketNumber || "");
    if (!/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
      return false;
    }

    if (existingCase) {
      return true;
    }

    const text = normalizeText(
      [
        item.caseName,
        item.category,
        item.natureOfSuit,
        item.cause
      ].join(" | ")
    );

    return (
      tags.length > 0 ||
      /\b(820|830|840)\b/.test(text) ||
      /\bschedule a\b/.test(text) ||
      /\btemporary restraining order\b/.test(text) ||
      /\btrademark\b|\bcopyright\b|\bpatent\b|\blanham act\b|\bcounterfeit\b|\binfringement\b/.test(text)
    );
  }

  ingestRecentFilingsItems(sourceResult, caseIndex, { preexistingCaseIds = new Set() } = {}) {
    let casesUpserted = 0;
    const lookupCandidates = new Map();
    const followUpCandidates = new Map();
    const timestamp = new Date().toISOString();

    for (const item of sourceResult.items || []) {
      const primaryKey = buildCourtDocketKey(item.courtId, item.docketNumber);
      const fallbackKey = buildCourtDocketKey(item.courtName, item.docketNumber);
      const existingCase = caseIndex.get(primaryKey) || caseIndex.get(fallbackKey) || null;
      const existedBeforeRun = Boolean(existingCase?.id) && preexistingCaseIds.has(Number(existingCase.id));
      const tags = this.classifyRecentFilingsItem(item);

      if (!this.shouldTrackRecentFilingsItem(item, existingCase, tags)) {
        continue;
      }

      const parties = deriveParties({
        caseName: item.caseName,
        party: []
      });
      const mergedRaw = {
        ...(existingCase?.raw || {}),
        recent_filings: {
          ...(existingCase?.raw?.recent_filings || {}),
          [item.sourceId]: {
            sourceUrl: item.sourceUrl,
            caseUrl: item.caseUrl,
            docketUrl: item.docketUrl,
            rawDocketNumber: item.rawDocketNumber || item.docketNumber,
            judge: item.judge || null,
            category: item.category || null,
            dateFiled: item.dateFiled || null,
            natureOfSuit: item.natureOfSuit || null,
            cause: item.cause || null,
            pacerCaseId: item.pacerCaseId || null,
            syncedAt: timestamp,
            note: sourceResult.note || null,
            rawMeta: item.rawMeta || {}
          }
        }
      };

      const savedCase = this.store.upsertCase({
        source_case_key:
          existingCase?.source_case_key ||
          `recentfilings:${item.sourceId}:${normalizeDocketIdentity(item.docketNumber)}`,
        primary_source: existingCase?.primary_source || "recentfilings",
        source_case_id: existingCase?.source_case_id || item.pacerCaseId || item.caseUrl || item.docketNumber,
        courtlistener_docket_id: existingCase?.courtlistener_docket_id ?? null,
        pacer_case_id: existingCase?.pacer_case_id ?? item.pacerCaseId ?? null,
        court_id: item.courtId || existingCase?.court_id || null,
        court_name: item.courtName || existingCase?.court_name || null,
        case_name:
          item.sourceId === "61tro"
            ? existingCase?.case_name || item.caseName || null
            : item.caseName || existingCase?.case_name || null,
        docket_number: item.docketNumber || existingCase?.docket_number || null,
        date_filed: existingCase?.date_filed || item.dateFiled || null,
        date_terminated: existingCase?.date_terminated || null,
        cause: existingCase?.cause || item.cause || null,
        nature_of_suit: existingCase?.nature_of_suit || item.natureOfSuit || null,
        status: existingCase?.status || "open",
        tags_marker: buildTagsMarker([...(existingCase?.tags || []), ...tags]),
        docket_url: item.caseUrl || item.docketUrl || existingCase?.docket_url || null,
        source_urls: [
          ...(existingCase?.source_urls || []),
          item.sourceUrl,
          item.caseUrl,
          item.docketUrl
        ].filter(Boolean),
        plaintiffs: existingCase?.plaintiffs?.length ? existingCase.plaintiffs : parties.plaintiffs,
        defendants: existingCase?.defendants?.length ? existingCase.defendants : parties.defendants,
        recent_activity_summary:
          existingCase?.recent_activity_summary ||
          [item.category, item.natureOfSuit, item.cause].filter(Boolean).join(" | ") ||
          sourceResult.note ||
          null,
        latest_docket_filed_at: laterIso(existingCase?.latest_docket_filed_at, item.dateFiled) || null,
        latest_docket_number: existingCase?.latest_docket_number || null,
        docket_count: existingCase?.docket_count || 0,
        last_seen_at: item.dateFiled || timestamp,
        last_synced_at: timestamp,
        last_docket_sync_at: existingCase?.last_docket_sync_at || null,
        raw: mergedRaw
      });

      if (!savedCase) {
        continue;
      }

      casesUpserted += 1;

      if (existedBeforeRun) {
        const followUpCandidate = this.buildRecentFilingsCrossSourceFollowUpCandidate(item, existingCase, savedCase, tags);
        this.mergeCrossSourceFollowUpCandidate(followUpCandidates, followUpCandidate);
      }

      for (const key of [primaryKey, fallbackKey].filter(Boolean)) {
        caseIndex.set(key, savedCase);
      }

      if (!savedCase.courtlistener_docket_id && item.docketNumber) {
        const lookupKey = buildCourtDocketKey(item.courtId, item.docketNumber) || buildCourtDocketKey(item.courtName, item.docketNumber);
        if (lookupKey && !lookupCandidates.has(lookupKey)) {
          lookupCandidates.set(lookupKey, {
            docketNumber: item.docketNumber,
            caseName: item.caseName,
            courtName: item.courtName,
            priority: tags.includes("seller_tro") ? 0 : tags.includes("tro") || tags.includes("schedule_a") ? 1 : 2,
            filedAt: item.dateFiled || timestamp
          });
        }
      }
    }

    return {
      casesUpserted,
      lookupCandidates,
      followUpCandidates
    };
  }

  classifyCourtFeedItem(item) {
    return classifyCase(
      {
        caseName: item.caseName,
        case_name_full: item.caseName,
        court: item.courtName,
        party: [],
        recap_documents: [
          {
            short_description: item.documentType,
            description: item.description
          }
        ]
      },
      []
    );
  }

  matchesCourtFeedWatchKeywords(item) {
    const text = buildCourtFeedWatchText(item);
    if (!text) {
      return false;
    }

    const keywords = asArray(this.config.courtFeeds.watchKeywords)
      .map((value) => normalizeText(value))
      .filter(Boolean);
    const lawFirms = asArray(this.config.courtFeeds.watchLawFirms)
      .map((value) => normalizeText(value))
      .filter(Boolean);

    return keywords.some((value) => text.includes(value)) || lawFirms.some((value) => text.includes(value));
  }

  shouldTrackCourtFeedItem(item, existingCase, tags) {
    if (!/\b\d{2}-cv-\d{3,6}\b/i.test(String(item.docketNumber || ""))) {
      return false;
    }

    if (hasCourtFeedNegativeSignals(item, existingCase)) {
      return false;
    }

    const { text, cause, natureOfSuit } = buildCourtFeedTrackingContext(item, existingCase);

    if (existingCase) {
      if (!hasHighConfidenceTargetCase(existingCase) && !this.matchesCourtFeedWatchKeywords(item)) {
        return false;
      }
      return hasTargetSignals({ text, tags, cause, natureOfSuit, existingCase }) || this.matchesCourtFeedWatchKeywords(item);
    }

    if (this.config.courtFeeds.requireKeywordHit && this.matchesCourtFeedWatchKeywords(item)) {
      return true;
    }

    return hasTargetSignals({ text, tags, cause, natureOfSuit });
  }

  buildCrossSourceFollowUpCandidate({
    existingCase,
    savedCase,
    tags = [],
    filedAt = null,
    text = "",
    cause = "",
    natureOfSuit = "",
    hasSignal = false
  } = {}) {
    if (!existingCase?.id || !savedCase?.id || !hasCivilCaseDocket(savedCase) || !hasSignal) {
      return null;
    }
    const hasStrongTargetSignals = hasTargetSignals({
      text,
      tags,
      cause,
      natureOfSuit,
      existingCase: savedCase
    });
    if (!hasStrongTargetSignals) {
      return null;
    }

    const hasCourtListenerPath = Boolean(savedCase.courtlistener_docket_id || savedCase.docket_number || savedCase.case_name);
    const hasPriorityFeedPath = shouldAttemptPriorityFeedForCase(savedCase, { force: false });
    const hasRecentFilingsPath = this.canEnrichCaseWithRecentFilings(savedCase);
    const hasLawFirm61troPath = this.canEnrichCaseWithLawFirmLookup(savedCase, { sourceIds: ["61tro"] });
    if (!hasCourtListenerPath && !hasPriorityFeedPath && !hasRecentFilingsPath && !hasLawFirm61troPath) {
      return null;
    }

    const priority =
      tags.includes("seller_tro")
        ? 0
        : tags.includes("tro") || tags.includes("schedule_a")
          ? 1
          : hasPriorityFeedPath
            ? 2
            : 3;

    return {
      caseId: Number(savedCase.id),
      docketNumber: savedCase.docket_number || existingCase.docket_number || null,
      filedAt: filedAt || savedCase.latest_docket_filed_at || new Date().toISOString(),
      priority,
      priorityFeed: hasPriorityFeedPath,
      courtListener: hasCourtListenerPath,
      recentFilings: hasRecentFilingsPath,
      lawFirm61tro: hasLawFirm61troPath
    };
  }

  buildCourtFeedCrossSourceFollowUpCandidate(item, existingCase, savedCase, tags, savedEntry) {
    if (hasCourtFeedNegativeSignals(item, savedCase)) {
      return null;
    }

    if (!hasHighConfidenceTargetCase(existingCase || savedCase)) {
      return null;
    }

    const { text, cause, natureOfSuit } = buildCourtFeedTrackingContext(item, savedCase);
    return this.buildCrossSourceFollowUpCandidate({
      existingCase,
      savedCase,
      tags,
      filedAt: item.filedAt,
      text,
      cause,
      natureOfSuit,
      hasSignal: Boolean(savedEntry)
    });
  }

  buildRecentFilingsCrossSourceFollowUpCandidate(item, existingCase, savedCase, tags) {
    const text = normalizeText([
      item.caseName,
      item.courtName,
      item.category,
      item.natureOfSuit,
      item.cause,
      item.docketNumber
    ].filter(Boolean).join(" | "));

    return this.buildCrossSourceFollowUpCandidate({
      existingCase,
      savedCase,
      tags,
      filedAt: item.dateFiled,
      text,
      cause: normalizeText(item.cause || savedCase?.cause || ""),
      natureOfSuit: normalizeText(item.natureOfSuit || savedCase?.nature_of_suit || ""),
      hasSignal: Boolean(item.dateFiled)
    });
  }

  buildLawFirmCrossSourceFollowUpCandidate(item, existingCase, savedCase, tags, latestEntry, itemDocketEntriesUpserted) {
    const text = normalizeText([
      item.title,
      item.caseName,
      item.summary,
      item.lawFirm,
      latestEntry?.description,
      item.docketNumber
    ].filter(Boolean).join(" | "));

    return this.buildCrossSourceFollowUpCandidate({
      existingCase,
      savedCase,
      tags,
      filedAt: latestEntry?.filedAt || item.dateFiled,
      text,
      cause: normalizeText(savedCase?.cause || ""),
      natureOfSuit: normalizeText(savedCase?.nature_of_suit || ""),
      hasSignal: Number(itemDocketEntriesUpserted || 0) > 0 || Boolean(latestEntry?.filedAt || item.dateFiled)
    });
  }

  mergeCrossSourceFollowUpCandidate(targetMap, candidate) {
    if (!candidate?.caseId) {
      return;
    }

    const existing = targetMap.get(candidate.caseId);
    if (!existing) {
      targetMap.set(candidate.caseId, candidate);
      return;
    }

    targetMap.set(candidate.caseId, {
      ...existing,
      priority: Math.min(Number(existing.priority ?? 99), Number(candidate.priority ?? 99)),
      filedAt: laterIso(existing.filedAt, candidate.filedAt) || existing.filedAt || candidate.filedAt || null,
      priorityFeed: Boolean(existing.priorityFeed || candidate.priorityFeed),
      courtListener: Boolean(existing.courtListener || candidate.courtListener),
      recentFilings: Boolean(existing.recentFilings || candidate.recentFilings),
      lawFirm61tro: Boolean(existing.lawFirm61tro || candidate.lawFirm61tro)
    });
  }

  ingestCourtFeedItems(feedResult, caseIndex, { preexistingCaseIds = new Set() } = {}) {
    let casesUpserted = 0;
    let docketEntriesUpserted = 0;
    const lookupCandidates = new Map();
    const followUpCandidates = new Map();
    const timestamp = new Date().toISOString();

    for (const item of feedResult.items || []) {
      const primaryKey = buildCourtDocketKey(item.courtId, item.docketNumber);
      const fallbackKey = buildCourtDocketKey(item.courtName, item.docketNumber);
      const existingCase = caseIndex.get(primaryKey) || caseIndex.get(fallbackKey) || null;
      const existedBeforeRun = Boolean(existingCase?.id) && preexistingCaseIds.has(Number(existingCase.id));
      const tags = this.classifyCourtFeedItem(item);

      if (!this.shouldTrackCourtFeedItem(item, existingCase, tags)) {
        continue;
      }

      const parties = deriveParties({
        caseName: item.caseName,
        party: []
      });
      const latestDocketFiledAt = laterIso(existingCase?.latest_docket_filed_at, item.filedAt);
      const latestDocketNumber = higherOrderValue(existingCase?.latest_docket_number, item.documentNumber);
      const docketCountFloor = parseNumericLike(item.documentNumber) || 0;
      const mergedRaw = {
        ...(existingCase?.raw || {}),
        court_feed: {
          ...(existingCase?.raw?.court_feed || {}),
          syncedAt: timestamp,
          lastFeedId: item.feedId,
          lastFeedUrl: item.feedUrl,
          lastBuildDate: feedResult.lastBuildDate || existingCase?.raw?.court_feed?.lastBuildDate || null,
          lastSeenAt: item.filedAt || timestamp,
          latestGuid: item.guid,
          latestDocumentNumber: item.documentNumber || null,
          latestDocumentType: item.documentType || null,
          feeds: {
            ...(existingCase?.raw?.court_feed?.feeds || {}),
            [item.feedId]: {
              guid: item.guid,
              link: item.link,
              documentUrl: item.documentUrl,
              documentNumber: item.documentNumber || null,
              sequenceNumber: item.sequenceNumber || null,
              seenAt: item.filedAt || timestamp,
              lastBuildDate: feedResult.lastBuildDate || null
            }
          }
        }
      };

      const savedCase = this.store.upsertCase({
        source_case_key:
          existingCase?.source_case_key ||
          `courtfeed:${item.courtId}:${item.caseId || item.reportCaseId || normalizeDocketIdentity(item.docketNumber)}`,
        primary_source: existingCase?.primary_source || "courtfeed",
        source_case_id: existingCase?.source_case_id || item.caseId || item.reportCaseId || item.docketNumber,
        courtlistener_docket_id: existingCase?.courtlistener_docket_id ?? null,
        pacer_case_id: existingCase?.pacer_case_id ?? null,
        court_id: item.courtId || existingCase?.court_id || null,
        court_name: item.courtName || existingCase?.court_name || null,
        case_name: item.caseName || existingCase?.case_name || null,
        docket_number: item.docketNumber || existingCase?.docket_number || null,
        date_filed: existingCase?.date_filed || (item.filedAt ? item.filedAt.slice(0, 10) : null),
        date_terminated: existingCase?.date_terminated || null,
        cause: existingCase?.cause || null,
        nature_of_suit: existingCase?.nature_of_suit || null,
        status: existingCase?.status || "open",
        tags_marker: buildTagsMarker([...(existingCase?.tags || []), ...tags]),
        docket_url: item.link || existingCase?.docket_url || null,
        source_urls: [...(existingCase?.source_urls || []), item.link, item.documentUrl, item.feedUrl].filter(Boolean),
        plaintiffs: existingCase?.plaintiffs?.length ? existingCase.plaintiffs : parties.plaintiffs,
        defendants: existingCase?.defendants?.length ? existingCase.defendants : parties.defendants,
        recent_activity_summary: item.description || item.documentType || existingCase?.recent_activity_summary || null,
        latest_docket_filed_at: latestDocketFiledAt || existingCase?.latest_docket_filed_at || null,
        latest_docket_number: latestDocketNumber || existingCase?.latest_docket_number || null,
        docket_count: Math.max(existingCase?.docket_count || 0, docketCountFloor),
        last_seen_at: item.filedAt || timestamp,
        last_synced_at: timestamp,
        last_docket_sync_at: existingCase?.last_docket_sync_at || null,
        raw: mergedRaw
      });

      if (!savedCase) {
        continue;
      }

      casesUpserted += 1;

      const entryKey = `courtfeed:${item.courtId}:${item.guid || `${item.caseId || item.reportCaseId}:${item.sequenceNumber || item.documentNumber}`}`;
      const savedEntry = this.store.upsertDocketEntry({
        case_id: savedCase.id,
        source_entry_key: entryKey,
        primary_source: "courtfeed",
        source_entry_id: item.sequenceNumber || item.documentNumber || item.guid,
        document_type: item.documentType || "Court RSS Entry",
        entry_number: item.documentNumber || item.sequenceNumber || null,
        document_number: item.documentNumber || null,
        filed_at: item.filedAt,
        description: item.description || item.documentType || item.title,
        absolute_url: item.documentUrl || item.link,
        is_available: item.documentUrl ? 1 : 0,
        page_count: null,
        pacer_doc_id: item.sequenceNumber || null,
        raw: item,
        last_synced_at: timestamp
      });

      if (savedEntry) {
        docketEntriesUpserted += 1;

        if (existedBeforeRun) {
          const followUpCandidate = this.buildCourtFeedCrossSourceFollowUpCandidate(
            item,
            existingCase,
            savedCase,
            tags,
            savedEntry
          );
          this.mergeCrossSourceFollowUpCandidate(followUpCandidates, followUpCandidate);
        }
      }

      for (const key of [primaryKey, fallbackKey].filter(Boolean)) {
        caseIndex.set(key, savedCase);
      }

      if (!savedCase.courtlistener_docket_id && item.docketNumber) {
        const lookupKey = buildCourtDocketKey(item.courtId, item.docketNumber) || buildCourtDocketKey(item.courtName, item.docketNumber);
        if (lookupKey && !lookupCandidates.has(lookupKey)) {
          lookupCandidates.set(lookupKey, {
            docketNumber: item.docketNumber,
            caseName: item.caseName,
            courtName: item.courtName,
            priority: tags.includes("seller_tro") ? 0 : tags.includes("tro") || tags.includes("schedule_a") ? 1 : 2,
            filedAt: item.filedAt || timestamp
          });
        }
      }
    }

    return {
      casesUpserted,
      docketEntriesUpserted,
      lookupCandidates
    };
  }

  async lookupCourtFeedCandidates(candidates) {
    return this.lookupDiscoveryCandidates(candidates, this.courtFeeds.maxLookupsPerRun);
  }

  async lookupRecentFilingsCandidates(candidates) {
    return this.lookupDiscoveryCandidates(candidates, this.recentFilings.maxLookupsPerRun);
  }

  async lookupLawFirmCandidates(candidates) {
    return this.lookupDiscoveryCandidates(candidates, this.config.lawFirms.maxLookupsPerRun);
  }

  async lookupDiscoveryCandidates(candidates, maxLookups = 8) {
    const ordered = [...candidates.values()]
      .sort((left, right) => {
        if (left.priority !== right.priority) {
          return left.priority - right.priority;
        }

        return String(right.filedAt || "").localeCompare(String(left.filedAt || ""));
      })
      .slice(0, maxLookups);

    let lookupsTriggered = 0;
    let imported = 0;
    let matched = 0;

    for (const candidate of ordered) {
      const result = await this.importLookup(candidate.docketNumber, {
        courtName: candidate.courtName,
        caseName: candidate.caseName
      });
      lookupsTriggered += 1;
      imported += Number(result.imported || 0);
      matched += Number(result.matched || 0);
    }

    return {
      lookupsTriggered,
      imported,
      matched
    };
  }

  pickLatestLawFirmEntry(entries = []) {
    const rows = asArray(entries).filter(Boolean);
    if (!rows.length) {
      return null;
    }

    return rows
      .slice()
      .sort((left, right) => {
        const dateCompare = String(right.filedAt || "").localeCompare(String(left.filedAt || ""));
        if (dateCompare !== 0) {
          return dateCompare;
        }

        const numericCompare = (parseNumericLike(right.documentNumber || right.entryNumber) || -1) -
          (parseNumericLike(left.documentNumber || left.entryNumber) || -1);
        if (numericCompare !== 0) {
          return numericCompare;
        }

        return String(right.description || "").length - String(left.description || "").length;
      })[0];
  }

  classifyLawFirmItem(item) {
    return classifyCase(
      {
        caseName: item.caseName,
        case_name_full: item.title || item.caseName,
        court: item.courtName,
        party: [],
        recap_documents: asArray(item.entries).map((entry) => ({
          short_description: entry.documentType,
          description: entry.description
        })),
        raw: {
          brand: item.rawMeta?.brand || null,
          law_firm: item.rawMeta?.listedLawFirm || item.lawFirm || null
        }
      },
      []
    );
  }

  augmentLawFirmTags(item, tags = []) {
    const nextTags = new Set(asArray(tags));
    const text = normalizeText(
      [
        item.caseName,
        item.title,
        item.summary,
        ...asArray(item.entries).map((entry) => entry.description)
      ].join(" | ")
    );
    const isStructuredTroFirm = item.sourceId === "sriplaw" || item.sourceId === "gbc";
    const looksLikeSellerCaption =
      text.includes("et al") ||
      text.includes("does") ||
      text.includes("schedule a") ||
      text.includes("seller") ||
      text.includes("marketplace") ||
      text.includes("temporary restraining order") ||
      text.includes("preliminary injunction");

    if (isStructuredTroFirm && !nextTags.size && (looksLikeSellerCaption || asArray(item.entries).length >= 8)) {
      nextTags.add("seller_tro");
    }

    if (text.includes("temporary restraining order")) {
      nextTags.add("tro");
    }

    if (text.includes("schedule a")) {
      nextTags.add("schedule_a");
    }

    return [...nextTags];
  }

  shouldTrackLawFirmItem(item, existingCase, tags) {
    if (!/\b\d{2}-cv-\d{3,6}\b/i.test(String(item.docketNumber || ""))) {
      return false;
    }

    if (existingCase) {
      return true;
    }

    if (item.sourceId === "sriplaw" || item.sourceId === "gbc" || item.sourceId === "61tro") {
      return tags.length > 0 || asArray(item.entries).length > 0;
    }

    return tags.length > 0;
  }

  ingestLawFirmItems(sourceResult, caseIndex, { preexistingCaseIds = new Set() } = {}) {
    let casesUpserted = 0;
    let docketEntriesUpserted = 0;
    const lookupCandidates = new Map();
    const followUpCandidates = new Map();
    const timestamp = new Date().toISOString();

    for (const item of sourceResult.items || []) {
      const primaryKey = buildCourtDocketKey(item.courtId, item.docketNumber);
      const fallbackKey = buildCourtDocketKey(item.courtName, item.docketNumber);
      const existingCase =
        caseIndex.get(primaryKey) ||
        caseIndex.get(fallbackKey) ||
        ((!item.courtId && !item.courtName) ? this.store.findCaseByDocketNumber(item.docketNumber, getDiscoveryStartDate(this.config)) : null) ||
        null;
      const existedBeforeRun = Boolean(existingCase?.id) && preexistingCaseIds.has(Number(existingCase.id));
      const tags = this.augmentLawFirmTags(item, this.classifyLawFirmItem(item));

      if (!this.shouldTrackLawFirmItem(item, existingCase, tags)) {
        continue;
      }

      const latestEntry = this.pickLatestLawFirmEntry(item.entries);
      const parties = deriveParties({
        caseName: item.caseName,
        party: []
      });
      const mergedFirms = uniqueByNormalized([...(asArray(existingCase?.raw?.firm)), item.lawFirm]);
      const mergedSourceUrls = [
        ...(existingCase?.source_urls || []),
        item.caseUrl,
        ...asArray(item.entries).map((entry) => entry.absoluteUrl)
      ].filter(Boolean);
      const mergedRaw = {
        ...(existingCase?.raw || {}),
        firm: mergedFirms,
        law_firm_sites: {
          ...(existingCase?.raw?.law_firm_sites || {}),
          [item.sourceId]: {
            sourceLabel: item.sourceLabel,
            lawFirm: item.lawFirm,
            caseUrl: item.caseUrl,
            sourceCaseId: item.sourceCaseId,
            title: item.title || item.caseName || null,
            courtId: item.courtId || null,
            courtName: item.courtName || null,
            dateFiled: item.dateFiled || null,
            entryCount: asArray(item.entries).length,
            syncedAt: item.syncedAt || timestamp,
            note: sourceResult.note || null,
            rawMeta: item.rawMeta || {}
          }
        },
        [item.sourceId]: {
          ...(existingCase?.raw?.[item.sourceId] || {}),
          caseUrl: item.caseUrl,
          sourceCaseId: item.sourceCaseId,
          title: item.title || item.caseName || null,
          entryCount: asArray(item.entries).length,
          syncedAt: item.syncedAt || timestamp
        }
      };

      const savedCase = this.store.upsertCase({
        source_case_key:
          existingCase?.source_case_key ||
          `${item.sourceId}:${item.courtId || normalizeLookupText(item.courtName) || "unknown"}:${normalizeDocketIdentity(item.docketNumber)}`,
        primary_source: existingCase?.primary_source || item.sourceId,
        source_case_id: existingCase?.source_case_id || item.sourceCaseId || item.docketNumber,
        courtlistener_docket_id: existingCase?.courtlistener_docket_id ?? null,
        pacer_case_id: existingCase?.pacer_case_id ?? null,
        court_id: item.courtId || existingCase?.court_id || null,
        court_name: item.courtName || existingCase?.court_name || null,
        case_name: item.caseName || existingCase?.case_name || null,
        docket_number: item.docketNumber || existingCase?.docket_number || null,
        date_filed:
          existingCase?.date_filed ||
          item.dateFiled ||
          (latestEntry?.filedAt ? String(latestEntry.filedAt).slice(0, 10) : null),
        date_terminated: existingCase?.date_terminated || null,
        cause: existingCase?.cause || null,
        nature_of_suit: existingCase?.nature_of_suit || null,
        status: existingCase?.status || "open",
        tags_marker: buildTagsMarker([...(existingCase?.tags || []), ...tags]),
        docket_url: item.caseUrl || existingCase?.docket_url || null,
        source_urls: mergedSourceUrls,
        plaintiffs: existingCase?.plaintiffs?.length ? existingCase.plaintiffs : parties.plaintiffs,
        defendants: existingCase?.defendants?.length ? existingCase.defendants : parties.defendants,
        recent_activity_summary:
          latestEntry?.description || item.summary || existingCase?.recent_activity_summary || null,
        latest_docket_filed_at:
          laterIso(existingCase?.latest_docket_filed_at, latestEntry?.filedAt) ||
          laterIso(existingCase?.latest_docket_filed_at, item.dateFiled) ||
          null,
        latest_docket_number:
          higherOrderValue(existingCase?.latest_docket_number, latestEntry?.documentNumber || latestEntry?.entryNumber) ||
          higherOrderValue(existingCase?.latest_docket_number, item.latestDocketNumber) ||
          null,
        docket_count: Math.max(
          existingCase?.docket_count || 0,
          asArray(item.entries).length,
          parseNumericLike(item.latestDocketNumber) || 0
        ),
        last_seen_at: latestEntry?.filedAt || item.dateFiled || item.syncedAt || timestamp,
        last_synced_at: timestamp,
        last_docket_sync_at: existingCase?.last_docket_sync_at || null,
        raw: mergedRaw
      });

      if (!savedCase) {
        continue;
      }

      casesUpserted += 1;
      let itemDocketEntriesUpserted = 0;

      for (const entry of asArray(item.entries)) {
        const savedEntry = this.store.upsertDocketEntry({
          case_id: savedCase.id,
          source_entry_key: buildLawFirmEntryKey(item, savedCase, entry),
          primary_source: item.sourceId,
          source_entry_id: entry.sourceEntryId || null,
          document_type: entry.documentType || "Docket Entry",
          entry_number: entry.entryNumber || null,
          document_number: entry.documentNumber || null,
          filed_at: entry.filedAt || item.dateFiled || null,
          description: entry.description || null,
          absolute_url: entry.absoluteUrl || item.caseUrl || null,
          is_available: entry.absoluteUrl ? 1 : 0,
          page_count: null,
          pacer_doc_id: null,
          raw: {
            ...entry,
            source_case_url: item.caseUrl,
            source_label: item.sourceLabel,
            law_firm: item.lawFirm
          },
          last_synced_at: timestamp
        });

        if (savedEntry) {
          docketEntriesUpserted += 1;
          itemDocketEntriesUpserted += 1;
        }
      }

      if (existedBeforeRun) {
        const followUpCandidate = this.buildLawFirmCrossSourceFollowUpCandidate(
          item,
          existingCase,
          savedCase,
          tags,
          latestEntry,
          itemDocketEntriesUpserted
        );
        this.mergeCrossSourceFollowUpCandidate(followUpCandidates, followUpCandidate);
      }

      for (const key of [primaryKey, fallbackKey].filter(Boolean)) {
        caseIndex.set(key, savedCase);
      }

      if (!savedCase.courtlistener_docket_id && item.docketNumber) {
        const lookupKey = primaryKey || fallbackKey || `docket:${normalizeDocket(item.docketNumber)}`;
        if (lookupKey && !lookupCandidates.has(lookupKey)) {
          lookupCandidates.set(lookupKey, {
            docketNumber: item.docketNumber,
            caseName: item.caseName,
            courtName: item.courtName,
            priority: item.sourceId === "sriplaw" ? 0 : item.sourceId === "gbc" ? 1 : 2,
            filedAt: latestEntry?.filedAt || item.dateFiled || timestamp
          });
        }
      }
    }

    return {
      casesUpserted,
      docketEntriesUpserted,
      lookupCandidates,
      followUpCandidates
    };
  }

  async followUpCrossSourceCases(followUpCandidates = new Map(), { maxCasesPerRun = null } = {}) {
    const selectedLimit = Math.max(
      0,
      Number(
        maxCasesPerRun === null
          ? this.config.courtFeeds.crossSourceFollowUpMaxCasesPerRun
          : maxCasesPerRun
      ) || 0
    );
    if (!selectedLimit || !followUpCandidates.size) {
      return {
        candidateCount: followUpCandidates.size,
        triggeredCases: 0,
        priorityFeedTriggered: 0,
        priorityFeedSynced: 0,
        courtListenerTriggered: 0,
        courtListenerSynced: 0,
        recentFilingsTriggered: 0,
        recentFilingsSynced: 0,
        lawFirmTriggered: 0,
        lawFirmSynced: 0,
        failedCases: 0
      };
    }

    const selected = [...followUpCandidates.values()]
      .sort((left, right) => {
        if (Number(left.priority ?? 99) !== Number(right.priority ?? 99)) {
          return Number(left.priority ?? 99) - Number(right.priority ?? 99);
        }

        const filedCompare = compareCaseActivityDesc(left.filedAt, right.filedAt);
        if (filedCompare !== 0) {
          return filedCompare;
        }

        return Number(right.caseId || 0) - Number(left.caseId || 0);
      })
      .slice(0, selectedLimit);

    let triggeredCases = 0;
    let priorityFeedTriggered = 0;
    let priorityFeedSynced = 0;
    let courtListenerTriggered = 0;
    let courtListenerSynced = 0;
    let recentFilingsTriggered = 0;
    let recentFilingsSynced = 0;
    let lawFirmTriggered = 0;
    let lawFirmSynced = 0;
    let failedCases = 0;

    for (const candidate of selected) {
      const caseId = Number(candidate.caseId || 0);
      if (!caseId) {
        continue;
      }

      let attempted = false;
      try {
        if (candidate.priorityFeed) {
          attempted = true;
          priorityFeedTriggered += 1;
          const result = await this.enrichCaseWithPriorityFeed(caseId, { force: true });
          if (result.enriched) {
            priorityFeedSynced += 1;
          }
        }

        if (candidate.courtListener) {
          attempted = true;
          courtListenerTriggered += 1;
          const result = await this.enrichCaseWithCourtListener(caseId, { force: true });
          if (result.enriched) {
            courtListenerSynced += 1;
          }
        }

        if (candidate.recentFilings) {
          attempted = true;
          recentFilingsTriggered += 1;
          const result = await this.enrichCaseWithRecentFilings(caseId, { force: true });
          if (result.enriched) {
            recentFilingsSynced += 1;
          }
        }

        if (candidate.lawFirm61tro) {
          attempted = true;
          lawFirmTriggered += 1;
          const result = await this.enrichCaseWithLawFirmLookup(caseId, {
            sourceIds: ["61tro"],
            force: true
          });
          if (result.enriched) {
            lawFirmSynced += 1;
          }
        }

        if (attempted) {
          triggeredCases += 1;
        }
      } catch {
        failedCases += 1;
      }
    }

    return {
      candidateCount: followUpCandidates.size,
      triggeredCases,
      priorityFeedTriggered,
      priorityFeedSynced,
      courtListenerTriggered,
      courtListenerSynced,
      recentFilingsTriggered,
      recentFilingsSynced,
      lawFirmTriggered,
      lawFirmSynced,
      failedCases
    };
  }

  isRecentFiledExistingCase(caseRow, recentFiledWindowDays = 2) {
    const recentFiledCutoff = Date.now() - Math.max(1, Number(recentFiledWindowDays || 2)) * 24 * 60 * 60 * 1000;
    const createdAtMs = Date.parse(String(caseRow?.created_at || ""));
    const activityAtMs = Date.parse(String(getCasePriorityActivityAt(caseRow) || ""));
    return Number.isFinite(createdAtMs) && Number.isFinite(activityAtMs) && createdAtMs < recentFiledCutoff && activityAtMs >= recentFiledCutoff;
  }

  buildRecentExistingCaseFollowUpCandidates() {
    const maxCases = Math.max(0, Number(this.config.sync.recentExistingCaseFollowUpMaxCasesPerRun || 0));
    if (!maxCases) {
      return {
        candidates: new Map(),
        activityAheadCaseIds: new Set()
      };
    }

    const recentFiledWindowDays = this.config.sync.recentDocketFollowUpDays;
    const followUpPoolSize = Math.max(maxCases * 3, 18);
    const candidates = new Map();
    const activityAheadCaseIds = new Set();

    const activityAheadRows = this.store.getCasesWithActivityAheadOfTimeline(
      Math.max(followUpPoolSize, maxCases * 6),
      {
        recentFiledWindowDays,
        startDate: getDiscoveryStartDate(this.config)
      }
    );
    for (const row of activityAheadRows) {
      const caseId = Number(row.id);
      if (!caseId) {
        continue;
      }
      activityAheadCaseIds.add(caseId);
      this.mergeCrossSourceFollowUpCandidate(candidates, {
        caseId,
        docketNumber: row.docket_number || null,
        filedAt: getCasePriorityActivityAt(row),
        priority:
          row.tags_marker?.includes("|seller_tro|")
            ? -3
            : row.tags_marker?.includes("|tro|") || row.tags_marker?.includes("|schedule_a|")
              ? -2
              : -1,
        priorityFeed: shouldAttemptPriorityFeedForCase(row, { force: false }),
        courtListener: Boolean(row.courtlistener_docket_id || row.docket_number || row.case_name),
        recentFilings: this.canEnrichCaseWithRecentFilings(row),
        lawFirm61tro: this.canEnrichCaseWithLawFirmLookup(row, { sourceIds: ["61tro"] })
      });
    }

    const priorityRows = this.store.getCasesNeedingPriorityFeedSync(
      Math.max(followUpPoolSize, maxCases * 4),
      this.config.priorityFeed.staleAfterHours,
      {
        recentFiledWindowDays,
        preferKnownPriorityFeed: false
      }
    );
    for (const row of priorityRows) {
      if (!this.isRecentFiledExistingCase(row, recentFiledWindowDays)) {
        continue;
      }
      this.mergeCrossSourceFollowUpCandidate(candidates, {
        caseId: Number(row.id),
        docketNumber: row.docket_number || null,
        filedAt: getCasePriorityActivityAt(row),
        priority:
          row.tags_marker?.includes("|seller_tro|")
            ? 0
            : row.tags_marker?.includes("|tro|") || row.tags_marker?.includes("|schedule_a|")
              ? 1
              : 2,
        priorityFeed: true,
        courtListener: Boolean(row.courtlistener_docket_id || row.docket_number || row.case_name),
        recentFilings: this.canEnrichCaseWithRecentFilings(row),
        lawFirm61tro: this.canEnrichCaseWithLawFirmLookup(row, { sourceIds: ["61tro"] })
      });
    }

    const courtListenerRows = this.store.getCasesNeedingCourtListenerFollowUp(
      Math.max(followUpPoolSize, maxCases * 4),
      {
        recentFiledWindowDays,
        startDate: getDiscoveryStartDate(this.config)
      }
    );
    for (const row of courtListenerRows) {
      this.mergeCrossSourceFollowUpCandidate(candidates, {
        caseId: Number(row.id),
        docketNumber: row.docket_number || null,
        filedAt: getCasePriorityActivityAt(row),
        priority:
          row.tags_marker?.includes("|seller_tro|")
            ? 0
            : row.tags_marker?.includes("|tro|") || row.tags_marker?.includes("|schedule_a|")
              ? 1
              : 2,
        priorityFeed: shouldAttemptPriorityFeedForCase(row, { force: false }),
        courtListener: Boolean(row.courtlistener_docket_id || row.docket_number || row.case_name),
        recentFilings: this.canEnrichCaseWithRecentFilings(row),
        lawFirm61tro: this.canEnrichCaseWithLawFirmLookup(row, { sourceIds: ["61tro"] })
      });
    }

    const selected = [...candidates.values()]
      .sort((left, right) => {
        if (Number(left.priority ?? 99) !== Number(right.priority ?? 99)) {
          return Number(left.priority ?? 99) - Number(right.priority ?? 99);
        }

        const filedCompare = compareCaseActivityDesc(left.filedAt, right.filedAt);
        if (filedCompare !== 0) {
          return filedCompare;
        }

        return Number(right.caseId || 0) - Number(left.caseId || 0);
      })
      .slice(0, maxCases);

    return {
      candidates: new Map(selected.map((item) => [Number(item.caseId), item])),
      activityAheadCaseIds
    };
  }

  async syncRecentExistingCaseFollowUps(mode = "recent") {
    if (mode !== "recent") {
      return {
        candidateCount: 0,
        triggeredCases: 0,
        priorityFeedTriggered: 0,
        priorityFeedSynced: 0,
        courtListenerTriggered: 0,
        courtListenerSynced: 0,
        failedCases: 0,
        note: null
      };
    }

    return this.store.batchMutations(async () => {
      const {
        candidates: followUpCandidates,
        activityAheadCaseIds
      } = this.buildRecentExistingCaseFollowUpCandidates();
      const followUpResult = await this.followUpCrossSourceCases(followUpCandidates, {
        maxCasesPerRun: Math.max(0, Number(this.config.sync.recentExistingCaseFollowUpMaxCasesPerRun || 0))
      });
      const selectedActivityAheadCount = [...followUpCandidates.keys()].filter((caseId) => activityAheadCaseIds.has(caseId)).length;
      const note = followUpResult.triggeredCases
        ? `旧案 recent-filed 跟进队列本轮命中 ${followUpResult.triggeredCases} 个案件，其中案级节点领先时间线 ${selectedActivityAheadCount} 个（worldtro ${followUpResult.priorityFeedTriggered}/${followUpResult.priorityFeedSynced}，CourtListener ${followUpResult.courtListenerTriggered}/${followUpResult.courtListenerSynced}，RecentFilings ${followUpResult.recentFilingsTriggered}/${followUpResult.recentFilingsSynced}，61TRO ${followUpResult.lawFirmTriggered}/${followUpResult.lawFirmSynced}${followUpResult.failedCases ? `，失败 ${followUpResult.failedCases}` : ""}）。`
        : followUpCandidates.size
          ? `旧案 recent-filed 跟进队列本轮候选 ${followUpCandidates.size} 个，其中案级节点领先时间线 ${selectedActivityAheadCount} 个，但未形成新的跨源补抓。`
          : "旧案 recent-filed 跟进队列当前没有候选案件。";

      return {
        ...followUpResult,
        activityAheadCandidateCount: activityAheadCaseIds.size,
        selectedActivityAheadCount,
        note
      };
    });
  }

  async syncCourtFeedsRecent(mode = "recent") {
    return this.store.batchMutations(async () => {
      if (!this.courtFeeds.enabled) {
        return {
          successfulFeeds: 0,
          failedFeeds: 0,
          casesUpserted: 0,
          docketEntriesUpserted: 0,
          lookupsTriggered: 0,
          note: "官方法院 RSS 发现层已关闭。"
        };
      }

      await this.courtFeeds.ensureFeedsLoaded();

      const caseIndex = this.buildCourtFeedCaseIndex();
      const preexistingCaseIds = new Set(
        [...new Set([...caseIndex.values()].map((row) => Number(row.id)).filter((value) => Number.isFinite(value) && value > 0))]
      );
      const lookupCandidates = new Map();
      const followUpCandidates = new Map();
      let successfulFeeds = 0;
      let failedFeeds = 0;
      let casesUpserted = 0;
      let docketEntriesUpserted = 0;

      for (const feed of this.courtFeeds.listFeeds()) {
        const checkpointKey = `courtfeed:${feed.id}:recent`;
        const checkpoint = this.store.getCheckpoint(checkpointKey) || {};
        const seenGuids = new Set(Array.isArray(checkpoint.seenGuids) ? checkpoint.seenGuids : []);

        try {
          const feedResult = await this.courtFeeds.fetchFeed(feed);
          const freshItems = (feedResult.items || []).filter((item) => item.guid && !seenGuids.has(item.guid));
          const ingest = this.ingestCourtFeedItems(
            {
              ...feedResult,
              items: freshItems
            },
            caseIndex,
            { preexistingCaseIds }
          );
          casesUpserted += ingest.casesUpserted;
          docketEntriesUpserted += ingest.docketEntriesUpserted;

          for (const [key, value] of ingest.lookupCandidates.entries()) {
            if (!lookupCandidates.has(key)) {
              lookupCandidates.set(key, value);
            }
          }

          for (const [key, value] of ingest.followUpCandidates.entries()) {
            this.mergeCrossSourceFollowUpCandidate(followUpCandidates, {
              ...value,
              caseId: Number(key || value.caseId)
            });
          }

          this.store.saveCheckpoint(checkpointKey, {
            seenGuids: (feedResult.items || []).map((item) => item.guid).filter(Boolean).slice(0, 400),
            lastBuildDate: feedResult.lastBuildDate,
            updatedAt: new Date().toISOString(),
            lastItemCount: Number(feedResult.items?.length || 0),
            newItemCount: freshItems.length
          });
          successfulFeeds += 1;
        } catch (error) {
          failedFeeds += 1;
          this.store.saveCheckpoint(checkpointKey, {
            seenGuids: [...seenGuids].slice(0, 400),
            updatedAt: new Date().toISOString(),
            error: error.message
          });
        }
      }

      const lookupResult =
        mode === "recent" && lookupCandidates.size
          ? await this.lookupCourtFeedCandidates(lookupCandidates)
          : { lookupsTriggered: 0, imported: 0, matched: 0 };
      const followUpResult =
        mode === "recent" && followUpCandidates.size
          ? await this.followUpCrossSourceCases(followUpCandidates)
          : {
              candidateCount: 0,
              triggeredCases: 0,
              priorityFeedTriggered: 0,
              priorityFeedSynced: 0,
              courtListenerTriggered: 0,
              courtListenerSynced: 0,
              failedCases: 0
            };

      const note =
        successfulFeeds || failedFeeds
          ? `官方法院 RSS 本轮巡检 ${successfulFeeds} 个法院${failedFeeds ? `，${failedFeeds} 个法院源暂时失败` : ""}；补进 ${casesUpserted} 条案件更新、${docketEntriesUpserted} 条官方 docket${lookupResult.lookupsTriggered ? `，并触发 ${lookupResult.lookupsTriggered} 次 CourtListener 精确补抓` : ""}${followUpResult.triggeredCases ? `；另对 ${followUpResult.triggeredCases} 个旧目标案触发跨源跟进（worldtro ${followUpResult.priorityFeedTriggered}/${followUpResult.priorityFeedSynced}，CourtListener ${followUpResult.courtListenerTriggered}/${followUpResult.courtListenerSynced}，RecentFilings ${followUpResult.recentFilingsTriggered}/${followUpResult.recentFilingsSynced}，61TRO ${followUpResult.lawFirmTriggered}/${followUpResult.lawFirmSynced}${followUpResult.failedCases ? `，失败 ${followUpResult.failedCases}` : ""}）` : ""}。`
          : "官方法院 RSS 当前没有已配置法院。";

      return {
        successfulFeeds,
        failedFeeds,
        casesUpserted,
        docketEntriesUpserted,
        lookupsTriggered: lookupResult.lookupsTriggered,
        crossSourceFollowUpCases: followUpResult.triggeredCases,
        priorityFeedFollowUpsTriggered: followUpResult.priorityFeedTriggered,
        priorityFeedFollowUpsSynced: followUpResult.priorityFeedSynced,
        courtListenerFollowUpsTriggered: followUpResult.courtListenerTriggered,
        courtListenerFollowUpsSynced: followUpResult.courtListenerSynced,
        note
      };
    });
  }

  async syncRecentFilingsRecent(mode = "recent") {
    return this.store.batchMutations(async () => {
      if (!this.recentFilings.enabled) {
        return {
          successfulSources: 0,
          failedSources: 0,
          casesUpserted: 0,
          lookupsTriggered: 0,
          note: "官方近期立案页补源已关闭。"
        };
      }

      const caseIndex = this.buildCourtFeedCaseIndex();
      const preexistingCaseIds = new Set(
        [...new Set([...caseIndex.values()].map((row) => Number(row.id)).filter((value) => Number.isFinite(value) && value > 0))]
      );
      const lookupCandidates = new Map();
      const followUpCandidates = new Map();
      let successfulSources = 0;
      let failedSources = 0;
      let casesUpserted = 0;

      for (const source of this.recentFilings.listSources()) {
        try {
          const sourceResult = await this.recentFilings.fetchRecentForCourt(source);
          const ingest = this.ingestRecentFilingsItems(sourceResult, caseIndex, { preexistingCaseIds });
          casesUpserted += ingest.casesUpserted;

          for (const [key, value] of ingest.lookupCandidates.entries()) {
            if (!lookupCandidates.has(key)) {
              lookupCandidates.set(key, value);
            }
          }

          for (const [key, value] of ingest.followUpCandidates.entries()) {
            this.mergeCrossSourceFollowUpCandidate(followUpCandidates, {
              ...value,
              caseId: Number(key || value.caseId)
            });
          }

          successfulSources += 1;
        } catch {
          failedSources += 1;
        }
      }

      const lookupResult =
        mode === "recent" && lookupCandidates.size
          ? await this.lookupRecentFilingsCandidates(lookupCandidates)
          : { lookupsTriggered: 0, imported: 0, matched: 0 };
      const followUpResult =
        mode === "recent" && followUpCandidates.size
          ? await this.followUpCrossSourceCases(followUpCandidates)
          : {
              candidateCount: 0,
              triggeredCases: 0,
              priorityFeedTriggered: 0,
              priorityFeedSynced: 0,
              courtListenerTriggered: 0,
              courtListenerSynced: 0,
              failedCases: 0
            };

      const note =
        successfulSources || failedSources
          ? `官方近期立案页本轮巡检 ${successfulSources} 个法院${failedSources ? `，${failedSources} 个法院页暂时失败` : ""}；补进 ${casesUpserted} 条案件线索${lookupResult.lookupsTriggered ? `，并触发 ${lookupResult.lookupsTriggered} 次 CourtListener 精确补抓` : ""}${followUpResult.triggeredCases ? `；另对 ${followUpResult.triggeredCases} 个旧目标案触发跨源跟进（worldtro ${followUpResult.priorityFeedTriggered}/${followUpResult.priorityFeedSynced}，CourtListener ${followUpResult.courtListenerTriggered}/${followUpResult.courtListenerSynced}，RecentFilings ${followUpResult.recentFilingsTriggered}/${followUpResult.recentFilingsSynced}，61TRO ${followUpResult.lawFirmTriggered}/${followUpResult.lawFirmSynced}${followUpResult.failedCases ? `，失败 ${followUpResult.failedCases}` : ""}）` : ""}。`
          : "官方近期立案页当前没有已配置法院。";

      return {
        successfulSources,
        failedSources,
        casesUpserted,
        lookupsTriggered: lookupResult.lookupsTriggered,
        crossSourceFollowUpCases: followUpResult.triggeredCases,
        note
      };
    });
  }

  async syncLawFirmRecent(mode = "recent") {
    return this.store.batchMutations(async () => {
      if (!this.lawFirms.enabled) {
        return {
          successfulSources: 0,
          failedSources: 0,
          casesUpserted: 0,
          docketEntriesUpserted: 0,
          lookupsTriggered: 0,
          note: "律所官网补源已关闭。"
        };
      }

      const caseIndex = this.buildCourtFeedCaseIndex();
      const preexistingCaseIds = new Set(
        [...new Set([...caseIndex.values()].map((row) => Number(row.id)).filter((value) => Number.isFinite(value) && value > 0))]
      );
      const lookupCandidates = new Map();
      const followUpCandidates = new Map();
      let successfulSources = 0;
      let failedSources = 0;
      let casesUpserted = 0;
      let docketEntriesUpserted = 0;

      for (const source of this.lawFirms.listSources()) {
        try {
          const sourceResult = await this.lawFirms.fetchRecentForSource(source);
          const ingest = this.ingestLawFirmItems(sourceResult, caseIndex, { preexistingCaseIds });
          casesUpserted += ingest.casesUpserted;
          docketEntriesUpserted += ingest.docketEntriesUpserted;

          for (const [key, value] of ingest.lookupCandidates.entries()) {
            if (!lookupCandidates.has(key)) {
              lookupCandidates.set(key, value);
            }
          }

          for (const [key, value] of ingest.followUpCandidates.entries()) {
            this.mergeCrossSourceFollowUpCandidate(followUpCandidates, {
              ...value,
              caseId: Number(key || value.caseId)
            });
          }

          successfulSources += 1;
        } catch {
          failedSources += 1;
        }
      }

      const lookupResult =
        mode === "recent" && lookupCandidates.size
          ? await this.lookupLawFirmCandidates(lookupCandidates)
          : { lookupsTriggered: 0, imported: 0, matched: 0 };
      const followUpResult =
        mode === "recent" && followUpCandidates.size
          ? await this.followUpCrossSourceCases(followUpCandidates)
          : {
              candidateCount: 0,
              triggeredCases: 0,
              priorityFeedTriggered: 0,
              priorityFeedSynced: 0,
              courtListenerTriggered: 0,
              courtListenerSynced: 0,
              failedCases: 0
            };

      const note =
        successfulSources || failedSources
          ? `律所官网本轮巡检 ${successfulSources} 个来源${failedSources ? `，${failedSources} 个来源暂时失败` : ""}；补进 ${casesUpserted} 条案件更新、${docketEntriesUpserted} 条律所公开文书${lookupResult.lookupsTriggered ? `，并触发 ${lookupResult.lookupsTriggered} 次 CourtListener 精确补抓` : ""}${followUpResult.triggeredCases ? `；另对 ${followUpResult.triggeredCases} 个旧目标案触发跨源跟进（worldtro ${followUpResult.priorityFeedTriggered}/${followUpResult.priorityFeedSynced}，CourtListener ${followUpResult.courtListenerTriggered}/${followUpResult.courtListenerSynced}，RecentFilings ${followUpResult.recentFilingsTriggered}/${followUpResult.recentFilingsSynced}，61TRO ${followUpResult.lawFirmTriggered}/${followUpResult.lawFirmSynced}${followUpResult.failedCases ? `，失败 ${followUpResult.failedCases}` : ""}）` : ""}。`
          : "律所官网当前没有已配置来源。";

      return {
        successfulSources,
        failedSources,
        casesUpserted,
        docketEntriesUpserted,
        lookupsTriggered: lookupResult.lookupsTriggered,
        crossSourceFollowUpCases: followUpResult.triggeredCases,
        note
      };
    });
  }

  canEnrichCaseWithRecentFilings(caseRow = {}) {
    const docketNumber = String(caseRow?.docket_number || "").trim();
    if (!this.recentFilings.enabled || !docketLooksLike(docketNumber)) {
      return false;
    }

    const caseCourt = normalizeText(caseRow?.court_name || "");
    if (!caseCourt) {
      return false;
    }

    return this.recentFilings.listSources().some((source) => {
      const sourceCourt = normalizeText(source?.courtName || "");
      return sourceCourt && (sourceCourt === caseCourt || sourceCourt.includes(caseCourt) || caseCourt.includes(sourceCourt));
    });
  }

  canEnrichCaseWithLawFirmLookup(caseRow = {}, { sourceIds = ["61tro"] } = {}) {
    const docketNumber = String(caseRow?.docket_number || "").trim();
    if (!this.lawFirms.enabled || !docketLooksLike(docketNumber)) {
      return false;
    }

    const normalizedSourceIds = [...new Set(asArray(sourceIds).map((value) => String(value || "").trim().toLowerCase()).filter(Boolean))];
    if (!normalizedSourceIds.length) {
      return false;
    }

    const selectedIds = new Set(this.lawFirms.listSources().map((source) => String(source.id || "").trim().toLowerCase()));
    if (!normalizedSourceIds.some((sourceId) => selectedIds.has(sourceId))) {
      return false;
    }

    return Boolean(
      caseRow?.insights?.is_seller_case ||
      caseRow?.insights?.is_tro_case ||
      caseRow?.insights?.is_schedule_a_case ||
      hasCaseLevelActivityLead(this.store, caseRow)
    );
  }

  async enrichCaseWithRecentFilings(caseId, { force = false } = {}) {
    return this.store.batchMutations(async () => {
      const caseRow = this.store.getCase(caseId);
      const docketNumber = String(caseRow?.docket_number || "").trim();
      if (!caseRow || !this.canEnrichCaseWithRecentFilings(caseRow)) {
        return { enriched: false, reason: "not-applicable" };
      }

      const trackedSource = this.recentFilings.listSources().find((source) => {
        const sourceCourt = normalizeText(source?.courtName || "");
        const caseCourt = normalizeText(caseRow?.court_name || "");
        if (!sourceCourt || !caseCourt) {
          return false;
        }
        return sourceCourt === caseCourt || sourceCourt.includes(caseCourt) || caseCourt.includes(sourceCourt);
      });
      if (!trackedSource) {
        return { enriched: false, reason: "not-applicable" };
      }

      const sourceState = caseRow.raw?.recent_filings?.[trackedSource.id] || {};
      const syncedAt = sourceState.syncedAt ? Date.parse(sourceState.syncedAt) : 0;
      const hasActivityAheadOfTimeline = hasCaseLevelActivityLead(this.store, caseRow);
      const staleAfterMs = 4 * 60 * 60 * 1000;
      if (!force && !hasActivityAheadOfTimeline && syncedAt && Date.now() - syncedAt < staleAfterMs) {
        return { enriched: false, reason: "fresh" };
      }

      let casesUpserted = 0;
      let matchedRecentCase = false;
      const caseIndex = this.buildCourtFeedCaseIndex();
      const recentMatch = await this.recentFilings.lookupByDocket(docketNumber, {
        courtName: caseRow.court_name || ""
      }).catch(() => null);
      if (recentMatch?.item) {
        const ingest = this.ingestRecentFilingsItems(
          {
            sourceId: recentMatch.source?.id || trackedSource.id,
            note: recentMatch.note || null,
            items: [recentMatch.item]
          },
          caseIndex
        );
        casesUpserted += Number(ingest.casesUpserted || 0);
        matchedRecentCase = true;
      }

      const federalRecordLookup = trackedSource.id === "ilnd"
        ? await this.recentFilings.lookupFederalRecordByDocket(docketNumber, {
            courtName: caseRow.court_name || ""
          }).catch(() => null)
        : null;

      if (federalRecordLookup?.source) {
        const refreshedCase = this.store.getCase(caseId) || caseRow;
        const timestamp = new Date().toISOString();
        const sourceId = trackedSource.id;
        this.store.upsertCase({
          source_case_key: refreshedCase.source_case_key,
          primary_source: refreshedCase.primary_source,
          source_case_id: refreshedCase.source_case_id,
          courtlistener_docket_id: refreshedCase.courtlistener_docket_id,
          pacer_case_id: refreshedCase.pacer_case_id,
          court_id: refreshedCase.court_id,
          court_name: refreshedCase.court_name,
          case_name: refreshedCase.case_name,
          docket_number: refreshedCase.docket_number,
          date_filed: refreshedCase.date_filed,
          date_terminated: refreshedCase.date_terminated,
          cause: refreshedCase.cause,
          nature_of_suit: refreshedCase.nature_of_suit,
          status: refreshedCase.status,
          tags_marker: refreshedCase.tags_marker,
          docket_url: refreshedCase.docket_url,
          source_urls: [
            ...(refreshedCase.source_urls || []),
            federalRecordLookup.source.recordLookupUrl || null
          ].filter(Boolean),
          plaintiffs: refreshedCase.plaintiffs || [],
          defendants: refreshedCase.defendants || [],
          recent_activity_summary: refreshedCase.recent_activity_summary,
          latest_docket_filed_at: refreshedCase.latest_docket_filed_at,
          latest_docket_number: refreshedCase.latest_docket_number,
          docket_count: refreshedCase.docket_count,
          last_seen_at: refreshedCase.last_seen_at || timestamp,
          last_synced_at: timestamp,
          last_docket_sync_at: refreshedCase.last_docket_sync_at,
          raw: {
            ...(refreshedCase.raw || {}),
            recent_filings: {
              ...(refreshedCase.raw?.recent_filings || {}),
              [sourceId]: {
                ...(refreshedCase.raw?.recent_filings?.[sourceId] || {}),
                syncedAt: timestamp,
                federalRecordLookup: {
                  state: federalRecordLookup.state || "not_found",
                  records: federalRecordLookup.records || [],
                  note: federalRecordLookup.note || null,
                  sourceUrl: federalRecordLookup.source.recordLookupUrl || federalRecordLookup.source.url || null,
                  updatedAt: timestamp
                }
              }
            }
          }
        });
      }

      return {
        enriched: Boolean(casesUpserted || matchedRecentCase || (federalRecordLookup?.records || []).length),
        reason:
          casesUpserted || matchedRecentCase || (federalRecordLookup?.records || []).length
            ? "completed"
            : federalRecordLookup
              ? federalRecordLookup.state || "not-found"
              : "not-found",
        casesUpserted,
        recentMatch: matchedRecentCase,
        federalRecordLookup: federalRecordLookup
          ? {
              state: federalRecordLookup.state,
              recordCount: Number(federalRecordLookup.records?.length || 0)
            }
          : null
      };
    });
  }

  async enrichCaseWithLawFirmLookup(caseId, { sourceIds = ["61tro"], force = false } = {}) {
    return this.store.batchMutations(async () => {
      const caseRow = this.store.getCase(caseId);
      const docketNumber = String(caseRow?.docket_number || "").trim();
      if (!caseRow || !this.canEnrichCaseWithLawFirmLookup(caseRow, { sourceIds })) {
        return { enriched: false, reason: "not-applicable" };
      }

      const normalizedSourceIds = [...new Set(asArray(sourceIds).map((value) => String(value || "").trim().toLowerCase()).filter(Boolean))];
      if (!normalizedSourceIds.length) {
        return { enriched: false, reason: "not-applicable" };
      }

      const latestSourceSyncAt = normalizedSourceIds.reduce((latest, sourceId) => {
        const syncedAt = Date.parse(String(caseRow.raw?.law_firm_sites?.[sourceId]?.syncedAt || ""));
        return Number.isFinite(syncedAt) ? Math.max(latest, syncedAt) : latest;
      }, 0);
      const hasActivityAheadOfTimeline = hasCaseLevelActivityLead(this.store, caseRow);
      const staleAfterMs = 4 * 60 * 60 * 1000;
      if (!force && !hasActivityAheadOfTimeline && latestSourceSyncAt && Date.now() - latestSourceSyncAt < staleAfterMs) {
        return { enriched: false, reason: "fresh" };
      }

      const sourceResult = await this.lawFirms.lookupByDocket(docketNumber, {
        sourceIds: normalizedSourceIds,
        courtName: caseRow.court_name || "",
        caseName: caseRow.case_name || "",
        plaintiffs: caseRow.plaintiffs || [],
        firms: caseRow.raw?.firm || []
      }).catch(() => null);
      if (!sourceResult?.item) {
        return { enriched: false, reason: "not-found" };
      }

      const caseIndex = this.buildCourtFeedCaseIndex();
      const ingest = this.ingestLawFirmItems(
        {
          source: sourceResult.source || null,
          note: sourceResult.note || null,
          items: [sourceResult.item]
        },
        caseIndex
      );
      this.store.refreshCaseDocketSummary(caseId);

      return {
        enriched: Boolean(ingest.casesUpserted || ingest.docketEntriesUpserted),
        reason:
          ingest.docketEntriesUpserted > 0
            ? "entries"
            : ingest.casesUpserted > 0
              ? "case"
              : "matched",
        casesUpserted: Number(ingest.casesUpserted || 0),
        docketEntriesUpserted: Number(ingest.docketEntriesUpserted || 0)
      };
    });
  }

  async syncPacerRecent(mode = "recent") {
    return this.store.batchMutations(async () => {
      if (!this.pacer.config.enabled) {
        return {
          casesUpserted: 0,
          pagesFetched: 0,
          note: "PACER 官方发现链路已关闭。"
        };
      }

      if (!this.pacer.hasCredentials()) {
        return {
          casesUpserted: 0,
          pagesFetched: 0,
          note: "PACER 已启用，但缺少登录凭据。"
        };
      }

      const result = await this.pacer.discoverRecentCases({ mode });
      const ingest = this.ingestPacerDiscoveryResults(result.items || []);

      return {
        casesUpserted: ingest.casesUpserted,
        pagesFetched: result.pagesFetched || 0,
        note: result.note
      };
    });
  }

  async enrichCaseWithPriorityFeed(caseId, { force = false } = {}) {
    return this.store.batchMutations(async () => {
      const caseRow = this.store.getCase(caseId);
      if (!caseRow || !this.priorityFeed.enabled || !shouldAttemptPriorityFeedForCase(caseRow, { force })) {
        return { enriched: false, reason: "not-applicable" };
      }

      const syncedAt = getPriorityFeedSyncedAt(caseRow) ? Date.parse(getPriorityFeedSyncedAt(caseRow)) : 0;
      const staleAfterMs = this.config.priorityFeed.staleAfterHours * 60 * 60 * 1000;
      if (!force && syncedAt && Date.now() - syncedAt < staleAfterMs) {
        return { enriched: false, reason: "fresh" };
      }

      return this.syncSinglePriorityFeedCase(caseRow);
    });
  }

  persistCourtListenerRecapFetchState(caseRow, recapFetch = {}, { docketId = null } = {}) {
    if (!caseRow?.id) {
      return caseRow;
    }

    const timestamp = new Date().toISOString();
    const nextDocketId = Number(docketId || caseRow.courtlistener_docket_id || 0) || null;
    const previousState = caseRow.raw?.courtlistener?.recapFetch || {};
    const recapFetchState = {
      ...previousState,
      ...recapFetch,
      id: Number(recapFetch.id || previousState.id || 0) || null,
      status: recapFetch.status ?? previousState.status ?? null,
      docket: recapFetch.docket || previousState.docket || null,
      docketId: nextDocketId || Number(previousState.docketId || 0) || null,
      errorMessage: recapFetch.error_message || recapFetch.errorMessage || previousState.errorMessage || "",
      requestedAt: previousState.requestedAt || timestamp,
      updatedAt: timestamp
    };

    return this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: caseRow.primary_source,
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: nextDocketId,
      pacer_case_id: caseRow.pacer_case_id,
      court_id: caseRow.court_id,
      court_name: caseRow.court_name,
      case_name: caseRow.case_name,
      docket_number: caseRow.docket_number,
      date_filed: caseRow.date_filed,
      date_terminated: caseRow.date_terminated,
      cause: caseRow.cause,
      nature_of_suit: caseRow.nature_of_suit,
      status: caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: caseRow.docket_url,
      source_urls: caseRow.source_urls || [],
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: caseRow.recent_activity_summary,
      latest_docket_filed_at: caseRow.latest_docket_filed_at,
      latest_docket_number: caseRow.latest_docket_number,
      docket_count: caseRow.docket_count,
      last_seen_at: caseRow.last_seen_at || timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: caseRow.last_docket_sync_at,
      raw: {
        ...(caseRow.raw || {}),
        courtlistener: {
          ...(caseRow.raw?.courtlistener || {}),
          recapFetch: recapFetchState
        }
      }
    });
  }

  persistCourtListenerDocketAlertState(caseRow, docketAlert = {}, {
    syncedAt = null,
    state = null,
    lastWebhookAt = null,
    lastWebhookEventType = null,
    lastWebhookEntryId = null,
    lastWebhookEntryCount = null,
    lastWebhookIdempotencyKey = null,
    reuppedAt = null
  } = {}) {
    if (!caseRow?.id) {
      return caseRow;
    }

    const timestamp = syncedAt || new Date().toISOString();
    const previousState = caseRow.raw?.courtlistener?.docketAlert || {};
    const alertType = normalizeCourtListenerAlertType(
      docketAlert.alert_type,
      docketAlert.alertType ?? previousState.alertType ?? previousState.alert_type ?? null
    );
    const nextState = {
      ...previousState,
      ...docketAlert,
      id: Number(docketAlert.id || previousState.id || 0) || null,
      docket: Number(docketAlert.docket || caseRow.courtlistener_docket_id || previousState.docket || 0) || null,
      alertType,
      secretKey: docketAlert.secret_key || docketAlert.secretKey || previousState.secretKey || "",
      dateCreated: docketAlert.date_created || docketAlert.dateCreated || previousState.dateCreated || null,
      dateModified: docketAlert.date_modified || docketAlert.dateModified || previousState.dateModified || timestamp,
      dateLastHit: docketAlert.date_last_hit || docketAlert.dateLastHit || previousState.dateLastHit || null,
      syncedAt: timestamp,
      state: state || (alertType === 1 ? "active" : alertType === 0 ? "disabled" : previousState.state || "unknown"),
      lastWebhookAt: lastWebhookAt || previousState.lastWebhookAt || null,
      lastWebhookEventType: lastWebhookEventType || previousState.lastWebhookEventType || null,
      lastWebhookEntryId: lastWebhookEntryId ?? previousState.lastWebhookEntryId ?? null,
      lastWebhookEntryCount: lastWebhookEntryCount ?? previousState.lastWebhookEntryCount ?? null,
      lastWebhookIdempotencyKey: lastWebhookIdempotencyKey || previousState.lastWebhookIdempotencyKey || null,
      reuppedAt: reuppedAt || previousState.reuppedAt || null
    };

    return this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: caseRow.primary_source,
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: caseRow.courtlistener_docket_id,
      pacer_case_id: caseRow.pacer_case_id,
      court_id: caseRow.court_id,
      court_name: caseRow.court_name,
      case_name: caseRow.case_name,
      docket_number: caseRow.docket_number,
      date_filed: caseRow.date_filed,
      date_terminated: caseRow.date_terminated,
      cause: caseRow.cause,
      nature_of_suit: caseRow.nature_of_suit,
      status: caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: caseRow.docket_url,
      source_urls: caseRow.source_urls || [],
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: caseRow.recent_activity_summary,
      latest_docket_filed_at: caseRow.latest_docket_filed_at,
      latest_docket_number: caseRow.latest_docket_number,
      docket_count: caseRow.docket_count,
      last_seen_at: caseRow.last_seen_at || timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: caseRow.last_docket_sync_at,
      raw: {
        ...(caseRow.raw || {}),
        courtlistener: {
          ...(caseRow.raw?.courtlistener || {}),
          docketAlert: nextState
        }
      }
    });
  }

  async ensureCourtListenerDocketAlert(caseRow, { force = false } = {}) {
    if (!caseRow?.id || !this.courtListener.hasDocketAlertAccess()) {
      return { subscribed: false, reason: "not-applicable" };
    }

    const docketId = Number(caseRow.courtlistener_docket_id || 0);
    if (!docketId) {
      return { subscribed: false, reason: "missing-docket-id" };
    }

    const previousState = caseRow.raw?.courtlistener?.docketAlert || {};
    const previousSyncedAtMs = Date.parse(String(previousState.syncedAt || previousState.updatedAt || previousState.dateModified || ""));
    const staleAfterMs = Math.max(Number(this.config.courtListener?.docketAlertSyncStaleAfterHours || 24), 1) * 60 * 60 * 1000;
    const previousAlertType = normalizeCourtListenerAlertType(previousState.alertType, previousState.alert_type);

    if (!force && Number(previousState.id || 0) > 0 && previousAlertType === 1 && Number.isFinite(previousSyncedAtMs) && Date.now() - previousSyncedAtMs < staleAfterMs) {
      return {
        subscribed: true,
        created: false,
        reupped: false,
        skipped: true,
        reason: "fresh",
        alertId: Number(previousState.id || 0)
      };
    }

    const existingAlerts = await this.courtListener.getDocketAlertsByDocket(docketId);
    const preferredAlert = [...existingAlerts].sort(compareCourtListenerAlerts)[0] || null;
    const timestamp = new Date().toISOString();

    if (preferredAlert?.id) {
      if (normalizeCourtListenerAlertType(preferredAlert.alert_type, preferredAlert.alertType) !== 1) {
        const updatedAlert = await this.courtListener.updateDocketAlert(preferredAlert.id, { alertType: 1 });
        this.persistCourtListenerDocketAlertState(caseRow, updatedAlert || preferredAlert, {
          syncedAt: timestamp,
          state: "active",
          reuppedAt: timestamp
        });
        return {
          subscribed: true,
          created: false,
          reupped: true,
          skipped: false,
          alertId: Number((updatedAlert || preferredAlert).id || 0)
        };
      }

      this.persistCourtListenerDocketAlertState(caseRow, preferredAlert, {
        syncedAt: timestamp,
        state: "active"
      });
      return {
        subscribed: true,
        created: false,
        reupped: false,
        skipped: false,
        alertId: Number(preferredAlert.id || 0)
      };
    }

    const createdAlert = await this.courtListener.createDocketAlert(docketId);
    if (!createdAlert?.id) {
      return { subscribed: false, reason: "create-failed" };
    }

    this.persistCourtListenerDocketAlertState(caseRow, createdAlert, {
      syncedAt: timestamp,
      state: "active"
    });
    return {
      subscribed: true,
      created: true,
      reupped: false,
      skipped: false,
      alertId: Number(createdAlert.id || 0)
    };
  }

  async syncCourtListenerAlertSubscriptions({
    limit = null,
    startDate = null,
    force = false
  } = {}) {
    if (!this.courtListener.hasDocketAlertAccess()) {
      return {
        candidateCases: 0,
        subscribedCases: 0,
        createdAlerts: 0,
        reuppedAlerts: 0,
        skippedCases: 0,
        failedCases: 0,
        note: "CourtListener docket alert API 未启用或没有 token。"
      };
    }

    const maxCases = Math.max(
      Number(limit || this.config.courtListener?.docketAlertSyncMaxCasesPerRun || 0),
      1
    );
    const rows = this.store.getCasesNeedingCourtListenerAlertSync(maxCases, {
      startDate: startDate || this.config.sync.startDate,
      staleAfterHours: this.config.courtListener?.docketAlertSyncStaleAfterHours || 24,
      force
    });

    let subscribedCases = 0;
    let createdAlerts = 0;
    let reuppedAlerts = 0;
    let skippedCases = 0;
    let failedCases = 0;

    for (const row of rows) {
      try {
        const result = await this.ensureCourtListenerDocketAlert(row, { force });
        if (result.subscribed) {
          subscribedCases += 1;
        }
        if (result.created) {
          createdAlerts += 1;
        }
        if (result.reupped) {
          reuppedAlerts += 1;
        }
        if (result.skipped) {
          skippedCases += 1;
        }
      } catch (error) {
        failedCases += 1;
        console.error(`[courtlistener-alerts] case ${row.id} failed: ${error.message}`);
      }
    }

    return {
      candidateCases: rows.length,
      subscribedCases,
      createdAlerts,
      reuppedAlerts,
      skippedCases,
      failedCases,
      note: rows.length
        ? `CourtListener docket alert 本轮处理 ${rows.length} 个案件。`
        : "CourtListener docket alert 本轮没有待订阅或待续订案件。"
    };
  }

  async handleCourtListenerWebhookEvent(caseId, {
    eventType = null,
    idempotencyKey = null,
    latestEntryId = null,
    entryCount = 0
  } = {}) {
    const caseRow = this.store.getCase(caseId);
    if (!caseRow) {
      return null;
    }

    this.persistCourtListenerDocketAlertState(this.store.getCase(caseId) || caseRow, {}, {
      syncedAt: new Date().toISOString(),
      state: "active",
      lastWebhookAt: new Date().toISOString(),
      lastWebhookEventType: eventType,
      lastWebhookEntryId: latestEntryId,
      lastWebhookEntryCount: entryCount,
      lastWebhookIdempotencyKey: idempotencyKey
    });

    return {
      caseId: Number(caseRow.id || 0),
      courtlistenerDocketId: Number(caseRow.courtlistener_docket_id || 0) || null,
      persisted: true
    };
  }

  async reupCourtListenerAlertsFromWebhook(alerts = []) {
    if (!this.courtListener.hasDocketAlertAccess() || !this.config.courtListener?.autoReupAlerts) {
      return { processed: 0, skipped: alerts.length, failed: 0 };
    }

    let processed = 0;
    let skipped = 0;
    let failed = 0;

    for (const alert of alerts) {
      const docketId = Number(alert?.docket || 0);
      const caseRow = this.store.getCaseByCourtListenerDocketId(docketId);

      if (!caseRow || !Number(alert?.id || 0)) {
        skipped += 1;
        continue;
      }

      try {
        const updated = await this.courtListener.updateDocketAlert(alert.id, { alertType: 1 });
        this.persistCourtListenerDocketAlertState(caseRow, updated || alert, {
          syncedAt: new Date().toISOString(),
          state: "active",
          reuppedAt: new Date().toISOString()
        });
        processed += 1;
      } catch (error) {
        failed += 1;
        console.error(`[courtlistener-alerts] re-up alert ${alert.id} failed: ${error.message}`);
      }
    }

    return { processed, skipped, failed };
  }

  async runCourtListenerRecapFetch(caseRow, { force = false } = {}) {
    if (!caseRow || !this.courtListener.hasRecapFetchAccess() || !hasCivilCaseDocket(caseRow)) {
      return { requested: false, reason: "not-applicable" };
    }

    const previousState = caseRow.raw?.courtlistener?.recapFetch || {};
    const lastTouchedAtMs = Date.parse(String(previousState.updatedAt || previousState.requestedAt || ""));
    if (!force && Number.isFinite(lastTouchedAtMs) && Date.now() - lastTouchedAtMs < 30 * 60 * 1000) {
      return { requested: false, reason: "fresh" };
    }

    const request = await this.courtListener.requestDocketViaRecapFetch({
      docketId: caseRow.courtlistener_docket_id,
      docketNumber: caseRow.docket_number,
      court: caseRow.court_id || "",
      pacerCaseId: caseRow.pacer_case_id
    });
    if (!request?.id) {
      return { requested: false, reason: "request-failed" };
    }

    let current = request;
    let docketId = parseCourtListenerResourceId(current.docket) || Number(caseRow.courtlistener_docket_id || 0) || null;
    this.persistCourtListenerRecapFetchState(caseRow, {
      ...current,
      requestedAt: new Date().toISOString()
    }, { docketId });

    const activeStatuses = new Set([1, 4, 5]);
    const deadline = Date.now() + this.courtListener.recapFetchMaxPollMs;

    while (current?.id && activeStatuses.has(Number(current.status || 0)) && Date.now() < deadline) {
      await waitForMs(this.courtListener.recapFetchPollIntervalMs);
      const next = await this.courtListener.fetchRecapFetchRequest(current.id);
      if (!next) {
        break;
      }

      current = next;
      docketId = parseCourtListenerResourceId(current.docket) || docketId;
      const refreshedCase = this.store.getCase(caseRow.id) || caseRow;
      this.persistCourtListenerRecapFetchState(refreshedCase, current, { docketId });
    }

    const status = Number(current?.status || 0);
    return {
      requested: true,
      completed: status === 2,
      status,
      docketId,
      reason:
        status === 2
          ? "completed"
          : activeStatuses.has(status)
            ? "queued"
            : status === 6
              ? "invalid"
              : status === 7
                ? "insufficient-metadata"
                : "error",
      errorMessage: current?.error_message || ""
    };
  }

  async enrichCaseWithCourtListener(caseId, { force = false } = {}) {
    return this.store.batchMutations(async () => {
      const caseRow = this.store.getCase(caseId);
      if (!caseRow || !this.courtListener.hasDocketAccess()) {
        return { enriched: false, reason: "not-applicable" };
      }

      const syncedAt = caseRow.last_docket_sync_at ? Date.parse(caseRow.last_docket_sync_at) : 0;
      const staleAfterMs = 2 * 60 * 60 * 1000;
      const activityAheadOfTimeline = hasCaseLevelActivityLead(this.store, caseRow);
      if (!force && !activityAheadOfTimeline && syncedAt && Date.now() - syncedAt < staleAfterMs) {
        return { enriched: false, reason: "fresh" };
      }

      const lookupTerm = caseRow.docket_number || caseRow.case_name;
      if (lookupTerm) {
        await this.importLookup(lookupTerm, {
          courtName: caseRow.court_name,
          caseName: caseRow.case_name
        });
      }

      let refreshedCase =
        this.store.getCase(caseId) ||
        this.store.findCaseByCourtAndDocket({
          courtId: caseRow.court_id,
          courtName: caseRow.court_name,
          docketNumber: caseRow.docket_number,
          startDate: getDiscoveryStartDate(this.config)
        });

      if (!refreshedCase?.courtlistener_docket_id) {
        const recapFetchResult = await this.runCourtListenerRecapFetch(caseRow, {
          force: force || activityAheadOfTimeline
        });
        refreshedCase =
          this.store.getCase(caseId) ||
          this.store.findCaseByCourtAndDocket({
            courtId: caseRow.court_id,
            courtName: caseRow.court_name,
            docketNumber: caseRow.docket_number,
            startDate: getDiscoveryStartDate(this.config)
          });

        if (!refreshedCase?.courtlistener_docket_id) {
          return {
            enriched: false,
            reason: recapFetchResult.requested ? recapFetchResult.reason : "not-found"
          };
        }
      }

      let result = await this.syncSingleCourtListenerDocket(refreshedCase);
      const shouldEscalateToRecapFetch =
        this.courtListener.hasRecapFetchAccess() &&
        (force || activityAheadOfTimeline || hasCaseLevelActivityLead(this.store, refreshedCase)) &&
        (result.reason === "metadata-only" || result.reason === "empty" || !result.enriched);

      if (!shouldEscalateToRecapFetch) {
        return result;
      }

      const recapFetchResult = await this.runCourtListenerRecapFetch(refreshedCase, { force: true });
      const fetchedCase =
        this.store.getCase(caseId) ||
        this.store.findCaseByCourtAndDocket({
          courtId: refreshedCase.court_id,
          courtName: refreshedCase.court_name,
          docketNumber: refreshedCase.docket_number,
          startDate: getDiscoveryStartDate(this.config)
        });

      if (fetchedCase?.courtlistener_docket_id && recapFetchResult.completed) {
        result = await this.syncSingleCourtListenerDocket(fetchedCase);
      }

      return {
        ...result,
        recapFetch: recapFetchResult.requested ? {
          status: recapFetchResult.status,
          reason: recapFetchResult.reason
        } : null
      };
    });
  }

  async enrichCaseWithPacerMonitor(caseId, { force = false } = {}) {
    return this.store.batchMutations(async () => {
      const caseRow = this.store.getCase(caseId);
      if (!caseRow || !this.pacerMonitor.enabled) {
        return { enriched: false, reason: "not-applicable" };
      }

      const syncedAt = caseRow.raw?.pacermonitor?.syncedAt ? Date.parse(caseRow.raw.pacermonitor.syncedAt) : 0;
      const state = String(caseRow.raw?.pacermonitor?.state || "").toLowerCase();
      const retryHours =
        state === "challenge" || state === "rate_limited"
          ? this.config.pacerMonitor.blockedRetryAfterHours
          : state === "not_found"
            ? this.config.pacerMonitor.notFoundRetryAfterHours
          : this.config.pacerMonitor.staleAfterHours;
      const staleAfterMs = retryHours * 60 * 60 * 1000;

      if (!force && syncedAt && Date.now() - syncedAt < staleAfterMs) {
        return { enriched: false, reason: "fresh" };
      }

      return this.syncSinglePacerMonitorCase(caseRow);
    });
  }

  async enrichCaseWithDocketAlarm(caseId, { force = false } = {}) {
    return this.store.batchMutations(async () => {
      const caseRow = this.store.getCase(caseId);
      if (!caseRow || !this.docketAlarm.enabled || !this.docketAlarm.hasCredentials()) {
        return { enriched: false, reason: "not-applicable" };
      }

      const syncedAt = caseRow.raw?.docketalarm?.syncedAt ? Date.parse(caseRow.raw.docketalarm.syncedAt) : 0;
      const state = String(caseRow.raw?.docketalarm?.state || "").toLowerCase();
      const retryHours =
        state === "not_found"
          ? this.config.docketAlarm.notFoundRetryAfterHours
          : this.config.docketAlarm.staleAfterHours;
      const staleAfterMs = retryHours * 60 * 60 * 1000;

      if (!force && syncedAt && Date.now() - syncedAt < staleAfterMs) {
        return { enriched: false, reason: "fresh" };
      }

      return this.syncSingleDocketAlarmCase(caseRow);
    });
  }

  async enrichCaseWithUniCourt(caseId, { force = false } = {}) {
    return this.store.batchMutations(async () => {
      const caseRow = this.store.getCase(caseId);
      const uniCourtConfig = this.config.uniCourt || {
        staleAfterHours: 24,
        notFoundRetryAfterHours: 24
      };
      if (!caseRow || !this.uniCourt.enabled || !this.uniCourt.hasCredentials()) {
        return { enriched: false, reason: "not-applicable" };
      }

      const syncedAt = caseRow.raw?.unicourt?.syncedAt ? Date.parse(caseRow.raw.unicourt.syncedAt) : 0;
      const state = String(caseRow.raw?.unicourt?.state || "").toLowerCase();
      const retryHours =
        state === "not_found"
          ? uniCourtConfig.notFoundRetryAfterHours
          : uniCourtConfig.staleAfterHours;
      const staleAfterMs = retryHours * 60 * 60 * 1000;

      if (!force && syncedAt && Date.now() - syncedAt < staleAfterMs) {
        return { enriched: false, reason: "fresh" };
      }

      return this.syncSingleUniCourtCase(caseRow);
    });
  }

  async syncPriorityFeedRecent(mode = "recent", { forceDiscovery = false } = {}) {
    return this.store.batchMutations(async () => {
      if (!this.priorityFeed.enabled) {
        return {
          syncedCases: 0,
          note: "优先目录 公开补源已关闭。"
        };
      }

      const shouldForceDiscovery = Boolean(forceDiscovery || process.env.PRIORITY_FEED_FORCE_DISCOVERY === "1");
      const discoveryResult = await this.syncPriorityFeedDiscovery({ force: shouldForceDiscovery });

      const maxCases =
        mode === "backfill"
          ? this.config.priorityFeed.backfillMaxCasesPerRun
          : this.config.priorityFeed.maxCasesPerRun;
      const progressEnabled = process.env.PRIORITY_FEED_PROGRESS === "1";
      const candidates = this.store.getCasesNeedingPriorityFeedSync(
        maxCases,
        this.config.priorityFeed.staleAfterHours,
        {
          preferKnownPriorityFeed: mode === "backfill",
          recentFiledWindowDays: this.config.sync.recentDocketFollowUpDays
        }
      );

      if (progressEnabled) {
        console.log(`[sync] catalog candidates ${JSON.stringify({
          mode,
          maxCases,
          candidateCount: candidates.length,
          staleAfterHours: this.config.priorityFeed.staleAfterHours
        })}`);
      }

      let syncedCases = 0;
      let failedCases = 0;
      let notFoundCases = 0;
      for (const [index, caseRow] of candidates.entries()) {
        const startedAt = Date.now();
        if (progressEnabled) {
          console.log(`[sync] catalog case starting ${JSON.stringify({
            index: index + 1,
            total: candidates.length,
            caseId: caseRow.id,
            docketNumber: caseRow.docket_number,
            court: caseRow.court_id || caseRow.court_name || null
          })}`);
        }

        try {
          const result = await this.syncSinglePriorityFeedCase(caseRow);
          if (result.enriched) {
            syncedCases += 1;
          } else if (result.reason === "not-found") {
            notFoundCases += 1;
          }

          if (progressEnabled) {
            console.log(`[sync] catalog case completed ${JSON.stringify({
              index: index + 1,
              total: candidates.length,
              caseId: caseRow.id,
              docketNumber: caseRow.docket_number,
              enriched: Boolean(result.enriched),
              reason: result.reason || null,
              entries: Number(result.entries || 0),
              elapsedMs: Date.now() - startedAt
            })}`);
          }
        } catch (error) {
          failedCases += 1;
          if (progressEnabled) {
            console.log(`[sync] catalog case failed ${JSON.stringify({
              index: index + 1,
              total: candidates.length,
              caseId: caseRow.id,
              docketNumber: caseRow.docket_number,
              elapsedMs: Date.now() - startedAt,
              error: error.message
            })}`);
          }
        }
      }

      return {
        syncedCases,
        failedCases,
        notFoundCases,
        candidateCount: candidates.length,
        discoveredCases: Number(discoveryResult.discoveredCases || 0),
        attachedCases: Number(discoveryResult.attachedCases || 0),
        createdCases: Number(discoveryResult.createdCases || 0),
        totalCatalogCases: Number(discoveryResult.totalCatalogCases || 0),
        discoverySkipped: Boolean(discoveryResult.skipped),
        note: syncedCases
          ? `优先目录 本轮${discoveryResult.skipped ? "复用目录缓存" : `登记 ${discoveryResult.discoveredCases} 个公开案件`}，并补齐 ${syncedCases} 个案件的公开时间线${notFoundCases ? `，${notFoundCases} 个案件暂未找到公开页` : ""}${failedCases ? `，另有 ${failedCases} 个案件待重试` : ""}。`
          : failedCases || notFoundCases
            ? `优先目录 本轮尝试 ${candidates.length} 个案件，${notFoundCases ? `${notFoundCases} 个案件暂未找到公开页` : ""}${notFoundCases && failedCases ? "，" : ""}${failedCases ? `${failedCases} 个案件待重试` : ""}。`
            : discoveryResult.discoveredCases
              ? `优先目录 本轮新增登记 ${discoveryResult.discoveredCases} 个公开案件，当前没有待补源案件。`
              : discoveryResult.skipped
                ? `优先目录 目录缓存仍有效（共 ${discoveryResult.totalCatalogCases || 0} 个公开案件），当前没有待补源案件。`
                : "优先目录 本轮没有待补源案件。"
      };
    });
  }

  async syncPriorityFeedDiscovery({ force = false } = {}) {
      if (!this.priorityFeed.enabled) {
        return { discoveredCases: 0 };
      }

    const checkpointKey = PRIORITY_FEED_DISCOVERY_CHECKPOINT;
    const staleAfterMs = Number(this.config.priorityFeed.discoveryStaleAfterHours || 0) * 60 * 60 * 1000;
    const checkpoint = this.store.getCheckpoint(checkpointKey) || {};
    const lastCatalogSyncMs = checkpoint.lastSyncedAt ? Date.parse(checkpoint.lastSyncedAt) : 0;
    if (!force && staleAfterMs > 0 && lastCatalogSyncMs && Date.now() - lastCatalogSyncMs < staleAfterMs) {
      return {
        discoveredCases: 0,
        attachedCases: Number(checkpoint.attachedCases || 0),
        createdCases: Number(checkpoint.createdCases || 0),
        totalCatalogCases: Number(checkpoint.totalCatalogCases || 0),
        skipped: true
      };
    }

    const listings = await this.priorityFeed.discoverCases();
    const discoveryIndex = buildPriorityFeedDiscoveryIndex(this.store.getHydratedCases(FULL_CATALOG_START_DATE));
    let discoveredCases = 0;
    let attachedCases = 0;
    let createdCases = 0;

    for (const item of listings) {
      const existingCase = findBestPriorityFeedDiscoveryCase(item, discoveryIndex) || null;
      const timestamp = new Date().toISOString();
      const sourceCaseKey =
        existingCase?.source_case_key || `${PRIORITY_FEED_ENTRY_SOURCE}:${item.stateCode}:${item.year}:${item.serial}`;
      const savedCase = this.store.upsertCase({
        source_case_key: sourceCaseKey,
        primary_source: existingCase?.primary_source || PRIORITY_FEED_ENTRY_SOURCE,
        source_case_id: existingCase?.source_case_id || item.pageId || `${item.stateCode}-${item.year}-${item.serial}`,
        courtlistener_docket_id: existingCase?.courtlistener_docket_id ?? null,
        pacer_case_id: existingCase?.pacer_case_id ?? null,
        court_id: existingCase?.court_id || item.stateCode.toLowerCase() || null,
        court_name: existingCase?.court_name || item.courtName || null,
        case_name: existingCase?.case_name || item.plaintiff || null,
        docket_number: existingCase?.docket_number || item.docketNumber,
        date_filed: existingCase?.date_filed || item.dateFiled || null,
        date_terminated: existingCase?.date_terminated || null,
        cause: existingCase?.cause || null,
        nature_of_suit: existingCase?.nature_of_suit || null,
        status: existingCase?.status || "open",
        tags_marker: buildTagsMarker([...(existingCase?.tags || []), "seller_tro", "tro"]),
        docket_url: item.caseUrl || existingCase?.docket_url || null,
        source_urls: [...(existingCase?.source_urls || []), item.caseUrl].filter(Boolean),
        plaintiffs: existingCase?.plaintiffs?.length ? existingCase.plaintiffs : (item.plaintiff ? [item.plaintiff] : []),
        defendants: existingCase?.defendants || [],
        recent_activity_summary: existingCase?.recent_activity_summary || (item.brand ? `优先目录 案件目录：${item.brand}` : "优先目录 案件目录"),
        latest_docket_filed_at: existingCase?.latest_docket_filed_at || item.dateFiled || null,
        latest_docket_number: existingCase?.latest_docket_number || null,
        docket_count: existingCase?.docket_count || 0,
        last_seen_at: timestamp,
        last_synced_at: timestamp,
        last_docket_sync_at: existingCase?.last_docket_sync_at || null,
        raw: mergePriorityFeedRaw(existingCase?.raw, {
          url: item.caseUrl,
          stateCode: item.stateCode,
          year: item.year,
          serial: item.serial,
          lawFirm: item.lawFirm || getPriorityFeedRaw(existingCase)?.lawFirm || null,
          brand: item.brand || getPriorityFeedRaw(existingCase)?.brand || null,
          catalogSeenAt: timestamp,
          catalogPageId: item.pageId || null
        })
      });

      if (!savedCase) {
        continue;
      }

      if (!existingCase || !existingCase.source_urls?.some((url) => sourceUrlUsesPriorityFeed(url))) {
        discoveredCases += 1;
        if (existingCase) {
          attachedCases += 1;
        } else {
          createdCases += 1;
        }
      }

      void savedCase;
    }

    this.store.saveCheckpoint(checkpointKey, {
      lastSyncedAt: new Date().toISOString(),
      totalCatalogCases: listings.length,
      discoveredCases,
      attachedCases,
      createdCases
    });

    return {
      discoveredCases,
      attachedCases,
      createdCases,
      totalCatalogCases: listings.length
    };
  }

  async syncPacerMonitorRecent(mode = "recent") {
    return this.store.batchMutations(async () => {
      if (!this.pacerMonitor.enabled) {
        return {
          syncedCases: 0,
          note: "PACERMonitor 精确补充链路已关闭。"
        };
      }

      const maxCases =
        mode === "backfill"
          ? this.config.pacerMonitor.backfillMaxCasesPerRun
          : this.config.pacerMonitor.maxCasesPerRun;
      const candidates = this.store.getCasesNeedingPacerMonitorSync(maxCases, {
        staleAfterHours: this.config.pacerMonitor.staleAfterHours,
        blockedRetryAfterHours: this.config.pacerMonitor.blockedRetryAfterHours,
        notFoundRetryAfterHours: this.config.pacerMonitor.notFoundRetryAfterHours,
        recentWindowDays: this.config.pacerMonitor.recentWindowDays,
        startDate: getDiscoveryStartDate(this.config)
      });

      let syncedCases = 0;
      let notFoundCases = 0;
      let blockedCases = 0;
      let emptyCases = 0;
      let failedCases = 0;

      for (const caseRow of candidates) {
        try {
          const result = await this.syncSinglePacerMonitorCase(caseRow);
          if (result.enriched) {
            syncedCases += 1;
            continue;
          }

          if (result.reason === "not_found") {
            notFoundCases += 1;
          } else if (result.reason === "challenge" || result.reason === "rate_limited") {
            blockedCases += 1;
          } else if (result.reason === "empty") {
            emptyCases += 1;
          }
        } catch {
          failedCases += 1;
        }
      }

      if (syncedCases) {
        return {
          syncedCases,
          note: `PACERMonitor 本轮补齐 ${syncedCases} 个案件的缺口${blockedCases ? `，${blockedCases} 个案件触发验证或限流稍后再试` : ""}${notFoundCases ? `，${notFoundCases} 个案件暂未找到公开页` : ""}${failedCases ? `，${failedCases} 个案件待重试` : ""}。`
        };
      }

      if (blockedCases || notFoundCases || emptyCases || failedCases) {
        return {
          syncedCases: 0,
          note: `PACERMonitor 本轮未补齐成功${blockedCases ? `，${blockedCases} 个案件触发验证或限流` : ""}${notFoundCases ? `，${notFoundCases} 个案件暂未找到公开页` : ""}${emptyCases ? `，${emptyCases} 个案件公开页暂无时间线` : ""}${failedCases ? `，${failedCases} 个案件待重试` : ""}。`
        };
      }

      return {
        syncedCases: 0,
        note: "PACERMonitor 本轮没有需要补齐的案件。"
      };
    });
  }

  async syncDocketAlarmRecent(mode = "recent") {
    return this.store.batchMutations(async () => {
      if (!this.docketAlarm.enabled) {
        return {
          syncedCases: 0,
          note: "Docket Alarm 官方 API 补源已关闭。"
        };
      }

      if (!this.docketAlarm.hasCredentials()) {
        return {
          syncedCases: 0,
          note: "Docket Alarm 已启用，但缺少账号凭据。"
        };
      }

      const maxCases =
        mode === "backfill"
          ? this.config.docketAlarm.backfillMaxCasesPerRun
          : this.config.docketAlarm.maxCasesPerRun;
      const candidatePool = this.store.getCasesNeedingSupplementalDocketSync(
        Math.max(maxCases * 8, maxCases),
        {
          recentWindowDays: Math.max(this.config.pacerMonitor.recentWindowDays || 0, 120),
          startDate: getDiscoveryStartDate(this.config)
        }
      );

      const staleBefore = Date.now() - this.config.docketAlarm.staleAfterHours * 60 * 60 * 1000;
      const notFoundBefore = Date.now() - this.config.docketAlarm.notFoundRetryAfterHours * 60 * 60 * 1000;
      const candidates = candidatePool
        .filter((caseRow) => {
          const syncedAt = caseRow.raw?.docketalarm?.syncedAt ? Date.parse(caseRow.raw.docketalarm.syncedAt) : 0;
          const state = String(caseRow.raw?.docketalarm?.state || "").toLowerCase();
          const freshnessCutoff = state === "not_found" ? notFoundBefore : staleBefore;
          return !syncedAt || syncedAt < freshnessCutoff;
        })
        .slice(0, maxCases);

      let syncedCases = 0;
      let notFoundCases = 0;
      let emptyCases = 0;
      let failedCases = 0;

      for (const caseRow of candidates) {
        try {
          const result = await this.syncSingleDocketAlarmCase(caseRow);
          if (result.enriched) {
            syncedCases += 1;
            continue;
          }

          if (result.reason === "not_found") {
            notFoundCases += 1;
          } else if (result.reason === "empty") {
            emptyCases += 1;
          }
        } catch {
          failedCases += 1;
        }
      }

      if (syncedCases) {
        return {
          syncedCases,
          note: `Docket Alarm 本轮通过官方 API 补齐 ${syncedCases} 个案件${notFoundCases ? `，${notFoundCases} 个案件暂未找到` : ""}${emptyCases ? `，${emptyCases} 个案件缓存副本暂无时间线` : ""}${failedCases ? `，${failedCases} 个案件待重试` : ""}。`
        };
      }

      if (notFoundCases || emptyCases || failedCases) {
        return {
          syncedCases: 0,
          note: `Docket Alarm 本轮未补齐成功${notFoundCases ? `，${notFoundCases} 个案件暂未找到` : ""}${emptyCases ? `，${emptyCases} 个案件缓存副本暂无时间线` : ""}${failedCases ? `，${failedCases} 个案件待重试` : ""}。`
        };
      }

      return {
        syncedCases: 0,
        note: "Docket Alarm 本轮没有需要补齐的案件。"
      };
    });
  }

  async syncUniCourtRecent(mode = "recent") {
    return this.store.batchMutations(async () => {
      const uniCourtConfig = this.config.uniCourt || {
        maxCasesPerRun: 0,
        backfillMaxCasesPerRun: 0,
        staleAfterHours: 24,
        notFoundRetryAfterHours: 24
      };
      if (!this.uniCourt.enabled) {
        return {
          syncedCases: 0,
          note: "UniCourt API 补源已关闭。"
        };
      }

      if (!this.uniCourt.hasCredentials()) {
        return {
          syncedCases: 0,
          note: "UniCourt 已启用，但缺少 API token 或账号凭据。"
        };
      }

      const maxCases =
        mode === "backfill"
          ? uniCourtConfig.backfillMaxCasesPerRun
          : uniCourtConfig.maxCasesPerRun;
      const candidatePool = this.store.getCasesNeedingSupplementalDocketSync(
        Math.max(maxCases * 8, maxCases),
        {
          recentWindowDays: Math.max(this.config.pacerMonitor.recentWindowDays || 0, 120),
          startDate: getDiscoveryStartDate(this.config)
        }
      );

      const staleBefore = Date.now() - uniCourtConfig.staleAfterHours * 60 * 60 * 1000;
      const notFoundBefore = Date.now() - uniCourtConfig.notFoundRetryAfterHours * 60 * 60 * 1000;
      const candidates = candidatePool
        .filter((caseRow) => {
          const syncedAt = caseRow.raw?.unicourt?.syncedAt ? Date.parse(caseRow.raw.unicourt.syncedAt) : 0;
          const state = String(caseRow.raw?.unicourt?.state || "").toLowerCase();
          const freshnessCutoff = state === "not_found" ? notFoundBefore : staleBefore;
          return !syncedAt || syncedAt < freshnessCutoff;
        })
        .slice(0, maxCases);

      let syncedCases = 0;
      let notFoundCases = 0;
      let emptyCases = 0;
      let failedCases = 0;

      for (const caseRow of candidates) {
        try {
          const result = await this.syncSingleUniCourtCase(caseRow);
          if (result.enriched) {
            syncedCases += 1;
            continue;
          }

          if (result.reason === "not_found") {
            notFoundCases += 1;
          } else if (result.reason === "empty") {
            emptyCases += 1;
          }
        } catch {
          failedCases += 1;
        }
      }

      if (syncedCases) {
        return {
          syncedCases,
          note: `UniCourt 本轮补齐 ${syncedCases} 个案件${notFoundCases ? `，${notFoundCases} 个案件暂未找到` : ""}${emptyCases ? `，${emptyCases} 个案件详情里暂无 docket` : ""}${failedCases ? `，${failedCases} 个案件待重试` : ""}。`
        };
      }

      if (notFoundCases || emptyCases || failedCases) {
        return {
          syncedCases: 0,
          note: `UniCourt 本轮未补齐成功${notFoundCases ? `，${notFoundCases} 个案件暂未找到` : ""}${emptyCases ? `，${emptyCases} 个案件详情里暂无 docket` : ""}${failedCases ? `，${failedCases} 个案件待重试` : ""}。`
        };
      }

      return {
        syncedCases: 0,
        note: "UniCourt 本轮没有需要补齐的案件。"
      };
    });
  }

  async syncSinglePriorityFeedCase(caseRow) {
    const payload = await this.priorityFeed.enrichCase(caseRow);
    if (!payload || !payload.entries.length) {
      const timestamp = new Date().toISOString();
      const mergedRaw = mergePriorityFeedRaw(caseRow.raw, {
        syncedAt: timestamp,
        rowCount: 0,
        missing: true
      });

      this.store.upsertCase({
        source_case_key: caseRow.source_case_key,
        primary_source: caseRow.primary_source,
        source_case_id: caseRow.source_case_id,
        courtlistener_docket_id: caseRow.courtlistener_docket_id,
        pacer_case_id: caseRow.pacer_case_id,
        court_id: caseRow.court_id,
        court_name: caseRow.court_name,
        case_name: caseRow.case_name,
        docket_number: caseRow.docket_number,
        date_filed: caseRow.date_filed,
        date_terminated: caseRow.date_terminated,
        cause: caseRow.cause,
        nature_of_suit: caseRow.nature_of_suit,
        status: caseRow.status,
        tags_marker: caseRow.tags_marker,
        docket_url: caseRow.docket_url,
        source_urls: caseRow.source_urls || [],
        plaintiffs: caseRow.plaintiffs || [],
        defendants: caseRow.defendants || [],
        recent_activity_summary: caseRow.recent_activity_summary,
        latest_docket_filed_at: caseRow.latest_docket_filed_at,
        latest_docket_number: caseRow.latest_docket_number,
        docket_count: caseRow.docket_count || 0,
        last_seen_at: caseRow.last_seen_at || timestamp,
        last_synced_at: timestamp,
        last_docket_sync_at: caseRow.last_docket_sync_at,
        raw: mergedRaw
      });

      return { enriched: false, reason: "not-found" };
    }

    const timestamp = new Date().toISOString();
    this.store.deleteDocketEntriesNotFromSourceForRelatedCases(caseRow, PRIORITY_FEED_ENTRY_SOURCE);
    this.store.deleteDocketEntriesBySourceForRelatedCases(caseRow, PRIORITY_FEED_ENTRY_SOURCE);

    const mergedRaw = mergePriorityFeedRaw(caseRow.raw, {
      url: payload.url,
      title: payload.title,
      lawFirm: payload.lawFirm,
      brand: payload.brand,
      rowCount: payload.entries.length,
      stateCode: payload.stateCode,
      year: payload.year,
      serial: payload.serial,
      matchQuality: payload.matchQuality || null,
      syncedAt: payload.syncedAt || timestamp
    });

    const savedCase = this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: caseRow.primary_source,
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: caseRow.courtlistener_docket_id,
      pacer_case_id: caseRow.pacer_case_id,
      court_id: caseRow.court_id,
      court_name: caseRow.court_name,
      case_name: caseRow.case_name,
      docket_number: caseRow.docket_number,
      date_filed: caseRow.date_filed,
      date_terminated: caseRow.date_terminated,
      cause: caseRow.cause,
      nature_of_suit: caseRow.nature_of_suit,
      status: caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: caseRow.docket_url,
      source_urls: [...(caseRow.source_urls || []), payload.url],
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: payload.entries[0]?.description || caseRow.recent_activity_summary,
      latest_docket_filed_at: payload.entries[0]?.filed_at || caseRow.latest_docket_filed_at,
      latest_docket_number: payload.entries[0]?.row_number || caseRow.latest_docket_number,
      docket_count: payload.entries.length,
      docket_count_exact: true,
      last_seen_at: timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: timestamp,
      raw: mergedRaw
    });

    if (!savedCase) {
      return { enriched: false, reason: "filtered" };
    }

    for (const entry of payload.entries) {
      const digest = crypto
        .createHash("sha1")
        .update(`${payload.url}|${entry.row_number}|${entry.filed_at}|${entry.description}`)
        .digest("hex")
        .slice(0, 16);

      this.store.upsertDocketEntry({
        case_id: savedCase.id,
        source_entry_key: `${PRIORITY_FEED_ENTRY_SOURCE}:${savedCase.id}:${entry.row_number}:${digest}`,
        primary_source: PRIORITY_FEED_ENTRY_SOURCE,
        source_entry_id: String(entry.row_number || ""),
        document_type: "优先目录 Entry",
        entry_number: String(entry.row_number || ""),
        document_number: String(entry.row_number || ""),
        filed_at: entry.filed_at,
        description: entry.description,
        absolute_url: payload.url,
        is_available: 0,
        page_count: null,
        pacer_doc_id: null,
        raw: entry,
        last_synced_at: timestamp
      });
    }

    const coverage = this.store.getEntryCoverageForCaseIds([savedCase.id]).get(savedCase.id) || null;
    const refreshedCase = this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: caseRow.primary_source,
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: caseRow.courtlistener_docket_id,
      pacer_case_id: caseRow.pacer_case_id,
      court_id: caseRow.court_id,
      court_name: caseRow.court_name,
      case_name: caseRow.case_name,
      docket_number: caseRow.docket_number,
      date_filed: caseRow.date_filed,
      date_terminated: caseRow.date_terminated,
      cause: caseRow.cause,
      nature_of_suit: caseRow.nature_of_suit,
      status: caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: caseRow.docket_url,
      source_urls: [...(caseRow.source_urls || []), payload.url].filter(Boolean),
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: payload.entries?.[0]?.description || caseRow.recent_activity_summary,
      latest_docket_filed_at: payload.entries?.[0]?.filed_at || caseRow.latest_docket_filed_at,
      latest_docket_number: higherOrderValue(
        payload.entries?.[0]?.row_number || caseRow.latest_docket_number,
        coverage?.lastNumber ? String(coverage.lastNumber) : null
      ),
      docket_count: Math.max(payload.entries?.length || 0, coverage?.totalEntries || 0),
      last_seen_at: timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: timestamp,
      raw: mergedRaw
    });

    if (!refreshedCase) {
      return {
        enriched: false,
        reason: "filtered",
        url: payload.url || null
      };
    }

    return {
      enriched: true,
      entries: payload.entries.length,
      url: payload.url
    };
  }

  async syncSinglePacerMonitorCase(caseRow) {
    if (this.store.caseGroupHasPriorityFeedAuthority(caseRow)) {
      return { enriched: false, reason: "priority-authoritative" };
    }

    const payload = await this.pacerMonitor.enrichCase(caseRow);
    const timestamp = new Date().toISOString();
    if (!payload) {
      return { enriched: false, reason: "not-applicable" };
    }

    const mergedRaw = {
      ...(caseRow.raw || {}),
      pacermonitor: {
        ...(caseRow.raw?.pacermonitor || {}),
        caseUrl: payload.url || caseRow.raw?.pacermonitor?.caseUrl || null,
        title: payload.title || null,
        metaDescription: payload.metaDescription || null,
        rowCount: payload.entries?.length || 0,
        state: payload.state || "empty",
        syncedAt: payload.syncedAt || timestamp
      }
    };

    const savedCase = this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: caseRow.primary_source,
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: caseRow.courtlistener_docket_id,
      pacer_case_id: caseRow.pacer_case_id,
      court_id: caseRow.court_id,
      court_name: caseRow.court_name,
      case_name: caseRow.case_name,
      docket_number: caseRow.docket_number,
      date_filed: caseRow.date_filed,
      date_terminated: caseRow.date_terminated,
      cause: caseRow.cause,
      nature_of_suit: caseRow.nature_of_suit,
      status: caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: caseRow.docket_url,
      source_urls: [...(caseRow.source_urls || []), payload.url].filter(Boolean),
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: payload.entries?.[0]?.description || caseRow.recent_activity_summary,
      latest_docket_filed_at: payload.entries?.[0]?.filed_at || caseRow.latest_docket_filed_at,
      latest_docket_number: payload.entries?.[0]?.row_number || caseRow.latest_docket_number,
      docket_count: Math.max(caseRow.docket_count || 0, payload.entries?.length || 0),
      last_seen_at: timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: caseRow.last_docket_sync_at,
      raw: mergedRaw
    });

    if (!savedCase) {
      return {
        enriched: false,
        reason: "filtered",
        url: payload.url || null
      };
    }

    if (!payload.entries?.length) {
      return {
        enriched: false,
        reason: payload.state || "empty",
        url: payload.url || null
      };
    }

    for (const entry of payload.entries) {
      const digest = crypto
        .createHash("sha1")
        .update(`${payload.url}|${entry.row_number}|${entry.filed_at}|${entry.description}`)
        .digest("hex")
        .slice(0, 16);

      this.store.upsertDocketEntry({
        case_id: savedCase.id,
        source_entry_key: `pacermonitor:${savedCase.id}:${entry.row_number || "na"}:${digest}`,
        primary_source: "pacermonitor",
        source_entry_id: String(entry.row_number || ""),
        document_type: "PACERMonitor Docket Entry",
        entry_number: String(entry.row_number || ""),
        document_number: String(entry.row_number || ""),
        filed_at: entry.filed_at,
        description: entry.description,
        absolute_url: payload.url,
        is_available: 0,
        page_count: null,
        pacer_doc_id: null,
        raw: entry,
        last_synced_at: timestamp
      });
    }

    return {
      enriched: true,
      entries: payload.entries.length,
      url: payload.url
    };
  }

  async syncSingleDocketAlarmCase(caseRow) {
    const payload = await this.docketAlarm.enrichCase(caseRow);
    const timestamp = new Date().toISOString();
    if (!payload) {
      return { enriched: false, reason: "not-applicable" };
    }

    const mergedRaw = {
      ...(caseRow.raw || {}),
      docketalarm: {
        ...(caseRow.raw?.docketalarm || {}),
        caseUrl: payload.url || caseRow.raw?.docketalarm?.caseUrl || null,
        title: payload.title || null,
        court: payload.court || null,
        docket: payload.docket || caseRow.docket_number || null,
        rowCount: payload.entries?.length || 0,
        state: payload.state || "empty",
        syncedAt: payload.syncedAt || timestamp
      }
    };

    let latestEntryNumber = caseRow.latest_docket_number || null;
    let latestFiledAt = caseRow.latest_docket_filed_at || null;
    let latestSummary = caseRow.recent_activity_summary || null;

    for (const entry of payload.entries || []) {
      const candidateNumber = entry.row_number || null;
      const candidateFiledAt = entry.filed_at || null;
      const nextLatestNumber = higherOrderValue(latestEntryNumber, candidateNumber);
      const nextLatestFiledAt = laterIso(latestFiledAt, candidateFiledAt);

      if (nextLatestNumber !== latestEntryNumber || nextLatestFiledAt !== latestFiledAt) {
        latestSummary = entry.description || latestSummary;
      }

      latestEntryNumber = nextLatestNumber;
      latestFiledAt = nextLatestFiledAt;
    }

    const savedCase = this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: caseRow.primary_source,
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: caseRow.courtlistener_docket_id,
      pacer_case_id: caseRow.pacer_case_id,
      court_id: caseRow.court_id,
      court_name: caseRow.court_name,
      case_name: caseRow.case_name,
      docket_number: caseRow.docket_number,
      date_filed: caseRow.date_filed,
      date_terminated: caseRow.date_terminated,
      cause: caseRow.cause,
      nature_of_suit: caseRow.nature_of_suit,
      status: caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: caseRow.docket_url,
      source_urls: [...(caseRow.source_urls || []), payload.url].filter(Boolean),
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: latestSummary,
      latest_docket_filed_at: latestFiledAt,
      latest_docket_number: latestEntryNumber,
      docket_count: Math.max(caseRow.docket_count || 0, payload.entries?.length || 0),
      last_seen_at: timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: caseRow.last_docket_sync_at,
      raw: mergedRaw
    });

    if (!savedCase) {
      return {
        enriched: false,
        reason: "filtered",
        url: payload.url || null
      };
    }

    if (!payload.entries?.length) {
      return {
        enriched: false,
        reason: payload.state || "empty",
        url: payload.url || null
      };
    }

    this.store.deleteDocketEntriesBySourceForRelatedCases(caseRow, "docketalarm");

    for (const entry of payload.entries) {
      const digest = crypto
        .createHash("sha1")
        .update(`${payload.url}|${entry.row_number}|${entry.filed_at}|${entry.description}`)
        .digest("hex")
        .slice(0, 16);

      this.store.upsertDocketEntry({
        case_id: savedCase.id,
        source_entry_key: `docketalarm:${savedCase.id}:${entry.row_number || "na"}:${digest}`,
        primary_source: "docketalarm",
        source_entry_id: String(entry.row_number || ""),
        document_type: "Docket Alarm Docket Entry",
        entry_number: String(entry.row_number || ""),
        document_number: String(entry.row_number || ""),
        filed_at: entry.filed_at,
        description: entry.description,
        absolute_url: entry.absolute_url || payload.url,
        is_available: 0,
        page_count: null,
        pacer_doc_id: null,
        raw: entry.raw || entry,
        last_synced_at: timestamp
      });
    }

    const coverage = this.store.getEntryCoverageForCaseIds([savedCase.id]).get(savedCase.id) || null;
    const refreshedCase = this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: caseRow.primary_source,
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: caseRow.courtlistener_docket_id,
      pacer_case_id: caseRow.pacer_case_id,
      court_id: caseRow.court_id,
      court_name: caseRow.court_name,
      case_name: caseRow.case_name,
      docket_number: caseRow.docket_number,
      date_filed: caseRow.date_filed,
      date_terminated: caseRow.date_terminated,
      cause: caseRow.cause,
      nature_of_suit: caseRow.nature_of_suit,
      status: caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: caseRow.docket_url,
      source_urls: [...(caseRow.source_urls || []), payload.url].filter(Boolean),
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: latestSummary,
      latest_docket_filed_at: latestFiledAt,
      latest_docket_number: higherOrderValue(latestEntryNumber, coverage?.lastNumber ? String(coverage.lastNumber) : null),
      docket_count: Math.max(payload.entries?.length || 0, coverage?.totalEntries || 0),
      last_seen_at: timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: caseRow.last_docket_sync_at,
      raw: mergedRaw
    });

    if (!refreshedCase) {
      return {
        enriched: false,
        reason: "filtered",
        url: payload.url || null
      };
    }

    return {
      enriched: true,
      entries: payload.entries.length,
      url: payload.url
    };
  }

  async syncSingleUniCourtCase(caseRow) {
    const payload = await this.uniCourt.enrichCase(caseRow);
    const timestamp = new Date().toISOString();
    if (!payload) {
      return { enriched: false, reason: "not-applicable" };
    }

    const mergedRaw = {
      ...(caseRow.raw || {}),
      unicourt: {
        ...(caseRow.raw?.unicourt || {}),
        caseId: payload.caseId || caseRow.raw?.unicourt?.caseId || null,
        caseUrl: payload.url || caseRow.raw?.unicourt?.caseUrl || null,
        title: payload.title || null,
        court: payload.court || null,
        docket: payload.docket || caseRow.docket_number || null,
        rowCount: payload.entries?.length || 0,
        state: payload.state || "empty",
        syncedAt: payload.syncedAt || timestamp
      }
    };

    let latestEntryNumber = caseRow.latest_docket_number || null;
    let latestFiledAt = caseRow.latest_docket_filed_at || null;
    let latestSummary = caseRow.recent_activity_summary || null;

    for (const entry of payload.entries || []) {
      const candidateNumber = entry.row_number || null;
      const candidateFiledAt = entry.filed_at || null;
      const nextLatestNumber = higherOrderValue(latestEntryNumber, candidateNumber);
      const nextLatestFiledAt = laterIso(latestFiledAt, candidateFiledAt);

      if (nextLatestNumber !== latestEntryNumber || nextLatestFiledAt !== latestFiledAt) {
        latestSummary = entry.description || latestSummary;
      }

      latestEntryNumber = nextLatestNumber;
      latestFiledAt = nextLatestFiledAt;
    }

    const savedCase = this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: caseRow.primary_source,
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: caseRow.courtlistener_docket_id,
      pacer_case_id: caseRow.pacer_case_id,
      court_id: caseRow.court_id,
      court_name: caseRow.court_name,
      case_name: caseRow.case_name,
      docket_number: caseRow.docket_number,
      date_filed: caseRow.date_filed,
      date_terminated: caseRow.date_terminated,
      cause: caseRow.cause,
      nature_of_suit: caseRow.nature_of_suit,
      status: caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: caseRow.docket_url,
      source_urls: [...(caseRow.source_urls || []), payload.url].filter(Boolean),
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: latestSummary,
      latest_docket_filed_at: latestFiledAt,
      latest_docket_number: latestEntryNumber,
      docket_count: Math.max(caseRow.docket_count || 0, payload.entries?.length || 0),
      last_seen_at: timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: caseRow.last_docket_sync_at,
      raw: mergedRaw
    });

    if (!savedCase) {
      return {
        enriched: false,
        reason: "filtered",
        url: payload.url || null
      };
    }

    if (!payload.entries?.length) {
      return {
        enriched: false,
        reason: payload.state || "empty",
        url: payload.url || null
      };
    }

    this.store.deleteDocketEntriesBySourceForRelatedCases(caseRow, "unicourt");

    for (const entry of payload.entries) {
      const digest = crypto
        .createHash("sha1")
        .update(`${payload.url}|${entry.row_number}|${entry.filed_at}|${entry.description}`)
        .digest("hex")
        .slice(0, 16);

      this.store.upsertDocketEntry({
        case_id: savedCase.id,
        source_entry_key: `unicourt:${savedCase.id}:${entry.row_number || "na"}:${digest}`,
        primary_source: "unicourt",
        source_entry_id: String(entry.row_number || ""),
        document_type: "UniCourt Docket Entry",
        entry_number: String(entry.row_number || ""),
        document_number: String(entry.row_number || ""),
        filed_at: entry.filed_at,
        description: entry.description,
        absolute_url: entry.absolute_url || payload.url,
        is_available: 0,
        page_count: null,
        pacer_doc_id: null,
        raw: entry.raw || entry,
        last_synced_at: timestamp
      });
    }

    const coverage = this.store.getEntryCoverageForCaseIds([savedCase.id]).get(savedCase.id) || null;
    const refreshedCase = this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: caseRow.primary_source,
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: caseRow.courtlistener_docket_id,
      pacer_case_id: caseRow.pacer_case_id,
      court_id: caseRow.court_id,
      court_name: caseRow.court_name,
      case_name: caseRow.case_name,
      docket_number: caseRow.docket_number,
      date_filed: caseRow.date_filed,
      date_terminated: caseRow.date_terminated,
      cause: caseRow.cause,
      nature_of_suit: caseRow.nature_of_suit,
      status: caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: caseRow.docket_url,
      source_urls: [...(caseRow.source_urls || []), payload.url].filter(Boolean),
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: latestSummary,
      latest_docket_filed_at: latestFiledAt,
      latest_docket_number: higherOrderValue(latestEntryNumber, coverage?.lastNumber ? String(coverage.lastNumber) : null),
      docket_count: Math.max(payload.entries?.length || 0, coverage?.totalEntries || 0),
      last_seen_at: timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: caseRow.last_docket_sync_at,
      raw: mergedRaw
    });

    if (!refreshedCase) {
      return {
        enriched: false,
        reason: "filtered",
        url: payload.url || null
      };
    }

    return {
      enriched: true,
      entries: payload.entries.length,
      url: payload.url
    };
  }

  async syncPreset(preset, mode) {
    const maxPages =
      mode === "backfill"
        ? this.config.sync.backfillMaxPagesPerRun
        : this.config.sync.discoveryMaxPagesPerRun;

    const checkpointKey = `courtlistener:${preset.key}:backfill`;
    const checkpoint = mode === "backfill" ? this.store.getCheckpoint(checkpointKey) : null;
    let cursorUrl = checkpoint?.nextUrl || null;
    let pageIndex = 0;
    let pagesFetched = 0;
    let casesUpserted = 0;
    let docketEntriesUpserted = 0;

    while (pageIndex < maxPages) {
      const payload = await this.courtListener.search({
        query: preset.query,
        cursorUrl,
        startDate: getDiscoveryStartDate(this.config)
      });

      pagesFetched += 1;
      pageIndex += 1;
      const ingest = this.ingestSearchResults(payload.results || [], preset);
      casesUpserted += ingest.casesUpserted;
      docketEntriesUpserted += ingest.docketEntriesUpserted;
      cursorUrl = payload.next;

      if (!cursorUrl) {
        break;
      }
    }

    if (mode === "backfill") {
      this.store.saveCheckpoint(checkpointKey, {
        nextUrl: cursorUrl,
        completed: !cursorUrl,
        updatedAt: new Date().toISOString()
      });
    }

    return {
      pagesFetched,
      casesUpserted,
      docketEntriesUpserted
    };
  }

  ingestSearchResults(results, preset) {
    let casesUpserted = 0;
    let docketEntriesUpserted = 0;

    for (const result of results) {
      const documents = asArray(result.recap_documents);
      const latestDoc = documents[0];
      const parties = deriveParties(result);
      const tags = classifyCase(result, preset.tags);
      const existingCase = this.store.findCaseByCourtAndDocket({
        courtId: valueOf(result.court_id),
        courtName: valueOf(result.court),
        docketNumber: valueOf(result.docketNumber),
        startDate: getDiscoveryStartDate(this.config)
      });

      if (!shouldTrackCourtListenerResult(result, existingCase, tags)) {
        continue;
      }

      const savedCase = this.store.upsertCase({
        source_case_key: existingCase?.source_case_key || `courtlistener:${result.docket_id}`,
        primary_source: "courtlistener",
        source_case_id: String(result.docket_id),
        courtlistener_docket_id: Number(result.docket_id),
        pacer_case_id: valueOf(result.pacer_case_id, existingCase?.pacer_case_id),
        court_id: valueOf(result.court_id),
        court_name: valueOf(result.court),
        case_name: valueOf(result.caseName, result.case_name_full),
        docket_number: valueOf(result.docketNumber),
        date_filed: valueOf(result.dateFiled, existingCase?.date_filed),
        date_terminated: valueOf(result.dateTerminated, existingCase?.date_terminated),
        cause: valueOf(result.cause, existingCase?.cause),
        nature_of_suit: valueOf(result.nature_of_suit, result.suitNature, existingCase?.nature_of_suit),
        status: result.dateTerminated ? "terminated" : existingCase?.status || "open",
        tags_marker: buildTagsMarker([...(existingCase?.tags || []), ...tags]),
        docket_url: this.courtListener.absoluteUrl(result.docket_absolute_url),
        source_urls: [...(existingCase?.source_urls || []), this.courtListener.absoluteUrl(result.docket_absolute_url)].filter(Boolean),
        plaintiffs: parties.plaintiffs.length ? parties.plaintiffs : existingCase?.plaintiffs || [],
        defendants: parties.defendants.length ? parties.defendants : existingCase?.defendants || [],
        recent_activity_summary: valueOf(latestDoc?.short_description, latestDoc?.description, existingCase?.recent_activity_summary),
        latest_docket_filed_at: valueOf(latestDoc?.entry_date_filed, latestDoc?.date_filed, existingCase?.latest_docket_filed_at, result.dateFiled),
        latest_docket_number: valueOf(latestDoc?.entry_number, latestDoc?.document_number, existingCase?.latest_docket_number),
        docket_count: Math.max(existingCase?.docket_count || 0, documents.length),
        last_seen_at: valueOf(result.meta?.timestamp, new Date().toISOString()),
        last_synced_at: new Date().toISOString(),
        raw: {
          ...(existingCase?.raw || {}),
          ...result,
          courtlistener: result
        }
      });

      if (!savedCase) {
        continue;
      }

      casesUpserted += 1;

      for (const document of documents) {
        const savedEntry = this.store.upsertDocketEntry({
          case_id: savedCase.id,
          source_entry_key: `courtlistener:recap:${document.id}`,
          primary_source: "courtlistener",
          source_entry_id: valueOf(document.id),
          document_type: valueOf(document.document_type),
          entry_number: valueOf(document.entry_number),
          document_number: valueOf(document.document_number),
          filed_at: valueOf(document.entry_date_filed, document.date_filed),
          description: valueOf(document.short_description, document.description),
          absolute_url: this.courtListener.absoluteUrl(document.absolute_url),
          is_available: document.is_available ? 1 : 0,
          page_count: valueOf(document.page_count),
          pacer_doc_id: valueOf(document.pacer_doc_id),
          raw: document,
          last_synced_at: new Date().toISOString()
        });

        if (savedEntry) {
          docketEntriesUpserted += 1;
        }
      }
    }

    return {
      casesUpserted,
      docketEntriesUpserted
    };
  }

  async syncCourtListenerDockets(mode = "recent") {
    return this.store.batchMutations(async () => {
      if (!this.courtListener.hasDocketAccess()) {
        return {
          syncedCases: 0,
          note: "CourtListener docket API 未开启或没有 token，当前只同步公开 search 结果和嵌入式 recap 文档。"
        };
      }

      const limit = mode === "backfill"
        ? this.config.courtListener.docketBackfillMaxCasesPerRun
        : this.config.courtListener.docketMaxCasesPerRun;
      const followUpCandidates = mode === "recent"
        ? this.store.getCasesNeedingCourtListenerFollowUp(Math.max(limit * 2, limit), {
            recentFiledWindowDays: this.config.sync.recentDocketFollowUpDays,
            startDate: getDiscoveryStartDate(this.config)
          })
        : [];
      const baseCandidates = this.store.getCasesNeedingDocketSync(Math.max(limit * 3, limit));
      const followUpIds = new Set(followUpCandidates.map((row) => Number(row.id)));
      const candidates = [];
      const seen = new Set();

      for (const row of [...followUpCandidates, ...baseCandidates]) {
        const id = Number(row.id || 0);
        if (!id || seen.has(id)) {
          continue;
        }
        seen.add(id);
        candidates.push(row);
        if (candidates.length >= limit) {
          break;
        }
      }
      let syncedCases = 0;
      let metadataOnlyMode = false;

      for (const caseRow of candidates) {
        try {
          const caseId = Number(caseRow.id || 0);
          const shouldForceFollowUp =
            followUpIds.has(caseId) ||
            hasCaseLevelActivityLead(this.store, caseRow);
          const result = caseRow.courtlistener_docket_id && !shouldForceFollowUp
            ? await this.syncSingleCourtListenerDocket(caseRow)
            : await this.enrichCaseWithCourtListener(caseId, { force: shouldForceFollowUp });
          if (result.enriched) {
            syncedCases += 1;
          }
          if (result.reason === "metadata-only") {
            metadataOnlyMode = true;
          }
        } catch (error) {
          return {
            syncedCases,
            note: `CourtListener docket 拉取中止：${error.message}`
          };
        }
      }

      return {
        syncedCases,
        note: metadataOnlyMode
          ? `CourtListener docket 元数据本轮更新 ${syncedCases} 个案件，但当前公开链路未返回可用 docket-entries。`
          : syncedCases
            ? `CourtListener docket API 本轮补齐 ${syncedCases} 个案件。`
            : "CourtListener docket API 已启用，但本轮没有待补齐案件。"
      };
    });
  }

  async syncSingleCourtListenerDocket(caseRow) {
    const docketId = caseRow.courtlistener_docket_id;
    if (!docketId) {
      return { enriched: false, reason: "not-found" };
    }

    const docket = await this.courtListener.fetchDocket(docketId);
    const entries = await this.courtListener.fetchDocketEntries(docketId);
    const metadataOnlyMode = !entries.length && !this.courtListener.hasDocketEntriesAccess();
    const timestamp = new Date().toISOString();
    let latestEntryNumber = caseRow.latest_docket_number || null;
    let latestFiledAt = valueOf(docket.date_last_filing, caseRow.latest_docket_filed_at);

    for (const entry of entries) {
      latestEntryNumber = higherOrderValue(
        latestEntryNumber,
        valueOf(entry.document_number, entry.documentNumber, entry.entry_number, entry.entryNumber)
      );
      latestFiledAt = laterIso(
        latestFiledAt,
        valueOf(entry.date_filed, entry.entry_date_filed, entry.filed_at)
      );
    }

    const mergedRaw = {
      ...(caseRow.raw || {}),
      ...docket,
      courtlistener: {
        ...(caseRow.raw?.courtlistener || {}),
        ...docket,
        docketId,
        docketEntryCount: entries.length,
        docketEntriesAccess: this.courtListener.hasDocketEntriesAccess(),
        syncedAt: timestamp
      }
    };

    for (const entry of entries) {
      this.store.upsertDocketEntry({
        case_id: caseRow.id,
        source_entry_key: `courtlistener:docket-entry:${entry.id ?? `${docketId}:${entry.entry_number ?? entry.document_number ?? Date.now()}`}`,
        primary_source: "courtlistener",
        source_entry_id: valueOf(entry.id),
        document_type: valueOf(entry.document_type, entry.documentType),
        entry_number: valueOf(entry.entry_number, entry.entryNumber),
        document_number: valueOf(entry.document_number, entry.documentNumber),
        filed_at: valueOf(entry.date_filed, entry.entry_date_filed, entry.filed_at),
        description: valueOf(entry.description, entry.short_description, entry.docket_text, entry.text),
        absolute_url: this.courtListener.absoluteUrl(valueOf(entry.absolute_url)),
        is_available: entry.is_available === undefined ? null : entry.is_available ? 1 : 0,
        page_count: valueOf(entry.page_count),
        pacer_doc_id: valueOf(entry.pacer_doc_id),
        raw: entry,
        last_synced_at: timestamp
      });
    }

    const coverage = this.store.getEntryCoverageForCaseIds([caseRow.id]).get(caseRow.id) || null;
    const stabilizedLatestNumber = higherOrderValue(
      latestEntryNumber,
      coverage?.lastNumber ? String(coverage.lastNumber) : null
    );
    const stabilizedDocketCount = Math.max(entries.length, coverage?.totalEntries || 0);

    this.store.upsertCase({
      source_case_key: caseRow.source_case_key,
      primary_source: "courtlistener",
      source_case_id: caseRow.source_case_id,
      courtlistener_docket_id: docketId,
      pacer_case_id: valueOf(docket.pacer_case_id, caseRow.pacer_case_id),
      court_id: valueOf(docket.court_id, caseRow.court_id),
      court_name: valueOf(docket.court_name, caseRow.court_name, docket.court),
      case_name: valueOf(docket.case_name, docket.caseName, caseRow.case_name),
      docket_number: valueOf(docket.docket_number, docket.docketNumber, caseRow.docket_number),
      date_filed: valueOf(docket.date_filed, docket.dateFiled, caseRow.date_filed),
      date_terminated: valueOf(docket.date_terminated, docket.dateTerminated, caseRow.date_terminated),
      cause: valueOf(docket.cause, caseRow.cause),
      nature_of_suit: valueOf(docket.nature_of_suit, docket.natureOfSuit, caseRow.nature_of_suit),
      status: valueOf(docket.date_terminated, docket.dateTerminated) ? "terminated" : caseRow.status,
      tags_marker: caseRow.tags_marker,
      docket_url: valueOf(this.courtListener.absoluteUrl(docket.absolute_url), caseRow.docket_url),
      source_urls: [...(caseRow.source_urls || []), this.courtListener.absoluteUrl(docket.absolute_url)].filter(Boolean),
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: caseRow.recent_activity_summary,
      latest_docket_filed_at: latestFiledAt,
      latest_docket_number: stabilizedLatestNumber,
      docket_count: stabilizedDocketCount,
      last_seen_at: timestamp,
      last_synced_at: timestamp,
      last_docket_sync_at: timestamp,
      raw: mergedRaw
    });

    if (this.courtListener.hasDocketAlertAccess()) {
      const refreshedCase = this.store.getCase(caseRow.id) || caseRow;
      await this.ensureCourtListenerDocketAlert(refreshedCase, { force: false }).catch(() => null);
    }

    this.store.refreshCaseDocketSummary(caseRow.id);
    this.store.touchCaseDocketSync(caseRow.id);

    return {
      enriched: true,
      reason: metadataOnlyMode ? "metadata-only" : entries.length ? "ok" : "empty"
    };
  }
}
