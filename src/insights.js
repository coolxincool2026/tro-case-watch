import { getPriorityFeedRaw } from "./priority-feed.js";

const IP_TERMS = [
  "trademark",
  "copyright",
  "patent",
  "lanham act",
  "counterfeit",
  "infringement",
  "design patent"
];

const PLATFORM_TERMS = [
  "amazon",
  "aliexpress",
  "dhgate",
  "ebay",
  "marketplace",
  "seller",
  "seller ids",
  "e-commerce",
  "ecommerce",
  "shop",
  "store",
  "delivery",
  "alibaba",
  "walmart"
];

const NEGATIVE_TERMS = [
  "habeas",
  "immigration",
  "detention",
  "bankruptcy",
  "prison",
  "warden",
  "social security",
  "custody",
  "deport",
  "medicaid"
];

const SCHEDULE_A_PATTERNS = [
  "identified on schedule a",
  "schedule a defendants",
  "schedule a",
  "partnerships and unincorporated associations",
  "unincorporated associations identified on schedule a"
];

const TRO_PATTERNS = [
  "temporary restraining order",
  "motion for temporary restraining order",
  "order granting ex parte application for entry of temporary restraining order",
  "order on motion for temporary restraining order"
];

const BANKRUPTCY_TERMS = [
  "schedule a/b",
  "summary of assets and liabilities",
  "declaration about individual debtors schedules",
  "chapter 7",
  "chapter 11",
  "chapter 13",
  "debtor",
  "trustee"
];

const PATENT_TERMS = ["patent", "design patent"];
const TRADEMARK_TERMS = ["trademark", "lanham act", "counterfeit", "trade dress"];
const COPYRIGHT_TERMS = ["copyright"];

const PATENT_CAUSE_PATTERNS = [/\b35:271\b/i, /\b35:281\b/i, /\b35:283\b/i];
const TRADEMARK_CAUSE_PATTERNS = [/\b15:1114\b/i, /\b15:1116\b/i, /\b15:1117\b/i, /\b15:1125\b/i];
const COPYRIGHT_CAUSE_PATTERNS = [/\b17:501\b/i];

const COMPANY_SUFFIX = /\b(llc|incorporated|inc|corp|corporation|company|co|ltd|limited|gmbh|pllc|llp|lp|p\.a\.|s\.a\.|sarl|b\.v\.)\b\.?,?/gi;

function normalize(value) {
  return String(value || "").trim();
}

export function normalizeText(value) {
  return normalize(value).toLowerCase();
}

function uniqueStrings(values) {
  return [...new Set(values.filter(Boolean).map((value) => normalize(value)))];
}

function normalizedKey(value) {
  return normalizeText(value).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
}

function pullPlaintiffNames(caseLike) {
  const title = normalize(caseLike.case_name);
  const titlePieces = title.split(/\s(?:v\.|vs\.)\s/i);
  const plaintiffFromCaption = titlePieces[0] || "";
  const defendantFromCaption = titlePieces.slice(1).join(" v. ").trim();
  const parties = Array.isArray(caseLike.raw?.party) ? uniqueStrings(caseLike.raw.party) : [];

  if (defendantFromCaption && parties.length) {
    const defendantKey = normalizedKey(defendantFromCaption);
    const defendantIndex = parties.findIndex((party) => normalizedKey(party) === defendantKey);
    if (defendantIndex > 0) {
      return uniqueStrings(parties.slice(0, defendantIndex));
    }
  }

  if (Array.isArray(caseLike.plaintiffs) && caseLike.plaintiffs.length) {
    return uniqueStrings(caseLike.plaintiffs);
  }

  if (title) {
    if (plaintiffFromCaption) {
      return [plaintiffFromCaption];
    }
  }

  return parties[0] ? [parties[0]] : [];
}

function pullPlaintiffName(caseLike) {
  return pullPlaintiffNames(caseLike)[0] || "";
}

function pullLawFirms(caseLike) {
  const priorityFeedFirm = normalize(getPriorityFeedRaw(caseLike.raw)?.lawFirm);
  if (priorityFeedFirm) {
    return [priorityFeedFirm];
  }

  const rawFirms = caseLike.raw?.firm;
  if (Array.isArray(rawFirms) && rawFirms.length) {
    return uniqueStrings(rawFirms);
  }

  return [];
}

function cleanBrandName(value) {
  return normalize(value)
    .replace(COMPANY_SUFFIX, " ")
    .replace(/\s*,\s*/g, " ")
    .replace(/[.]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function collectTexts(caseLike) {
  const rawDocs = Array.isArray(caseLike.raw?.recap_documents) ? caseLike.raw.recap_documents : [];
  const entries = Array.isArray(caseLike.entries) ? caseLike.entries : [];

  return [
    caseLike.case_name,
    caseLike.cause,
    caseLike.nature_of_suit,
    caseLike.recent_activity_summary,
    ...(caseLike.plaintiffs || []),
    ...(caseLike.defendants || []),
    ...rawDocs.flatMap((doc) => [doc.short_description, doc.description, doc.snippet]),
    ...entries.map((entry) => entry.description)
  ]
    .filter(Boolean)
    .join(" | ");
}

function countMatches(text, patterns) {
  return patterns.reduce((count, pattern) => count + (text.includes(pattern) ? 1 : 0), 0);
}

function includesOne(text, patterns) {
  return patterns.some((pattern) => text.includes(pattern));
}

function countRegexMatches(text, patterns) {
  return patterns.reduce((count, pattern) => count + (pattern.test(text) ? 1 : 0), 0);
}

function deriveIpCaseType(caseLike, text) {
  const cause = normalizeText(caseLike.cause || "");
  const natureOfSuit = normalizeText(caseLike.nature_of_suit || "");
  const combined = [text, cause, natureOfSuit].filter(Boolean).join(" | ");
  const scores = {
    Patent: 0,
    Trademark: 0,
    Copyright: 0
  };

  if (/\b(830|835)\b/.test(natureOfSuit)) scores.Patent += 4;
  if (/\b840\b/.test(natureOfSuit)) scores.Trademark += 4;
  if (/\b820\b/.test(natureOfSuit)) scores.Copyright += 4;

  scores.Patent += countRegexMatches(cause, PATENT_CAUSE_PATTERNS) * 3;
  scores.Trademark += countRegexMatches(cause, TRADEMARK_CAUSE_PATTERNS) * 3;
  scores.Copyright += countRegexMatches(cause, COPYRIGHT_CAUSE_PATTERNS) * 3;

  scores.Patent += countMatches(combined, PATENT_TERMS);
  scores.Trademark += countMatches(combined, TRADEMARK_TERMS);
  scores.Copyright += countMatches(combined, COPYRIGHT_TERMS);

  const ranked = Object.entries(scores).sort((left, right) => right[1] - left[1]);
  if (!ranked[0] || ranked[0][1] <= 0) {
    return null;
  }

  if (ranked[1] && ranked[1][1] === ranked[0][1]) {
    return null;
  }

  return ranked[0][0];
}

function sanitizeDefendants(caseLike, plaintiffNames = []) {
  const plaintiffKeys = new Set(
    (Array.isArray(plaintiffNames) ? plaintiffNames : [plaintiffNames])
      .map((value) => normalizedKey(value))
      .filter(Boolean)
  );
  const unique = new Map();

  for (const rawValue of caseLike.defendants || []) {
    const value = normalize(rawValue);
    if (!value) {
      continue;
    }

    const key = normalizedKey(value);
    if (!key || plaintiffKeys.has(key)) {
      continue;
    }

    if (!unique.has(key)) {
      unique.set(key, value);
    }
  }

  const title = normalize(caseLike.case_name);
  const pieces = title.split(/\s(?:v\.|vs\.)\s/i);
  if (pieces.length >= 2) {
    const captionDefendant = normalize(pieces.slice(1).join(" v. "));
    const captionKey = normalizedKey(captionDefendant);
    if (captionKey && !plaintiffKeys.has(captionKey) && !unique.has(captionKey)) {
      unique.set(captionKey, captionDefendant);
    }
  }

  return [...unique.values()];
}

function deriveStatus(caseLike, text) {
  const dateTerminated = normalize(caseLike.date_terminated);

  if (
    dateTerminated ||
    text.includes("voluntary dismissal") ||
    text.includes("case terminated") ||
    text.includes("dismisses the instant action")
  ) {
    return {
      key: "closed",
      label: "已出现撤案/结案",
      tone: "muted"
    };
  }

  if (text.includes("preliminary injunction") && (text.includes("grant") || text.includes("grants"))) {
    return {
      key: "pi",
      label: "已到 PI / 初步禁令",
      tone: "danger"
    };
  }

  if (
    text.includes("temporary restraining order") &&
    (text.includes("grant") || text.includes("order") || text.includes("entered"))
  ) {
    return {
      key: "tro_granted",
      label: "TRO 已下发",
      tone: "danger"
    };
  }

  if (text.includes("motion for temporary restraining order")) {
    return {
      key: "tro_pending",
      label: "TRO 申请中",
      tone: "warn"
    };
  }

  if (text.includes("notice of settlement")) {
    return {
      key: "settlement",
      label: "有和解迹象",
      tone: "good"
    };
  }

  if (text.includes("service") || text.includes("summons")) {
    return {
      key: "service",
      label: "送达推进中",
      tone: "warn"
    };
  }

  return {
    key: "watch",
    label: "持续观察",
    tone: "neutral"
  };
}

function buildHighlights(text) {
  const items = [];

  if (text.includes("order granting ex parte application for entry of temporary restraining order")) {
    items.push("已看到 TRO 签发文书");
  } else if (text.includes("motion for temporary restraining order")) {
    items.push("已出现 TRO 申请");
  }

  if (text.includes("extend temporary restraining order")) {
    items.push("TRO 延长期限值得关注");
  }

  if (text.includes("preliminary injunction") && (text.includes("grant") || text.includes("grants"))) {
    items.push("已进入 PI / 初步禁令");
  } else if (text.includes("preliminary injunction")) {
    items.push("已出现 PI 相关节点");
  }

  if (text.includes("certificate of service") || text.includes("service")) {
    items.push("送达程序在推进");
  }

  if (text.includes("settlement")) {
    items.push("已有和解迹象");
  }

  if (text.includes("dismiss")) {
    items.push("已有撤案/驳回迹象");
  }

  return [...new Set(items)].slice(0, 4);
}

function buildActionItems(statusKey) {
  if (statusKey === "tro_granted") {
    return ["先核对 TRO 是否已签发", "继续盯 PI 听证和答辩期限", "确认冻结范围和被告名单"];
  }

  if (statusKey === "pi") {
    return ["重点查看 PI 是否覆盖全部被告", "关注后续 settlement / dismissal", "核对是否仍有账户冻结"];
  }

  if (statusKey === "settlement") {
    return ["继续盯是否有 dismissal", "核对你店铺是否已被单独移出", "保留和解或撤诉证据"];
  }

  if (statusKey === "closed") {
    return ["核对法院终局文书", "继续确认平台是否已解除冻结", "留存撤案或结案材料"];
  }

  if (statusKey === "service") {
    return ["盯住送达完成时间", "关注 TRO / PI 是否紧接着推进", "确认是否已出现被告名单"];
  }

  return ["重点盯 TRO 文书", "重点盯 PI / Preliminary Injunction", "重点盯 settlement / dismissal"];
}

function buildNarrative({ status, sellerRelevant, isScheduleACase, isTroCase, defendantCount }) {
  if (!sellerRelevant) {
    return "当前以公开 docket 为准继续观察。";
  }

  if (status.key === "tro_granted") {
    return "这是典型卖家冻结案阶段，法院已经出现 TRO 签发信号，平台冻结和送达通常是当前核心风险点。";
  }

  if (status.key === "pi") {
    return "案件已经推进到 PI 阶段，约束通常比最初 TRO 更稳定，适合重点跟进是否持续冻结。";
  }

  if (status.key === "settlement") {
    return "案件已出现和解信号，可以继续观察是否有部分被告陆续退出案件。";
  }

  if (status.key === "closed") {
    return "案件已出现结案或撤案信号，重点应转向平台端是否同步解冻。";
  }

  if (isScheduleACase) {
    return `这是典型 Schedule A 卖家案件，当前可见被告线索 ${defendantCount} 个，重点继续盯 TRO / PI / settlement。`;
  }

  if (isTroCase) {
    return "这是卖家相关 TRO 案件，建议优先跟踪 TRO 是否签发、是否延长、以及是否进入 PI。";
  }

  return "当前仍处于持续观察阶段，建议优先盯 TRO、PI、送达、和解和撤案。";
}

export function deriveCaseInsights(caseLike) {
  const text = normalizeText(collectTexts(caseLike));
  const plaintiffNames = pullPlaintiffNames(caseLike);
  const plaintiffName = plaintiffNames[0] || "";
  const defendants = sanitizeDefendants(
    caseLike,
    plaintiffNames.length ? plaintiffNames : [plaintiffName]
  );
  const lawFirms = pullLawFirms(caseLike);
  const tags = Array.isArray(caseLike.tags) ? caseLike.tags : [];
  const ipHits = countMatches(text, IP_TERMS);
  const platformHits = countMatches(text, PLATFORM_TERMS);
  const negativeHits = countMatches(text, NEGATIVE_TERMS);
  const hasTroTerm = includesOne(text, TRO_PATTERNS);
  const hasScheduleATerm = includesOne(text, SCHEDULE_A_PATTERNS);
  const hasBankruptcyTerm =
    includesOne(text, BANKRUPTCY_TERMS) || normalizeText(caseLike.court_name).includes("bankruptcy");
  const defendantCount = defendants.length;

  const isScheduleACase =
    !hasBankruptcyTerm &&
    (tags.includes("schedule_a") ||
      text.includes("identified on schedule a") ||
      text.includes("schedule a defendants") ||
      text.includes("partnerships and unincorporated associations") ||
      (hasScheduleATerm && (ipHits >= 1 || platformHits >= 1 || hasTroTerm)));
  const isTroCase = tags.includes("tro") || hasTroTerm;

  let sellerRelevanceScore = 0;

  if (isScheduleACase) sellerRelevanceScore += 8;
  if (isTroCase) sellerRelevanceScore += 2;
  sellerRelevanceScore += ipHits >= 1 ? 3 : 0;
  sellerRelevanceScore += platformHits >= 2 ? 4 : platformHits >= 1 ? 2 : 0;
  sellerRelevanceScore += defendantCount >= 5 ? 2 : 0;
  sellerRelevanceScore -= negativeHits >= 1 && ipHits === 0 ? 6 : 0;
  sellerRelevanceScore -= hasBankruptcyTerm ? 8 : 0;

  const sellerRelevant =
    !hasBankruptcyTerm &&
    (isScheduleACase ||
      ((ipHits >= 1 || isTroCase || text.includes("temporary restraining order")) &&
        (platformHits >= 1 || defendantCount >= 5 || text.includes("seller ids") || text.includes("marketplace"))));
  const status = deriveStatus(caseLike, text);
  const brandName = cleanBrandName(getPriorityFeedRaw(caseLike.raw)?.brand || plaintiffName);
  const highlights = buildHighlights(text);
  const actionItems = buildActionItems(status.key);
  const narrative = buildNarrative({
    status,
    sellerRelevant,
    isScheduleACase,
    isTroCase,
    defendantCount
  });
  const ipCaseTypeLabel = deriveIpCaseType(caseLike, text);

  const badges = [];
  if (isScheduleACase) badges.push("Schedule A");
  if (isTroCase) badges.push("TRO");
  if (sellerRelevant) badges.push("跨境卖家相关");
  if (lawFirms[0]) badges.push(`原告律所 ${lawFirms[0]}`);

  return {
    plaintiff_name: plaintiffName || null,
    brand_name: brandName || null,
    lead_law_firm: lawFirms[0] || null,
    law_firms: lawFirms,
    defendant_count: defendantCount,
    defendant_preview: defendants.slice(0, 6),
    seller_relevance_score: sellerRelevanceScore,
    is_schedule_a_case: isScheduleACase,
    is_tro_case: isTroCase,
    is_seller_case: sellerRelevant,
    is_bankruptcy_case: hasBankruptcyTerm,
    ip_case_type_label: ipCaseTypeLabel,
    status,
    highlights,
    action_items: actionItems,
    narrative,
    badges
  };
}

export function docketLooksLike(value) {
  return /\b\d{2}-cv-\d{3,6}\b/i.test(String(value || "")) || /\b\d+:\d{2}-cv-\d{3,6}\b/i.test(String(value || ""));
}

export function normalizeDocket(value) {
  return normalizeText(value).replace(/^[a-z]{1,4}[-:]/i, "").replace(/^\d+:/, "");
}

export function normalizeDocketIdentity(value) {
  return normalizeText(value)
    .replace(/^[a-z]{1,6}[-:](?=\d+:)/i, "")
    .replace(/^(\d+):20(\d{2})-/i, "$1:$2-")
    .replace(/^20(\d{2})-/i, "$1-")
    .replace(/\s+/g, "");
}
