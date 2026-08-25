function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function stripTags(value) {
  return String(value || "").replace(/<[^>]+>/g, " ");
}

function decodeHtml(value) {
  return String(value || "")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCharCode(Number.parseInt(code, 16)))
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");
}

function cleanText(value) {
  return decodeHtml(stripTags(value)).replace(/\s+/g, " ").trim();
}

function normalizeLookupText(value) {
  return cleanText(value).toLowerCase().replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
}

function absoluteUrl(value, baseUrl) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }

  try {
    return new URL(raw, baseUrl).toString();
  } catch {
    return null;
  }
}

function normalizePdfTitle(value) {
  return decodeURIComponent(String(value || ""))
    .replace(/\+/g, " ")
    .replace(/[_-]+/g, " ")
    .replace(/\.pdf$/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function parseUsDate(value) {
  const match = String(value || "").match(/\b(\d{1,2})\/(\d{1,2})\/(\d{4})\b/);
  if (!match) {
    return null;
  }

  return `${match[3]}-${match[1].padStart(2, "0")}-${match[2].padStart(2, "0")}`;
}

function normalizePageUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }

  try {
    const url = new URL(raw);
    url.hash = "";
    if (url.hostname.includes("dropbox.com")) {
      url.search = "";
    }
    return url.toString();
  } catch {
    return raw;
  }
}

function parseDocketNumber(value) {
  const match = String(value || "").match(/\b(?:(\d+):)?(?:(?:20)?(\d{2}))-cv-(\d{3,6})\b/i);
  if (!match) {
    return "";
  }

  const division = match[1] ? `${match[1]}:` : "";
  return `${division}${match[2]}-cv-${match[3]}`;
}

function normalizeLawFirmCourtLookup(value) {
  const raw = cleanText(value);
  if (!raw) {
    return "";
  }

  const noWhitespaceKey = raw.replace(/\s+/g, "");
  const mappedChinese = TRO61_COURT_NAME_TO_META[noWhitespaceKey];
  if (mappedChinese?.courtName) {
    return normalizeLookupText(mappedChinese.courtName);
  }

  const normalized = normalizeLookupText(raw)
    .replace(/^district court\s+/i, "")
    .trim();

  const directionalAbbrev = normalized.match(/\b([nsewcm]) d ([a-z]+(?: [a-z]+)*)\b/i);
  if (directionalAbbrev) {
    const direction = DISTRICT_DIRECTION_MAP[String(directionalAbbrev[1] || "").toUpperCase()];
    const stateName = directionalAbbrev[2];
    if (direction && stateName) {
      return normalizeLookupText(`${direction} District of ${stateName}`);
    }
  }

  const courtCodeMatch = normalized.match(/\b([a-z]{4,5})\b/i);
  if (courtCodeMatch) {
    const courtName = courtIdToName(courtCodeMatch[1]);
    if (courtName) {
      return normalizeLookupText(courtName);
    }
  }

  return normalized;
}

function lawFirmMatchesCourtName(item = {}, courtName = "") {
  const left = normalizeLawFirmCourtLookup(item?.courtName || "");
  const right = normalizeLawFirmCourtLookup(courtName);
  if (!right) {
    return true;
  }
  if (!left) {
    return false;
  }

  return left === right || left.includes(right) || right.includes(left);
}

function cleanCaseName(value) {
  return cleanText(
    String(value || "").replace(/\s*(?:-|–|—)?\s*Case No\.?:?\s*(?:\d+:)?(?:20)?\d{2}-cv-\d{3,6}.*$/i, "")
  );
}

const DISTRICT_DIRECTION_MAP = {
  N: "Northern",
  S: "Southern",
  E: "Eastern",
  W: "Western",
  C: "Central",
  M: "Middle"
};

const STATE_CODE_TO_NAME = {
  CA: "California",
  FL: "Florida",
  GA: "Georgia",
  IL: "Illinois",
  NY: "New York",
  PA: "Pennsylvania",
  TN: "Tennessee",
  TX: "Texas",
  WA: "Washington"
};

const COURT_CODE_TO_ID = {
  CACD: "cacd",
  CAND: "cand",
  CASD: "casd",
  FLSD: "flsd",
  GAND: "gand",
  GASD: "gasd",
  ILND: "ilnd",
  NYSD: "nysd",
  NYED: "nyed",
  PAED: "paed",
  MDPA: "mdpa",
  PAWD: "pawd",
  TNED: "tned",
  TNMD: "tnmd",
  TNWD: "tnwd",
  TXSD: "txsd",
  WAED: "waed",
  WAWD: "wawd"
};

const COURT_ID_TO_NAME = {
  cacd: "Central District of California",
  cand: "Northern District of California",
  casd: "Southern District of California",
  flsd: "Southern District of Florida",
  gand: "Northern District of Georgia",
  gasd: "Southern District of Georgia",
  ilnd: "Northern District of Illinois",
  mdpa: "Middle District of Pennsylvania",
  nyed: "Eastern District of New York",
  nysd: "Southern District of New York",
  paed: "Eastern District of Pennsylvania",
  pawd: "Western District of Pennsylvania",
  tned: "Eastern District of Tennessee",
  tnmd: "Middle District of Tennessee",
  tnwd: "Western District of Tennessee",
  txsd: "Southern District of Texas",
  waed: "Eastern District of Washington",
  wawd: "Western District of Washington"
};

const TRO61_COURT_NAME_TO_META = {
  伊利诺伊州北区法院: {
    courtId: "ilnd",
    courtName: "Northern District of Illinois"
  },
  佛罗里达州南区法院: {
    courtId: "flsd",
    courtName: "Southern District of Florida"
  },
  纽约州南区法院: {
    courtId: "nysd",
    courtName: "Southern District of New York"
  },
  纽约州东区法院: {
    courtId: "nyed",
    courtName: "Eastern District of New York"
  },
  加利福尼亚州北区法院: {
    courtId: "cand",
    courtName: "Northern District of California"
  },
  加利福尼亚州中区法院: {
    courtId: "cacd",
    courtName: "Central District of California"
  },
  加利福尼亚州南区法院: {
    courtId: "casd",
    courtName: "Southern District of California"
  },
  佐治亚州南区法院: {
    courtId: "gasd",
    courtName: "Southern District of Georgia"
  },
  佐治亚州北区法院: {
    courtId: "gand",
    courtName: "Northern District of Georgia"
  },
  宾夕法尼亚州东区法院: {
    courtId: "paed",
    courtName: "Eastern District of Pennsylvania"
  },
  宾夕法尼亚州西区法院: {
    courtId: "pawd",
    courtName: "Western District of Pennsylvania"
  },
  华盛顿州西区法院: {
    courtId: "wawd",
    courtName: "Western District of Washington"
  },
  华盛顿州东区法院: {
    courtId: "waed",
    courtName: "Eastern District of Washington"
  },
  得克萨斯州南区法院: {
    courtId: "txsd",
    courtName: "Southern District of Texas"
  },
  田纳西州东区法院: {
    courtId: "tned",
    courtName: "Eastern District of Tennessee"
  },
  田纳西州中区法院: {
    courtId: "tnmd",
    courtName: "Middle District of Tennessee"
  },
  田纳西州西区法院: {
    courtId: "tnwd",
    courtName: "Western District of Tennessee"
  },
  宾夕法尼亚州中区法院: {
    courtId: "mdpa",
    courtName: "Middle District of Pennsylvania"
  }
};

function courtCodeToName(value) {
  const code = String(value || "").trim().toUpperCase().replace(/[^A-Z]/g, "");
  if (!code) {
    return "";
  }

  const directionalMatch = code.match(/^([NSEWCM])D([A-Z]{2})$/);
  if (directionalMatch) {
    const direction = DISTRICT_DIRECTION_MAP[directionalMatch[1]];
    const stateName = STATE_CODE_TO_NAME[directionalMatch[2]];
    if (direction && stateName) {
      return `${direction} District of ${stateName}`;
    }
  }

  const districtMatch = code.match(/^D([A-Z]{2})$/);
  if (districtMatch) {
    const stateName = STATE_CODE_TO_NAME[districtMatch[1]];
    if (stateName) {
      return `District of ${stateName}`;
    }
  }

  return code;
}

function courtCodeToId(value) {
  const code = String(value || "").trim().toUpperCase().replace(/[^A-Z]/g, "");
  return COURT_CODE_TO_ID[code] || null;
}

function courtIdToName(value) {
  const key = String(value || "").trim().toLowerCase();
  return COURT_ID_TO_NAME[key] || "";
}

const DEFAULT_LAW_FIRM_SOURCES = [
  {
    id: "sriplaw",
    label: "SRIPLAW",
    lawFirm: "SRipLaw",
    baseUrl: "https://sriplaw.com",
    strategy: "sriplaw-notice",
    discoveryUrl: "https://sriplaw.com/notice/"
  },
  {
    id: "gbc",
    label: "GBC",
    lawFirm: "Greer, Burns & Crain, Ltd.",
    baseUrl: "https://gbc.law",
    strategy: "gbc-pages",
    discoveryUrl:
      "https://gbc.law/wp-json/wp/v2/pages?per_page=100&_fields=id,link,slug,title,modified,content.rendered"
  },
  {
    id: "61tro",
    label: "61TRO",
    lawFirm: "61TRO案件查询网",
    baseUrl: "https://61tro.com",
    strategy: "61tro-search",
    discoveryUrl: "https://61tro.com/"
  },
  {
    id: "whitewood",
    label: "Whitewood",
    lawFirm: "Whitewood Law",
    baseUrl: "https://whitewoodlaw.com",
    strategy: "sitemap-probe",
    discoveryUrl: "https://whitewoodlaw.com/page-sitemap.xml"
  },
  {
    id: "jiangip",
    label: "Keith / JiangIP",
    lawFirm: "Keith A. Vogt PLLC",
    baseUrl: "https://jiangip.com",
    strategy: "page-probe",
    discoveryUrl: "https://jiangip.com/"
  }
];

function extractSitemapLocs(xml) {
  return [...String(xml || "").matchAll(/<loc>(.*?)<\/loc>/gi)]
    .map((match) => cleanText(match[1]))
    .filter(Boolean);
}

function extractSameDomainLinks(html, baseUrl) {
  const links = new Set();
  for (const match of String(html || "").matchAll(/href=['"]([^'"]+)['"]/gi)) {
    const normalized = absoluteUrl(match[1], baseUrl);
    if (!normalized) {
      continue;
    }

    try {
      const url = new URL(normalized);
      if (url.origin === new URL(baseUrl).origin) {
        links.add(normalizePageUrl(normalized));
      }
    } catch {
      continue;
    }
  }

  return [...links];
}

function looksLikeStructuredCaseLink(url) {
  return /\/case\//i.test(url) || /\/case-no-/i.test(url) || /\/caseno-/i.test(url);
}

function normalizeDocketLookupKey(value) {
  const docket = parseDocketNumber(value);
  return String(docket || value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function normalizeDocketLookupCoreKey(value) {
  const docket = parseDocketNumber(value);
  return String(docket || value || "")
    .toLowerCase()
    .replace(/^[a-z]{1,6}[-:]/, "")
    .replace(/^\d+:/, "")
    .replace(/[^a-z0-9]+/g, "");
}

function docketNumbersMatchForLookup(left, right) {
  if (normalizeDocketLookupCoreKey(left) !== normalizeDocketLookupCoreKey(right)) {
    return false;
  }
  const leftOffice = String(parseDocketNumber(left) || left || "").match(/\b(\d+):(?:20)?\d{2}-cv-/i)?.[1];
  const rightOffice = String(parseDocketNumber(right) || right || "").match(/\b(\d+):(?:20)?\d{2}-cv-/i)?.[1];
  return !leftOffice || !rightOffice || leftOffice === rightOffice;
}

function clean61troValue(value) {
  return cleanText(value)
    .replace(/\s*翻译\s*$/u, "")
    .replace(/\s+扫码.*$/u, "")
    .trim();
}

function normalize61troTagKey(value) {
  return normalizeLookupText(value)
    .replace(/\b(?:corp(?:oration)?|co(?:mpany)?|inc(?:orporated)?|llc|ltd|limited)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extract61troLookupLeader(value) {
  const text = cleanText(value);
  if (!text) {
    return "";
  }

  const split = text.split(/\s+(?:v\.?|vs\.?|versus)\s+/i);
  return cleanText(split[0] || text);
}

function build61troLookupHints({ caseName = "", plaintiffs = [], firms = [] } = {}) {
  const hints = [];
  const seen = new Set();

  const pushHint = (value) => {
    const cleaned = cleanText(value);
    const key = normalize61troTagKey(cleaned);
    if (!cleaned || !key || seen.has(key)) {
      return;
    }

    if (/^the partnerships\b/i.test(cleaned) || /^the entities\b/i.test(cleaned)) {
      return;
    }

    seen.add(key);
    hints.push(cleaned);
  };

  pushHint(extract61troLookupLeader(caseName));

  for (const plaintiff of asArray(plaintiffs)) {
    if (typeof plaintiff === "string") {
      pushHint(plaintiff);
      continue;
    }

    pushHint(plaintiff?.name || plaintiff?.party_name || plaintiff?.display_name || "");
  }

  for (const firm of asArray(firms)) {
    pushHint(firm);
  }

  return hints;
}

function extract61troTextValue(html, label) {
  const match = String(html || "").match(new RegExp(`${label}[：:]([\\s\\S]*?)<\\/span>`, "i"));
  return cleanText(match?.[1] || "");
}

function extract61troDetailLinks(html, baseUrl) {
  const links = [];
  const seen = new Set();

  for (const match of String(html || "").matchAll(/<a[^>]+href=['"]([^'"]*(?:\/detail\/\d+\.html|\/view\/id\/[^'"]+\.html))['"][^>]*>([\s\S]*?)<\/a>/gi)) {
    const href = absoluteUrl(match[1], baseUrl);
    if (!href || seen.has(href)) {
      continue;
    }

    seen.add(href);
    links.push({
      href,
      title: cleanText(match[2])
    });
  }

  return links;
}

function extract61troRecentLinks(html, baseUrl) {
  return extract61troDetailLinks(html, baseUrl).map((item) => item.href);
}

function build61troSearchTerms(docketNumber) {
  const match = String(docketNumber || "").match(/\b(?:(\d+):)?(?:(20)?(\d{2}))-cv-(\d{3,6})\b/i);
  if (!match) {
    return [String(docketNumber || "").trim()].filter(Boolean);
  }

  const division = match[1] ? `${match[1]}:` : "";
  const shortYear = match[3];
  const fullYear = `${match[2] ? "20" : "20"}${shortYear}`;
  const number = match[4];

  return [...new Set([
    `${fullYear}-cv-${number}`,
    `${shortYear}-cv-${number}`,
    division ? `${division}${fullYear}-cv-${number}` : "",
    division ? `${division}${shortYear}-cv-${number}` : ""
  ].filter(Boolean))];
}

function resolve61troCourtId(value) {
  const raw = cleanText(value);
  if (!raw) {
    return "";
  }

  const noWhitespaceKey = raw.replace(/\s+/g, "");
  const mappedChinese = TRO61_COURT_NAME_TO_META[noWhitespaceKey];
  if (mappedChinese?.courtId) {
    return mappedChinese.courtId;
  }

  const normalized = normalizeLawFirmCourtLookup(raw);
  if (!normalized) {
    return "";
  }

  for (const [courtId, courtName] of Object.entries(COURT_ID_TO_NAME)) {
    const normalizedName = normalizeLawFirmCourtLookup(courtName);
    if (
      normalizedName &&
      (normalizedName === normalized || normalizedName.includes(normalized) || normalized.includes(normalizedName))
    ) {
      return courtId;
    }
  }

  const codeMatch = raw.match(/\b([a-z]{4,5})\b/i);
  if (codeMatch) {
    const candidate = String(codeMatch[1] || "").toLowerCase();
    if (COURT_ID_TO_NAME[candidate]) {
      return candidate;
    }
  }

  return "";
}

function build61troDirectViewCandidateUrls(source, docketNumber, { courtName = "" } = {}) {
  const courtId = resolve61troCourtId(courtName);
  const match = String(docketNumber || "").match(/\b(?:(\d+):)?(?:(20)?(\d{2}))-cv-(\d{3,6})\b/i);
  if (!courtId || !match) {
    return [];
  }

  const division = match[1] ? `${match[1]}:` : "";
  const shortYear = match[3];
  const fullYear = `20${shortYear}`;
  const number = match[4];
  const viewIds = [...new Set([
    `${courtId}-${division}${fullYear}-cv-${number}`,
    division ? `${courtId}-${division}${shortYear}-cv-${number}` : "",
    `${courtId}-${fullYear}-cv-${number}`,
    `${courtId}-${shortYear}-cv-${number}`
  ].filter(Boolean))];

  return viewIds.map((value) => `${source.baseUrl}/view/id/${encodeURIComponent(value)}.html`);
}

function extract61troSearchResultLink(html, docketNumber, baseUrl) {
  const targetKey = normalizeDocketLookupCoreKey(docketNumber);
  const links = extract61troDetailLinks(html, baseUrl);

  const exact = links.find(
    (item) => normalizeDocketLookupCoreKey(item.title) === targetKey && /\/view\/id\//i.test(item.href)
  );
  if (exact) {
    return exact.href;
  }

  const compatible = links.find((item) => normalizeDocketLookupCoreKey(item.title) === targetKey);
  return compatible?.href || null;
}

function resolve61troCourtMeta(url, courtNameText) {
  const rawCourtName = cleanText(courtNameText);
  const courtNameKey = rawCourtName.replace(/\s+/g, "");
  const known = TRO61_COURT_NAME_TO_META[courtNameKey];
  if (known) {
    return known;
  }

  const urlMatch = String(url || "").match(/\/view\/id\/([a-z]{4,5})-/i);
  if (urlMatch) {
    const courtId = urlMatch[1].toLowerCase();
    return {
      courtId,
      courtName: courtIdToName(courtId) || rawCourtName || ""
    };
  }

  return {
    courtId: null,
    courtName: rawCourtName || ""
  };
}

function parse61troEntries(html) {
  const entries = [];
  let index = 0;

  for (const match of String(html || "").matchAll(/<div class="layui-timeline-item">([\s\S]*?)<\/div>\s*<\/div>/gi)) {
    const block = match[1];
    const filedAt = parseUsDate(String(block).match(/layui-timeline-title">([\s\S]*?)<\/h3>/i)?.[1] || "");
    const lines = [...String(block).matchAll(/<p[^>]*>([\s\S]*?)<\/p>/gi)]
      .map((item) => clean61troValue(item[1]))
      .filter(Boolean);
    const description = lines.length > 1
      ? `${lines[0]}\n${lines.slice(1).join("\n")}`
      : (lines[0] || "");
    if (!filedAt && !description) {
      continue;
    }

    index += 1;
    entries.push({
      sourceEntryId: `${filedAt || "unknown"}:${normalizeLookupText(description || String(index))}`,
      entryNumber: null,
      documentNumber: null,
      documentType: "Docket Entry",
      description: description || `61TRO timeline item ${index}`,
      filedAt,
      absoluteUrl: null
    });
  }

  return entries;
}

function parse61troCasePage(html, url, source) {
  const title = cleanText(String(html || "").match(/<title>([\s\S]*?)<\/title>/i)?.[1] || "");
  const docketNumber =
    parseDocketNumber(String(html || "").match(/<div class="post__title">[\s\S]*?<h2>([\s\S]*?)<\/h2>/i)?.[1] || "") ||
    parseDocketNumber(title) ||
    parseDocketNumber(url);
  if (!docketNumber) {
    return null;
  }

  const caseName =
    cleanText(String(html || "").match(/<div class="post__options">[\s\S]*?<h4>([\s\S]*?)<\/h4>/i)?.[1] || "") ||
    cleanCaseName(title);
  const rawCourtName = extract61troTextValue(html, "法院");
  const courtMeta = resolve61troCourtMeta(url, rawCourtName);
  const entries = parse61troEntries(html);
  const latestEntry = entries[0] || entries[entries.length - 1] || null;
  const earliestEntry = entries
    .slice()
    .filter((entry) => entry?.filedAt)
    .sort((left, right) => String(left.filedAt || "").localeCompare(String(right.filedAt || "")))[0] || null;
  const updatedAt = cleanText(String(html || "").match(/最近更新：([\d-]{10})/i)?.[1] || "");
  const brand = extract61troTextValue(html, "品牌");
  const state = extract61troTextValue(html, "州");
  const lawFirm = extract61troTextValue(html, "律所");

  return {
    sourceId: source.id,
    sourceLabel: source.label,
    lawFirm: source.lawFirm,
    sourceCaseId: url,
    caseUrl: normalizePageUrl(url),
    title,
    caseName: caseName || title,
    docketNumber,
    courtCode: "",
    courtId: courtMeta.courtId || null,
    courtName: courtMeta.courtName || rawCourtName || "",
    dateFiled: earliestEntry?.filedAt || null,
    summary: latestEntry?.description || `最近更新：${updatedAt || "未知"}`,
    latestDocketNumber: null,
    entries,
    syncedAt: new Date().toISOString(),
    rawMeta: {
      discoveryUrl: source.discoveryUrl,
      updatedAt: updatedAt || null,
      state: state || null,
      brand: brand || null,
      listedLawFirm: lawFirm || null,
      rowCount: entries.length
    }
  };
}

function extract61troPaginationLinks(html, pageUrl) {
  const current = absoluteUrl(pageUrl, pageUrl);
  if (!current) {
    return [];
  }

  let currentUrl;
  try {
    currentUrl = new URL(current);
  } catch {
    return [];
  }

  const pagination = [];
  const seen = new Set();
  for (const link of extractSameDomainLinks(html, currentUrl.origin)) {
    try {
      const url = new URL(link);
      if (url.pathname !== currentUrl.pathname || !url.searchParams.get("page")) {
        continue;
      }

      const normalized = normalizePageUrl(url.toString());
      if (!normalized || seen.has(normalized)) {
        continue;
      }

      seen.add(normalized);
      pagination.push(normalized);
    } catch {
      continue;
    }
  }

  return pagination.sort((left, right) => {
    const leftPage = Number.parseInt(new URL(left).searchParams.get("page") || "0", 10);
    const rightPage = Number.parseInt(new URL(right).searchParams.get("page") || "0", 10);
    return leftPage - rightPage;
  });
}

function build61troHintUrls(source, { caseName = "", plaintiffs = [], firms = [] } = {}, sitemapUrls = []) {
  const hints = build61troLookupHints({ caseName, plaintiffs, firms });
  if (!hints.length) {
    return [];
  }

  const normalizedSitemapUrls = [...new Set(asArray(sitemapUrls).map((item) => normalizePageUrl(item)).filter(Boolean))];
  const sitemapSet = new Set(normalizedSitemapUrls);
  const tagIndex = new Map();

  for (const url of normalizedSitemapUrls) {
    const match = String(url).match(/\/tag\/([^/?#]+)\.html/i);
    if (!match) {
      continue;
    }

    const decoded = decodeURIComponent(match[1]);
    const key = normalize61troTagKey(decoded);
    if (!key) {
      continue;
    }

    const bucket = tagIndex.get(key) || [];
    bucket.push(url);
    tagIndex.set(key, bucket);
  }

  const urls = [];
  const pushUrl = (value) => {
    const normalized = normalizePageUrl(value);
    if (!normalized || urls.includes(normalized)) {
      return;
    }
    urls.push(normalized);
  };

  for (const hint of hints) {
    const explicitTagUrl = normalizePageUrl(`${source.baseUrl}/tag/${encodeURIComponent(hint)}.html`);
    if (explicitTagUrl) {
      pushUrl(explicitTagUrl);
    }

    for (const candidate of tagIndex.get(normalize61troTagKey(hint)) || []) {
      pushUrl(candidate);
    }
  }

  return urls.slice(0, 12);
}

function limit61troDetailCandidates(links = [], limit = 12) {
  const ordered = [];
  const seen = new Set();

  for (const item of links) {
    const href = normalizePageUrl(item?.href || item);
    if (!href || seen.has(href)) {
      continue;
    }

    seen.add(href);
    ordered.push(href);
    if (ordered.length >= limit) {
      break;
    }
  }

  return ordered;
}

async function findMatching61troItemFromDetailCandidates(client, source, links, docketNumber, { courtName = "" } = {}) {
  const candidates = limit61troDetailCandidates(links);
  for (const candidateUrl of candidates) {
    try {
      const detailHtml = await client.fetchText(candidateUrl);
      const item = parse61troCasePage(detailHtml, candidateUrl, source);
      if (
        item?.docketNumber &&
        docketNumbersMatchForLookup(item.docketNumber, docketNumber) &&
        lawFirmMatchesCourtName(item, courtName)
      ) {
        return item;
      }
    } catch {
      continue;
    }
  }

  return null;
}

function parseSriplawNoticeRows(html, baseUrl) {
  const table = String(html || "").match(/<table id="tablepress-[^"]+"[\s\S]*?<\/table>/i)?.[0] || "";
  const rows = [];
  for (const match of table.matchAll(/<tr class="row-\d+">([\s\S]*?)<\/tr>/gi)) {
    const rowHtml = match[1];
    if (!/<td\b/i.test(rowHtml)) {
      continue;
    }

    const columns = [...rowHtml.matchAll(/<td class="column-\d+">([\s\S]*?)<\/td>/gi)].map((item) => item[1]);
    if (columns.length < 5) {
      continue;
    }

    const caseUrl = absoluteUrl(columns[0].match(/href=['"]([^'"]+)['"]/i)?.[1] || "", baseUrl);
    const caseName = cleanText(columns[0]);
    const courtCode = cleanText(columns[1]).toUpperCase();
    const docketNumber = cleanText(columns[2]);
    const matterId = cleanText(columns[3]);
    const dateFiled = parseUsDate(columns[4]);

    if (!caseUrl || !docketNumber) {
      continue;
    }

    rows.push({
      caseUrl,
      caseName,
      courtCode,
      courtId: courtCodeToId(courtCode),
      courtName: courtCodeToName(courtCode),
      docketNumber,
      matterId,
      dateFiled
    });
  }

  return rows;
}

function parseSriplawEntries(html, baseUrl) {
  const table = String(html || "").match(/<table id="tablepress-[^"]+"[\s\S]*?<\/table>/i)?.[0] || "";
  const entries = [];
  for (const match of table.matchAll(/<tr class="row-\d+">([\s\S]*?)<\/tr>/gi)) {
    const rowHtml = match[1];
    if (!/<td\b/i.test(rowHtml)) {
      continue;
    }

    const columns = [...rowHtml.matchAll(/<td class="column-\d+">([\s\S]*?)<\/td>/gi)].map((item) => item[1]);
    if (columns.length < 3) {
      continue;
    }

    const entryNumber = cleanText(columns[0]);
    const documentUrl = absoluteUrl(columns[1].match(/href=['"]([^'"]+)['"]/i)?.[1] || "", baseUrl);
    const description = cleanText(columns[1]);
    const filedAt = parseUsDate(columns[2]);
    if (!entryNumber || !description) {
      continue;
    }

    entries.push({
      sourceEntryId: entryNumber,
      entryNumber,
      documentNumber: entryNumber,
      documentType: "Docket Entry",
      description,
      filedAt,
      absoluteUrl: documentUrl
    });
  }

  return entries;
}

function parseSriplawCasePage(html, row, source) {
  const title = cleanText(String(html || "").match(/<title>([\s\S]*?)<\/title>/i)?.[1] || row.caseName);
  const entries = parseSriplawEntries(html, source.baseUrl);
  const latestEntry = entries[entries.length - 1] || null;

  return {
    sourceId: source.id,
    sourceLabel: source.label,
    lawFirm: source.lawFirm,
    sourceCaseId: row.matterId || row.docketNumber || row.caseUrl,
    caseUrl: normalizePageUrl(row.caseUrl),
    title,
    caseName: row.caseName || cleanCaseName(title),
    docketNumber: row.docketNumber || parseDocketNumber(title),
    courtCode: row.courtCode || "",
    courtId: row.courtId || null,
    courtName: row.courtName || "",
    dateFiled: row.dateFiled || null,
    summary: latestEntry?.description || title,
    latestDocketNumber: latestEntry?.documentNumber || null,
    entries,
    syncedAt: new Date().toISOString(),
    rawMeta: {
      matterId: row.matterId || null,
      discoveryUrl: source.discoveryUrl,
      rowCount: entries.length
    }
  };
}

function extractCourtNameFromGbc(content) {
  const match = String(content || "").match(/United States District Court(?: for the)? ([A-Za-z ,]+?District of [A-Za-z ]+)/i);
  if (!match) {
    return "";
  }

  return cleanText(match[1]).replace(/,\s*(Eastern|Western|Northern|Southern|Central|Middle)\s+Division$/i, "");
}

function parseGbcEntries(content, caseUrl) {
  const entries = [];
  const strongPattern = /<p[^>]*>\s*<strong>([\s\S]*?)<\/strong>[\s\S]*?<a href="([^"]+\.pdf)"/gi;
  for (const match of String(content || "").matchAll(strongPattern)) {
    const title = cleanText(match[1]);
    const documentUrl = absoluteUrl(match[2], caseUrl);
    if (!title || !documentUrl) {
      continue;
    }

    entries.push({
      sourceEntryId: normalizePdfTitle(documentUrl.split("/").pop()),
      entryNumber: null,
      documentNumber: null,
      documentType: "Docket Document",
      description: title,
      filedAt: null,
      absoluteUrl: normalizePageUrl(documentUrl)
    });
  }

  if (entries.length) {
    return entries;
  }

  for (const match of String(content || "").matchAll(/href="([^"]+\.pdf)"/gi)) {
    const documentUrl = absoluteUrl(match[1], caseUrl);
    if (!documentUrl) {
      continue;
    }

    entries.push({
      sourceEntryId: normalizePdfTitle(documentUrl.split("/").pop()),
      entryNumber: null,
      documentNumber: null,
      documentType: "Docket Document",
      description: normalizePdfTitle(documentUrl.split("/").pop()),
      filedAt: null,
      absoluteUrl: normalizePageUrl(documentUrl)
    });
  }

  return entries;
}

function parseGbcCasePage(page, source) {
  const content = String(page.content?.rendered || "");
  const title = cleanText(page.title?.rendered || "");
  const docketNumber = parseDocketNumber(title || page.link || page.slug);
  const caseName = cleanCaseName(title) || cleanCaseName(content.match(/<h[1-4][^>]*>([\s\S]*?)<\/h[1-4]>/i)?.[1] || "");
  const courtName = extractCourtNameFromGbc(content);
  const entries = parseGbcEntries(content, page.link);

  return {
    sourceId: source.id,
    sourceLabel: source.label,
    lawFirm: source.lawFirm,
    sourceCaseId: String(page.id || page.link),
    caseUrl: normalizePageUrl(page.link),
    title,
    caseName,
    docketNumber,
    courtCode: "",
    courtId: null,
    courtName,
    dateFiled: null,
    summary: entries[entries.length - 1]?.description || title,
    latestDocketNumber: null,
    entries,
    syncedAt: new Date().toISOString(),
    rawMeta: {
      modifiedAt: page.modified || null,
      discoveryUrl: source.discoveryUrl,
      rowCount: entries.length
    }
  };
}

function parseGenericCasePage(html, url, source) {
  const title = cleanText(String(html || "").match(/<title>([\s\S]*?)<\/title>/i)?.[1] || "");
  const docketNumber = parseDocketNumber(title || html);
  if (!docketNumber) {
    return null;
  }

  const caseName = cleanCaseName(title) || cleanText(String(html || "").match(/<h1[^>]*>([\s\S]*?)<\/h1>/i)?.[1] || "");
  const entries = [];
  for (const match of String(html || "").matchAll(/href=['"]([^'"]+\.pdf)['"]/gi)) {
    const documentUrl = absoluteUrl(match[1], url);
    if (!documentUrl) {
      continue;
    }

    entries.push({
      sourceEntryId: normalizePdfTitle(documentUrl.split("/").pop()),
      entryNumber: null,
      documentNumber: null,
      documentType: "Docket Document",
      description: normalizePdfTitle(documentUrl.split("/").pop()),
      filedAt: null,
      absoluteUrl: normalizePageUrl(documentUrl)
    });
  }

  return {
    sourceId: source.id,
    sourceLabel: source.label,
    lawFirm: source.lawFirm,
    sourceCaseId: url,
    caseUrl: normalizePageUrl(url),
    title,
    caseName: caseName || title,
    docketNumber,
    courtCode: "",
    courtId: null,
    courtName: "",
    dateFiled: null,
    summary: entries[entries.length - 1]?.description || title,
    latestDocketNumber: null,
    entries,
    syncedAt: new Date().toISOString(),
    rawMeta: {
      discoveryUrl: source.discoveryUrl,
      rowCount: entries.length
    }
  };
}

export class LawFirmClient {
  constructor(config) {
    this.enabled = Boolean(config.enabled);
    this.timeoutMs = Number(config.timeoutMs || 15000);
    this.minIntervalMs = Number(config.minIntervalMs || 1000);
    this.maxCasesPerSource = Number(config.maxCasesPerSource || 20);
    this.maxLookupsPerRun = Number(config.maxLookupsPerRun || 8);
    const selectedIds = new Set((config.sources || []).map((item) => String(item || "").trim().toLowerCase()).filter(Boolean));
    this.sources = DEFAULT_LAW_FIRM_SOURCES.filter((source) => !selectedIds.size || selectedIds.has(source.id));
    this.lastRequestAt = 0;
    this.sitemapCache = new Map();
  }

  getStatus() {
    return {
      enabled: this.enabled,
      trackedSources: this.sources.length,
      sources: this.sources.map((source) => ({
        id: source.id,
        label: source.label,
        law_firm: source.lawFirm,
        strategy: source.strategy
      }))
    };
  }

  listSources() {
    return this.sources.map((source) => ({ ...source }));
  }

  async fetchRecentForSource(source) {
    if (!this.enabled) {
      return { source, items: [], note: "律所官网补源已关闭。" };
    }

    if (source.strategy === "sriplaw-notice") {
      return this.fetchSriplawSource(source);
    }

    if (source.strategy === "gbc-pages") {
      return this.fetchGbcSource(source);
    }

    if (source.strategy === "61tro-search") {
      return this.fetch61troSource(source);
    }

    return this.fetchProbeSource(source);
  }

  async fetchSriplawSource(source) {
    const html = await this.fetchText(source.discoveryUrl);
    const rows = parseSriplawNoticeRows(html, source.baseUrl).slice(0, this.maxCasesPerSource);
    const items = [];
    let failedItems = 0;

    for (const row of rows) {
      try {
        const pageHtml = await this.fetchText(row.caseUrl);
        items.push(parseSriplawCasePage(pageHtml, row, source));
      } catch {
        failedItems += 1;
      }
    }

    return {
      source,
      items,
      totalCandidates: rows.length,
      failedItems,
      note: items.length
        ? `SRIPLAW 官网本轮复核 ${rows.length} 个公开案件页${failedItems ? `，${failedItems} 个页面待重试` : ""}。`
        : "SRIPLAW 官网本轮没有抓到可用案件页。"
    };
  }

  async fetchGbcSource(source) {
    const pages = await this.fetchJson(source.discoveryUrl);
    const candidates = (Array.isArray(pages) ? pages : [])
      .filter((page) => /\/case-no-|\/caseno-/i.test(String(page.link || "")) || /case-no|caseno/i.test(String(page.slug || "")))
      .sort((left, right) => String(right.modified || "").localeCompare(String(left.modified || "")))
      .slice(0, this.maxCasesPerSource);

    return {
      source,
      items: candidates.map((page) => parseGbcCasePage(page, source)).filter((item) => item.docketNumber),
      totalCandidates: candidates.length,
      failedItems: 0,
      note: candidates.length
        ? `GBC 官网本轮复核 ${candidates.length} 个公开案件页。`
        : "GBC 官网当前没有识别到公开案件页。"
    };
  }

  async fetchProbeSource(source) {
    const body = await this.fetchText(source.discoveryUrl);
    const links = source.discoveryUrl.endsWith(".xml")
      ? extractSitemapLocs(body)
      : extractSameDomainLinks(body, source.baseUrl);
    const candidates = [...new Set(links.filter((link) => looksLikeStructuredCaseLink(link)))].slice(0, this.maxCasesPerSource);
    const items = [];
    let failedItems = 0;

    for (const link of candidates) {
      try {
        const html = await this.fetchText(link);
        const item = parseGenericCasePage(html, link, source);
        if (item?.docketNumber) {
          items.push(item);
        }
      } catch {
        failedItems += 1;
      }
    }

    return {
      source,
      items,
      totalCandidates: candidates.length,
      failedItems,
      note: candidates.length
        ? `${source.label} 官网探测到 ${candidates.length} 个疑似案件页${failedItems ? `，${failedItems} 个页面待重试` : ""}。`
        : `${source.label} 官网当前未暴露可稳定抓取的公开案件页。`
    };
  }

  async fetch61troSource(source) {
    const html = await this.fetchText(source.discoveryUrl);
    const candidates = extract61troRecentLinks(html, source.baseUrl).slice(0, this.maxCasesPerSource);
    const items = [];
    let failedItems = 0;

    for (const link of candidates) {
      try {
        const pageHtml = await this.fetchText(link);
        const item = parse61troCasePage(pageHtml, link, source);
        if (item?.docketNumber) {
          items.push(item);
        }
      } catch {
        failedItems += 1;
      }
    }

    return {
      source,
      items,
      totalCandidates: candidates.length,
      failedItems,
      note: candidates.length
        ? `61TRO 本轮复核 ${candidates.length} 个公开案件页${failedItems ? `，${failedItems} 个页面待重试` : ""}。`
        : "61TRO 当前没有识别到可稳定抓取的公开案件页。"
    };
  }

  async lookupByDocket(docketNumber, { sourceIds = [], courtName = "", caseName = "", plaintiffs = [], firms = [] } = {}) {
    if (!this.enabled) {
      return null;
    }

    const requestedIds = new Set(asArray(sourceIds).map((value) => String(value || "").trim().toLowerCase()).filter(Boolean));
    const sources = requestedIds.size
      ? this.sources.filter((source) => requestedIds.has(source.id))
      : this.sources;

    for (const source of sources) {
      if (source.strategy !== "61tro-search") {
        continue;
      }

      const item = await this.lookup61troByDocket(source, docketNumber, {
        courtName,
        caseName,
        plaintiffs,
        firms
      });
      if (item && lawFirmMatchesCourtName(item, courtName)) {
        return {
          source,
          item,
          note: `61TRO 通过案件号命中 ${item.docketNumber}。`
        };
      }
    }

    return null;
  }

  async fetch61troSitemapUrls(source) {
    const cacheKey = `${source.id}:${source.baseUrl}`;
    if (!this.sitemapCache.has(cacheKey)) {
      this.sitemapCache.set(
        cacheKey,
        this.fetchText(`${source.baseUrl}/sitemap.txt`)
          .then((text) => String(text || "").split(/\r?\n/).map((line) => line.trim()).filter(Boolean))
          .catch(() => [])
      );
    }

    return this.sitemapCache.get(cacheKey);
  }

  async resolve61troDetailUrlFromIndex(source, pageUrl, docketNumber) {
    const queue = [pageUrl];
    const visited = new Set();

    while (queue.length && visited.size < 15) {
      const currentUrl = queue.shift();
      const normalizedUrl = normalizePageUrl(currentUrl);
      if (!normalizedUrl || visited.has(normalizedUrl)) {
        continue;
      }

      visited.add(normalizedUrl);
      let html = "";
      try {
        html = await this.fetchText(normalizedUrl);
      } catch {
        continue;
      }

      const detailUrl = extract61troSearchResultLink(html, docketNumber, source.baseUrl);
      if (detailUrl) {
        return detailUrl;
      }

      const matchedItem = await findMatching61troItemFromDetailCandidates(
        this,
        source,
        extract61troDetailLinks(html, source.baseUrl),
        docketNumber
      );
      if (matchedItem?.caseUrl) {
        return matchedItem.caseUrl;
      }

      for (const nextPage of extract61troPaginationLinks(html, normalizedUrl)) {
        if (!visited.has(nextPage)) {
          queue.push(nextPage);
        }
      }
    }

    return null;
  }

  async lookup61troByDocket(source, docketNumber, { courtName = "", caseName = "", plaintiffs = [], firms = [] } = {}) {
    for (const detailUrl of build61troDirectViewCandidateUrls(source, docketNumber, { courtName })) {
      let pageHtml = "";
      try {
        pageHtml = await this.fetchText(detailUrl);
      } catch {
        continue;
      }

      const item = parse61troCasePage(pageHtml, detailUrl, source);
      if (
        item?.docketNumber &&
        docketNumbersMatchForLookup(item.docketNumber, docketNumber) &&
        lawFirmMatchesCourtName(item, courtName)
      ) {
        return item;
      }
    }

    for (const term of build61troSearchTerms(docketNumber)) {
      const searchUrl = `${source.baseUrl}/search.html?sn=${encodeURIComponent(term)}`;
      let html = "";
      try {
        html = await this.fetchText(searchUrl);
      } catch {
        continue;
      }

      const detailUrl = extract61troSearchResultLink(html, docketNumber, source.baseUrl);
      if (detailUrl) {
        let pageHtml = "";
        try {
          pageHtml = await this.fetchText(detailUrl);
        } catch {
          pageHtml = "";
        }

        const item = parse61troCasePage(pageHtml, detailUrl, source);
        if (
          item?.docketNumber &&
          docketNumbersMatchForLookup(item.docketNumber, docketNumber) &&
          lawFirmMatchesCourtName(item, courtName)
        ) {
          return item;
        }
      }

      const matchedItem = await findMatching61troItemFromDetailCandidates(
        this,
        source,
        extract61troDetailLinks(html, source.baseUrl),
        docketNumber,
        { courtName }
      );
      if (matchedItem) {
        return matchedItem;
      }
    }

    const sitemapUrls = await this.fetch61troSitemapUrls(source);
    const hintUrls = build61troHintUrls(source, {
      caseName,
      plaintiffs,
      firms
    }, sitemapUrls);

    for (const hintUrl of hintUrls) {
      const detailUrl = await this.resolve61troDetailUrlFromIndex(source, hintUrl, docketNumber);
      if (!detailUrl) {
        continue;
      }

      let pageHtml = "";
      try {
        pageHtml = await this.fetchText(detailUrl);
      } catch {
        continue;
      }

      const item = parse61troCasePage(pageHtml, detailUrl, source);
      if (
        item?.docketNumber &&
        docketNumbersMatchForLookup(item.docketNumber, docketNumber) &&
        lawFirmMatchesCourtName(item, courtName)
      ) {
        return item;
      }
    }

    return null;
  }

  async fetchJson(url, options = {}) {
    const text = await this.fetchText(url, options);
    return text ? JSON.parse(text) : {};
  }

  async fetchText(url, options = {}, attempt = 0) {
    const now = Date.now();
    const waitMs = Math.max(0, this.minIntervalMs - (now - this.lastRequestAt));
    if (waitMs > 0) {
      await wait(waitMs);
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    this.lastRequestAt = Date.now();

    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
        headers: {
          "user-agent": "Mozilla/5.0 (compatible; TroTrackerBot/1.0; +https://www.trotracker.com)",
          accept: "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
          ...(options.headers || {})
        }
      });

      if (response.status === 404) {
        return "";
      }

      if ([408, 409, 425, 429, 500, 502, 503, 504].includes(response.status)) {
        if (attempt >= 2) {
          throw new Error(`LawFirm fetch failed (${response.status})`);
        }

        await wait((attempt + 1) * 1000);
        return this.fetchText(url, options, attempt + 1);
      }

      if (!response.ok) {
        throw new Error(`LawFirm fetch failed (${response.status})`);
      }

      return await response.text();
    } catch (error) {
      if (attempt < 2 && (error.name === "AbortError" || error.cause || /fetch/i.test(String(error.message || "")))) {
        await wait((attempt + 1) * 1000);
        return this.fetchText(url, options, attempt + 1);
      }

      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }
}
