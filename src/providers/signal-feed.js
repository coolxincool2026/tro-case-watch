const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (compatible; TROTracker/1.0; +https://www.trotracker.com)";

export const SIGNAL_FEED_PROVIDER_KEY = "signal-feed";

function htmlDecode(value) {
  return String(value || "")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

function normalizeDocketNumber(value) {
  return String(value || "")
    .trim()
    .replace(/^(\d+):20(\d{2})-cv-/i, "$1:$2-cv-")
    .replace(/\s+/g, "");
}

function unpackAstroValue(value) {
  if (Array.isArray(value)) {
    if (value.length === 2 && Number.isInteger(value[0])) {
      if (value[0] === 0) {
        return unpackAstroValue(value[1]);
      }
      if (value[0] === 1) {
        return Array.isArray(value[1]) ? value[1].map(unpackAstroValue) : [];
      }
    }
    return value.map(unpackAstroValue);
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, nestedValue]) => [key, unpackAstroValue(nestedValue)])
    );
  }

  return value;
}

function absoluteUrl(value, baseUrl) {
  try {
    return new URL(String(value || ""), baseUrl).toString();
  } catch {
    return null;
  }
}

function parsePublicRows(html, publicCasesUrl) {
  const items = [];
  const rows = String(html || "").match(/<tr\b[^>]*>[\s\S]*?<\/tr>/gi) || [];

  for (const row of rows) {
    const linkMatch = row.match(/href="([^"]*\/cases\/[^"]+)"/i);
    const docketMatch = row.match(/>(\d+:20\d{2}-cv-\d+)</i);
    if (!linkMatch || !docketMatch) {
      continue;
    }

    const titleValues = [...row.matchAll(/\btitle="([^"]+)"/gi)].map((match) => htmlDecode(match[1]).trim());
    const dateMatch = row.match(/>([A-Z][a-z]{2}\s+\d{1,2},\s+20\d{2})<\/td>/);
    const detailUrl = absoluteUrl(htmlDecode(linkMatch[1]), publicCasesUrl);
    const idMatch = String(linkMatch[1]).match(/\/cases\/([a-f0-9]{32})-/i);
    const filedAt = dateMatch ? new Date(`${dateMatch[1]} 00:00:00 UTC`) : null;

    items.push({
      docketId: idMatch?.[1] || normalizeDocketNumber(docketMatch[1]),
      docketNumber: normalizeDocketNumber(docketMatch[1]),
      plaintiff: titleValues[0] || null,
      courtName: titleValues[1] || null,
      dateFiled: filedAt && Number.isFinite(filedAt.getTime()) ? filedAt.toISOString().slice(0, 10) : null,
      detailUrl
    });
  }

  return [...new Map(items.map((item) => [item.docketId, item])).values()];
}

function parsePublicDetail(html, detailUrl) {
  const islandMatch = String(html || "").match(
    /<astro-island\b[^>]*component-url="[^"]*CaseDetail[^>]*\bprops="([^"]+)"/i
  );
  if (!islandMatch) {
    return null;
  }

  let props;
  try {
    props = unpackAstroValue(JSON.parse(htmlDecode(islandMatch[1])));
  } catch {
    return null;
  }

  const item = props?.troCase;
  if (!item?.case_number) {
    return null;
  }

  return {
    docketId: item.docket_id || null,
    docketNumber: normalizeDocketNumber(item.case_number),
    caseName: item.case_name || null,
    caseUrl: item.case_url || null,
    detailUrl,
    plaintiff: item.plaintiff || null,
    plaintiffs: parseJsonArray(item.plaintiffs_json, item.plaintiff),
    defendant: item.defendant || null,
    defendants: item.defendant ? [item.defendant] : [],
    courtId: item.court_id || null,
    courtName: item.court || null,
    dateFiled: item.case_date || null,
    status: item.is_active === false ? "terminated" : "open",
    judge: item.judge || null,
    counsel: item.plaintiff_counsel || null,
    counselFirm: item.plaintiff_counsel_firm || null,
    entries: (Array.isArray(item.entries) ? item.entries : []).map((entry) => ({
      entryId: entry.entry_id || entry.db_doc_id || null,
      title: entry.title || null,
      filedAt: entry.entry_date || null,
      documentNumber: entry.doc_number ?? null,
      documentUrl: entry.docLink || null,
      downloadable: Boolean(entry.downloadable),
      restricted: Boolean(entry.restricted),
      attachments: (Array.isArray(entry.attachments) ? entry.attachments : []).map((attachment) => ({
        id: attachment.id ?? null,
        title: attachment.title || null,
        number: attachment.att_number ?? null,
        documentUrl: attachment.docLink || null,
        downloadable: Boolean(attachment.downloadable),
        restricted: Boolean(attachment.restricted)
      }))
    }))
  };
}

function parseJsonArray(rawValue, fallbackValue = null) {
  try {
    const parsed = JSON.parse(String(rawValue || "[]"));
    if (Array.isArray(parsed)) {
      return parsed.map((value) => String(value || "").trim()).filter(Boolean);
    }
  } catch {}
  return fallbackValue ? [String(fallbackValue).trim()].filter(Boolean) : [];
}

function normalizeApiRecentItem(item = {}) {
  return {
    docketId: item.docketID || item.docketId || null,
    docketNumber: normalizeDocketNumber(item.caseNumber),
    caseName: item.title || null,
    plaintiff: item.plaintiff || null,
    plaintiffs: item.plaintiff ? [item.plaintiff] : [],
    defendants: [],
    courtName: item.court || null,
    dateFiled: item.caseDate || null,
    entries: []
  };
}

function normalizeApiEntry(entry = {}) {
  return {
    entryId: entry.entryID || entry.entryId || null,
    title: entry.title || null,
    filedAt: entry.entryDate || null,
    documentNumber: entry.docNumber ?? null,
    documentUrl: entry.docLink || null,
    downloadable: Boolean(entry.downloadable),
    restricted: Boolean(entry.restricted),
    attachments: (Array.isArray(entry.attachments) ? entry.attachments : []).map((attachment) => ({
      id: attachment.id ?? null,
      title: attachment.title || null,
      number: attachment.attachmentNumber ?? attachment.attNumber ?? null,
      documentUrl: attachment.docLink || null,
      downloadable: Boolean(attachment.downloadable),
      restricted: Boolean(attachment.restricted)
    }))
  };
}

export class SignalFeedClient {
  constructor(options = {}) {
    this.enabled = Boolean(options.enabled && options.publicCasesUrl);
    this.publicCasesUrl = String(options.publicCasesUrl || "").trim();
    this.apiBaseUrl = String(options.apiBaseUrl || "").replace(/\/+$/, "");
    this.apiKey = String(options.apiKey || "").trim();
    this.timeoutMs = Math.max(Number(options.timeoutMs || 15_000), 1000);
    this.minIntervalMs = Math.max(Number(options.minIntervalMs || 1500), 0);
    this.recentDays = Math.min(Math.max(Number(options.recentDays || 7), 1), 7);
    this.recentLimit = Math.min(Math.max(Number(options.recentLimit || 100), 1), 100);
    // Public discovery exposes 15 rows; the authenticated API supports up to 100.
    this.maxCasesPerRun = Math.min(Math.max(Number(options.maxCasesPerRun || 100), 1), 100);
    this.lastRequestAt = 0;
  }

  getStatus() {
    return {
      enabled: this.enabled,
      state: this.enabled ? "ready" : "disabled",
      apiEnabled: Boolean(this.apiKey && this.apiBaseUrl),
      maxCasesPerRun: this.maxCasesPerRun
    };
  }

  async fetchRecent({ hydrate = true, maxCases = this.maxCasesPerRun } = {}) {
    if (!this.enabled) {
      return { items: [], source: "disabled" };
    }

    const candidates = this.apiKey && this.apiBaseUrl
      ? await this.fetchApiRecent()
      : await this.fetchPublicRecent();
    const selected = candidates.slice(0, Math.min(Math.max(Number(maxCases || 1), 1), this.maxCasesPerRun));
    if (!hydrate) {
      return { items: selected, source: this.apiKey ? "api" : "public" };
    }

    const items = [];
    for (const candidate of selected) {
      try {
        const detail = candidate.detailUrl
          ? await this.fetchPublicDetail(candidate.detailUrl)
          : this.apiKey
            ? await this.fetchApiEntries(candidate)
            : null;
        items.push({ ...candidate, ...(detail || {}) });
      } catch {
        items.push(candidate);
      }
    }

    return { items, source: this.apiKey ? "api" : "public" };
  }

  async lookupByDocket(docketNumber) {
    if (!this.enabled) {
      return null;
    }

    const normalized = normalizeDocketNumber(docketNumber).toLowerCase();
    const candidates = this.apiKey && this.apiBaseUrl
      ? await this.fetchApiRecent()
      : await this.fetchPublicRecent();
    const candidate = candidates.find((item) => normalizeDocketNumber(item.docketNumber).toLowerCase() === normalized);
    if (!candidate) {
      return null;
    }

    const detail = candidate.detailUrl
      ? await this.fetchPublicDetail(candidate.detailUrl)
      : this.apiKey
        ? await this.fetchApiEntries(candidate)
        : null;
    return { ...candidate, ...(detail || {}) };
  }

  async fetchKnownCase(candidate = {}) {
    if (!this.enabled) {
      return null;
    }

    if (this.apiKey && this.apiBaseUrl && candidate.docketId) {
      return this.fetchApiEntries(candidate);
    }

    if (!candidate.detailUrl) {
      return null;
    }

    let lastError = null;
    for (const detailUrl of this.buildPublicDetailCandidates(candidate.detailUrl)) {
      try {
        const detail = await this.fetchPublicDetail(detailUrl);
        if (detail) {
          return { ...candidate, ...detail, detailUrl };
        }
      } catch (error) {
        lastError = error;
        if (Number(error?.status || 0) !== 404) {
          throw error;
        }
      }
    }

    if (lastError) {
      throw lastError;
    }
    return null;
  }

  buildPublicDetailCandidates(detailUrl) {
    const urls = new Set();
    const append = (value) => {
      const normalized = String(value || "").trim();
      if (!normalized) {
        return;
      }
      urls.add(normalized);
      urls.add(normalized.endsWith("/") ? normalized.slice(0, -1) : `${normalized}/`);
    };

    append(detailUrl);
    append(String(detailUrl || "").replace("/en/cases/", "/cases/"));
    append(String(detailUrl || "").replace("/cases/", "/en/cases/"));
    return [...urls];
  }

  async fetchPublicRecent() {
    const html = await this.requestText(this.publicCasesUrl);
    const rows = parsePublicRows(html, this.publicCasesUrl);
    if (rows.length) {
      return rows;
    }

    const pageUrl = new URL(this.publicCasesUrl);
    const languageRoot = pageUrl.pathname.startsWith("/en/") ? "/en/" : "/";
    const fallbackUrl = new URL(languageRoot, pageUrl.origin).toString();
    if (fallbackUrl === this.publicCasesUrl) {
      return rows;
    }
    const fallbackHtml = await this.requestText(fallbackUrl);
    return parsePublicRows(fallbackHtml, fallbackUrl);
  }

  async fetchPublicDetail(detailUrl) {
    const html = await this.requestText(detailUrl);
    return parsePublicDetail(html, detailUrl);
  }

  async fetchApiRecent() {
    const payload = await this.requestJson(`${this.apiBaseUrl}/tro/recent`, {
      days: this.recentDays,
      limit: this.recentLimit
    });
    const rows = Array.isArray(payload?.data) ? payload.data : [];
    return rows.map(normalizeApiRecentItem).filter((item) => item.docketId && item.docketNumber);
  }

  async fetchApiEntries(candidate) {
    const payload = await this.requestJson(`${this.apiBaseUrl}/tro/entries`, {
      docketID: candidate.docketId
    });
    const rows = Array.isArray(payload?.data) ? payload.data : Array.isArray(payload?.data?.entries) ? payload.data.entries : [];
    return {
      ...candidate,
      entries: rows.map(normalizeApiEntry)
    };
  }

  async requestText(url) {
    await this.throttle();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await fetch(url, {
        headers: {
          accept: "text/html,application/xhtml+xml",
          "user-agent": DEFAULT_USER_AGENT
        },
        signal: controller.signal
      });
      if (!response.ok) {
        const error = new Error(`signal feed request failed with HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }
      return await response.text();
    } finally {
      clearTimeout(timer);
      this.lastRequestAt = Date.now();
    }
  }

  async requestJson(url, body) {
    await this.throttle();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          "user-agent": DEFAULT_USER_AGENT,
          "x-api-key": this.apiKey
        },
        body: JSON.stringify(body),
        signal: controller.signal
      });
      if (!response.ok) {
        const error = new Error(`signal feed API failed with HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }
      return await response.json();
    } finally {
      clearTimeout(timer);
      this.lastRequestAt = Date.now();
    }
  }

  async throttle() {
    const waitMs = this.minIntervalMs - (Date.now() - this.lastRequestAt);
    if (waitMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }
}

export const __test = {
  normalizeDocketNumber,
  parsePublicRows,
  parsePublicDetail,
  unpackAstroValue
};
