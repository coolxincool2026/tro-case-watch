function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stripTags(value) {
  return String(value || "").replace(/<[^>]+>/g, " ");
}

function cleanText(value) {
  return decodeHtml(stripTags(value)).replace(/\s+/g, " ").trim();
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

function parseDocket(value) {
  const match = String(value || "").match(/(?:\d+:)?(\d{2})-cv-(\d{3,6})/i);
  if (!match) {
    return null;
  }

  return {
    shortYear: match[1],
    year: `20${match[1]}`,
    serial: match[2]
  };
}

function normalizeCourtName(value) {
  return String(value || "").toLowerCase();
}

const COURT_STATE_OVERRIDES = new Map([
  ["dcd", "DC"]
]);

const STATE_NAMES = [
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
];

function deriveStateCode(caseRow) {
  const courtId = String(caseRow.court_id || "").toLowerCase();
  if (COURT_STATE_OVERRIDES.has(courtId)) {
    return COURT_STATE_OVERRIDES.get(courtId);
  }

  if (courtId.length >= 2 && /^[a-z]{2}/.test(courtId)) {
    return courtId.slice(0, 2).toUpperCase();
  }

  const courtName = normalizeCourtName(caseRow.court_name);
  for (const [code, name] of STATE_NAMES) {
    if (courtName.includes(name)) {
      return code;
    }
  }

  return null;
}

function parseDate(value) {
  const match = String(value || "").match(/(\d{2})\/(\d{2})\/(\d{4})/);
  if (!match) {
    return null;
  }

  return `${match[3]}-${match[1]}-${match[2]}`;
}

function parseIsoDateMs(value) {
  const ms = Date.parse(String(value || ""));
  return Number.isFinite(ms) ? ms : null;
}

function normalizeLookupText(value) {
  return cleanText(value).toLowerCase().replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
}

function collectExpectedTokens(caseRow) {
  const blob = [
    caseRow?.case_name,
    ...(Array.isArray(caseRow?.plaintiffs) ? caseRow.plaintiffs : []),
    ...(Array.isArray(caseRow?.defendants) ? caseRow.defendants : [])
  ]
    .map((item) => normalizeLookupText(item))
    .filter(Boolean)
    .join(" ");

  const stop = new Set([
    "plaintiff",
    "defendant",
    "llc",
    "inc",
    "ltd",
    "et",
    "al",
    "company",
    "corp",
    "corporation"
  ]);

  return [...new Set(
    blob
      .split(/\s+/)
      .map((item) => item.trim())
      .filter((item) => item.length >= 4 && !stop.has(item))
  )];
}

function looksLikeForeignNamedEntity(text) {
  return /\b[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})+\b/.test(String(text || ""));
}

export class WorldtroClient {
  constructor(config) {
    this.enabled = Boolean(config.enabled);
    this.baseUrl = String(config.baseUrl || "https://worldtro.com").replace(/\/$/, "");
    this.minIntervalMs = Number(config.minIntervalMs || 1500);
    this.timeoutMs = Number(config.timeoutMs || 15000);
    this.maxCasesPerRun = Number(config.maxCasesPerRun || 3);
    this.lastRequestAt = 0;
  }

  getStatus() {
    if (!this.enabled) {
      return {
        enabled: false,
        state: "disabled",
        note: "默认关闭。"
      };
    }

    return {
      enabled: true,
      state: "live",
      note: "启用公开页面补源，用于补品牌、律所和更完整的 docket 时间线。"
    };
  }

  async enrichCase(caseRow) {
    if (!this.enabled) {
      return null;
    }

    const docket = parseDocket(caseRow.docket_number);
    if (!docket) {
      return null;
    }

    const stateCode = deriveStateCode(caseRow);
    if (!stateCode) {
      return null;
    }

    const pagePath = await this.lookupCasePath({
      stateCode,
      year: docket.year,
      serial: docket.serial
    });

    if (!pagePath) {
      return null;
    }

    const pageUrl = new URL(pagePath, `${this.baseUrl}/`).toString();
    const html = await this.fetchText(pageUrl);
    const payload = this.parseCasePage(html, pageUrl, {
      stateCode,
      year: docket.year,
      serial: docket.serial
    });

    if (!this.isPlausiblePayload(caseRow, payload)) {
      return null;
    }

    return payload;
  }

  async lookupCasePath({ stateCode, year, serial }) {
    const body = new URLSearchParams();
    body.set("data[tl]", stateCode);
    body.set("data[year]", String(year));
    body.set("data[sn]", String(serial));
    body.set("is_form", "1");
    body.set("is_admin", "1");

    try {
      const payload = await this.fetchJson(`${this.baseUrl}/index.php?s=case&c=home&m=search`, {
        method: "POST",
        headers: {
          "content-type": "application/x-www-form-urlencoded; charset=UTF-8"
        },
        body: body.toString()
      });

      if (payload?.code === 1 && payload.url) {
        return payload.url;
      }
    } catch {
      return `/case-${stateCode}-${year}-cv-${serial}.html`;
    }

    return `/case-${stateCode}-${year}-cv-${serial}.html`;
  }

  parseCasePage(html, pageUrl, meta) {
    const canonicalUrl = new URL(pageUrl);
    canonicalUrl.search = "";
    canonicalUrl.hash = "";
    const lawFirm = cleanText(html.match(/<p[^>]*>\s*原告律所：([\s\S]*?)<\/p>/i)?.[1]);
    const brand = cleanText(html.match(/<p[^>]*>\s*品牌：([\s\S]*?)<\/p>/i)?.[1]);
    const title = cleanText(html.match(/<title>([\s\S]*?)<\/title>/i)?.[1]);
    const entries = [];
    const rowPattern =
      /<tr>\s*<td class="tro1">([\s\S]*?)<\/td>[\s\S]*?<td class="tro2">[\s\S]*?<div>([\s\S]*?)<\/div>[\s\S]*?<td class="tro3[^"]*"[^>]*>\s*<span class="txt\d+">([\s\S]*?)<\/span>([\s\S]*?)<\/td>[\s\S]*?<\/tr>/gi;

    let match;
    while ((match = rowPattern.exec(html)) !== null) {
      const rowNumber = cleanText(match[1]);
      const filedAt = parseDate(cleanText(match[2]));
      const description = cleanText(match[3]);
      const attachmentTexts = [...match[4].matchAll(/<option>([\s\S]*?)<\/option>/gi)]
        .map((item) => cleanText(item[1]))
        .filter(Boolean);

      if (!description) {
        continue;
      }

      entries.push({
        row_number: rowNumber,
        filed_at: filedAt,
        description: attachmentTexts.length ? `${description} 附件: ${attachmentTexts.join(" / ")}` : description
      });
    }

    return {
      url: canonicalUrl.toString(),
      title,
      lawFirm: lawFirm || null,
      brand: brand || null,
      entries,
      stateCode: meta.stateCode,
      year: meta.year,
      serial: meta.serial,
      syncedAt: new Date().toISOString()
    };
  }

  isPlausiblePayload(caseRow, payload) {
    if (!payload || !Array.isArray(payload.entries) || !payload.entries.length) {
      return Boolean(payload);
    }

    const caseFiledMs = parseIsoDateMs(caseRow?.date_filed);
    const datedEntries = payload.entries
      .map((entry) => ({ ...entry, filedAtMs: parseIsoDateMs(entry.filed_at) }))
      .filter((entry) => entry.filedAtMs !== null);

    if (caseFiledMs !== null && datedEntries.length) {
      const earliestMs = Math.min(...datedEntries.map((entry) => entry.filedAtMs));
      if (earliestMs < caseFiledMs - 24 * 60 * 60 * 1000) {
        return false;
      }
    }

    const expectedTokens = collectExpectedTokens(caseRow);
    if (!expectedTokens.length) {
      return true;
    }

    const sampleText = payload.entries
      .slice(0, 8)
      .map((entry) => String(entry.description || ""))
      .join(" ");
    const normalizedSample = normalizeLookupText(sampleText);
    const tokenHits = expectedTokens.filter((token) => normalizedSample.includes(token)).length;
    if (tokenHits > 0) {
      return true;
    }

    return !looksLikeForeignNamedEntity(sampleText);
  }

  async fetchJson(url, options = {}) {
    const text = await this.fetchText(url, {
      ...options,
      headers: {
        accept: "application/json, text/javascript, */*; q=0.01",
        ...(options.headers || {})
      }
    });

    return JSON.parse(text);
  }

  async fetchText(url, options = {}) {
    const waitMs = this.lastRequestAt + this.minIntervalMs - Date.now();
    if (waitMs > 0) {
      await wait(waitMs);
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeoutMs);

    let response;
    try {
      response = await fetch(url, {
        ...options,
        signal: controller.signal,
        headers: {
          "user-agent": "tro-case-watch/0.1 (+public enrichment)",
          accept: "text/html,application/xhtml+xml,application/json",
          ...(options.headers || {})
        }
      });
    } catch (error) {
      if (error?.name === "AbortError") {
        throw new Error(`WorldTRO request timed out after ${this.timeoutMs}ms`);
      }

      throw error;
    } finally {
      clearTimeout(timeoutId);
    }

    this.lastRequestAt = Date.now();

    const text = await response.text();
    if (!response.ok) {
      const error = new Error(`WorldTRO request failed: ${response.status}`);
      error.status = response.status;
      error.body = text;
      throw error;
    }

    return text;
  }
}
