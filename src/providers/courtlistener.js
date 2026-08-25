export class CourtListenerClient {
  constructor(config, pacerConfig = {}) {
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.apiToken = config.apiToken || "";
    this.enableDocketSync = Boolean(config.enableDocketSync);
    this.enableDocketAlerts = Boolean(config.enableDocketAlerts);
    this.recapFetchEnabled = Boolean(config.recapFetchEnabled);
    this.recapFetchPollIntervalMs = Math.max(500, Number(config.recapFetchPollIntervalMs || 2000));
    this.recapFetchMaxPollMs = Math.max(this.recapFetchPollIntervalMs, Number(config.recapFetchMaxPollMs || 12000));
    this.recapFetchShowPartiesAndCounsel = Boolean(config.recapFetchShowPartiesAndCounsel);
    this.pacerUsername = pacerConfig.loginId || "";
    this.pacerPassword = pacerConfig.password || "";
    this.pacerClientCode = pacerConfig.clientCode || "";
    this.rateLimitedUntil = 0;
    this.capabilities = {
      docket: Boolean(this.apiToken) && this.enableDocketSync,
      docketEntries: Boolean(this.apiToken) && this.enableDocketSync,
      docketAlerts: Boolean(this.apiToken) && this.enableDocketAlerts,
      recapFetch:
        Boolean(this.apiToken) &&
        this.recapFetchEnabled &&
        Boolean(this.pacerUsername) &&
        Boolean(this.pacerPassword)
    };
  }

  hasDocketAccess() {
    return this.capabilities.docket;
  }

  hasDocketEntriesAccess() {
    return this.capabilities.docketEntries;
  }

  hasDocketAlertAccess() {
    return this.capabilities.docketAlerts;
  }

  hasRecapFetchAccess() {
    return this.capabilities.recapFetch;
  }

  absoluteUrl(value) {
    if (!value) {
      return null;
    }

    if (value.startsWith("http://") || value.startsWith("https://")) {
      return value;
    }

    return `https://www.courtlistener.com${value}`;
  }

  async search({ query, cursorUrl = null, startDate, pageSize = 20 }) {
    const url = cursorUrl ? new URL(cursorUrl) : new URL(`${this.baseUrl}/search/`);

    if (!cursorUrl) {
      url.searchParams.set("type", "r");
      url.searchParams.set("q", query);
      url.searchParams.set("filed_after", startDate);
      url.searchParams.set("order_by", "dateFiled desc");
      url.searchParams.set("page_size", String(pageSize));
    }

    return this.fetchJson(url.toString(), { requiresAuth: false });
  }

  async fetchDocket(docketId) {
    return this.fetchJson(`${this.baseUrl}/dockets/${docketId}/`, { requiresAuth: true });
  }

  async fetchDocketEntries(docketId, pageSize = 100) {
    if (!this.capabilities.docketEntries) {
      return [];
    }

    let nextUrl = `${this.baseUrl}/docket-entries/?docket=${encodeURIComponent(docketId)}&page_size=${pageSize}`;
    const entries = [];

    while (nextUrl) {
      let payload;
      try {
        payload = await this.fetchJson(nextUrl, { requiresAuth: true });
      } catch (error) {
        if (error.status === 403) {
          this.capabilities.docketEntries = false;
          return [];
        }
        throw error;
      }
      entries.push(...(payload.results || []));
      nextUrl = payload.next;
      if (entries.length >= pageSize) {
        break;
      }
    }

    return entries;
  }

  async listDocketAlerts({ cursorUrl = null, docketId = null, pageSize = 100, orderBy = "-date_created" } = {}) {
    if (!this.hasDocketAlertAccess()) {
      return { count: 0, next: null, previous: null, results: [] };
    }

    const url = cursorUrl ? new URL(cursorUrl) : new URL(`${this.baseUrl}/docket-alerts/`);
    if (!cursorUrl) {
      if (Number(docketId) > 0) {
        url.searchParams.set("docket", String(docketId));
      }
      url.searchParams.set("page_size", String(pageSize));
      url.searchParams.set("order_by", orderBy);
    }

    return this.fetchJson(url.toString(), { requiresAuth: true });
  }

  async getDocketAlertsByDocket(docketId, { pageSize = 20 } = {}) {
    if (!this.hasDocketAlertAccess() || !Number(docketId)) {
      return [];
    }

    let nextUrl = null;
    const results = [];

    do {
      const payload = await this.listDocketAlerts({
        cursorUrl: nextUrl,
        docketId,
        pageSize
      });
      results.push(...(payload.results || []));
      nextUrl = payload.next || null;
    } while (nextUrl);

    return results;
  }

  async createDocketAlert(docketId) {
    if (!this.hasDocketAlertAccess() || !Number(docketId)) {
      return null;
    }

    const body = new URLSearchParams();
    body.set("docket", String(docketId));
    return this.fetchJson(`${this.baseUrl}/docket-alerts/`, {
      requiresAuth: true,
      method: "POST",
      body,
      contentType: "application/x-www-form-urlencoded; charset=utf-8"
    });
  }

  async updateDocketAlert(alertId, { alertType = 1 } = {}) {
    if (!this.hasDocketAlertAccess() || !Number(alertId)) {
      return null;
    }

    const body = new URLSearchParams();
    body.set("alert_type", String(alertType));
    return this.fetchJson(`${this.baseUrl}/docket-alerts/${encodeURIComponent(String(alertId))}/`, {
      requiresAuth: true,
      method: "PATCH",
      body,
      contentType: "application/x-www-form-urlencoded; charset=utf-8"
    });
  }

  async requestDocketViaRecapFetch({
    docketId = null,
    docketNumber = "",
    court = "",
    pacerCaseId = "",
    showPartiesAndCounsel = this.recapFetchShowPartiesAndCounsel
  } = {}) {
    if (!this.hasRecapFetchAccess()) {
      return null;
    }

    const body = new URLSearchParams();
    body.set("request_type", "1");
    body.set("pacer_username", this.pacerUsername);
    body.set("pacer_password", this.pacerPassword);

    if (this.pacerClientCode) {
      body.set("client_code", this.pacerClientCode);
    }

    if (showPartiesAndCounsel) {
      body.set("show_parties_and_counsel", "true");
    }

    if (Number(docketId) > 0) {
      body.set("docket", String(docketId));
    } else if (String(pacerCaseId || "").trim() && String(court || "").trim()) {
      body.set("pacer_case_id", String(pacerCaseId).trim());
      body.set("court", String(court).trim());
    } else if (String(docketNumber || "").trim() && String(court || "").trim()) {
      body.set("docket_number", String(docketNumber).trim());
      body.set("court", String(court).trim());
    } else {
      return null;
    }

    return this.fetchJson(`${this.baseUrl}/recap-fetch/`, {
      requiresAuth: true,
      method: "POST",
      body,
      contentType: "application/x-www-form-urlencoded; charset=utf-8"
    });
  }

  async fetchRecapFetchRequest(requestId) {
    if (!this.hasRecapFetchAccess() || !Number(requestId)) {
      return null;
    }

    return this.fetchJson(`${this.baseUrl}/recap-fetch/${encodeURIComponent(String(requestId))}/`, {
      requiresAuth: true
    });
  }

  async fetchJson(url, options = {}) {
    return this.fetchJsonWithRetry(url, options, 0);
  }

  async fetchJsonWithRetry(url, options, attempt) {
    if (Date.now() < this.rateLimitedUntil) {
      const error = new Error("CourtListener rate limit cooldown active");
      error.status = 429;
      error.retryAfterMs = this.rateLimitedUntil - Date.now();
      throw error;
    }

    const { requiresAuth, method = "GET", body = undefined, contentType = "" } = options || {};
    const headers = {
      accept: "application/json",
      "user-agent": "tro-case-watch/0.1"
    };

    if (contentType) {
      headers["content-type"] = contentType;
    }

    if (requiresAuth && this.apiToken) {
      headers.authorization = `Token ${this.apiToken}`;
    }

    try {
      const response = await fetch(url, { method, headers, body });
      const text = await response.text();

      if (!response.ok) {
        if (attempt < 2 && shouldRetryStatus(response.status)) {
          await delay(retryDelayMs(response.headers.get("retry-after"), attempt));
          return this.fetchJsonWithRetry(url, options, attempt + 1);
        }

        const error = new Error(`CourtListener request failed: ${response.status}`);
        error.status = response.status;
        error.body = text;
        error.requiresAuth = requiresAuth;
        error.retryAfterMs = retryDelayMs(response.headers.get("retry-after"), attempt);
        if (response.status === 429) {
          this.rateLimitedUntil = Date.now() + Math.max(error.retryAfterMs, 15 * 60_000);
        }
        throw error;
      }

      return JSON.parse(text);
    } catch (error) {
      if (attempt < 2 && shouldRetryError(error)) {
        await delay(retryDelayMs(null, attempt));
        return this.fetchJsonWithRetry(url, options, attempt + 1);
      }

      throw error;
    }
  }
}

export function extractCourtListenerWebhookDocketId(entry = {}) {
  if (Number(entry?.docket) > 0) {
    return String(Number(entry.docket));
  }

  if (entry?.docket) {
    const match = String(entry.docket).match(/dockets\/(\d+)/);
    if (match?.[1]) {
      return String(match[1]);
    }
  }

  if (Number(entry?.docket_id) > 0) {
    return String(Number(entry.docket_id));
  }

  if (entry?.docket_id) {
    return String(entry.docket_id);
  }

  return null;
}

function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function shouldRetryStatus(status) {
  return [408, 409, 425, 429, 500, 502, 503, 504].includes(Number(status));
}

function shouldRetryError(error) {
  return error instanceof TypeError || String(error?.message || "").includes("fetch failed");
}

function retryDelayMs(retryAfterHeader, attempt) {
  const retryAfter = Number.parseInt(String(retryAfterHeader || ""), 10);
  if (Number.isFinite(retryAfter) && retryAfter > 0) {
    return retryAfter * 1000;
  }

  return 1500 * (attempt + 1);
}
