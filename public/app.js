const state = {
  page: 1,
  pageSize: 15,
  category: "watchlist",
  court: "",
  search: "",
  selectedCaseId: null,
  pageCount: 1
};

const detailCache = new Map();
const detailCacheTtlMs = 2 * 60 * 1000;
let currentCasesPayload = null;
let detailRequestToken = 0;
let casesRequestToken = 0;
let detailRefreshTimer = null;
let prefetchDetailsTimer = null;
let lookupPendingRetryTimer = null;
let newDocketPollTimer = null;
let currentDetailSnapshot = null; // { caseId, latestDocketFiledAt, docketCount }
const inflightDetailRequests = new Map();
const caseRoutePattern = /^\/case\/(\d+)\/?$/;
let latestStatusPayload = null;

const caseList = document.querySelector("#case-list");
const detailPanel = document.querySelector("#detail-panel");
const heroStats = document.querySelector("#hero-stats");
const casesSummary = document.querySelector("#cases-summary");
const pageIndicator = document.querySelector("#page-indicator");
const courtFilter = document.querySelector("#court-filter");
const lookupForm = document.querySelector("#lookup-form");
const lookupInput = document.querySelector("#lookup-input");
const lookupSubmitButton = lookupForm?.querySelector('button[type="submit"]');
const troDailyUpdates = document.querySelector("#tro-daily-updates");
const prevPageButton = document.querySelector("#prev-page");
const nextPageButton = document.querySelector("#next-page");
const refreshButton = document.querySelector("#refresh-button");
const contentGrid = document.querySelector(".content-grid");
const copyWechatButton = document.querySelector("#copy-wechat-button");
const contactChips = Array.from(document.querySelectorAll(".contact-methods .contact-chip"));
const qrCards = Array.from(document.querySelectorAll(".frost-card"));
const statusPollMs = 5 * 60 * 1000;
const troDailyUpdatesPollMs = 30 * 60 * 1000;
const apiBase = "";
let lookupThawTimer = null;
const publicApiToken =
  document.querySelector('meta[name="tt-public-api-token"]')?.getAttribute("content")?.trim() || "";
const bootstrapPayload = (() => {
  try {
    const raw = document.querySelector("#tt-bootstrap")?.textContent?.trim();
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
})();

function detailCacheKey(caseId, { full = false } = {}) {
  return `${Number(caseId)}:${full ? "full" : "light"}`;
}

function getRouteCaseId() {
  const match = window.location.pathname.match(caseRoutePattern);
  if (!match) {
    return null;
  }

  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

function setCaseRoute(caseId, { replace = false } = {}) {
  if (!window.history?.pushState) {
    return;
  }

  const url = new URL(window.location.href);
  url.pathname = caseId ? `/case/${caseId}` : "/";
  url.hash = caseId ? "detail-panel" : "";
  const method = replace ? "replaceState" : "pushState";
  window.history[method](null, "", url);
}

function formatDate(value) {
  if (!value) {
    return "未知";
  }

  return new Date(value).toLocaleDateString("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
}

function toDateMs(value) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function latestTimelineFiledAt(entries = []) {
  return entries.reduce((latest, entry) => {
    const entryFiledAt = entry?.filed_at || null;
    if (!entryFiledAt) {
      return latest;
    }

    if (!latest || toDateMs(entryFiledAt) > toDateMs(latest)) {
      return entryFiledAt;
    }

    return latest;
  }, null);
}

function request(path, options) {
  const headers = new Headers(options?.headers || {});
  if (publicApiToken) {
    headers.set("x-tt-public-token", publicApiToken);
  }
  headers.set("x-requested-with", "trotracker-web");

  return fetch(`${apiBase}${path}`, {
    ...options,
    headers
  }).then(async (response) => {
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return response.json();
  });
}

function toneClass(status) {
  return status?.tone || "neutral";
}

function heroCard(label, value) {
  return `
    <article class="stat-card">
      <span>${label}</span>
      <strong>${value}</strong>
    </article>
  `;
}

function formatTroDailyUpdatesTime(value) {
  if (!value) {
    return "今日";
  }

  return new Date(value).toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function troDailyUpdatesMeta(item = {}) {
  const sources = Array.isArray(item.sources) ? item.sources.filter(Boolean) : [];
  if (sources.length > 1) {
    return `站内整理 · 多方公开动态`;
  }
  if (sources.length === 1) {
    return "站内整理";
  }
  return "站内整理";
}

function renderTroDailyUpdatesLoading() {
  if (!troDailyUpdates) {
    return;
  }

  troDailyUpdates.innerHTML = `
    <div class="tro-briefing-card is-loading">
      <div class="tro-briefing-head">
        <strong>TRO 今日动态</strong>
        <span>整理中</span>
      </div>
      <p class="tro-briefing-empty">正在载入今日重点动态。</p>
    </div>
  `;
}

function renderTroDailyUpdates(payload = {}) {
  if (!troDailyUpdates) {
    return;
  }

  const items = Array.isArray(payload.items) ? payload.items.slice(0, 3) : [];
  if (!items.length) {
    troDailyUpdates.innerHTML = `
      <div class="tro-briefing-card">
        <div class="tro-briefing-head">
          <strong>TRO 今日动态</strong>
          <span>${formatTroDailyUpdatesTime(payload.updatedAt)}</span>
        </div>
        <p class="tro-briefing-empty">今日还没有整理出可展示的重点动态。</p>
      </div>
    `;
    return;
  }

  const loopItems = items.length > 1 ? items.concat(items) : items;
  const durationSeconds = Math.max(12, items.length * 7);
  troDailyUpdates.innerHTML = `
    <div class="tro-briefing-card">
      <div class="tro-briefing-head">
        <strong>TRO 今日动态</strong>
        <span>${formatTroDailyUpdatesTime(payload.updatedAt)}</span>
      </div>
      <div class="tro-briefing-marquee">
        <div class="tro-briefing-track ${items.length > 1 ? "is-animated" : ""}" style="--tro-briefing-duration:${durationSeconds}s;">
          ${loopItems
            .map(
              (item) => `
                <a class="tro-briefing-item" href="${item.href || "/#wechat-contact"}">
                  <span class="tro-briefing-source">${troDailyUpdatesMeta(item)}</span>
                  <strong>${item.title || "今日动态"}</strong>
                </a>
              `
            )
            .join("")}
        </div>
      </div>
    </div>
  `;
}

function renderHero(status) {
  latestStatusPayload = status;
  const totals = status.dashboard?.totals || {};
  const recentSync = status.dashboard?.recentSync;
  const cards = [
    heroCard("站内总库/当前监控池", `${totals.total_cases || 0} / ${totals.watchlist_cases || 0}`),
    heroCard("TRO诉讼/Schedule A案件数", `${totals.tro_cases || 0} / ${totals.schedule_a_cases || 0}`),
    heroCard("今日新增收录", totals.today_added_watchlist || 0),
    heroCard(
      "最近同步",
      recentSync?.finished_at
        ? new Date(recentSync.finished_at).toLocaleTimeString("zh-CN")
        : recentSync?.status === "running"
          ? "同步中"
          : "未同步"
    )
  ];

  heroStats.innerHTML = cards.join("");
}

function isDetailPanelVisible() {
  const rect = detailPanel.getBoundingClientRect();
  return rect.top < window.innerHeight * 0.8 && rect.bottom > window.innerHeight * 0.2;
}

function jumpToDetailPanel() {
  if (!detailPanel) {
    return;
  }

  if (!isDetailPanelVisible() || window.innerWidth < 1180) {
    detailPanel.scrollIntoView({
      behavior: "smooth",
      block: "start"
    });
  }

  if (window.location.hash !== "#detail-panel" && window.history?.replaceState) {
    const url = new URL(window.location.href);
    url.hash = "detail-panel";
    window.history.replaceState(null, "", url);
  }
}

function revealResultsIfNeeded() {
  if (!contentGrid) {
    return;
  }

  const top = contentGrid.getBoundingClientRect().top;
  if (top > window.innerHeight * 0.68) {
    contentGrid.scrollIntoView({
      behavior: "smooth",
      block: "start"
    });
  }
}

function thawLookupSurface() {
  if (!lookupForm) {
    return;
  }

  lookupForm.classList.remove("is-frozen");
  lookupForm.classList.add("is-thawed");
}

function thawQrSurface({ persist = false } = {}) {
  if (!qrCards.length) {
    return;
  }

  qrCards.forEach((card) => {
    card.classList.add("is-thawed");
    if (persist) {
      card.classList.add("is-forced-thawed");
    }
  });
}

function thawInteractiveSurfaces({ persistQr = false } = {}) {
  thawLookupSurface();
  thawQrSurface({ persist: persistQr });
}

function runLookupSearch() {
  state.search = lookupInput.value.trim();
  state.page = 1;
  loadCases({
    autoSelectFirst: shouldAutoOpenFirstResult(),
    preserveSelection: false
  })
    .then((payload) => {
      if (payload) {
        revealResultsIfNeeded();
      }
    })
    .catch(console.error);
}

function looksLikeDirectDocketSearch(value = "") {
  const search = String(value || "").trim();
  return /^\d{5,6}$/.test(search) || /^(?:\d+:)?\d{2}-cv-\d{5,6}$/i.test(search);
}

function shouldAutoOpenFirstResult() {
  return looksLikeDirectDocketSearch(state.search);
}

function clearLookupPendingRetry() {
  if (lookupPendingRetryTimer) {
    window.clearTimeout(lookupPendingRetryTimer);
    lookupPendingRetryTimer = null;
  }
}

function scheduleLookupPendingRetry(searchValue, attempt = 0) {
  const delays = [1200, 2800, 5000];
  if (attempt >= delays.length) {
    return;
  }

  clearLookupPendingRetry();
  lookupPendingRetryTimer = window.setTimeout(async () => {
    lookupPendingRetryTimer = null;
    if (state.search !== searchValue) {
      return;
    }

    try {
      const payload = await loadCases({
        autoSelectFirst: false,
        preserveSelection: true,
        silent: true
      });
      if (payload?.lookupPending) {
        scheduleLookupPendingRetry(searchValue, attempt + 1);
      }
    } catch {
      scheduleLookupPendingRetry(searchValue, attempt + 1);
    }
  }, delays[attempt]);
}

function playLookupThawSequence(onComplete) {
  if (!lookupForm) {
    onComplete?.();
    return;
  }

  const isFrozen = lookupForm.classList.contains("is-frozen");
  thawQrSurface();

  if (!isFrozen) {
    onComplete?.();
    return;
  }

  window.clearTimeout(lookupThawTimer);
  lookupForm.classList.add("is-thawing");
  onComplete?.();

  lookupThawTimer = window.setTimeout(() => {
    lookupForm.classList.remove("is-thawing");
    thawLookupSurface();
  }, 180);
}

function bindQrCardFrost() {
  if (!qrCards.length) {
    return;
  }

  qrCards.forEach((card) => {
    card.classList.remove("is-thawed");

    card.addEventListener("pointerenter", () => {
      card.classList.add("is-thawed");
    });

    card.addEventListener("pointerleave", () => {
      if (card.classList.contains("is-forced-thawed")) {
        return;
      }
      card.classList.remove("is-thawed");
    });

    card.addEventListener("focusin", () => {
      card.classList.add("is-thawed");
    });

    card.addEventListener("focusout", () => {
      if (card.classList.contains("is-forced-thawed")) {
        return;
      }
      card.classList.remove("is-thawed");
    });
  });
}

function renderCourtOptions(courts) {
  const selected = state.court;
  courtFilter.innerHTML = [`<option value="">全部法院</option>`]
    .concat(
      courts.map(
        (court) =>
          `<option value="${court.court_id || ""}" ${court.court_id === selected ? "selected" : ""}>${court.court_name} (${court.total})</option>`
      )
    )
    .join("");
}

function renderCasesLoading(message = "正在检索案件，请稍候。") {
  casesSummary.textContent = message;
  caseList.innerHTML = `
    <article class="case-row">
      <h3>正在载入案件列表</h3>
      <p>已收到你的检索请求，正在同步站内结果。</p>
    </article>
  `;
}

function caseStatusBadge(item) {
  const status = item.insights?.status || {};
  return `<span class="status-pill ${toneClass(status)}">${status.label || "持续观察"}</span>`;
}

function tagPills(item) {
  const values = [...new Set(item.insights?.badges || [])];
  return values.map((value) => `<span class="tag-pill">${value}</span>`).join("");
}

function highlightPills(item) {
  const values = item.insights?.highlights || [];
  return values.map((value) => `<span class="highlight-pill">${value}</span>`).join("");
}

function displayEntryType(entry) {
  const type = String(entry.document_type || "").trim();
  if (!type) {
    return "Docket Entry";
  }

  if (/pacer document/i.test(type)) {
    return "Docket Document";
  }

  return type;
}

const PLAINTIFF_TAB_TERM_REPLACEMENTS = [
  ["专利", "Patent"],
  ["商标", "Trademark"],
  ["版权", "Copyright"],
  ["原告", "Plaintiff"],
  ["品牌", "Brand"],
  ["律所", "Counsel"],
  ["待识别", "Unknown"]
];

function translatePlaintiffTabValue(value) {
  let text = String(value ?? "").trim();
  for (const [source, target] of PLAINTIFF_TAB_TERM_REPLACEMENTS) {
    text = text.replaceAll(source, target);
  }
  return text;
}

function formatPlaintiffBrandValue(item) {
  const insights = item.insights || {};
  const baseValue = translatePlaintiffTabValue(insights.brand_name || insights.plaintiff_name || "待识别");
  const caseTypeLabel = String(insights.ip_case_type_label || "").trim();

  if (!caseTypeLabel) {
    return baseValue;
  }

  if (baseValue === "Unknown") {
    return caseTypeLabel;
  }

  return `${baseValue} · ${caseTypeLabel}`;
}

function usesBrandFirstPresentation(item) {
  return String(item?.presentation_mode || "").trim().toLowerCase() === "brand_first";
}

function formatCaseListTitle(item) {
  if (usesBrandFirstPresentation(item)) {
    return formatPlaintiffBrandValue(item);
  }

  const insights = item.insights || {};
  return insights.brand_name || insights.plaintiff_name || item.case_name || "未命名案件";
}

function renderCaseRow(item) {
  const insights = item.insights || {};
  const plaintiff = formatCaseListTitle(item);
  const lawFirm = insights.lead_law_firm || "待识别";
  const summary =
    insights.narrative || item.recent_activity_summary || "当前没有抓到可展示的 docket 摘要。";

  return `
    <button class="case-row ${item.id === state.selectedCaseId ? "is-active" : ""}" type="button" data-case-id="${item.id}">
      <div class="case-row-top">
        <div>
          <span class="case-docket">${item.docket_number || "No docket number"}</span>
          <h3>${plaintiff}</h3>
          <div class="case-meta">
            <span>${item.court_name || "Unknown Court"}</span>
            <span>Filed ${formatDate(item.date_filed)}</span>
            <span>原告律所 ${lawFirm}</span>
          </div>
        </div>
        <div class="status-row">
          ${caseStatusBadge(item)}
        </div>
      </div>
      <div class="tag-row">${tagPills(item)}</div>
      <div class="tag-row highlights-row">${highlightPills(item)}</div>
      <p class="case-summary">${summary}</p>
      <div class="case-foot">
        <span>原告/品牌 ${plaintiff}</span>
        <span>被告数 ${insights.defendant_count || 0}</span>
        <span>最近案级节点 ${formatDate(item.latest_docket_filed_at || item.date_filed)}</span>
      </div>
    </button>
  `;
}

function renderCases(payload) {
  currentCasesPayload = payload;
  state.pageCount = payload.pageCount || 1;
  renderCourtOptions(payload.courts || []);

  const messages = [];
  if (!state.search) {
    const totalCases = Number(latestStatusPayload?.dashboard?.totals?.total_cases || 0);
    messages.push(`当前监控池共 ${payload.total} 个案件`);
    if (totalCases > 0) {
      messages.push(`站内总库共 ${totalCases} 个案件`);
    }
    messages.push(`当前第 ${payload.page} / ${payload.pageCount} 页`);
  } else {
    messages.push(`命中 ${payload.total} 个案件，当前第 ${payload.page} / ${payload.pageCount} 页`);
  }
  if (payload.categoryRelaxed) {
    const relaxedLabel = {
      all: "全库",
      watchlist: "监控池",
      seller_watch: "卖家监控",
      tro: "TRO",
      schedule_a: "Schedule A"
    }[payload.relaxedCategory] || "更宽范围";
    messages.push(`当前分类没有直接命中，已放宽到${relaxedLabel}结果`);
  }
  if (payload.liveImported?.imported) {
    messages.push(`已实时导入 ${payload.liveImported.imported} 个匹配案件`);
  }
  if (payload.lookupPending) {
    messages.push("正在后台补抓该案公开 docket，结果会自动刷新");
  }
  if (payload.lookupError) {
    messages.push(`实时导入失败：${payload.lookupError}`);
  }

  casesSummary.textContent = messages.join(" · ");
  pageIndicator.textContent = `第 ${payload.page} / ${payload.pageCount} 页`;

  if (!payload.items.length) {
    caseList.innerHTML = `
      <article class="case-row">
        <h3>${payload.lookupPending ? "正在后台补抓案件" : "没有命中案件"}</h3>
        <p>${payload.lookupPending ? "系统已经开始后台补抓公开 docket，结果会自动刷新。" : "你可以继续输完整案号，系统会尝试现场补抓公开案件。"}</p>
      </article>
    `;
    detailPanel.innerHTML = `
      <div class="panel-head">
        <h2>Docket 展示页</h2>
        <p>${payload.lookupPending ? "后台补抓进行中，稍后会自动刷新结果。" : "当前搜索没有命中。"}</p>
      </div>
    `;
    return;
  }

  const routeCaseId = getRouteCaseId();
  const selectedItem = payload.items.find((item) => item.id === state.selectedCaseId) || null;
  const fallbackItem = payload.items[0] || null;

  if ((!state.selectedCaseId || !selectedItem) && fallbackItem && !routeCaseId) {
    if (shouldAutoOpenFirstResult()) {
      state.selectedCaseId = fallbackItem.id;
      loadCaseDetail(state.selectedCaseId, {
        summaryItem: fallbackItem,
        updateRoute: true
      }).catch(console.error);
    } else {
      detailPanel.innerHTML = `
        <div class="panel-head">
          <h2>Docket 展示页</h2>
          <p>已加载搜索结果，点击左侧案件查看最新 docket 时间线。</p>
        </div>
      `;
    }
  }

  caseList.innerHTML = payload.items.map(renderCaseRow).join("");
  updateActiveCaseRow();
  caseList.querySelectorAll("[data-case-id]").forEach((button) => {
    const caseId = Number(button.dataset.caseId);
    button.addEventListener("mouseenter", () => {
      prefetchCaseDetail(caseId);
    });
    button.addEventListener("focus", () => {
      prefetchCaseDetail(caseId);
    });
    button.addEventListener("click", () => {
      state.selectedCaseId = caseId;
      const summaryItem = payload.items.find((item) => item.id === state.selectedCaseId) || null;
      updateActiveCaseRow();
      loadCaseDetail(state.selectedCaseId, {
        summaryItem,
        focus: true,
        updateRoute: true
      }).catch(console.error);
    });
  });
}

function updateActiveCaseRow() {
  caseList.querySelectorAll("[data-case-id]").forEach((button) => {
    button.classList.toggle("is-active", Number(button.dataset.caseId) === state.selectedCaseId);
  });
}

function adviceByStatus(statusKey) {
  if (statusKey === "tro_granted") {
    return "通常说明法院已经签 TRO，卖家更需要优先核对送达、平台冻结范围、以及 PI 听证时间。";
  }

  if (statusKey === "pi") {
    return "案件已经进入 Preliminary Injunction 阶段，冻结和约束通常比最初 TRO 更稳定。";
  }

  if (statusKey === "settlement") {
    return "最近 docket 出现 settlement / dismissal 迹象，适合继续观察是否有被告陆续撤出。";
  }

  if (statusKey === "closed") {
    return "案件已出现撤案、终结或大面积 voluntary dismissal 文书。";
  }

  if (statusKey === "service") {
    return "当前更像送达/应诉准备阶段，重点看被告名单、送达回证和后续 TRO / PI。";
  }

  return "当前仍处于持续观察阶段，建议重点盯 TRO、PI、Settlement、Dismissal 这几类节点。";
}

function timelineSourceLabel(entry) {
  return entry.timeline_label || "Docket 时间线";
}

function timelineSignals(entry) {
  const text = `${entry.document_type || ""} ${entry.description || ""}`.toLowerCase();
  const markers = [];

  if (/(temporary restraining order|\btro\b)/i.test(text)) {
    markers.push("TRO");
  }
  if (/(preliminary injunction|\bpi\b)/i.test(text)) {
    markers.push("PI");
  }
  if (/(settlement|stipulation)/i.test(text)) {
    markers.push("和解");
  }
  if (/(dismiss|terminated|closing)/i.test(text)) {
    markers.push("结案");
  }
  if (/(service|serve|summons)/i.test(text)) {
    markers.push("送达");
  }

  return [...new Set(markers)];
}

function timelineSourceSummary(entries) {
  const summary = new Map();

  entries.forEach((entry) => {
    const label = timelineSourceLabel(entry);
    summary.set(label, (summary.get(label) || 0) + 1);
  });

  return [...summary.entries()]
    .map(([label, count]) => `${label} ${count} 条`)
    .join(" · ");
}

function renderDetail(item) {
  const insights = item.insights || {};
  const entries = item.entries || [];
  const entriesSequential = Boolean(item.entries_sequential);
  const entriesTruncated = Boolean(item.entries_truncated);
  const newestTimelineFiledAt = latestTimelineFiledAt(entries);
  const hasCaseLevelUpdateGap =
    Boolean(item.latest_docket_filed_at) &&
    Boolean(newestTimelineFiledAt) &&
    toDateMs(item.latest_docket_filed_at) > toDateMs(newestTimelineFiledAt);
  const hydrationPending = item.hydration_pending?.pending;
  const timelineSummary = entries.length ? timelineSourceSummary(entries) : "当前只有案件级摘要";
  const summaryCards = [
    { label: "Plaintiff / Brand", value: formatPlaintiffBrandValue(item) },
    { label: "Plaintiff Counsel", value: translatePlaintiffTabValue(insights.lead_law_firm || "待识别") },
    { label: "程序阶段", value: insights.status?.label || "持续观察" },
    { label: "被告数量", value: insights.defendant_count || 0 },
    { label: "站内 docket", value: entries.length ? `${entries.length} 条` : "仅案件摘要" }
  ];
  const defendantPreview = insights.defendant_preview?.length
    ? insights.defendant_preview.join(" / ")
    : "当前没有可展示的被告样本。";
  const highlights = (insights.highlights || [])
    .map((value) => `<span class="highlight-pill">${value}</span>`)
    .join("");

  detailPanel.innerHTML = `
    <div class="panel-head detail-header">
      <span class="case-docket">${item.docket_number || "No docket number"}</span>
      <h3>${item.case_name || insights.brand_name || "未命名案件"}</h3>
      <div class="status-row">
        ${caseStatusBadge(item)}
        ${tagPills(item)}
      </div>
      <div class="detail-meta">
        <span>${item.court_name || "Unknown Court"}</span>
        <span>Filed ${formatDate(item.date_filed)}</span>
        <span>最近案级节点 ${formatDate(item.latest_docket_filed_at || item.date_filed)}</span>
      </div>
      <div class="summary-grid">
        ${summaryCards
          .map(
            (card) => `
              <article class="summary-card">
                <span>${card.label}</span>
                <strong>${card.value}</strong>
              </article>
            `
          )
          .join("")}
      </div>
      <div class="timeline-toolbar">
        <div class="timeline-toolbar-copy">
          <strong>站内直接查看案件时间线</strong>
          <p>以下 docket 已同步展示在本站，默认不要求跳转到外部来源才能看进展。</p>
          ${hydrationPending ? '<p class="focus-text">当前条目还在后台继续补抓，页面会自动刷新补进来的 docket。</p>' : ""}
        </div>
        <div class="timeline-toolbar-stats">
          <span class="tag-pill">最近案级节点 ${formatDate(item.latest_docket_filed_at || item.date_filed)}</span>
          ${entries.length ? `<span class="tag-pill">最新站内 docket ${formatDate(newestTimelineFiledAt)}</span>` : ""}
          <span class="tag-pill">${timelineSummary}</span>
          ${entriesTruncated ? '<span class="tag-pill">完整时间线继续加载中</span>' : ""}
        </div>
      </div>
    </div>

    <section class="detail-note">
      <p>${adviceByStatus(insights.status?.key)}</p>
      <p>${insights.narrative || "当前仍在持续观察。"} 被告样本：${defendantPreview}</p>
      <div class="tag-row detail-highlight-row">${highlights || '<span class="tag-pill">暂未识别到明确程序节点</span>'}</div>
    </section>

    <section class="timeline">
      <div class="timeline-head">
        <div>
          <h3>站内 Docket 时间线</h3>
          <p>按时间倒序归档公开可见 docket 文本，适合卖家直接判断当前是否已签 TRO、是否进入 PI、是否有和解或结案信号。</p>
          ${entriesTruncated ? '<p class="focus-text">为了先把页面打开，当前先展示最近一段时间线；完整 docket 会继续在后台补齐。</p>' : ""}
        </div>
      </div>
      ${
        hasCaseLevelUpdateGap
          ? `
            <article class="timeline-item">
              <div class="timeline-item-head">
                <time>${formatDate(item.latest_docket_filed_at)}</time>
                <span class="status-pill neutral">案件级更新</span>
              </div>
              <h3>发现源已抓到更晚的案级更新，逐条 docket 仍在补抓</h3>
              <p>${item.recent_activity_summary || "当前已经记录到案件级更新信号，但详细 docket 条目还没完全同步进站内时间线。"}</p>
              <p class="focus-text">所以列表里会显示更晚的日期，但下方逐条 docket 可能暂时还停在更早日期。</p>
              ${item.recent_activity_summary_zh ? `<p class="timeline-zh">${item.recent_activity_summary_zh}</p>` : ""}
            </article>
          `
          : ""
      }
      ${
        (() => {
          const visibleEntries = entries.filter(e => e.description || e.description_zh);
          if (!visibleEntries.length) {
            return `
              <article class="timeline-item">
                <time>${formatDate(item.latest_docket_filed_at || item.date_filed)}</time>
                <h3>最近进展</h3>
                <p>${item.recent_activity_summary || "当前只有案件级元数据，没有补到逐条 docket。"}</p>
                <p class="focus-text">当前公开来源只补到了案件级摘要。等详细 docket 补进来后，这里会直接显示站内时间线。</p>
                ${item.recent_activity_summary_zh ? `<p class="timeline-zh">${item.recent_activity_summary_zh}</p>` : ""}
              </article>
            `;
          }
          return visibleEntries
            .map((entry) => {
              const signals = timelineSignals(entry)
                .map((value) => `<span class="highlight-pill">${value}</span>`)
                .join("");
              const numPrefix = entriesSequential && (entry.document_number || entry.entry_number)
                ? (entry.document_number || entry.entry_number) + " · "
                : "";
              return `
                <article class="timeline-item">
                  <div class="timeline-item-head">
                    <time>${formatDate(entry.filed_at)}</time>
                    <span class="status-pill neutral">${timelineSourceLabel(entry)}</span>
                  </div>
                  <h3>${numPrefix}${displayEntryType(entry)}</h3>
                  ${signals ? `<div class="tag-row timeline-tags">${signals}</div>` : ""}
                  <p>${entry.description || entry.description_zh}</p>
                  ${entry.description_zh && entry.description ? `<p class="timeline-zh">${entry.description_zh}</p>` : ""}
                  <p class="timeline-source-note">来源已归档到本站，如需转换成中文，请在Chrome浏览器右键点击翻译</p>
                </article>
              `;
            })
            .join("");
        })()
      }
    </section>
  `;

  // Track snapshot for new-docket polling
  if (item.id) {
    currentDetailSnapshot = {
      caseId: item.id,
      latestDocketFiledAt: item.latest_docket_filed_at || null,
      docketCount: Number(item.docket_count || 0)
    };
    startNewDocketPoll(item.id);
  }
}

function renderDetailLoading(item = {}) {
  const docket = item.docket_number || "No docket number";
  const title = item.case_name || item.insights?.brand_name || "正在载入案件详情";
  const court = item.court_name || "正在读取法院信息";
  const filedAt = formatDate(item.date_filed);

  detailPanel.innerHTML = `
    <div class="panel-head detail-header">
      <span class="case-docket">${docket}</span>
      <h3>${title}</h3>
      <div class="detail-meta">
        <span>${court}</span>
        <span>Filed ${filedAt}</span>
      </div>
    </div>
    <div class="detail-empty is-loading">
      <h3>正在打开 Docket 详细页</h3>
      <p>已响应你的点击，正在加载该案件的站内时间线。</p>
    </div>
  `;
}

function getCachedDetail(caseId) {
  const fullCached = detailCache.get(detailCacheKey(caseId, { full: true }));
  if (fullCached) {
    if (Date.now() - fullCached.cachedAt > detailCacheTtlMs) {
      detailCache.delete(detailCacheKey(caseId, { full: true }));
    } else {
      return fullCached.item;
    }
  }

  const cached = detailCache.get(detailCacheKey(caseId, { full: false }));
  if (!cached) {
    return null;
  }

  if (Date.now() - cached.cachedAt > detailCacheTtlMs) {
    detailCache.delete(detailCacheKey(caseId, { full: false }));
    return null;
  }

  return cached.item;
}

function shouldCacheDetail(item) {
  const entriesCount = item.entries?.length || 0;
  return !(item.insights?.badges || []).includes("跨境卖家相关") || entriesCount >= 12;
}

function shouldRefreshIncompleteDetail(item) {
  return Boolean(item?.hydration_pending?.pending);
}

function clearDetailRefreshTimer() {
  if (detailRefreshTimer) {
    window.clearTimeout(detailRefreshTimer);
    detailRefreshTimer = null;
  }
}

function clearNewDocketPollTimer() {
  if (newDocketPollTimer) {
    window.clearInterval(newDocketPollTimer);
    newDocketPollTimer = null;
  }
}

function startNewDocketPoll(caseId) {
  clearNewDocketPollTimer();
  const pollIntervalMs = 3 * 60 * 1000; // every 3 minutes
  newDocketPollTimer = window.setInterval(async () => {
    if (document.visibilityState !== "visible") return;
    if (state.selectedCaseId !== caseId) {
      clearNewDocketPollTimer();
      return;
    }
    if (!currentDetailSnapshot || currentDetailSnapshot.caseId !== caseId) return;
    try {
      const res = await request(`/api/cases/${caseId}`);
      if (state.selectedCaseId !== caseId) return;
      const newFiledAt = res.latest_docket_filed_at || null;
      const newCount = Number(res.docket_count || 0);
      const snap = currentDetailSnapshot;
      if (
        (newFiledAt && newFiledAt !== snap.latestDocketFiledAt) ||
        (newCount > snap.docketCount)
      ) {
        showNewDocketBanner(caseId, res);
      }
    } catch {
      // ignore poll errors
    }
  }, pollIntervalMs);
}

function showNewDocketBanner(caseId, freshSummary) {
  // Remove any existing banner first
  const existing = detailPanel.querySelector(".new-docket-banner");
  if (existing) existing.remove();

  const banner = document.createElement("div");
  banner.className = "new-docket-banner";
  const latestDate = formatDate(freshSummary.latest_docket_filed_at);
  banner.innerHTML = `
    <span>发现新 docket（最近节点：${latestDate}），点击刷新查看</span>
    <button class="new-docket-refresh-btn" type="button">刷新</button>
    <button class="new-docket-dismiss-btn" type="button">✕</button>
  `;
  banner.querySelector(".new-docket-refresh-btn").addEventListener("click", () => {
    banner.remove();
    detailCache.delete(detailCacheKey(caseId, { full: false }));
    detailCache.delete(detailCacheKey(caseId, { full: true }));
    loadCaseDetail(caseId, { summaryItem: freshSummary }).catch(console.error);
  });
  banner.querySelector(".new-docket-dismiss-btn").addEventListener("click", () => {
    // Update snapshot so the banner doesn't re-appear for this same update
    if (currentDetailSnapshot && currentDetailSnapshot.caseId === caseId) {
      currentDetailSnapshot.latestDocketFiledAt = freshSummary.latest_docket_filed_at || currentDetailSnapshot.latestDocketFiledAt;
      currentDetailSnapshot.docketCount = Math.max(Number(freshSummary.docket_count || 0), currentDetailSnapshot.docketCount);
    }
    banner.remove();
  });

  // Insert at the top of the detail panel
  detailPanel.insertBefore(banner, detailPanel.firstChild);
}

function cacheDetailItem(caseId, item) {
  if (shouldCacheDetail(item)) {
    detailCache.set(detailCacheKey(caseId, { full: !item.entries_truncated }), {
      item,
      cachedAt: Date.now()
    });
    return;
  }

  detailCache.delete(detailCacheKey(caseId, { full: false }));
  detailCache.delete(detailCacheKey(caseId, { full: true }));
}

async function fetchCaseDetail(caseId, { force = false, full = false } = {}) {
  const requestKey = detailCacheKey(caseId, { full });
  let directCached = null;
  if (full) {
    const fullCached = detailCache.get(requestKey);
    if (fullCached) {
      if (Date.now() - fullCached.cachedAt > detailCacheTtlMs) {
        detailCache.delete(requestKey);
      } else {
        directCached = fullCached.item;
      }
    }
  }
  const cached = force ? null : (full ? directCached : getCachedDetail(caseId));
  if (cached) {
    return cached;
  }

  if (inflightDetailRequests.has(requestKey)) {
    return inflightDetailRequests.get(requestKey);
  }

  const requestPromise = request(`/api/cases/${caseId}${full ? "?full=1" : ""}`)
    .then((item) => {
      cacheDetailItem(caseId, item);
      return item;
    })
    .finally(() => {
      inflightDetailRequests.delete(requestKey);
    });

  inflightDetailRequests.set(requestKey, requestPromise);
  return requestPromise;
}

function prefetchCaseDetail(caseId) {
  if (!caseId || getCachedDetail(caseId) || inflightDetailRequests.has(detailCacheKey(caseId, { full: false }))) {
    return;
  }

  fetchCaseDetail(caseId).catch(() => {});
}

function scheduleFullDetailUpgrade(caseId, requestToken) {
  window.setTimeout(async () => {
    if (requestToken !== detailRequestToken || state.selectedCaseId !== caseId) {
      return;
    }

    try {
      const item = await fetchCaseDetail(caseId, { full: true });
      if (requestToken !== detailRequestToken || state.selectedCaseId !== caseId) {
        return;
      }

      renderDetail(item);
      if (shouldRefreshIncompleteDetail(item)) {
        scheduleIncompleteDetailRefresh(caseId, requestToken);
      }
    } catch {
      // Keep the lightweight detail visible if the full timeline takes longer.
    }
  }, 120);
}

function scheduleIncompleteDetailRefresh(caseId, requestToken, attempt = 0) {
  const delays = [2500, 6000, 12000];
  if (attempt >= delays.length) {
    return;
  }

  clearDetailRefreshTimer();
  detailRefreshTimer = window.setTimeout(async () => {
    if (requestToken !== detailRequestToken || state.selectedCaseId !== caseId) {
      return;
    }

    try {
      detailCache.delete(detailCacheKey(caseId, { full: false }));
      detailCache.delete(detailCacheKey(caseId, { full: true }));
      const item = await fetchCaseDetail(caseId, { force: true, full: true });
      if (requestToken !== detailRequestToken || state.selectedCaseId !== caseId) {
        return;
      }

      renderDetail(item);
      if (shouldRefreshIncompleteDetail(item)) {
        scheduleIncompleteDetailRefresh(caseId, requestToken, attempt + 1);
      }
    } catch {
      scheduleIncompleteDetailRefresh(caseId, requestToken, attempt + 1);
    }
  }, delays[attempt]);
}

function queueIdle(callback) {
  if (typeof window.requestIdleCallback === "function") {
    return window.requestIdleCallback(callback, { timeout: 900 });
  }
  return window.setTimeout(callback, 260);
}

function cancelIdle(taskId) {
  if (!taskId) {
    return;
  }

  if (typeof window.cancelIdleCallback === "function") {
    window.cancelIdleCallback(taskId);
    return;
  }

  window.clearTimeout(taskId);
}

function prefetchVisibleCaseDetails(items = [], maxItems = 2) {
  cancelIdle(prefetchDetailsTimer);
  if (Number(maxItems || 0) <= 0) {
    return;
  }
  const targets = items.slice(0, maxItems).map((item) => item.id).filter(Boolean);
  if (!targets.length) {
    return;
  }

  prefetchDetailsTimer = queueIdle(() => {
    prefetchDetailsTimer = null;
    targets.forEach((caseId) => prefetchCaseDetail(caseId));
  });
}

async function loadStatus() {
  const status = await request("/api/sync/status");
  renderHero(status);
}

async function loadTroDailyUpdates() {
  renderTroDailyUpdatesLoading();
  const payload = await request("/api/tro-daily-updates");
  renderTroDailyUpdates(payload);
}

async function loadCases({ autoSelectFirst = false, preserveSelection = true, silent = false } = {}) {
  const requestToken = ++casesRequestToken;
  clearLookupPendingRetry();
  lookupInput.value = state.search;
  if (!silent) {
    renderCasesLoading(state.search ? `正在检索 ${state.search} ...` : "正在刷新案件列表...");
  }

  const params = new URLSearchParams({
    category: state.category,
    court: state.court,
    search: state.search,
    page: String(state.page),
    pageSize: String(state.pageSize)
  });

  const payload = await request(`/api/cases?${params.toString()}`);
  if (requestToken !== casesRequestToken) {
    return null;
  }

  if (autoSelectFirst || !preserveSelection) {
    state.selectedCaseId = null;
  }

  renderCases(payload);
  if (payload.items?.length) {
    prefetchVisibleCaseDetails(payload.items, shouldAutoOpenFirstResult() ? 1 : 0);
  }
  if (payload.lookupPending && state.search) {
    scheduleLookupPendingRetry(state.search);
  }
  return payload;
}

async function loadCaseDetail(caseId, { summaryItem = null, focus = false, updateRoute = false } = {}) {
  const requestToken = ++detailRequestToken;
  clearDetailRefreshTimer();
  const cached = getCachedDetail(caseId);

  if (focus) {
    jumpToDetailPanel();
  }

  if (updateRoute) {
    setCaseRoute(caseId);
  }

  if (cached) {
    if (requestToken === detailRequestToken) {
      renderDetail(cached);
      if (cached.entries_truncated) {
        scheduleFullDetailUpgrade(caseId, requestToken);
      }
      if (shouldRefreshIncompleteDetail(cached)) {
        scheduleIncompleteDetailRefresh(caseId, requestToken);
      }
    }
    return;
  }

  if (summaryItem) {
    renderDetailLoading(summaryItem);
  }

  const item = await fetchCaseDetail(caseId);
  if (requestToken !== detailRequestToken) {
    return;
  }

  renderDetail(item);
  if (item.entries_truncated) {
    scheduleFullDetailUpgrade(caseId, requestToken);
  }
  if (shouldRefreshIncompleteDetail(item)) {
    scheduleIncompleteDetailRefresh(caseId, requestToken);
  }
}

lookupForm.addEventListener("submit", (event) => {
  event.preventDefault();
  playLookupThawSequence(runLookupSearch);
});

if (lookupSubmitButton) {
  lookupSubmitButton.addEventListener("pointerdown", () => {
    if (lookupForm?.classList.contains("is-frozen")) {
      lookupForm.classList.add("is-thawing");
    }
  });
}

contactChips.forEach((chip) => {
  chip.addEventListener("pointerdown", () => {
    thawInteractiveSurfaces({ persistQr: true });
  });

  chip.addEventListener("focus", () => {
    thawInteractiveSurfaces({ persistQr: true });
  });
});

courtFilter.addEventListener("change", (event) => {
  state.court = String(event.target.value || "");
  state.page = 1;
  loadCases({
    autoSelectFirst: shouldAutoOpenFirstResult(),
    preserveSelection: false
  }).then((payload) => {
    if (payload) {
      revealResultsIfNeeded();
    }
  }).catch(console.error);
});

prevPageButton.addEventListener("click", () => {
  if (state.page <= 1) {
    return;
  }
  state.page -= 1;
  state.selectedCaseId = null;
  loadCases({
    autoSelectFirst: shouldAutoOpenFirstResult(),
    preserveSelection: false
  }).catch(console.error);
});

nextPageButton.addEventListener("click", () => {
  if (state.page >= state.pageCount) {
    return;
  }
  state.page += 1;
  state.selectedCaseId = null;
  loadCases({
    autoSelectFirst: shouldAutoOpenFirstResult(),
    preserveSelection: false
  }).catch(console.error);
});

if (refreshButton) {
  refreshButton.addEventListener("click", async () => {
    refreshButton.disabled = true;
    refreshButton.textContent = "同步中...";
    try {
      await request("/api/admin/sync", {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify({ mode: "recent" })
      });
      await Promise.all([
        loadStatus(),
        loadCases({
          preserveSelection: true
        })
      ]);
    } finally {
      refreshButton.disabled = false;
      refreshButton.textContent = "立即刷新";
    }
  });
}

if (copyWechatButton) {
  copyWechatButton.addEventListener("click", async () => {
    const defaultLabel = "加微信组团和解";

    try {
      await navigator.clipboard.writeText("mylearnedfriend");
      copyWechatButton.textContent = "微信号已复制";
    } catch (error) {
      copyWechatButton.textContent = "微信号：mylearnedfriend";
    }

    window.setTimeout(() => {
      copyWechatButton.textContent = defaultLabel;
    }, 1800);
  });
}

async function boot() {
  bindQrCardFrost();
  if ("scrollRestoration" in window.history) {
    window.history.scrollRestoration = "manual";
  }
  const routeCaseId = getRouteCaseId();
  if (routeCaseId) {
    state.selectedCaseId = routeCaseId;
    renderDetailLoading({
      docket_number: `Case #${routeCaseId}`,
      case_name: "正在载入案件详情"
    });
  } else {
    window.scrollTo({
      top: 0,
      left: 0,
      behavior: "auto"
    });
  }

  if (bootstrapPayload?.status) {
    renderHero(bootstrapPayload.status);
  }

  if (bootstrapPayload?.cases) {
    renderCases(bootstrapPayload.cases);
  } else {
    await loadCases({
      autoSelectFirst: !routeCaseId,
      preserveSelection: Boolean(routeCaseId)
    });
  }

  if (!bootstrapPayload?.status) {
    window.setTimeout(() => {
      queueIdle(() => {
        loadStatus().catch(console.error);
      });
    }, 300);
  }

  if (bootstrapPayload?.cases) {
    window.setTimeout(() => {
      if (document.visibilityState !== "visible") {
        return;
      }
      queueIdle(() => {
        loadCases({
          preserveSelection: true,
          silent: true
        }).catch(() => {});
      });
    }, 45_000);
  }
  window.setTimeout(() => {
    queueIdle(() => {
      loadTroDailyUpdates().catch(() => {});
    });
  }, 2200);

  if (routeCaseId) {
    const summaryItem = currentCasesPayload?.items?.find((item) => item.id === routeCaseId) || null;
    loadCaseDetail(routeCaseId, {
      summaryItem,
      focus: true
    }).catch(console.error);
  }

  window.setInterval(() => {
    if (document.visibilityState !== "visible") {
      return;
    }
    loadStatus().catch(() => {});
  }, statusPollMs);

  window.setInterval(() => {
    if (document.visibilityState !== "visible") {
      return;
    }
    loadTroDailyUpdates().catch(() => {});
  }, troDailyUpdatesPollMs);
}

window.addEventListener("popstate", () => {
  const routeCaseId = getRouteCaseId();
  if (routeCaseId) {
    state.selectedCaseId = routeCaseId;
    updateActiveCaseRow();
    const summaryItem = currentCasesPayload?.items?.find((item) => item.id === routeCaseId) || null;
    loadCaseDetail(routeCaseId, {
      summaryItem,
      focus: true
    }).catch(console.error);
    return;
  }

  if (currentCasesPayload?.items?.length) {
    state.selectedCaseId = currentCasesPayload.items[0].id;
    updateActiveCaseRow();
    loadCaseDetail(state.selectedCaseId, {
      summaryItem: currentCasesPayload.items[0],
      focus: true
    }).catch(console.error);
  }
});

boot().catch((error) => {
  console.error(error);
  caseList.innerHTML = `<article class="case-row"><h3>加载失败</h3><p>${error.message}</p></article>`;
});
