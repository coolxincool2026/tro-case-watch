const state = {
  page: 1,
  pageSize: 15,
  category: "watchlist",
  court: "",
  search: "",
  selectedCaseId: null,
  pageCount: 1
};

const caseList = document.querySelector("#case-list");
const detailPanel = document.querySelector("#detail-panel");
const heroStats = document.querySelector("#hero-stats");
const casesSummary = document.querySelector("#cases-summary");
const pageIndicator = document.querySelector("#page-indicator");
const courtFilter = document.querySelector("#court-filter");
const lookupForm = document.querySelector("#lookup-form");
const lookupInput = document.querySelector("#lookup-input");
const prevPageButton = document.querySelector("#prev-page");
const nextPageButton = document.querySelector("#next-page");
const refreshButton = document.querySelector("#refresh-button");
const contentGrid = document.querySelector(".content-grid");
const copyWechatButton = document.querySelector("#copy-wechat-button");
const statusPollMs = 5 * 60 * 1000;
const apiBase =
  window.location.hostname === "www.trotracker.com" ? "https://tro-case-watch-production.up.railway.app" : "";

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

function request(path, options) {
  return fetch(`${apiBase}${path}`, options).then(async (response) => {
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

function renderHero(status) {
  const totals = status.dashboard?.totals || {};
  const recentSync = status.dashboard?.recentSync;
  const cards = [
    heroCard("TRO诉讼/Schedule A案件数", `${totals.tro_cases || 0} / ${totals.schedule_a_cases || 0}`),
    heroCard("卖家相关", totals.seller_cases || 0),
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

  if (/worldtro/i.test(type)) {
    return "Docket Entry";
  }

  if (/pacer document/i.test(type)) {
    return "Docket Document";
  }

  return type.replace(/worldtro/gi, "Docket");
}

function renderCaseRow(item) {
  const insights = item.insights || {};
  const plaintiff = insights.brand_name || insights.plaintiff_name || item.case_name || "未命名案件";
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
        <span>最近节点 ${formatDate(item.latest_docket_filed_at || item.date_filed)}</span>
      </div>
    </button>
  `;
}

function renderCases(payload) {
  state.pageCount = payload.pageCount || 1;
  renderCourtOptions(payload.courts || []);

  const messages = [`共 ${payload.total} 个案件，当前第 ${payload.page} / ${payload.pageCount} 页`];
  if (payload.categoryRelaxed) {
    messages.push("已优先返回精确案号匹配结果");
  }
  if (payload.liveImported?.imported) {
    messages.push(`已实时导入 ${payload.liveImported.imported} 个匹配案件`);
  }
  if (payload.lookupError) {
    messages.push(`实时导入失败：${payload.lookupError}`);
  }

  casesSummary.textContent = messages.join(" · ");
  pageIndicator.textContent = `第 ${payload.page} / ${payload.pageCount} 页`;

  if (!payload.items.length) {
    caseList.innerHTML = `
      <article class="case-row">
        <h3>没有命中案件</h3>
        <p>你可以继续输完整案号，系统会尝试现场补抓公开案件。</p>
      </article>
    `;
    detailPanel.innerHTML = `
      <div class="panel-head">
        <h2>Docket 展示页</h2>
        <p>当前搜索没有命中。</p>
      </div>
    `;
    return;
  }

  if (!payload.items.some((item) => item.id === state.selectedCaseId)) {
    state.selectedCaseId = payload.items[0].id;
    loadCaseDetail(state.selectedCaseId);
  }

  caseList.innerHTML = payload.items.map(renderCaseRow).join("");
  caseList.querySelectorAll("[data-case-id]").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedCaseId = Number(button.dataset.caseId);
      loadCaseDetail(state.selectedCaseId);
      loadCases();
    });
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
  const timelineSummary = entries.length ? timelineSourceSummary(entries) : "当前只有案件级摘要";
  const summaryCards = [
    { label: "原告/品牌", value: insights.brand_name || insights.plaintiff_name || "待识别" },
    { label: "原告律所", value: insights.lead_law_firm || "待识别" },
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
        <span>最近节点 ${formatDate(item.latest_docket_filed_at || item.date_filed)}</span>
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
        </div>
        <div class="timeline-toolbar-stats">
          <span class="tag-pill">最近节点 ${formatDate(item.latest_docket_filed_at || item.date_filed)}</span>
          <span class="tag-pill">${timelineSummary}</span>
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
        </div>
      </div>
      ${
        entries.length
          ? entries
              .map(
                (entry) => {
                  const signals = timelineSignals(entry)
                    .map((value) => `<span class="highlight-pill">${value}</span>`)
                    .join("");

                  return `
                  <article class="timeline-item">
                    <div class="timeline-item-head">
                      <time>${formatDate(entry.filed_at)}</time>
                      <span class="status-pill neutral">${timelineSourceLabel(entry)}</span>
                    </div>
                    <h3>${entry.document_number || entry.entry_number || "No.?"} · ${displayEntryType(entry)}</h3>
                    ${signals ? `<div class="tag-row timeline-tags">${signals}</div>` : ""}
                    <p>${entry.description || "无可显示文本"}</p>
                    ${entry.description_zh ? `<p class="timeline-zh">${entry.description_zh}</p>` : ""}
                    <p class="timeline-source-note">来源已归档到本站，如需转换成中文，请在Chrome浏览器右键点击翻译</p>
                  </article>
                `;
                }
              )
              .join("")
          : `
            <article class="timeline-item">
              <time>${formatDate(item.latest_docket_filed_at || item.date_filed)}</time>
              <h3>最近进展</h3>
              <p>${item.recent_activity_summary || "当前只有案件级元数据，没有补到逐条 docket。"}
              </p>
              <p class="focus-text">当前公开来源只补到了案件级摘要。等详细 docket 补进来后，这里会直接显示站内时间线。</p>
              ${item.recent_activity_summary_zh ? `<p class="timeline-zh">${item.recent_activity_summary_zh}</p>` : ""}
            </article>
          `
      }
    </section>
  `;
}

async function loadStatus() {
  const status = await request("/api/sync/status");
  renderHero(status);
}

async function loadCases() {
  lookupInput.value = state.search;

  const params = new URLSearchParams({
    category: state.category,
    court: state.court,
    search: state.search,
    page: String(state.page),
    pageSize: String(state.pageSize)
  });

  const payload = await request(`/api/cases?${params.toString()}`);
  renderCases(payload);
}

async function loadCaseDetail(caseId) {
  const item = await request(`/api/cases/${caseId}`);
  renderDetail(item);
}

lookupForm.addEventListener("submit", (event) => {
  event.preventDefault();
  state.search = lookupInput.value.trim();
  state.page = 1;
  loadCases().then(revealResultsIfNeeded).catch(console.error);
});

courtFilter.addEventListener("change", (event) => {
  state.court = String(event.target.value || "");
  state.page = 1;
  loadCases().then(revealResultsIfNeeded).catch(console.error);
});

prevPageButton.addEventListener("click", () => {
  if (state.page <= 1) {
    return;
  }
  state.page -= 1;
  loadCases();
});

nextPageButton.addEventListener("click", () => {
  if (state.page >= state.pageCount) {
    return;
  }
  state.page += 1;
  loadCases();
});

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
    await Promise.all([loadStatus(), loadCases()]);
  } finally {
    refreshButton.disabled = false;
    refreshButton.textContent = "立即刷新";
  }
});

if (copyWechatButton) {
  copyWechatButton.addEventListener("click", async () => {
    const defaultLabel = "点击添加微信";

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
  const [casesResult, statusResult] = await Promise.allSettled([loadCases(), loadStatus()]);
  if (casesResult.status === "rejected") {
    throw casesResult.reason;
  }

  if (statusResult.status === "rejected") {
    console.error(statusResult.reason);
  }

  window.setInterval(() => {
    loadStatus().catch(() => {});
  }, statusPollMs);
}

boot().catch((error) => {
  console.error(error);
  caseList.innerHTML = `<article class="case-row"><h3>加载失败</h3><p>${error.message}</p></article>`;
});
