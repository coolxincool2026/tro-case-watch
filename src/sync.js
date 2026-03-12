import crypto from "node:crypto";
import { buildTagsMarker, classifyCase, discoveryPresets } from "./queries.js";
import { docketLooksLike, normalizeDocket, normalizeText } from "./insights.js";

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

function deriveParties(result) {
  const parties = uniqueByNormalized(asArray(result.party));
  const caseName = String(valueOf(result.caseName, result.case_name_full) || "").trim();
  const captionPieces = caseName.split(/\s(?:v\.|vs\.)\s/i);
  const plaintiffFromCaption = captionPieces[0]?.trim() || "";
  const defendantFromCaption = captionPieces.slice(1).join(" v. ").trim();
  const plaintiffKey = normalizeText(plaintiffFromCaption).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();

  const plaintiffs = uniqueByNormalized([plaintiffFromCaption || parties[0]].filter(Boolean));
  const defendants = uniqueByNormalized(
    [defendantFromCaption, ...parties.filter((party) => {
      const partyKey = normalizeText(party).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
      return partyKey && partyKey !== plaintiffKey;
    })].filter(Boolean)
  );

  return {
    plaintiffs,
    defendants
  };
}

export class CaseSyncService {
  constructor({ config, store, courtListener, worldtro, pacerMonitor, pacer, translator }) {
    this.config = config;
    this.store = store;
    this.courtListener = courtListener;
    this.worldtro = worldtro;
    this.pacerMonitor = pacerMonitor;
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

    return {
      ...this.state,
      dashboard,
      backfill: this.getBackfillStatus(),
      providers: {
        courtlistener: {
          searchEnabled: true,
          docketEnabled: this.courtListener.hasDocketAccess(),
          docketEntriesEnabled: this.courtListener.hasDocketEntriesAccess() && !knownNoDocketEntries
        },
        worldtro: this.worldtro.getStatus(),
        pacermonitor: this.pacerMonitor.getStatus(),
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
      return this.getPublicStatus();
    }

    this.state.isRunning = true;
    this.state.currentMode = mode;
    this.state.lastStartedAt = new Date().toISOString();
    this.state.lastError = null;

    const runId = this.store.startSyncRun("system", mode);
    const stats = {
      pagesFetched: 0,
      casesUpserted: 0,
      docketEntriesUpserted: 0,
      docketCasesSynced: 0,
      worldtroCasesSynced: 0,
      translationsApplied: 0,
      notes: []
    };

    try {
      let successfulPresets = 0;
      for (const preset of discoveryPresets) {
        try {
          const result = await this.syncPreset(preset, mode);
          stats.pagesFetched += result.pagesFetched;
          stats.casesUpserted += result.casesUpserted;
          stats.docketEntriesUpserted += result.docketEntriesUpserted;
          successfulPresets += 1;
        } catch (error) {
          stats.notes.push(`CourtListener 搜索预设 ${preset.label} 失败：${error.message}`);
        }
      }

      if (!successfulPresets) {
        throw new Error("CourtListener 搜索预设全部失败");
      }

      try {
        const docketResult = await this.syncCourtListenerDockets();
        stats.docketCasesSynced += docketResult.syncedCases;
        if (docketResult.note) {
          stats.notes.push(docketResult.note);
        }
      } catch (error) {
        stats.notes.push(`CourtListener docket 补抓跳过：${error.message}`);
      }

      try {
        const worldtroResult = await this.syncWorldtroRecent(mode);
        stats.worldtroCasesSynced += worldtroResult.syncedCases;
        if (worldtroResult.note) {
          stats.notes.push(worldtroResult.note);
        }
      } catch (error) {
        stats.notes.push(`WorldTRO 补源跳过：${error.message}`);
      }

      try {
        const translationResult = await this.translator.translatePending();
        stats.translationsApplied += translationResult.translated || 0;
      } catch (error) {
        stats.notes.push(`翻译链路跳过：${error.message}`);
      }

      try {
        const pacerMonitorResult = await this.pacerMonitor.syncRecent();
        stats.notes.push(pacerMonitorResult.note);
      } catch (error) {
        stats.notes.push(`PACERMonitor 跳过：${error.message}`);
      }

      try {
        const pacerResult = await this.pacer.syncRecent();
        stats.notes.push(pacerResult.note);
      } catch (error) {
        stats.notes.push(`PACER 跳过：${error.message}`);
      }

      this.store.finishSyncRun(runId, "succeeded", stats);
      this.state.lastFinishedAt = new Date().toISOString();
      this.state.lastStats = stats;
      return this.getPublicStatus();
    } catch (error) {
      this.state.lastError = error.message;
      this.store.finishSyncRun(runId, "failed", stats, `${error.message}\n${error.body || ""}`.trim());
      throw error;
    } finally {
      this.state.isRunning = false;
      this.state.currentMode = null;
    }
  }

  async importLookup(term) {
    const rawTerm = String(term || "").trim();
    if (!rawTerm) {
      return { imported: 0, matched: 0 };
    }

    const candidates = [];
    const queries = this.buildLookupQueries(rawTerm);

    for (const query of queries) {
      const payload = await this.courtListener.search({
        query,
        startDate: this.config.sync.startDate,
        pageSize: 20
      });

      const matches = (payload.results || []).filter((result) => this.lookupMatches(rawTerm, result));
      for (const match of matches) {
        if (!candidates.some((item) => item.docket_id === match.docket_id)) {
          candidates.push(match);
        }
      }

      if (candidates.length >= 10) {
        break;
      }
    }

    const ingest = this.ingestSearchResults(candidates, {
      tags: []
    });

    return {
      imported: ingest.casesUpserted,
      matched: candidates.length
    };
  }

  buildLookupQueries(term) {
    const queries = [];
    const docketTerm = normalizeDocket(term);

    if (docketLooksLike(term)) {
      queries.push(`docketNumber:${docketTerm}`);
      queries.push(`"${docketTerm}"`);
    }

    queries.push(`"${term}"`);
    queries.push(term);

    return [...new Set(queries)];
  }

  lookupMatches(term, result) {
    const docketTerm = normalizeDocket(term);
    const resultDocket = normalizeDocket(result.docketNumber);
    const text = normalizeText([result.caseName, result.docketNumber, result.court].join(" | "));

    if (docketTerm) {
      if (resultDocket === docketTerm || resultDocket.endsWith(docketTerm)) {
        return true;
      }
    }

    return text.includes(normalizeText(term));
  }

  async enrichCaseWithWorldtro(caseId, { force = false } = {}) {
    const caseRow = this.store.getCase(caseId);
    if (!caseRow || !this.worldtro.enabled || !caseRow.insights?.is_seller_case) {
      return { enriched: false, reason: "not-applicable" };
    }

    const syncedAt = caseRow.raw?.worldtro?.syncedAt ? Date.parse(caseRow.raw.worldtro.syncedAt) : 0;
    const staleAfterMs = this.config.worldtro.staleAfterHours * 60 * 60 * 1000;
    if (!force && syncedAt && Date.now() - syncedAt < staleAfterMs) {
      return { enriched: false, reason: "fresh" };
    }

    return this.syncSingleWorldtroCase(caseRow);
  }

  async syncWorldtroRecent(mode = "recent") {
    if (!this.worldtro.enabled) {
      return {
        syncedCases: 0,
        note: "WorldTRO 公开补源已关闭。"
      };
    }

    const maxCases =
      mode === "backfill" ? this.config.worldtro.backfillMaxCasesPerRun : this.config.worldtro.maxCasesPerRun;
    const candidates = this.store.getCasesNeedingWorldtroSync(
      maxCases,
      this.config.worldtro.staleAfterHours
    );

    let syncedCases = 0;
    for (const caseRow of candidates) {
      try {
        const result = await this.syncSingleWorldtroCase(caseRow);
        if (result.enriched) {
          syncedCases += 1;
        }
      } catch (error) {
        return {
          syncedCases,
          note: `WorldTRO 补源中止：${error.message}`
        };
      }
    }

    return {
      syncedCases,
      note: syncedCases
        ? `WorldTRO 本轮补齐 ${syncedCases} 个案件的公开时间线。`
        : "WorldTRO 本轮没有待补源案件。"
    };
  }

  async syncSingleWorldtroCase(caseRow) {
    const payload = await this.worldtro.enrichCase(caseRow);
    if (!payload || !payload.entries.length) {
      return { enriched: false, reason: "not-found" };
    }

    const mergedRaw = {
      ...(caseRow.raw || {}),
      worldtro: {
        url: payload.url,
        title: payload.title,
        lawFirm: payload.lawFirm,
        brand: payload.brand,
        rowCount: payload.entries.length,
        stateCode: payload.stateCode,
        year: payload.year,
        serial: payload.serial,
        syncedAt: payload.syncedAt
      }
    };

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
      source_urls: [...(caseRow.source_urls || []), payload.url],
      plaintiffs: caseRow.plaintiffs || [],
      defendants: caseRow.defendants || [],
      recent_activity_summary: payload.entries[0]?.description || caseRow.recent_activity_summary,
      latest_docket_filed_at: payload.entries[0]?.filed_at || caseRow.latest_docket_filed_at,
      latest_docket_number: payload.entries[0]?.row_number || caseRow.latest_docket_number,
      docket_count: Math.max(caseRow.docket_count || 0, payload.entries.length),
      last_seen_at: new Date().toISOString(),
      last_synced_at: new Date().toISOString(),
      last_docket_sync_at: caseRow.last_docket_sync_at,
      raw: mergedRaw
    });

    for (const entry of payload.entries) {
      const digest = crypto
        .createHash("sha1")
        .update(`${payload.url}|${entry.row_number}|${entry.filed_at}|${entry.description}`)
        .digest("hex")
        .slice(0, 16);

      this.store.upsertDocketEntry({
        case_id: caseRow.id,
        source_entry_key: `worldtro:${caseRow.id}:${entry.row_number}:${digest}`,
        primary_source: "worldtro",
        source_entry_id: String(entry.row_number || ""),
        document_type: "WorldTRO Entry",
        entry_number: String(entry.row_number || ""),
        document_number: String(entry.row_number || ""),
        filed_at: entry.filed_at,
        description: entry.description,
        absolute_url: payload.url,
        is_available: 0,
        page_count: null,
        pacer_doc_id: null,
        raw: entry,
        last_synced_at: new Date().toISOString()
      });
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
        startDate: this.config.sync.startDate
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
      const savedCase = this.store.upsertCase({
        source_case_key: `courtlistener:${result.docket_id}`,
        primary_source: "courtlistener",
        source_case_id: String(result.docket_id),
        courtlistener_docket_id: Number(result.docket_id),
        pacer_case_id: valueOf(result.pacer_case_id),
        court_id: valueOf(result.court_id),
        court_name: valueOf(result.court),
        case_name: valueOf(result.caseName, result.case_name_full),
        docket_number: valueOf(result.docketNumber),
        date_filed: valueOf(result.dateFiled),
        date_terminated: valueOf(result.dateTerminated),
        cause: valueOf(result.cause),
        nature_of_suit: valueOf(result.nature_of_suit, result.suitNature),
        status: result.dateTerminated ? "terminated" : "open",
        tags_marker: buildTagsMarker(tags),
        docket_url: this.courtListener.absoluteUrl(result.docket_absolute_url),
        source_urls: [this.courtListener.absoluteUrl(result.docket_absolute_url)].filter(Boolean),
        plaintiffs: parties.plaintiffs,
        defendants: parties.defendants,
        recent_activity_summary: valueOf(latestDoc?.short_description, latestDoc?.description),
        latest_docket_filed_at: valueOf(latestDoc?.entry_date_filed, latestDoc?.date_filed, result.dateFiled),
        latest_docket_number: valueOf(latestDoc?.entry_number, latestDoc?.document_number),
        docket_count: documents.length,
        last_seen_at: valueOf(result.meta?.timestamp, new Date().toISOString()),
        last_synced_at: new Date().toISOString(),
        raw: result
      });

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

  async syncCourtListenerDockets() {
    if (!this.courtListener.hasDocketAccess()) {
      return {
        syncedCases: 0,
        note: "CourtListener docket API 未开启或没有 token，当前只同步公开 search 结果和嵌入式 recap 文档。"
      };
    }

    const candidates = this.store.getCasesNeedingDocketSync(this.config.courtListener.docketMaxCasesPerRun);
    let syncedCases = 0;
    let metadataOnlyMode = false;

    for (const caseRow of candidates) {
      const docketId = caseRow.courtlistener_docket_id;
      if (!docketId) {
        continue;
      }

      try {
        const docket = await this.courtListener.fetchDocket(docketId);
        const entries = await this.courtListener.fetchDocketEntries(docketId);
        if (!entries.length && !this.courtListener.hasDocketEntriesAccess()) {
          metadataOnlyMode = true;
        }

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
          source_urls: caseRow.source_urls || [],
          plaintiffs: caseRow.plaintiffs || [],
          defendants: caseRow.defendants || [],
          recent_activity_summary: caseRow.recent_activity_summary,
          latest_docket_filed_at: valueOf(docket.date_last_filing, caseRow.latest_docket_filed_at),
          latest_docket_number: caseRow.latest_docket_number,
          docket_count: Math.max(caseRow.docket_count || 0, entries.length),
          last_seen_at: new Date().toISOString(),
          last_synced_at: new Date().toISOString(),
          last_docket_sync_at: new Date().toISOString(),
          raw: docket
        });

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
            last_synced_at: new Date().toISOString()
          });
        }

        this.store.touchCaseDocketSync(caseRow.id);
        syncedCases += 1;
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
        ? `CourtListener docket 元数据本轮更新 ${syncedCases} 个案件，但当前 token 无权访问 docket-entries。`
        : syncedCases
          ? `CourtListener docket API 本轮补齐 ${syncedCases} 个案件。`
          : "CourtListener docket API 已启用，但本轮没有待补齐案件。"
    };
  }
}
