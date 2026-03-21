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

const DISTRICT_DIRECTION_MAP = {
  N: "Northern",
  S: "Southern",
  E: "Eastern",
  W: "Western",
  C: "Central",
  M: "Middle"
};

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

function laterIso(left, right) {
  if (!left) {
    return right || null;
  }

  if (!right) {
    return left || null;
  }

  return String(left).localeCompare(String(right)) >= 0 ? left : right;
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

function buildCourtDocketKey(courtId, docketNumber) {
  const normalizedDocket = normalizeDocket(docketNumber);
  if (!normalizedDocket) {
    return "";
  }

  const normalizedCourt = normalizeCourtLookupText(courtId);
  if (!normalizedCourt) {
    return "";
  }

  return `${normalizedCourt}|${normalizedDocket}`;
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

export class CaseSyncService {
  constructor({ config, store, courtFeeds, lawFirms, courtListener, worldtro, pacerMonitor, pacer, translator }) {
    this.config = config;
    this.store = store;
    this.courtFeeds = courtFeeds;
    this.lawFirms = lawFirms;
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
        lawfirms: this.lawFirms.getStatus(),
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

    const runId = this.store.claimSyncRun("system", mode);
    if (!runId) {
      this.state.isRunning = false;
      this.state.currentMode = null;
      return this.getPublicStatus();
    }
    const stats = {
      courtFeedCasesUpserted: 0,
      courtFeedEntriesUpserted: 0,
      courtFeedLookups: 0,
      lawFirmCasesUpserted: 0,
      lawFirmEntriesUpserted: 0,
      lawFirmLookups: 0,
      pagesFetched: 0,
      casesUpserted: 0,
      docketEntriesUpserted: 0,
      docketCasesSynced: 0,
      worldtroCasesSynced: 0,
      translationsApplied: 0,
      notes: []
    };

    try {
      return await this.store.batchMutations(async () => {
      let discoverySourceAvailable = false;
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

      if (!successfulPresets && !discoverySourceAvailable) {
        throw new Error("CourtListener 搜索预设全部失败");
      }

      try {
        const docketResult = await this.syncCourtListenerDockets(mode);
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
        const pacerMonitorResult = await this.syncPacerMonitorRecent(mode);
        if (pacerMonitorResult.note) {
          stats.notes.push(pacerMonitorResult.note);
        }
      } catch (error) {
        stats.notes.push(`PACERMonitor 补源跳过：${error.message}`);
      }

      try {
        const translationResult = await this.translator.translatePending();
        stats.translationsApplied += translationResult.translated || 0;
      } catch (error) {
        stats.notes.push(`翻译链路跳过：${error.message}`);
      }

      try {
        const pacerResult = await this.pacer.syncRecent();
        stats.notes.push(pacerResult.note);
      } catch (error) {
        stats.notes.push(`PACER 跳过：${error.message}`);
      }

      this.store.finishSyncRun(runId, "succeeded", stats);
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
      this.store.finishSyncRun(runId, "failed", stats, `${error.message}\n${error.body || ""}`.trim());
      throw error;
    } finally {
      this.state.isRunning = false;
      this.state.currentMode = null;
    }
  }

  async importLookup(term, { courtName = "", caseName = "" } = {}) {
    const rawTerm = String(term || "").trim();
    if (!rawTerm) {
      return { imported: 0, matched: 0 };
    }

    const candidates = [];
    const queries = this.buildLookupQueries(rawTerm, { courtName, caseName });

    for (const query of queries) {
      const payload = await this.courtListener.search({
        query,
        startDate: this.config.sync.startDate,
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

    const ingest = this.ingestSearchResults(candidates, {
      tags: []
    });

    return {
      imported: ingest.casesUpserted,
      matched: candidates.length
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

  buildCourtFeedCaseIndex() {
    const rows = this.store.getHydratedCases(this.config.sync.startDate);
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

  shouldTrackCourtFeedItem(item, existingCase, tags) {
    if (!/\b\d{2}-cv-\d{3,6}\b/i.test(String(item.docketNumber || ""))) {
      return false;
    }

    if (existingCase) {
      return true;
    }

    return tags.length > 0;
  }

  ingestCourtFeedItems(feedResult, caseIndex) {
    let casesUpserted = 0;
    let docketEntriesUpserted = 0;
    const lookupCandidates = new Map();
    const timestamp = new Date().toISOString();

    for (const item of feedResult.items || []) {
      const primaryKey = buildCourtDocketKey(item.courtId, item.docketNumber);
      const fallbackKey = buildCourtDocketKey(item.courtName, item.docketNumber);
      const existingCase = caseIndex.get(primaryKey) || caseIndex.get(fallbackKey) || null;
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
          `courtfeed:${item.courtId}:${item.caseId || item.reportCaseId || normalizeDocket(item.docketNumber)}`,
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
        }))
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

    if (item.sourceId === "sriplaw" || item.sourceId === "gbc") {
      return tags.length > 0 || asArray(item.entries).length > 0;
    }

    return tags.length > 0;
  }

  ingestLawFirmItems(sourceResult, caseIndex) {
    let casesUpserted = 0;
    let docketEntriesUpserted = 0;
    const lookupCandidates = new Map();
    const timestamp = new Date().toISOString();

    for (const item of sourceResult.items || []) {
      const primaryKey = buildCourtDocketKey(item.courtId, item.docketNumber);
      const fallbackKey = buildCourtDocketKey(item.courtName, item.docketNumber);
      const existingCase =
        caseIndex.get(primaryKey) ||
        caseIndex.get(fallbackKey) ||
        ((!item.courtId && !item.courtName) ? this.store.findCaseByDocketNumber(item.docketNumber, this.config.sync.startDate) : null) ||
        null;
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
          `${item.sourceId}:${item.courtId || normalizeLookupText(item.courtName) || "unknown"}:${normalizeDocket(item.docketNumber)}`,
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

      casesUpserted += 1;

      for (const entry of asArray(item.entries)) {
        const entryDigest = crypto
          .createHash("sha1")
          .update(
            [
              item.caseUrl,
              entry.sourceEntryId,
              entry.entryNumber,
              entry.documentNumber,
              entry.absoluteUrl,
              entry.description
            ].join("|")
          )
          .digest("hex")
          .slice(0, 16);

        const savedEntry = this.store.upsertDocketEntry({
          case_id: savedCase.id,
          source_entry_key: `${item.sourceId}:${savedCase.id}:${entryDigest}`,
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
        }
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
      lookupCandidates
    };
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

      const caseIndex = this.buildCourtFeedCaseIndex();
      const lookupCandidates = new Map();
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
            caseIndex
          );
          casesUpserted += ingest.casesUpserted;
          docketEntriesUpserted += ingest.docketEntriesUpserted;

          for (const [key, value] of ingest.lookupCandidates.entries()) {
            if (!lookupCandidates.has(key)) {
              lookupCandidates.set(key, value);
            }
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

      const note =
        successfulFeeds || failedFeeds
          ? `官方法院 RSS 本轮巡检 ${successfulFeeds} 个法院${failedFeeds ? `，${failedFeeds} 个法院源暂时失败` : ""}；补进 ${casesUpserted} 条案件更新、${docketEntriesUpserted} 条官方 docket${lookupResult.lookupsTriggered ? `，并触发 ${lookupResult.lookupsTriggered} 次 CourtListener 精确补抓` : ""}。`
          : "官方法院 RSS 当前没有已配置法院。";

      return {
        successfulFeeds,
        failedFeeds,
        casesUpserted,
        docketEntriesUpserted,
        lookupsTriggered: lookupResult.lookupsTriggered,
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
      const lookupCandidates = new Map();
      let successfulSources = 0;
      let failedSources = 0;
      let casesUpserted = 0;
      let docketEntriesUpserted = 0;

      for (const source of this.lawFirms.listSources()) {
        try {
          const sourceResult = await this.lawFirms.fetchRecentForSource(source);
          const ingest = this.ingestLawFirmItems(sourceResult, caseIndex);
          casesUpserted += ingest.casesUpserted;
          docketEntriesUpserted += ingest.docketEntriesUpserted;

          for (const [key, value] of ingest.lookupCandidates.entries()) {
            if (!lookupCandidates.has(key)) {
              lookupCandidates.set(key, value);
            }
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

      const note =
        successfulSources || failedSources
          ? `律所官网本轮巡检 ${successfulSources} 个来源${failedSources ? `，${failedSources} 个来源暂时失败` : ""}；补进 ${casesUpserted} 条案件更新、${docketEntriesUpserted} 条律所公开文书${lookupResult.lookupsTriggered ? `，并触发 ${lookupResult.lookupsTriggered} 次 CourtListener 精确补抓` : ""}。`
          : "律所官网当前没有已配置来源。";

      return {
        successfulSources,
        failedSources,
        casesUpserted,
        docketEntriesUpserted,
        lookupsTriggered: lookupResult.lookupsTriggered,
        note
      };
    });
  }

  async enrichCaseWithWorldtro(caseId, { force = false } = {}) {
    return this.store.batchMutations(async () => {
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
    });
  }

  async enrichCaseWithCourtListener(caseId, { force = false } = {}) {
    return this.store.batchMutations(async () => {
      const caseRow = this.store.getCase(caseId);
      if (!caseRow || !this.courtListener.hasDocketAccess()) {
        return { enriched: false, reason: "not-applicable" };
      }

      const syncedAt = caseRow.last_docket_sync_at ? Date.parse(caseRow.last_docket_sync_at) : 0;
      const staleAfterMs = 2 * 60 * 60 * 1000;
      if (!force && syncedAt && Date.now() - syncedAt < staleAfterMs) {
        return { enriched: false, reason: "fresh" };
      }

      const lookupTerm = caseRow.docket_number || caseRow.case_name;
      if (lookupTerm) {
        await this.importLookup(lookupTerm, {
          courtName: caseRow.court_name,
          caseName: caseRow.case_name
        });
      }

      const refreshedCase =
        this.store.getCase(caseId) ||
        this.store.findCaseByCourtAndDocket({
          courtId: caseRow.court_id,
          courtName: caseRow.court_name,
          docketNumber: caseRow.docket_number,
          startDate: this.config.sync.startDate
        });

      if (!refreshedCase?.courtlistener_docket_id) {
        return { enriched: false, reason: "not-found" };
      }

      return this.syncSingleCourtListenerDocket(refreshedCase);
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

  async syncWorldtroRecent(mode = "recent") {
    return this.store.batchMutations(async () => {
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
      let failedCases = 0;
      for (const caseRow of candidates) {
        try {
          const result = await this.syncSingleWorldtroCase(caseRow);
          if (result.enriched) {
            syncedCases += 1;
          }
        } catch (error) {
          failedCases += 1;
        }
      }

      return {
        syncedCases,
        note: syncedCases
          ? `WorldTRO 本轮补齐 ${syncedCases} 个案件的公开时间线${failedCases ? `，另有 ${failedCases} 个案件待重试` : ""}。`
          : failedCases
            ? `WorldTRO 本轮没有补齐成功，${failedCases} 个案件待重试。`
            : "WorldTRO 本轮没有待补源案件。"
      };
    });
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
        recentWindowDays: this.config.pacerMonitor.recentWindowDays
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

  async syncSingleWorldtroCase(caseRow) {
    const payload = await this.worldtro.enrichCase(caseRow);
    if (!payload || !payload.entries.length) {
      const timestamp = new Date().toISOString();
      const mergedRaw = {
        ...(caseRow.raw || {}),
        worldtro: {
          ...(caseRow.raw?.worldtro || {}),
          syncedAt: timestamp,
          rowCount: 0,
          missing: true
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

  async syncSinglePacerMonitorCase(caseRow) {
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
        case_id: caseRow.id,
        source_entry_key: `pacermonitor:${caseRow.id}:${entry.row_number || "na"}:${digest}`,
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
      const existingCase = this.store.findCaseByCourtAndDocket({
        courtId: valueOf(result.court_id),
        courtName: valueOf(result.court),
        docketNumber: valueOf(result.docketNumber),
        startDate: this.config.sync.startDate
      });
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
      const candidates = this.store.getCasesNeedingDocketSync(limit);
      let syncedCases = 0;
      let metadataOnlyMode = false;

      for (const caseRow of candidates) {
        try {
          const result = await this.syncSingleCourtListenerDocket(caseRow);
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

    return {
      enriched: true,
      reason: metadataOnlyMode ? "metadata-only" : entries.length ? "ok" : "empty"
    };
  }
}
