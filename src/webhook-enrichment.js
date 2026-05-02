const WEBHOOK_ENRICHMENT_QUEUE_VERSION = 1;

function nowIso() {
  return new Date().toISOString();
}

function toTimestamp(value, fallback = 0) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : fallback;
}

function uniqueProviders(values = []) {
  return [...new Set(
    (Array.isArray(values) ? values : [])
      .map((value) => String(value || "").trim())
      .filter(Boolean)
  )];
}

function buildLeaseId(caseId, nowMs) {
  return `${Number(caseId || 0)}:${nowMs}:${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeJob(raw = {}, { fallbackNow = nowIso() } = {}) {
  const caseId = Number(raw.caseId || 0);
  if (!Number.isFinite(caseId) || caseId <= 0) {
    return null;
  }

  return {
    caseId,
    providers: uniqueProviders(raw.providers),
    attempts: Math.max(Number(raw.attempts || 0), 0),
    enqueuedAt: raw.enqueuedAt || fallbackNow,
    runAfter: raw.runAfter || fallbackNow,
    priorityAt: raw.priorityAt || raw.lastWebhookFiledAt || raw.lastWebhookAt || fallbackNow,
    lastWebhookAt: raw.lastWebhookAt || fallbackNow,
    lastWebhookFiledAt: raw.lastWebhookFiledAt || null,
    eventType: raw.eventType ?? null,
    latestEntryId: Number(raw.latestEntryId || 0) || null,
    entryCount: Math.max(Number(raw.entryCount || 0), 0),
    idempotencyKey: raw.idempotencyKey || null,
    leaseId: raw.leaseId || null,
    leasedAt: raw.leasedAt || null,
    leaseExpiresAt: raw.leaseExpiresAt || null
  };
}

function compareJobs(left, right) {
  const runAfterDiff = toTimestamp(left.runAfter) - toTimestamp(right.runAfter);
  if (runAfterDiff !== 0) {
    return runAfterDiff;
  }

  const priorityDiff = toTimestamp(right.priorityAt, -Infinity) - toTimestamp(left.priorityAt, -Infinity);
  if (priorityDiff !== 0) {
    return priorityDiff;
  }

  const webhookDiff = toTimestamp(right.lastWebhookAt, -Infinity) - toTimestamp(left.lastWebhookAt, -Infinity);
  if (webhookDiff !== 0) {
    return webhookDiff;
  }

  const enqueuedDiff = toTimestamp(right.enqueuedAt, -Infinity) - toTimestamp(left.enqueuedAt, -Infinity);
  if (enqueuedDiff !== 0) {
    return enqueuedDiff;
  }

  return Number(left.caseId || 0) - Number(right.caseId || 0);
}

function mergeJobs(existing, incoming) {
  if (!existing) {
    return normalizeJob(incoming);
  }

  const normalizedExisting = normalizeJob(existing);
  const normalizedIncoming = normalizeJob(incoming, {
    fallbackNow:
      normalizedExisting?.lastWebhookAt ||
      normalizedExisting?.priorityAt ||
      normalizedExisting?.enqueuedAt ||
      nowIso()
  });
  if (!normalizedExisting) {
    return normalizedIncoming;
  }
  if (!normalizedIncoming) {
    return normalizedExisting;
  }

  const runAfter = toTimestamp(normalizedExisting.runAfter) <= toTimestamp(normalizedIncoming.runAfter)
    ? normalizedExisting.runAfter
    : normalizedIncoming.runAfter;
  const priorityAt = toTimestamp(normalizedExisting.priorityAt, -Infinity) >= toTimestamp(normalizedIncoming.priorityAt, -Infinity)
    ? normalizedExisting.priorityAt
    : normalizedIncoming.priorityAt;
  const lastWebhookAt = toTimestamp(normalizedExisting.lastWebhookAt, -Infinity) >= toTimestamp(normalizedIncoming.lastWebhookAt, -Infinity)
    ? normalizedExisting.lastWebhookAt
    : normalizedIncoming.lastWebhookAt;
  const lastWebhookFiledAt = toTimestamp(normalizedExisting.lastWebhookFiledAt, -Infinity) >= toTimestamp(normalizedIncoming.lastWebhookFiledAt, -Infinity)
    ? normalizedExisting.lastWebhookFiledAt
    : normalizedIncoming.lastWebhookFiledAt;

  return {
    ...normalizedExisting,
    providers: uniqueProviders([...normalizedIncoming.providers, ...normalizedExisting.providers]),
    attempts: Math.max(normalizedExisting.attempts, normalizedIncoming.attempts),
    enqueuedAt: normalizedExisting.enqueuedAt,
    runAfter,
    priorityAt,
    lastWebhookAt,
    lastWebhookFiledAt,
    eventType: normalizedIncoming.eventType ?? normalizedExisting.eventType ?? null,
    latestEntryId: normalizedIncoming.latestEntryId ?? normalizedExisting.latestEntryId ?? null,
    entryCount: Math.max(normalizedExisting.entryCount, normalizedIncoming.entryCount),
    idempotencyKey: normalizedIncoming.idempotencyKey || normalizedExisting.idempotencyKey || null
  };
}

function normalizeState(raw = {}) {
  const state = raw && typeof raw === "object" ? raw : {};
  return {
    version: WEBHOOK_ENRICHMENT_QUEUE_VERSION,
    pending: (Array.isArray(state.pending) ? state.pending : []).map((item) => normalizeJob(item)).filter(Boolean),
    leased: (Array.isArray(state.leased) ? state.leased : []).map((item) => normalizeJob(item)).filter(Boolean),
    updatedAt: state.updatedAt || null
  };
}

function upsertPendingJob(pending = [], job, { maxPending = 0 } = {}) {
  const normalizedJob = normalizeJob(job);
  if (!normalizedJob) {
    return [...pending];
  }

  const next = [...pending];
  const existingIndex = next.findIndex((item) => Number(item.caseId || 0) === normalizedJob.caseId);
  if (existingIndex !== -1) {
    next[existingIndex] = mergeJobs(next[existingIndex], normalizedJob);
  } else {
    next.push(normalizedJob);
  }

  next.sort(compareJobs);
  if (Number(maxPending || 0) > 0 && next.length > maxPending) {
    return next.slice(0, maxPending);
  }

  return next;
}

function reclaimExpiredLeases(state, { now = nowIso(), maxPending = 0 } = {}) {
  const nowMs = toTimestamp(now);
  const reclaimed = [];
  const stillLeased = [];

  for (const lease of state.leased) {
    if (toTimestamp(lease.leaseExpiresAt) <= nowMs) {
      reclaimed.push({
        ...lease,
        leaseId: null,
        leasedAt: null,
        leaseExpiresAt: null,
        runAfter: now
      });
    } else {
      stillLeased.push(lease);
    }
  }

  let pending = [...state.pending];
  for (const job of reclaimed) {
    pending = upsertPendingJob(pending, job, { maxPending });
  }

  return {
    state: {
      ...state,
      pending,
      leased: stillLeased
    },
    reclaimedCount: reclaimed.length
  };
}

export function normalizeWebhookEnrichmentQueueState(raw = {}) {
  return normalizeState(raw);
}

export function enqueueWebhookEnrichmentJobState(rawState, job, { maxPending = 0, now = nowIso() } = {}) {
  const state = normalizeState(rawState);
  const nextPending = upsertPendingJob(state.pending, {
    ...job,
    enqueuedAt: job?.enqueuedAt || now,
    lastWebhookAt: job?.lastWebhookAt || now
  }, { maxPending });
  const queuedJob = nextPending.find((item) => Number(item.caseId || 0) === Number(job?.caseId || 0)) || null;

  return {
    state: {
      ...state,
      pending: nextPending,
      updatedAt: now
    },
    queuedJob,
    pendingCount: nextPending.length
  };
}

export function claimWebhookEnrichmentJobState(rawState, {
  now = nowIso(),
  leaseTtlMs = 15 * 60 * 1000,
  maxPending = 0
} = {}) {
  const reclaimed = reclaimExpiredLeases(normalizeState(rawState), { now, maxPending });
  const state = reclaimed.state;
  const nowMs = toTimestamp(now);
  const pending = [...state.pending].sort(compareJobs);
  const readyIndex = pending.findIndex((item) => toTimestamp(item.runAfter) <= nowMs);

  if (readyIndex === -1) {
    return {
      state: {
        ...state,
        pending,
        updatedAt: now
      },
      job: null,
      reclaimedCount: reclaimed.reclaimedCount
    };
  }

  const [nextJob] = pending.splice(readyIndex, 1);
  const leasedJob = {
    ...nextJob,
    attempts: Math.max(Number(nextJob.attempts || 0), 0) + 1,
    leaseId: buildLeaseId(nextJob.caseId, nowMs),
    leasedAt: now,
    leaseExpiresAt: new Date(nowMs + Math.max(Number(leaseTtlMs || 0), 15 * 1000)).toISOString()
  };

  return {
    state: {
      ...state,
      pending,
      leased: [...state.leased, leasedJob],
      updatedAt: now
    },
    job: leasedJob,
    reclaimedCount: reclaimed.reclaimedCount
  };
}

export function finishWebhookEnrichmentJobState(rawState, {
  caseId,
  leaseId = null,
  now = nowIso(),
  requeueAfterMs = 0,
  requeueJob = null,
  maxPending = 0
} = {}) {
  const state = normalizeState(rawState);
  const normalizedCaseId = Number(caseId || 0);
  const nextLeased = [];
  let finishedJob = null;

  for (const lease of state.leased) {
    const sameCase = Number(lease.caseId || 0) === normalizedCaseId;
    const sameLease = !leaseId || String(lease.leaseId || "") === String(leaseId || "");
    if (!finishedJob && sameCase && sameLease) {
      finishedJob = lease;
      continue;
    }
    nextLeased.push(lease);
  }

  let pending = [...state.pending];
  if (finishedJob && Number(requeueAfterMs || 0) > 0) {
    pending = upsertPendingJob(pending, {
      ...finishedJob,
      ...requeueJob,
      leaseId: null,
      leasedAt: null,
      leaseExpiresAt: null,
      runAfter: new Date(toTimestamp(now) + Math.max(Number(requeueAfterMs || 0), 15 * 1000)).toISOString()
    }, { maxPending });
  }

  return {
    state: {
      ...state,
      pending,
      leased: nextLeased,
      updatedAt: now
    },
    finishedJob
  };
}

export function getWebhookEnrichmentQueueStats(rawState, { now = nowIso() } = {}) {
  const state = normalizeState(rawState);
  const nowMs = toTimestamp(now);
  const pending = [...state.pending].sort(compareJobs);
  const readyCount = pending.filter((item) => toTimestamp(item.runAfter) <= nowMs).length;
  const expiredLeaseCount = state.leased.filter((item) => toTimestamp(item.leaseExpiresAt) <= nowMs).length;

  return {
    pendingCount: pending.length,
    readyCount,
    leasedCount: state.leased.length,
    expiredLeaseCount,
    nextRunAt: pending[0]?.runAfter || null
  };
}

export { WEBHOOK_ENRICHMENT_QUEUE_VERSION };
