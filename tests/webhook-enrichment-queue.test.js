import test from "node:test";
import assert from "node:assert/strict";

import {
  claimWebhookEnrichmentJobState,
  enqueueWebhookEnrichmentJobState,
  finishWebhookEnrichmentJobState,
  getWebhookEnrichmentQueueStats
} from "../src/webhook-enrichment.js";

test("webhook enrichment queue prefers the most recent case when runAfter ties", () => {
  let state = null;

  ({ state } = enqueueWebhookEnrichmentJobState(state, {
    caseId: 101,
    providers: ["worldtro"],
    enqueuedAt: "2026-04-19T10:00:00.000Z",
    runAfter: "2026-04-19T10:05:00.000Z",
    priorityAt: "2026-04-18T10:00:00.000Z",
    lastWebhookAt: "2026-04-19T10:00:00.000Z"
  }));

  ({ state } = enqueueWebhookEnrichmentJobState(state, {
    caseId: 202,
    providers: ["worldtro"],
    enqueuedAt: "2026-04-19T10:01:00.000Z",
    runAfter: "2026-04-19T10:05:00.000Z",
    priorityAt: "2026-04-19T09:59:00.000Z",
    lastWebhookAt: "2026-04-19T10:01:00.000Z"
  }));

  const claimed = claimWebhookEnrichmentJobState(state, {
    now: "2026-04-19T10:05:00.000Z",
    leaseTtlMs: 60_000
  });

  assert.equal(claimed.job?.caseId, 202);
});

test("webhook enrichment queue merges repeated webhook jobs for the same case", () => {
  let state = null;

  ({ state } = enqueueWebhookEnrichmentJobState(state, {
    caseId: 101,
    providers: ["61tro"],
    enqueuedAt: "2026-04-19T10:00:00.000Z",
    runAfter: "2026-04-19T10:05:00.000Z",
    priorityAt: "2026-04-19T09:55:00.000Z",
    lastWebhookAt: "2026-04-19T10:00:00.000Z",
    latestEntryId: 10
  }));

  const merged = enqueueWebhookEnrichmentJobState(state, {
    caseId: 101,
    providers: ["worldtro", "61tro"],
    enqueuedAt: "2026-04-19T10:01:00.000Z",
    runAfter: "2026-04-19T10:03:00.000Z",
    priorityAt: "2026-04-19T10:00:30.000Z",
    lastWebhookAt: "2026-04-19T10:01:00.000Z",
    latestEntryId: 11
  });

  assert.equal(merged.pendingCount, 1);
  assert.deepEqual(merged.queuedJob?.providers, ["worldtro", "61tro"]);
  assert.equal(merged.queuedJob?.runAfter, "2026-04-19T10:03:00.000Z");
  assert.equal(merged.queuedJob?.latestEntryId, 11);
});

test("webhook enrichment queue can requeue a leased job after memory pressure", () => {
  let state = null;

  ({ state } = enqueueWebhookEnrichmentJobState(state, {
    caseId: 333,
    providers: ["worldtro", "61tro", "recentfilings"],
    enqueuedAt: "2026-04-19T10:00:00.000Z",
    runAfter: "2026-04-19T10:00:00.000Z",
    priorityAt: "2026-04-19T10:00:00.000Z",
    lastWebhookAt: "2026-04-19T10:00:00.000Z"
  }));

  const claimed = claimWebhookEnrichmentJobState(state, {
    now: "2026-04-19T10:00:01.000Z",
    leaseTtlMs: 60_000
  });

  const finished = finishWebhookEnrichmentJobState(claimed.state, {
    caseId: 333,
    leaseId: claimed.job?.leaseId,
    now: "2026-04-19T10:00:10.000Z",
    requeueAfterMs: 120_000,
    requeueJob: {
      providers: ["61tro", "recentfilings"]
    }
  });

  const stats = getWebhookEnrichmentQueueStats(finished.state, {
    now: "2026-04-19T10:00:11.000Z"
  });

  assert.equal(stats.pendingCount, 1);
  assert.equal(stats.readyCount, 0);
  assert.deepEqual(finished.state.pending[0]?.providers, ["61tro", "recentfilings"]);
});

test("webhook enrichment queue stats expose expired leases as runnable work", () => {
  let state = null;

  ({ state } = enqueueWebhookEnrichmentJobState(state, {
    caseId: 444,
    providers: ["worldtro"],
    enqueuedAt: "2026-04-19T10:00:00.000Z",
    runAfter: "2026-04-19T10:00:00.000Z",
    priorityAt: "2026-04-19T10:00:00.000Z",
    lastWebhookAt: "2026-04-19T10:00:00.000Z"
  }));

  const claimed = claimWebhookEnrichmentJobState(state, {
    now: "2026-04-19T10:00:01.000Z",
    leaseTtlMs: 60_000
  });
  const activeStats = getWebhookEnrichmentQueueStats(claimed.state, {
    now: "2026-04-19T10:00:30.000Z"
  });
  const expiredStats = getWebhookEnrichmentQueueStats(claimed.state, {
    now: "2026-04-19T10:01:02.000Z"
  });

  assert.equal(activeStats.expiredLeaseCount, 0);
  assert.equal(expiredStats.expiredLeaseCount, 1);
});
