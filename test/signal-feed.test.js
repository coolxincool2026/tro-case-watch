import assert from "node:assert/strict";
import test from "node:test";

import { SignalFeedClient } from "../src/providers/signal-feed.js";

function buildPublicRows(count) {
  return Array.from({ length: count }, (_, index) => {
    const id = String(index + 1).padStart(32, "0");
    const docket = `1:2026-cv-${String(index + 1).padStart(5, "0")}`;
    return `<tr><td><a href="/en/cases/${id}-${index + 1}">${docket}</a></td>` +
      `<td title="Plaintiff ${index + 1}"></td><td title="Northern District of Illinois"></td>` +
      `<td>Aug 19, 2026</td></tr>`;
  }).join("");
}

test("public discovery hydrates all 15 published cases by default", async () => {
  const originalFetch = globalThis.fetch;
  const listHtml = `<table>${buildPublicRows(15)}</table>`;
  let detailRequests = 0;

  globalThis.fetch = async (url) => {
    if (String(url) === "https://example.test/en/cases/") {
      return new Response(listHtml, { status: 200 });
    }
    detailRequests += 1;
    return new Response("<html></html>", { status: 200 });
  };

  try {
    const client = new SignalFeedClient({
      enabled: true,
      publicCasesUrl: "https://example.test/en/cases/",
      minIntervalMs: 1
    });
    const result = await client.fetchRecent();

    assert.equal(result.source, "public");
    assert.equal(result.items.length, 15);
    assert.equal(detailRequests, 15);
    assert.equal(client.getStatus().maxCasesPerRun, 100);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("authenticated discovery accepts the official 100-case ceiling", () => {
  const client = new SignalFeedClient({
    enabled: true,
    publicCasesUrl: "https://example.test/en/cases/",
    maxCasesPerRun: 500
  });

  assert.equal(client.getStatus().maxCasesPerRun, 100);
});

test("public discovery falls back to the localized home page", async () => {
  const originalFetch = globalThis.fetch;
  const homeHtml = `<table>${buildPublicRows(2)}</table>`;
  const requested = [];

  globalThis.fetch = async (url) => {
    requested.push(String(url));
    return new Response(String(url).endsWith("/en/") ? homeHtml : "<html></html>", { status: 200 });
  };

  try {
    const client = new SignalFeedClient({
      enabled: true,
      publicCasesUrl: "https://example.test/en/cases/",
      minIntervalMs: 1
    });
    const items = await client.fetchPublicRecent();

    assert.equal(items.length, 2);
    assert.deepEqual(requested, ["https://example.test/en/cases/", "https://example.test/en/"]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("known cases retry canonical public detail URL variants", async () => {
  const client = new SignalFeedClient({
    enabled: true,
    publicCasesUrl: "https://example.test/en/cases/"
  });
  const requested = [];
  client.fetchPublicDetail = async (url) => {
    requested.push(url);
    if (url.includes("/en/cases/")) {
      const error = new Error("not found");
      error.status = 404;
      throw error;
    }
    return { docketId: "abc", docketNumber: "2:26-cv-00651", entries: [{ entryId: "1" }] };
  };

  const item = await client.fetchKnownCase({
    docketId: "abc",
    docketNumber: "2:26-cv-00651",
    detailUrl: "https://example.test/en/cases/abc-case"
  });

  assert.equal(item.entries.length, 1);
  assert.ok(requested.some((url) => url.includes("/cases/") && !url.includes("/en/cases/")));
});

test("known cases do not retry URL variants after a transient failure", async () => {
  const client = new SignalFeedClient({
    enabled: true,
    publicCasesUrl: "https://example.test/en/cases/"
  });
  let requests = 0;
  client.fetchPublicDetail = async () => {
    requests += 1;
    const error = new Error("rate limited");
    error.status = 429;
    throw error;
  };

  await assert.rejects(() => client.fetchKnownCase({
    docketId: "abc",
    docketNumber: "2:26-cv-00651",
    detailUrl: "https://example.test/en/cases/abc-case"
  }), /rate limited/);
  assert.equal(requests, 1);
});
