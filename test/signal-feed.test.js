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
