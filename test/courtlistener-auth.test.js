import assert from "node:assert/strict";
import test from "node:test";

import { CourtListenerClient } from "../src/providers/courtlistener.js";

test("public CourtListener searches do not send an API token", async () => {
  const originalFetch = globalThis.fetch;
  let authorization = null;
  globalThis.fetch = async (_url, options) => {
    authorization = options.headers.authorization || null;
    return new Response(JSON.stringify({ results: [] }), { status: 200 });
  };

  try {
    const client = new CourtListenerClient({
      baseUrl: "https://example.test/api/rest/v4",
      apiToken: "blocked-token",
      enableDocketSync: true,
      enableDocketAlerts: true
    });
    await client.search({ query: "test", startDate: "2026-01-01" });
    assert.equal(authorization, null);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("authenticated CourtListener docket requests send the API token", async () => {
  const originalFetch = globalThis.fetch;
  let authorization = null;
  globalThis.fetch = async (_url, options) => {
    authorization = options.headers.authorization || null;
    return new Response(JSON.stringify({ id: 1 }), { status: 200 });
  };

  try {
    const client = new CourtListenerClient({
      baseUrl: "https://example.test/api/rest/v4",
      apiToken: "working-token",
      enableDocketSync: true,
      enableDocketAlerts: true
    });
    await client.fetchDocket(1);
    assert.equal(authorization, "Token working-token");
  } finally {
    globalThis.fetch = originalFetch;
  }
});
