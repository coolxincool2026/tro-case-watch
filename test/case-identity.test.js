import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { Store } from "../src/db.js";
import { normalizeDocketIdentity } from "../src/insights.js";

function caseRecord(sourceCaseKey, docketNumber) {
  return {
    source_case_key: sourceCaseKey,
    primary_source: "courtlistener",
    source_case_id: sourceCaseKey,
    court_id: "pawd",
    court_name: "Western District of Pennsylvania",
    case_name: `Test ${docketNumber} v. Schedule A Defendants`,
    docket_number: docketNumber,
    date_filed: "2026-04-15",
    status: "open",
    tags_marker: "|tro|schedule_a|",
    source_urls: [],
    plaintiffs: ["Test"],
    defendants: ["Schedule A Defendants"],
    docket_count: 1,
    raw: {}
  };
}

test("case identity preserves federal office numbers", () => {
  assert.equal(normalizeDocketIdentity("pawd-2:2026-cv-00651"), "2:26-cv-00651");
  assert.equal(normalizeDocketIdentity("3:26-cv-00651"), "3:26-cv-00651");
});

test("same court and serial in different offices remain separate cases", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "tro-case-identity-"));
  const store = new Store(path.join(directory, "test.sqlite"));
  try {
    store.upsertCase(caseRecord("case:2", "2:26-cv-00651"));
    store.upsertCase(caseRecord("case:3", "3:26-cv-00651"));

    assert.equal(store.getHydratedCases("2026-01-01").length, 2);
    assert.equal(store.findCaseByCourtAndDocket({
      courtId: "pawd",
      docketNumber: "2:26-cv-00651",
      startDate: "2026-01-01"
    }).source_case_key, "case:2");
    assert.equal(store.findCaseByCourtAndDocket({
      courtId: "pawd",
      docketNumber: "3:26-cv-00651",
      startDate: "2026-01-01"
    }).source_case_key, "case:3");
  } finally {
    store.db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
