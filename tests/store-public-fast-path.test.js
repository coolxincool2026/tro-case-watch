import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { Store } from "../src/db.js";

function createTempDbPath() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-fast-path-"));
  return {
    tempDir,
    dbPath: path.join(tempDir, "test.sqlite")
  };
}

test("Store backfills legacy public fast-path columns on reopen", () => {
  const { tempDir, dbPath } = createTempDbPath();
  let store = new Store(dbPath);

  try {
    const now = new Date().toISOString();
    store.db.prepare(`
      INSERT INTO cases (
        source_case_key,
        primary_source,
        source_case_id,
        court_id,
        court_name,
        case_name,
        docket_number,
        date_filed,
        tags_marker,
        is_watchlist,
        is_tro,
        is_schedule_a,
        is_seller_watch,
        priority_feed_row_count,
        priority_activity_at,
        source_urls_json,
        plaintiffs_json,
        defendants_json,
        recent_activity_summary,
        search_text,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      "legacy:1",
      "61tro",
      "legacy-1",
      "ilnd",
      "Northern District of Illinois",
      "Legacy TRO Plaintiff v. Example Sellers",
      "1:26-cv-10191",
      "2026-04-01",
      "|tro|seller_tro|",
      0,
      0,
      0,
      0,
      null,
      null,
      "[]",
      JSON.stringify(["Legacy TRO Plaintiff"]),
      "[]",
      "Temporary restraining order entered",
      "",
      now,
      now
    );

    store.db.close();
    store = new Store(dbPath);

    const legacyRow = store.db.prepare(`
      SELECT is_watchlist, is_tro, is_schedule_a, is_seller_watch, priority_activity_at, search_text
      FROM cases
      WHERE source_case_key = ?
    `).get("legacy:1");

    assert.equal(legacyRow.is_watchlist, 1);
    assert.equal(legacyRow.is_tro, 1);
    assert.equal(legacyRow.is_schedule_a, 0);
    assert.equal(legacyRow.is_seller_watch, 1);
    assert.ok(String(legacyRow.priority_activity_at || "").trim());
    assert.match(String(legacyRow.search_text || ""), /legacy tro plaintiff/i);

    const payload = store.listCases({
      startDate: "2025-01-01",
      category: "watchlist",
      page: 1,
      pageSize: 10
    });

    assert.equal(payload.total, 1);
    assert.equal(payload.items[0]?.docket_number, "1:26-cv-10191");
  } finally {
    try {
      store.db.close();
    } catch {
      // ignore cleanup failures in tests
    }
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
});
