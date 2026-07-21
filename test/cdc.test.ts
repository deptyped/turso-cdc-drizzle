import assert from "node:assert/strict";
import { test } from "node:test";
import type { InferSelectModel } from "drizzle-orm";
import { asc } from "drizzle-orm";
import type { TursoDatabaseDatabase } from "drizzle-orm/tursodatabase";
import { deleteEvents, disableCdc, enableCdc, getEvents, streamEvents, tursoCdc } from "../src/helpers.ts";
import type { CheckpointStrategy } from "../src/types.js";
import type { CdcEvent, ChangeId } from "../src/types.ts";
import { CdcChangeKind, cdcChangeKindToInt } from "../src/types.ts";
import { makeDb, t } from "./helpers.ts";

type TursoCdcRow = InferSelectModel<typeof tursoCdc>;

async function capture(db: TursoDatabaseDatabase): Promise<TursoCdcRow[]> {
  return db.select().from(tursoCdc).orderBy(asc(tursoCdc.changeId));
}

async function setup() {
  const { client, db } = await makeDb();
  return { client, db };
}

test("enableCdc + disableCdc run without error and capture changes", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const rows = await capture(db);
  assert.ok(rows.some((r) => r.tableName === "t" && r.changeType === cdcChangeKindToInt(CdcChangeKind.INSERT)));
  await disableCdc(db);
  await client.close?.();
});

test("getEvents returns only data changes (excludes COMMIT rows)", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const events = await getEvents(db, t);
  assert.equal(events.length, 2);
  assert.ok(events.every((e) => e.changeType !== "COMMIT"));
  assert.deepEqual(
    events.map((e) => e.rowId),
    [1, 2],
  );
  await disableCdc(db);
  await client.close?.();
});

test("getEvents filters by kinds", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("UPDATE t SET v = 'z' WHERE id = 1");
  await client.exec("DELETE FROM t WHERE id = 1");
  const deletes = await getEvents(db, t, { kinds: ["DELETE"], limit: 100 });
  assert.equal(deletes.length, 1);
  assert.equal(deletes[0]?.changeType, "DELETE");
  await disableCdc(db);
  await client.close?.();
});

test("getEvents beforeId returns only earlier changes", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const all = await getEvents(db, t, { limit: 100 });
  assert.ok(all.length >= 2);
  const cutoff = all[1]!.changeId;
  const before = await getEvents(db, t, { beforeId: cutoff, limit: 100 });
  assert.equal(before.length, 1);
  assert.equal(before[0]!.changeId, all[0]!.changeId);
  await disableCdc(db);
  await client.close?.();
});

test("getEvents afterId returns only later changes", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const first = await getEvents(db, t);
  const later = await getEvents(db, t, {
    afterId: first[first.length - 1]?.changeId,
    limit: 100,
  });
  assert.equal(later.length, 0);
  await disableCdc(db);
  await client.close?.();
});

test("getEvents limit caps results", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  await client.exec("INSERT INTO t(v) VALUES ('c')");
  const events = await getEvents(db, t, { limit: 2 });
  assert.equal(events.length, 2);
  await disableCdc(db);
  await client.close?.();
});

test("deleteEvents removes rows in the date range", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const all = await capture(db);
  const maxTime = Math.max(...all.map((r) => r.changeTime ?? 0));
  await deleteEvents(db, { date: { to: maxTime } });
  const remaining = await capture(db);
  assert.equal(remaining.length, 0);
  await disableCdc(db);
  await client.close?.();
});

test("deleteEvents date.from removes rows from a lower bound", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const all = await capture(db);
  const minTime = Math.min(...all.map((r) => r.changeTime ?? 0));
  await deleteEvents(db, { date: { from: minTime } });
  const remaining = await capture(db);
  assert.equal(remaining.filter((r) => r.changeType !== cdcChangeKindToInt(CdcChangeKind.COMMIT)).length, 0);
  await disableCdc(db);
  await client.close?.();
});

test("deleteEvents changeId.from removes rows from a lower bound", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const all = await capture(db);
  const minId = Math.min(...all.map((r) => r.changeId ?? 0));
  await deleteEvents(db, { changeId: { from: minId } });
  const remaining = await capture(db);
  assert.equal(remaining.filter((r) => r.changeType !== cdcChangeKindToInt(CdcChangeKind.COMMIT)).length, 0);
  await disableCdc(db);
  await client.close?.();
});

test("deleteEvents throws when both bounds are omitted", async () => {
  const { client, db } = await setup();
  await assert.rejects(() => deleteEvents(db, {}), /requires at least one range/);
  await client.close?.();
});

test("deleteEvents by changeId range", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const all = await capture(db);
  const maxId = Math.max(...all.map((r) => r.changeId ?? 0));
  await deleteEvents(db, { changeId: { to: maxId } });
  const remaining = await capture(db);
  assert.equal(remaining.length, 0);
  await disableCdc(db);
  await client.close?.();
});

test("deleteEvents by tableName scopes deletion", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const all = await capture(db);
  const maxId = Math.max(...all.map((r) => r.changeId ?? 0));
  await deleteEvents(db, { changeId: { to: maxId }, tableName: "nonexistent" });
  const remaining = await capture(db);
  assert.equal(remaining.length, all.length);
  await disableCdc(db);
  await client.close?.();
});

test("getEvents deleteAfterRead only deletes received events, not all", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  await client.exec("INSERT INTO t(v) VALUES ('c')");
  await client.exec("INSERT INTO t(v) VALUES ('d')");
  // read first 2 with deleteAfterRead
  const events = await getEvents(db, t, { limit: 2, deleteAfterRead: true });
  assert.equal(events.length, 2);
  // only those 2 should be deleted — remaining 2 are untouched
  const remaining = await capture(db);
  const dataRemaining = remaining.filter((r) => r.changeType !== cdcChangeKindToInt(CdcChangeKind.COMMIT));
  assert.equal(dataRemaining.length, 2);
  await disableCdc(db);
  await client.close?.();
});

test("streamEvents delivers events then stops on abort", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const seen: number[] = [];
  const ac = new AbortController();

  setTimeout(() => ac.abort(), 100);

  for await (const event of streamEvents(db, t, {
    pollIntervalMs: 10,
    signal: ac.signal,
  })) {
    seen.push(event.changeId);
  }

  assert.deepEqual(
    seen.sort((a, b) => a - b),
    [1, 3],
  );
  await disableCdc(db);
  await client.close?.();
});

test("streamEvents mode full yields events with data", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('hello')");
  const ac = new AbortController();
  const events: CdcEvent<typeof t>[] = [];

  setTimeout(() => ac.abort(), 100);

  for await (const event of streamEvents(db, t, {
    pollIntervalMs: 10,
    signal: ac.signal,
    mode: "full",
  })) {
    events.push(event);
  }

  assert.equal(events.length, 1);
  assert.equal(events[0]?.after?.v, "hello");
  await disableCdc(db);
  await client.close?.();
});

test("getEvents mode full returns blobs", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('hello')");
  const events = await getEvents(db, t, { mode: "full", limit: 100 });
  assert.equal(events.length, 1);
  assert.equal(events[0]?.after?.v, "hello");
  await disableCdc(db);
  await client.close?.();
});

test("enableCdc throws on invalid mode", async () => {
  const { client, db } = await setup();
  await assert.rejects(() => enableCdc(db, "invalid" as never), /Invalid CDC mode/);
  await client.close?.();
});

test("getEvents deleteAfterRead on empty result does not throw", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  const events = await getEvents(db, t, { deleteAfterRead: true, limit: 100 });
  assert.equal(events.length, 0);
  await disableCdc(db);
  await client.close?.();
});

test("streamEvents deleteAfterRead only deletes consumed events", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const ac = new AbortController();

  setTimeout(() => ac.abort(), 100);

  for await (const _ of streamEvents(db, t, {
    pollIntervalMs: 10,
    signal: ac.signal,
    deleteAfterRead: true,
  })) {
    // consume
  }

  // insert more events after stream completed
  await client.exec("INSERT INTO t(v) VALUES ('c')");
  await client.exec("INSERT INTO t(v) VALUES ('d')");

  // only the first 2 events (consumed by stream) should be deleted
  const remaining = await capture(db);
  const dataRemaining = remaining.filter((r) => r.changeType !== cdcChangeKindToInt(CdcChangeKind.COMMIT));
  assert.equal(dataRemaining.length, 2);
  await disableCdc(db);
  await client.close?.();
});

test("streamEvents deleteAfterRead batched only deletes consumed events", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const ac = new AbortController();

  setTimeout(() => ac.abort(), 100);

  for await (const _ of streamEvents(db, t, {
    pollIntervalMs: 10,
    signal: ac.signal,
    deleteAfterRead: true,
    deleteBatchSize: 2,
  })) {
    // consume
  }

  // insert more events after stream completed
  await client.exec("INSERT INTO t(v) VALUES ('c')");
  await client.exec("INSERT INTO t(v) VALUES ('d')");

  // only the first 2 events (consumed by stream) should be deleted
  const remaining = await capture(db);
  const dataRemaining = remaining.filter((r) => r.changeType !== cdcChangeKindToInt(CdcChangeKind.COMMIT));
  assert.equal(dataRemaining.length, 2);
  await disableCdc(db);
  await client.close?.();
});

test("batchSize limits events per poll loop", async () => {
  const { client, db } = await setup();
  await enableCdc(db);
  for (let i = 0; i < 10; i++) await client.exec("INSERT INTO t(v) VALUES ('x')");
  const ac = new AbortController();
  const seen: number[] = [];

  setTimeout(() => ac.abort(), 50);

  for await (const event of streamEvents(db, t, {
    batchSize: 3,
    pollIntervalMs: 5,
    signal: ac.signal,
  })) {
    seen.push(event.changeId);
  }

  assert.equal(seen.length, 10);
  await disableCdc(db);
  await client.close?.();
});

test("checkpoint.restore resumes stream from saved position", async () => {
  const { client, db } = await setup();
  await enableCdc(db);
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");
  const all = await getEvents(db, t, { limit: 100 });
  assert.ok(all.length >= 2);

  let saved: ChangeId | undefined;
  const cp: CheckpointStrategy = {
    save: async (id) => {
      saved = id;
    },
    restore: async () => saved,
  };

  const ac = new AbortController();
  setTimeout(() => ac.abort(), 50);

  for await (const _ of streamEvents(db, t, {
    checkpoint: cp,
    batchSize: 2,
    pollIntervalMs: 5,
    signal: ac.signal,
  })) {
  }

  const lastId = all.at(-1)!.changeId;
  assert.ok(saved !== undefined);
  assert.equal(saved!, lastId);

  await client.exec("INSERT INTO t(v) VALUES ('c')");

  const ac2 = new AbortController();
  setTimeout(() => ac2.abort(), 50);

  const resumed: number[] = [];
  for await (const event of streamEvents(db, t, {
    checkpoint: cp,
    batchSize: 2,
    pollIntervalMs: 5,
    signal: ac2.signal,
  })) {
    resumed.push(event.changeId);
  }

  assert.equal(resumed.length, 1);
  await disableCdc(db);
  await client.close?.();
});

test("checkpoint.save fires on abort", async () => {
  const { client, db } = await setup();
  await enableCdc(db);
  await client.exec("INSERT INTO t(v) VALUES ('a')");
  await client.exec("INSERT INTO t(v) VALUES ('b')");

  let saved: ChangeId | undefined;
  const cp: CheckpointStrategy = {
    save: async (id) => {
      saved = id;
    },
    restore: async () => undefined,
  };

  const ac = new AbortController();
  setTimeout(() => ac.abort(), 50);

  for await (const _ of streamEvents(db, t, {
    checkpoint: cp,
    batchSize: 2,
    pollIntervalMs: 5,
    signal: ac.signal,
  })) {
  }

  assert.ok(saved !== undefined);
  assert.ok(saved! > 0);
  await disableCdc(db);
  await client.close?.();
});

test("checkpoint.save error does not crash stream", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");

  const cp: CheckpointStrategy = {
    save: async () => {
      throw new Error("save failed");
    },
    restore: async () => undefined,
  };

  const ac = new AbortController();
  setTimeout(() => ac.abort(), 50);

  let yielded = false;
  for await (const event of streamEvents(db, t, {
    checkpoint: cp,
    batchSize: 1,
    pollIntervalMs: 5,
    signal: ac.signal,
    mode: "full",
  })) {
    yielded = true;
    assert.equal(event.after?.v, "a");
  }

  assert.ok(yielded);
  await disableCdc(db);
  await client.close?.();
});

test("checkpoint.restore error starts fresh (no prior checkpoint)", async () => {
  const { client, db } = await setup();
  await enableCdc(db, "full");
  await client.exec("INSERT INTO t(v) VALUES ('a')");

  const cp: CheckpointStrategy = {
    save: async () => {},
    restore: async () => {
      throw new Error("corrupted");
    },
  };

  const ac = new AbortController();
  setTimeout(() => ac.abort(), 50);

  let count = 0;
  for await (const _ of streamEvents(db, t, {
    checkpoint: cp,
    batchSize: 10,
    pollIntervalMs: 5,
    signal: ac.signal,
  })) {
    count++;
  }

  assert.equal(count, 1);
  await disableCdc(db);
  await client.close?.();
});
