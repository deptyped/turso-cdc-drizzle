import type { SQL } from "drizzle-orm";
import { and, asc, eq, getTableName, gt, gte, inArray, lt, lte, ne, sql } from "drizzle-orm";
import { type AnySQLiteTable, blob, int, sqliteTable, text } from "drizzle-orm/sqlite-core";
import type { TursoDatabaseDatabase } from "drizzle-orm/tursodatabase";
import type { CdcChangeKind, CdcEvent, ChangeId, CheckpointStrategy } from "./types.js";
import { assertCdcChangeKind, cdcChangeKindToInt } from "./types.js";

export type CdcMode = "id" | "before" | "after" | "full";

export const tursoCdc = sqliteTable("turso_cdc", {
  changeId: int("change_id"),
  changeType: int("change_type"),
  changeTime: int("change_time"),
  changeTxnId: int("change_txn_id"),
  tableName: text("table_name"),
  id: int("id"),
  before: blob("before", { mode: "buffer" }),
  after: blob("after", { mode: "buffer" }),
  updates: blob("updates", { mode: "buffer" }),
});

// PRAGMAs reject bound params — use sql.raw after validation
export async function enableCdc(db: TursoDatabaseDatabase, mode: CdcMode = "id"): Promise<void> {
  if (!["id", "before", "after", "full"].includes(mode)) throw new Error(`Invalid CDC mode: ${mode}`);

  await db.run(sql.raw(`PRAGMA capture_data_changes_conn('${mode}')`));
}

export async function disableCdc(db: TursoDatabaseDatabase): Promise<void> {
  await db.run(sql.raw(`PRAGMA capture_data_changes_conn('off')`));
}

export interface DeleteEventsOptions {
  changeId?: { from?: number; to?: number };
  date?: { from?: number; to?: number };
  tableName?: string;
}

export async function deleteEvents(db: TursoDatabaseDatabase, opts: DeleteEventsOptions): Promise<void> {
  if (
    opts.date?.from === undefined &&
    opts.date?.to === undefined &&
    opts.changeId?.from === undefined &&
    opts.changeId?.to === undefined
  )
    throw new Error("deleteEvents requires at least one range (date or changeId)");

  const conditions: SQL[] = [];
  if (opts.date?.from !== undefined) conditions.push(gte(tursoCdc.changeTime, opts.date.from));
  if (opts.date?.to !== undefined) conditions.push(lte(tursoCdc.changeTime, opts.date.to));
  if (opts.changeId?.from !== undefined) conditions.push(gte(tursoCdc.changeId, opts.changeId.from));
  if (opts.changeId?.to !== undefined) conditions.push(lte(tursoCdc.changeId, opts.changeId.to));
  if (opts.tableName !== undefined) conditions.push(eq(tursoCdc.tableName, opts.tableName));
  await db.delete(tursoCdc).where(and(...conditions));
}

const parse = (v: unknown) => (v != null ? JSON.parse(v as string) : null);

function toCdcEvent<TTable extends AnySQLiteTable>(row: {
  changeId: number | null;
  changeType: number | null;
  changeTime: number | null;
  changeTxnId: number | null;
  tableName: string | null;
  rowId: number | null;
  before?: unknown;
  after?: unknown;
  updates?: unknown;
}): CdcEvent<TTable> {
  return {
    changeId: row.changeId! as ChangeId,
    changeType: assertCdcChangeKind(row.changeType!),
    changeTime: row.changeTime,
    changeTxnId: row.changeTxnId,
    tableName: row.tableName ?? "",
    rowId: row.rowId,
    before: parse(row.before),
    after: parse(row.after),
    updates: parse(row.updates),
  };
}

export interface GetEventsOptions {
  afterId?: ChangeId;
  beforeId?: ChangeId;
  kinds?: CdcChangeKind[];
  mode?: "id" | "full";
  deleteAfterRead?: boolean;
  limit: number;
}

export async function getEvents<TTable extends AnySQLiteTable>(
  db: TursoDatabaseDatabase,
  table: TTable,
  opts?: GetEventsOptions,
): Promise<CdcEvent<TTable>[]> {
  const tableName = getTableName(table);
  const conditions = [eq(tursoCdc.tableName, tableName)];
  if (!opts?.kinds?.length) conditions.push(ne(tursoCdc.changeType, 2));
  if (opts?.afterId !== undefined) conditions.push(gt(tursoCdc.changeId, opts.afterId));
  if (opts?.beforeId !== undefined) conditions.push(lt(tursoCdc.changeId, opts.beforeId));
  if (opts?.kinds?.length) conditions.push(inArray(tursoCdc.changeType, opts.kinds.map(cdcChangeKindToInt)));

  if (opts?.mode === "full") {
    const cols = sql.raw(`table_columns_json_array('${tableName}')`);
    const qb = db
      .select({
        changeId: tursoCdc.changeId,
        changeType: tursoCdc.changeType,
        changeTime: tursoCdc.changeTime,
        changeTxnId: tursoCdc.changeTxnId,
        tableName: tursoCdc.tableName,
        rowId: tursoCdc.id,
        before: sql<string | null>`bin_record_json_object(${cols}, ${tursoCdc.before})`,
        after: sql<string | null>`bin_record_json_object(${cols}, ${tursoCdc.after})`,
        updates: sql<string | null>`bin_record_json_object(${cols}, ${tursoCdc.updates})`,
      })
      .from(tursoCdc)
      .where(and(...conditions))
      .orderBy(asc(tursoCdc.changeId));
    const rows = await (opts?.limit !== undefined ? qb.limit(opts.limit) : qb);
    const events = rows.map((row) => toCdcEvent<TTable>(row));
    if (opts?.deleteAfterRead && events.length > 0)
      await deleteEvents(db, { changeId: { to: events.at(-1)!.changeId } });
    return events;
  }

  const qb = db
    .select({
      changeId: tursoCdc.changeId,
      changeType: tursoCdc.changeType,
      changeTime: tursoCdc.changeTime,
      changeTxnId: tursoCdc.changeTxnId,
      tableName: tursoCdc.tableName,
      rowId: tursoCdc.id,
    })
    .from(tursoCdc)
    .where(and(...conditions))
    .orderBy(asc(tursoCdc.changeId));
  const rows = await (opts?.limit !== undefined ? qb.limit(opts.limit) : qb);
  const events = rows.map((row) => toCdcEvent<TTable>(row));
  if (opts?.deleteAfterRead && events.length > 0) await deleteEvents(db, { changeId: { to: events.at(-1)!.changeId } });
  return events;
}

export interface StreamEventsOptions {
  afterId?: ChangeId;
  beforeId?: ChangeId;
  kinds?: CdcChangeKind[];
  mode?: "id" | "full";
  deleteAfterRead?: boolean;
  pollIntervalMs?: number;
  signal?: AbortSignal;
  deleteBatchSize?: number;
  deleteBatchWaitMs?: number;
  batchSize?: number;
  checkpoint?: CheckpointStrategy;
}

export async function* streamEvents<TTable extends AnySQLiteTable>(
  db: TursoDatabaseDatabase,
  table: TTable,
  opts?: StreamEventsOptions,
): AsyncGenerator<CdcEvent<TTable>> {
  const interval = opts?.pollIntervalMs ?? 1000;
  const batchSize = opts?.batchSize ?? 100;
  let lastId = opts?.afterId;
  if (lastId === undefined && opts?.checkpoint?.restore) {
    try {
      lastId = await opts.checkpoint.restore(db);
    } catch {
      // no prior checkpoint — start fresh
    }
  }
  const deleteAfterRead = opts?.deleteAfterRead;
  const deleteBatchSize = opts?.deleteBatchSize;
  const deleteBatchWaitMs = opts?.deleteBatchWaitMs;
  const isBatched = deleteAfterRead && deleteBatchSize !== undefined;
  let batchCount = 0;
  let lastDeleteTime = 0;

  const checkpoint = opts?.checkpoint;

  while (true) {
    if (opts?.signal?.aborted) {
      if (lastId !== undefined && checkpoint?.save) {
        await checkpoint.save(lastId, db).catch(() => {});
      }
      return;
    }

    const queryOpts: GetEventsOptions = { limit: batchSize };
    if (lastId !== undefined) {
      queryOpts.afterId = lastId as ChangeId;
    }
    if (opts?.mode) {
      queryOpts.mode = opts.mode;
    }
    if (opts?.beforeId !== undefined) {
      queryOpts.beforeId = opts.beforeId;
    }
    if (opts?.kinds?.length) {
      queryOpts.kinds = opts.kinds;
    }
    const events = await getEvents(db, table, queryOpts);
    for (const event of events) {
      yield event;
      const id = event.changeId;
      if (id > (lastId ?? -1)) lastId = id;
      batchCount++;
    }

    if (deleteAfterRead && lastId !== undefined) {
      if (isBatched) {
        const elapsed = Date.now() - lastDeleteTime;
        if (batchCount >= deleteBatchSize! || (deleteBatchWaitMs && elapsed >= deleteBatchWaitMs)) {
          await deleteEvents(db, { changeId: { to: lastId } });
          batchCount = 0;
          lastDeleteTime = Date.now();
        }
      } else {
        await deleteEvents(db, { changeId: { to: lastId } });
      }
    }

    if (events.length > 0 && lastId !== undefined && checkpoint?.save) {
      await checkpoint.save(lastId, db).catch(() => {});
    }

    await new Promise((resolve) => setTimeout(resolve, interval));
  }
}
