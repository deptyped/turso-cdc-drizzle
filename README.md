# Drizzle ORM integration for [Turso CDC](https://docs.turso.tech/tursodb/cdc).

[Turso CDC](https://docs.turso.tech/tursodb/cdc) tracks every INSERT, UPDATE, and DELETE in your database as events. This library exposes those events through Drizzle ORM — read them in batches, consume them as a stream, or delete them after processing. All events are typed to your Drizzle table schemas.

```
npm install @deptyped/turso-cdc-drizzle
```

Requires `drizzle-orm` as a peer dependency.

## Quick start

Imports, table schema, and database instance.

```ts
import { drizzle } from 'drizzle-orm/tursodatabase';
import { sqliteTable, int, text } from 'drizzle-orm/sqlite-core';
import { enableCdc, getEvents, streamEvents } from '@deptyped/turso-cdc-drizzle';

const users = sqliteTable('users', {
  id: int('id').primaryKey(),
  name: text('name'),
});

const db = drizzle({ client });

// Enable CDC — required before any event queries
await enableCdc(db);
```

One-shot query with decoded row data, filtered by kind.

```ts
await db.insert(users).values({ id: 1, name: 'Alice' });

const events = await getEvents(db, users, { mode: 'full', kinds: ['INSERT'], limit: 10 });
// events[0]!.after — { id: 1, name: 'Alice' }
```

One-shot with auto-delete after read.

```ts
const events = await getEvents(db, users, { mode: 'full', limit: 10, deleteAfterRead: true });
// events are removed from the CDC table — next poll won't see them again
```

Streaming — poll for new changes.

```ts
for await (const event of streamEvents(db, users)) {
  console.log(event.changeType, event.rowId);
}

// With decoded data (requires enableCdc(db, 'full'))
for await (const event of streamEvents(db, users, { mode: 'full' })) {
  console.log(event.after?.name);
}
```

Streaming filtered by change kind.

```ts
for await (const event of streamEvents(db, users, { kinds: ['DELETE'] })) {
  console.log(event.rowId, 'was deleted');
}
```

Streaming with auto-delete after read.

```ts
for await (const event of streamEvents(db, users, { deleteAfterRead: true })) {
  process(event); // events are deleted after being yielded
}
```

## API

### `enableCdc(db, mode?)`

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `db` | `TursoDatabaseDatabase` | — | Drizzle Turso database |
| `mode` | `CdcMode` | `'id'` | `'id'` \| `'before'` \| `'after'` \| `'full'` |

Runs `PRAGMA capture_data_changes_conn`. Use `'full'` to capture row data (required for `mode: 'full'` queries). Tracked per-connection — call once per connection.

### `disableCdc(db)`

Disables CDC. No options.

### `getEvents(db, table, opts?)`

Returns `CdcEvent<TTable>[]`. COMMIT rows are filtered out automatically.

| Param | Type | Description |
|-------|------|-------------|
| `db` | `TursoDatabaseDatabase` | Drizzle Turso database |
| `table` | `TTable` | A Drizzle table definition |
| `opts.afterId` | `ChangeId` | Exclusive lower bound (gt) — events after this id |
| `opts.beforeId` | `ChangeId` | Exclusive upper bound (lt) — events before this id |
| `opts.kinds` | `CdcChangeKind[]` | `['INSERT']` \| `['UPDATE']` \| `['DELETE']` |
| `opts.mode` | `'id'` \| `'full'` | `'full'` decodes blob data into `before`/`after` |
| `opts.deleteAfterRead` | `boolean` | Auto-delete returned events |
| `opts.limit` | `number` | **Required.** Max events to return |

### `streamEvents(db, table, opts?)`

Returns `AsyncGenerator<CdcEvent<TTable>>`. Polls every `pollIntervalMs` (default 1000). Wrap in `for await`.

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `db` | `TursoDatabaseDatabase` | — | Drizzle Turso database |
| `table` | `TTable` | — | A Drizzle table definition |
| `opts.pollIntervalMs` | `number` | `1000` | Poll interval in ms |
| `opts.signal` | `AbortSignal` | — | Stop the stream via `AbortController` |
| `opts.afterId` | `ChangeId` | — | Resume from a previous event |
| `opts.beforeId` | `ChangeId` | — | Exclusive upper bound |
| `opts.mode` | `'id'` \| `'full'` | `'id'` | `'full'` includes decoded blob data |
| `opts.kinds` | `CdcChangeKind[]` | — | `['INSERT']` \| `['UPDATE']` \| `['DELETE']` |
| `opts.deleteAfterRead` | `boolean` | — | Auto-delete events after yielding |
| `opts.deleteBatchSize` | `number` | — | Batch delete every N events (requires `deleteAfterRead`) |
| `opts.deleteBatchWaitMs` | `number` | — | Max wait before flushing a partial batch (requires `deleteBatchSize`) |

### `deleteEvents(db, opts)`

Deletes events by `changeId` range, date range, or both (AND). Requires at least one range.

| Param | Type | Description |
|-------|------|-------------|
| `opts.changeId.from` | `number` | Inclusive lower bound |
| `opts.changeId.to` | `number` | Inclusive upper bound |
| `opts.date.from` | `number` | Unix timestamp, inclusive lower bound |
| `opts.date.to` | `number` | Unix timestamp, inclusive upper bound |
| `opts.tableName` | `string` | Scope deletion to a specific table |

## Types

### `CdcEvent<TTable>`

```ts
interface CdcEvent<TTable extends AnySQLiteTable = AnySQLiteTable> {
  changeId:   ChangeId;
  changeType: CdcChangeKind;
  changeTime: number | null;
  changeTxnId: number | null;
  tableName:  string;
  rowId:      number | null;
  before:     InferSelectModel<TTable> | null;
  after:      InferSelectModel<TTable> | null;
  updates:    Record<string, unknown> | null;
}
```

Pass a Drizzle table as the type parameter. `before`/`after` resolve to the table's row shape.

### `CdcChangeKind`

```ts
export const CdcChangeKind = {
  INSERT: 'INSERT',
  UPDATE: 'UPDATE',
  DELETE: 'DELETE',
  COMMIT: 'COMMIT',
} as const;

export type CdcChangeKind = (typeof CdcChangeKind)[keyof typeof CdcChangeKind];
```

`'COMMIT'` is an internal marker — `getEvents` never returns COMMIT rows.

### `ChangeId`

Branded `number` — use as-is from event fields, or cast yours with `id as ChangeId`.

### `CdcMode`

```ts
type CdcMode = 'id' | 'before' | 'after' | 'full';
```

Controls what data Turso captures at the PRAGMA level.

## Development

```sh
npm test
npm run build
npm run format
```
