type Brand<T, B extends string> = T & { readonly __brand: B };

export type ChangeId = Brand<number, "ChangeId">;

export const CdcChangeKind = {
  INSERT: "INSERT",
  UPDATE: "UPDATE",
  DELETE: "DELETE",
  COMMIT: "COMMIT",
} as const;

export type CdcChangeKind = (typeof CdcChangeKind)[keyof typeof CdcChangeKind];

export function assertCdcChangeKind(value: number): CdcChangeKind {
  switch (value) {
    case 1:
      return CdcChangeKind.INSERT;
    case 0:
      return CdcChangeKind.UPDATE;
    case -1:
      return CdcChangeKind.DELETE;
    case 2:
      return CdcChangeKind.COMMIT;
    default:
      throw new Error(`Unknown CDC change kind: ${value}`);
  }
}

export function cdcChangeKindToInt(kind: CdcChangeKind): number {
  switch (kind) {
    case CdcChangeKind.INSERT:
      return 1;
    case CdcChangeKind.UPDATE:
      return 0;
    case CdcChangeKind.DELETE:
      return -1;
    case CdcChangeKind.COMMIT:
      return 2;
  }
}

import type { InferSelectModel } from "drizzle-orm";
import type { AnySQLiteTable } from "drizzle-orm/sqlite-core";

export interface CdcEvent<TTable extends AnySQLiteTable = AnySQLiteTable> {
  changeId: ChangeId;
  changeType: CdcChangeKind;
  changeTime: number | null;
  changeTxnId: number | null;
  tableName: string;
  rowId: number | null;
  before: InferSelectModel<TTable> | null;
  after: InferSelectModel<TTable> | null;
  updates: Record<string, unknown> | null;
}
