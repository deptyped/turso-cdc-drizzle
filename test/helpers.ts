import { connect, type Database } from "@tursodatabase/database";
import { int, sqliteTable, text } from "drizzle-orm/sqlite-core";
import type { TursoDatabaseDatabase } from "drizzle-orm/tursodatabase";
import { drizzle } from "drizzle-orm/tursodatabase/database";

export const t = sqliteTable("t", {
  id: int("id").primaryKey(),
  v: text("v"),
});

export async function makeDb(): Promise<{ client: Database; db: TursoDatabaseDatabase }> {
  const client = await connect(":memory:");
  await client.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  const db = drizzle({ client });
  return { client, db };
}
