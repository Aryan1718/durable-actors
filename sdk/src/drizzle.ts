import { type DrizzleConfig, TransactionRollbackError } from "drizzle-orm"
import type { ExtractTablesWithRelations } from "drizzle-orm"
import type { SQLiteTransaction } from "drizzle-orm/sqlite-core"
import { type SqliteRemoteDatabase, type SqliteRemoteResult, drizzle as drizzleProxy } from "drizzle-orm/sqlite-proxy"
import { createHash } from "node:crypto"

import type { ActorDatabase, RuntimeActorDatabase, SqliteValue } from "./actor/database.js"

type Schema = Record<string, unknown>
type Method = "run" | "all" | "values" | "get"

/** A Drizzle database over an actor's SQLite. Transactions are nested actor transactions. */
type DurableActorsDatabase<TSchema extends Schema = Record<string, never>> = SqliteRemoteDatabase<TSchema>

type DurableActorsTransaction<TSchema extends Schema> = SQLiteTransaction<
    "async",
    SqliteRemoteResult,
    TSchema,
    ExtractTablesWithRelations<TSchema>
>

/** drizzle-kit output for actors: `migrations.js` generated with `driver: "durable-sqlite"`. */
interface DrizzleMigrations {
    readonly journal: {
        readonly entries: readonly { readonly idx: number; readonly when: number; readonly tag: string }[]
    }
    readonly migrations: Readonly<Record<string, string>>
}

/**
 * Creates a Drizzle database over `this.db`, using Drizzle's `sqlite-proxy` driver.
 * `transaction` maps onto {@link ActorDatabase.transaction}, so it nests and rolls back only its own writes,
 * and `batch` applies all of its statements or none.
 */
function drizzle<TSchema extends Schema = Record<string, never>>(
    db: ActorDatabase,
    config: DrizzleConfig<TSchema> = {}
): DurableActorsDatabase<TSchema> {
    // The adapter bundles with the actor while `db` comes from the host runtime, which may be older.
    if (!isRuntimeDatabase(db))
        throw new Error(
            "durable-actors/drizzle needs db.query and db.transaction, which this actor runtime does not provide; run the actor on a runtime at least as new as its durable-actors SDK"
        )
    const orm = drizzleProxy<TSchema>(
        async (sql, params, method) => execute(db, sql, params, method),
        async batch =>
            db.transaction(async () => batch.map(({ sql, params, method }) => execute(db, sql, params, method))),
        config
    )
    const transaction = <T>(operation: (tx: DurableActorsTransaction<TSchema>) => Promise<T>): Promise<T> =>
        db.transaction(() => operation(transactionScope()))
    const transactionScope = (): DurableActorsTransaction<TSchema> =>
        Object.assign(Object.create(orm) as DurableActorsTransaction<TSchema>, {
            transaction,
            rollback(): never {
                throw new TransactionRollbackError()
            }
        })
    orm.transaction = transaction
    return orm
}

function isRuntimeDatabase(db: ActorDatabase): db is RuntimeActorDatabase {
    return typeof (db as Partial<RuntimeActorDatabase>).query === "function" && typeof db.transaction === "function"
}

/** Applies each pending drizzle-kit migration in its own transaction; call it from the actor constructor. */
function migrate(db: ActorDatabase, { journal, migrations }: DrizzleMigrations): void {
    const ordered = journal.entries.map(entry => {
        const key = `m${entry.idx.toString().padStart(4, "0")}`
        const sql = migrations[key]
        if (sql === undefined) throw new Error(`Missing migration ${key} for journal entry ${entry.tag}`)
        return { sql, when: entry.when }
    })
    db.exec(
        'CREATE TABLE IF NOT EXISTS "__drizzle_migrations" (id INTEGER PRIMARY KEY AUTOINCREMENT, hash text NOT NULL, created_at numeric)'
    )
    const [latest] = db.exec<{ created_at: number }>(
        'SELECT created_at FROM "__drizzle_migrations" ORDER BY created_at DESC LIMIT 1'
    )
    for (const { sql, when } of ordered) {
        if (latest !== undefined && Number(latest.created_at) >= when) continue
        db.exec("BEGIN")
        try {
            for (const statement of sql.split("--> statement-breakpoint").map(part => part.trim()))
                if (statement !== "") db.exec(statement)
            db.exec(
                'INSERT INTO "__drizzle_migrations" (hash, created_at) VALUES (?, ?)',
                createHash("sha256").update(sql).digest("hex"),
                when
            )
            db.exec("COMMIT")
        } catch (error) {
            db.exec("ROLLBACK")
            throw error
        }
    }
}

// sqlite-proxy reads a single row for `get` (undefined when there is none) and row arrays otherwise.
function execute(db: RuntimeActorDatabase, sql: string, params: unknown[], method: Method): { rows: any[] } {
    const { rows } = db.query(sql, ...params.map(binding))
    if (method === "run") return { rows: [] }
    return { rows: method === "get" ? (rows[0] as any[]) : rows }
}

function binding(value: unknown): SqliteValue {
    if (typeof value === "boolean") return value ? 1 : 0
    if (
        value === null ||
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "bigint" ||
        value instanceof Uint8Array
    )
        return value
    throw new TypeError(`cannot bind ${typeof value} to an actor SQLite parameter`)
}

export { drizzle, migrate }
export type { DrizzleMigrations, DurableActorsDatabase, DurableActorsTransaction }
