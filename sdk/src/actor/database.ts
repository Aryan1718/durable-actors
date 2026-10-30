import { AsyncLocalStorage } from "node:async_hooks"

type SqliteValue = string | number | bigint | Uint8Array | null

interface ActorDatabase {
    /** Executes one SQL statement with positional bindings and returns its rows. */
    exec<Row extends object = Record<string, SqliteValue>>(sql: string, ...bindings: SqliteValue[]): Row[]
    /** Runs `operation` in a SQLite transaction: if it throws, its SQL and `@Persisted` writes roll back. Transactions nest. */
    transaction<T>(operation: () => Promise<T>): Promise<T>
}

// Not public: durable-actors/drizzle reads rows by position, which `exec` cannot return for duplicate column names.
interface RuntimeActorDatabase extends ActorDatabase {
    query(sql: string, ...bindings: SqliteValue[]): ActorQueryResult
}

interface ActorQueryResult {
    readonly columns: string[]
    readonly rows: SqliteValue[][]
}

interface ActorInvocationContext {
    readonly actor: object
    active: boolean
}

const invocation = new AsyncLocalStorage<ActorInvocationContext>()

function isActorInvocation(actor: object): boolean {
    const context = invocation.getStore()
    return context?.actor === actor && context.active
}

async function runWithActorDatabase<T>(actor: object, operation: () => Promise<T>): Promise<T> {
    const context: ActorInvocationContext = { actor, active: true }
    try {
        return await invocation.run(context, operation)
    } finally {
        context.active = false
    }
}

export { isActorInvocation, runWithActorDatabase }
export type { ActorDatabase, ActorQueryResult, RuntimeActorDatabase, SqliteValue }
