import { AsyncLocalStorage } from "node:async_hooks"

type SqliteValue = string | number | bigint | Uint8Array | null

interface ActorDatabase {
    /** Executes one SQL statement with positional bindings and returns its rows. */
    exec<Row extends object = Record<string, SqliteValue>>(sql: string, ...bindings: SqliteValue[]): Row[]
}

type PersistenceMode = "atomic" | "immediate"

interface ActorInvocationContext {
    readonly actor: object
    active: boolean
    readonly mode: PersistenceMode
}

const invocation = new AsyncLocalStorage<ActorInvocationContext>()

function isActorInvocation(actor: object): boolean {
    const context = invocation.getStore()
    return context?.actor === actor && context.active
}

function actorDatabaseMode(): PersistenceMode | undefined {
    const context = invocation.getStore()
    return context?.active ? context.mode : undefined
}

async function runWithActorDatabase<T>(actor: object, mode: PersistenceMode, operation: () => Promise<T>): Promise<T> {
    const context: ActorInvocationContext = { actor, active: true, mode }
    try {
        return await invocation.run(context, operation)
    } finally {
        context.active = false
    }
}

export { actorDatabaseMode, isActorInvocation, runWithActorDatabase }
export type { ActorDatabase, SqliteValue }
