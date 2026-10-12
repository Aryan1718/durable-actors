import { AsyncLocalStorage } from "node:async_hooks"

import { ActorDefinitionError } from "../errors.js"

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

const databases = new WeakMap<object, ActorDatabase>()
const invocation = new AsyncLocalStorage<ActorInvocationContext>()
const construction = new AsyncLocalStorage<{ database: ActorDatabase; actor?: object; active: boolean }>()

function actorDatabase(actor: object): ActorDatabase {
    const database = databases.get(actor)
    if (database === undefined)
        throw new ActorDefinitionError("actor database is unavailable outside the actor runtime")
    return database
}

function constructWithActorDatabase<T>(database: ActorDatabase, construct: () => T): T {
    const context = { database, active: true }
    try {
        return construction.run(context, construct)
    } finally {
        context.active = false
    }
}

function bindConstructingActorDatabase(actor: object): void {
    const context = construction.getStore()
    if (!context?.active || context.actor !== undefined) return
    context.actor = actor
    bindActorDatabase(actor, context.database)
}

function bindActorDatabase(actor: object, database: ActorDatabase): void {
    databases.set(actor, {
        exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
            assertActorDatabaseAccess(actor)
            return database.exec<Row>(sql, ...bindings)
        }
    })
}

function assertActorDatabaseAccess(actor: object): void {
    const context = invocation.getStore()
    const creating = construction.getStore()
    if (!(context?.actor === actor && context.active) && !(creating?.actor === actor && creating.active))
        throw new ActorDefinitionError("actor database is unavailable outside its invocation")
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

export {
    actorDatabase,
    actorDatabaseMode,
    assertActorDatabaseAccess,
    bindConstructingActorDatabase,
    constructWithActorDatabase,
    runWithActorDatabase
}
export type { ActorDatabase, SqliteValue }
