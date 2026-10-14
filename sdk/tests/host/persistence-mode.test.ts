import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { type SqliteValue, actorDatabaseMode } from "../../src/actor/database.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply } from "../fixtures/reply.js"

test("entrypoint selects persistence mode for SQL, properties, awaits and nested calls", async t => {
    const modes: unknown[] = []
    class RecordingDatabase extends DatabaseProxy {
        constructor() {
            super(new HostDatabase().call)
        }
        override persistField() {
            modes.push(actorDatabaseMode())
        }
        override exec<Row extends object>(_sql: string, ..._bindings: SqliteValue[]): Row[] {
            modes.push(actorDatabaseMode())
            return []
        }
    }
    class ModeProbe extends Actor {
        count = 0
        async atomic(nested: boolean) {
            this.count++
            await Promise.resolve()
            this.db.exec("SELECT 1")
            if (nested) await this.ordinary(false)
        }
        async ordinary(nested: boolean) {
            this.count++
            await Promise.resolve()
            this.db.exec("SELECT 1")
            if (nested) await this.atomic(false)
        }
    }
    const runtime = new ActorRuntime(
        registerActorClass(ModeProbe, {
            actorName: "ModeProbe",
            fields: [{ name: "count", persistence: Persistence.Persisted }],
            atomicMethods: ["atomic"]
        }),
        () => {},
        undefined,
        undefined,
        new RecordingDatabase()
    )
    t.after(() => runtime.close())
    for (const [method, expected] of [
        ["atomic", "atomic"],
        ["ordinary", "immediate"]
    ]) {
        modes.length = 0
        assertReply(
            await runtime.handle({
                type: "invoke",
                request_id: method!,
                actor: { project_id: "test", actor_name: "ModeProbe", actor_id: "one" },
                method: method!,
                args: [true]
            }),
            { type: "invoked", result: null }
        )
        assert.deepEqual(modes, [expected, expected, expected, expected])
        assert.equal(actorDatabaseMode(), undefined)
    }
})
