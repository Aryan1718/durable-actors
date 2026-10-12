import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime, SqliteActorDatabase } from "../fixtures/actor-runtime.js"
import { recover, seed } from "../fixtures/litestream.js"
import { assertReply } from "../fixtures/reply.js"

test("persisted assignments update SQLite before the method finishes", async t => {
    const database = new SqliteActorDatabase()
    let instance!: PersistedWriteCounter
    class PersistedWriteCounter extends Actor {
        count = 0
        temporary = 0

        async write() {
            instance = this
            assert.deepEqual(database.fields(), { count: 0 })
            this.count = 7
            this.temporary = 8
            assert.deepEqual(database.fields(), { count: 7 })
            assert.throws(() => {
                this.count = 1n as unknown as number
            }, /serializ/i)
            assert.equal(this.count, 7)
            return this.count
        }
    }
    const runtime = new ActorRuntime(
        registerActorClass(PersistedWriteCounter, {
            actorName: "PersistedWriteCounter",
            fields: [
                { name: "count", persistence: Persistence.Persisted },
                { name: "temporary", persistence: Persistence.Ephemeral }
            ]
        }),
        () => {},
        undefined,
        undefined,
        database
    )
    t.after(() => runtime.close())
    assertReply(
        await runtime.handle({
            type: "invoke",
            request_id: "write",
            actor: { project_id: "test", actor_name: "PersistedWriteCounter", actor_id: "one" },
            method: "write",
            args: [],
            sqlite: await seed()
        }),
        { type: "invoked", result: 7 }
    )
    assert.throws(() => {
        instance.count = 9
    }, /outside its invocation/)
    assert.equal(instance.count, 7)
})

test("hydration does not write defaults over restored persisted fields", async t => {
    const database = new SqliteActorDatabase()
    const sqlite = await seed()
    database.restore(sqlite)
    database.persistField("count", 42)
    await database.snapshot()
    class RestoredPersistedWriteCounter extends Actor {
        count = 0
        async read() {
            assert.deepEqual(database.fields(), { count: 42 })
            this.count += 1
            assert.deepEqual(database.fields(), { count: 43 })
            return this.count
        }
    }
    const runtime = new ActorRuntime(
        registerActorClass(RestoredPersistedWriteCounter, {
            actorName: "RestoredPersistedWriteCounter",
            fields: [{ name: "count", persistence: Persistence.Persisted }]
        }),
        () => {},
        undefined,
        undefined,
        database
    )
    t.after(() => runtime.close())
    assertReply(
        await runtime.handle({
            type: "invoke",
            request_id: "read",
            actor: { project_id: "test", actor_name: "RestoredPersistedWriteCounter", actor_id: "one" },
            method: "read",
            args: [],
            sqlite
        }),
        { type: "invoked", result: 43 }
    )
})

test("persisted objects require reassignment, including across actor restoration", async t => {
    const database = new SqliteActorDatabase()
    class ReassignedPersistedItems extends Actor {
        items: string[] = []
        async mutate() {
            this.items.push("unsaved")
            assert.deepEqual(database.fields(), { items: [] })
        }
        async assign() {
            this.items = ["saved"]
            assert.deepEqual(database.fields(), { items: ["saved"] })
            this.items.push("unsaved")
        }
        async read() {
            return this.items
        }
    }
    const definition = registerActorClass(ReassignedPersistedItems, {
        actorName: "ReassignedPersistedItems",
        fields: [{ name: "items", persistence: Persistence.Persisted }]
    })
    const runtime = new ActorRuntime(definition, () => {}, undefined, undefined, database)
    t.after(() => runtime.close())
    const sqlite = await seed()
    const command = {
        type: "invoke" as const,
        request_id: "reassign",
        actor: { project_id: "test", actor_name: "ReassignedPersistedItems", actor_id: "one" },
        method: "mutate",
        args: [],
        sqlite
    }
    for (const [method, expected] of [
        ["mutate", []],
        ["assign", ["saved"]]
    ] as const) {
        const reply = await runtime.handle({ ...command, method })
        assert.equal(reply.type, "invoked")
        if (reply.type !== "invoked") return
        assert.deepEqual(database.fields(), { items: expected })
        const restored = new ActorRuntime(definition, () => {})
        t.after(() => restored.close())
        assertReply(
            await restored.handle({ ...command, method: "read", sqlite: await recover(sqlite, reply.sqlite) }),
            {
                type: "invoked",
                result: [...expected]
            }
        )
    }
})

test("initializing added fields preserves stored values and survives restoration", async t => {
    const database = new SqliteActorDatabase()
    const sqlite = await seed()
    database.restore(sqlite)
    database.persistField("count", 42)
    await database.snapshot()
    database.initializeFields({ count: 0, theme: "light" })
    assert.deepEqual(database.fields(), { count: 42, theme: "light" })
    const saved = await database.snapshot()
    database.initializeFields({ count: -1, theme: "dark" })
    assert.deepEqual(await database.snapshot(), saved)
    database.close()

    class AddedPersistedProperty extends Actor {
        count = 0
        theme = "dark"
        enabled = true
        async read() {
            return { count: this.count, theme: this.theme, enabled: this.enabled }
        }
    }
    const definition = registerActorClass(AddedPersistedProperty, {
        actorName: "AddedPersistedProperty",
        fields: ["count", "theme", "enabled"].map(name => ({ name, persistence: Persistence.Persisted }))
    })
    const runtime = new ActorRuntime(definition, () => {})
    t.after(() => runtime.close())
    const command = {
        type: "invoke" as const,
        request_id: "added",
        actor: { project_id: "test", actor_name: "AddedPersistedProperty", actor_id: "one" },
        method: "read",
        args: [],
        sqlite: await recover(sqlite, saved)
    }
    const reply = await runtime.handle(command)
    assertReply(reply, { type: "invoked", result: { count: 42, theme: "light", enabled: true } })
    assert.equal(reply.type, "invoked")
    if (reply.type !== "invoked") return
    const restored = new ActorRuntime(definition, () => {})
    t.after(() => restored.close())
    const recovered = await recover(command.sqlite, reply.sqlite)
    const again = await restored.handle({ ...command, sqlite: recovered })
    assertReply(again, { type: "invoked", result: { count: 42, theme: "light", enabled: true } })
    if (again.type === "invoked") assert.equal(again.sqlite.txid, recovered.txid)
})
