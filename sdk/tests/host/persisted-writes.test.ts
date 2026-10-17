import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply } from "../fixtures/reply.js"

test("persisted assignments update SQLite before the method finishes", async t => {
    const host = new HostDatabase()
    let instance!: PersistedWriteCounter
    class PersistedWriteCounter extends Actor {
        count = 0
        temporary = 0

        async write() {
            instance = this
            assert.deepEqual(host.fields(), { count: 0 })
            this.count = 7
            this.temporary = 8
            assert.deepEqual(host.fields(), { count: 7 })
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
        new DatabaseProxy(host.call)
    )
    t.after(() => runtime.close(true))
    assertReply(
        await runtime.handle({
            type: "invoke",
            request_id: "write",
            actor: { project_id: "test", actor_name: "PersistedWriteCounter", actor_id: "one" },
            method: "write",
            args: []
        }),
        { type: "invoked", result: 7 }
    )
    assert.throws(() => {
        instance.count = 9
    }, /outside its invocation/)
    assert.equal(instance.count, 7)
})

test("hydration does not write defaults over restored persisted fields", async t => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    database.persistField("count", 42)
    database.commit()
    class RestoredPersistedWriteCounter extends Actor {
        count = 0
        async read() {
            assert.deepEqual(host.fields(), { count: 42 })
            this.count += 1
            assert.deepEqual(host.fields(), { count: 43 })
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
    t.after(() => runtime.close(true))
    assertReply(
        await runtime.handle({
            type: "invoke",
            request_id: "read",
            actor: { project_id: "test", actor_name: "RestoredPersistedWriteCounter", actor_id: "one" },
            method: "read",
            args: []
        }),
        { type: "invoked", result: 43 }
    )
})

test("persisted objects require reassignment, including across actor restoration", async t => {
    let host = new HostDatabase()
    class ReassignedPersistedItems extends Actor {
        items: string[] = []
        async mutate() {
            this.items.push("unsaved")
            assert.deepEqual(host.fields(), { items: [] })
        }
        async assign() {
            this.items = ["saved"]
            assert.deepEqual(host.fields(), { items: ["saved"] })
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
    const command = {
        type: "invoke" as const,
        request_id: "reassign",
        actor: { project_id: "test", actor_name: "ReassignedPersistedItems", actor_id: "one" },
        args: []
    }
    for (const [method, expected] of [
        ["mutate", []],
        ["assign", ["saved"]]
    ] as const) {
        const runtime = new ActorRuntime(definition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
        t.after(() => runtime.close(true))
        const reply = await runtime.handle({ ...command, method })
        assert.equal(reply.type, "invoked")
        assert.deepEqual(host.fields(), { items: expected })
        host = host.reopen()
        const restored = new ActorRuntime(definition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
        t.after(() => restored.close(true))
        assertReply(await restored.handle({ ...command, method: "read" }), {
            type: "invoked",
            result: [...expected]
        })
    }
    host.close()
})

test("initializing added fields preserves stored values and survives restoration", async t => {
    let host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    database.persistField("count", 42)
    database.commit()
    database.initializeFields({ count: 0, theme: "light" })
    assert.deepEqual(database.fields(), { count: 42, theme: "light" })
    database.commit()
    const saved = host.commits
    database.initializeFields({ count: -1, theme: "dark" })
    database.commit()
    assert.equal(host.commits, saved)

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
    const command = {
        type: "invoke" as const,
        request_id: "added",
        actor: { project_id: "test", actor_name: "AddedPersistedProperty", actor_id: "one" },
        method: "read",
        args: []
    }
    for (const commits of [1, 0]) {
        host = host.reopen()
        const runtime = new ActorRuntime(definition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
        t.after(() => runtime.close(true))
        assertReply(await runtime.handle(command), {
            type: "invoked",
            result: { count: 42, theme: "light", enabled: true }
        })
        assert.equal(host.commits, commits)
    }
    host.close()
})
