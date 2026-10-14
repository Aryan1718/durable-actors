import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persisted } from "../../src/actor/decorators.js"
import { Persistence } from "../../src/actor/schema.js"
import type { InvokeCommand } from "../../src/host/protocol.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"

class ObservableRoom extends Actor {
    @Persisted messages: string[] = []
    @Persisted title = "Room"
    @Persisted private secret = "secret"
    @Persisted protected internal = "internal"
    @Persisted status?: string = "online"

    async change() {
        this.messages = [...this.messages, "first"]
        this.messages = [...this.messages, "second"]
        this.title = "Renamed"
        this.secret = "changed"
        this.status = undefined
    }

    async unchanged() {
        this.messages.push("temporary")
        this.messages.pop()
    }

    async fail() {
        this.messages = [...this.messages, "failed"]
        throw new Error("failed")
    }
}

const fields = [
    { name: "messages", persistence: Persistence.Persisted, emittable: true },
    { name: "title", persistence: Persistence.Persisted },
    { name: "secret", persistence: Persistence.Persisted, visibility: "private" as const },
    { name: "internal", persistence: Persistence.Persisted, visibility: "protected" as const },
    { name: "status", persistence: Persistence.Persisted, emittable: true }
]
const definition = registerActorClass(ObservableRoom, { actorName: "ObservableRoom", fields })
const actor = { project_id: "default", actor_name: "ObservableRoom", actor_id: "room" }

test("initial snapshots expose only emittable fields while still saving all persisted state", async () => {
    const host = new HostDatabase()
    const runtime = new ActorRuntime(definition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
    const connection = { id: "connection", metadata: {}, tags: [] }
    const reply = await runtime.handle({
        type: "websocket_event",
        request_id: "connect",
        actor,
        connections: [connection],
        event: { type: "connect", connection }
    })
    assert.equal(reply.type, "websocket_handled")
    if (reply.type !== "websocket_handled") return
    assert.deepEqual(host.fields(), {
        messages: [],
        title: "Room",
        secret: "secret",
        internal: "internal",
        status: "online"
    })
    assert.deepEqual(reply.effects, [
        {
            type: "state_snapshot",
            connection_id: "connection",
            state: { messages: [], status: "online" }
        }
    ])
})

test("coalesces assignments and removals into one final update without live publication", async () => {
    const host = new HostDatabase()
    const runtime = new ActorRuntime(
        definition,
        () => {},
        async () => assert.fail("automatic changes must wait for commit"),
        undefined,
        new DatabaseProxy(host.call)
    )
    const reply = await runtime.handle(invocation("change"))
    assert.equal(reply.type, "invoked")
    if (reply.type !== "invoked") return
    assert.deepEqual(reply.effects, [
        {
            type: "state_update",
            changes: { messages: ["first", "second"] },
            removed: ["status"]
        }
    ])
    assert.deepEqual(host.fields(), {
        messages: ["first", "second"],
        title: "Renamed",
        secret: "changed",
        internal: "internal"
    })
})

test("does not emit intermediate values, unchanged fields, or failed operations", async () => {
    const runtime = new ActorRuntime(
        definition,
        () => {},
        async () => assert.fail("unexpected live output")
    )
    const unchanged = await runtime.handle(invocation("unchanged"))
    assert.equal(unchanged.type, "invoked")
    if (unchanged.type === "invoked") assert.equal(unchanged.effects, undefined)
    const failed = await runtime.handle(invocation("fail"))
    assert.equal(failed.type, "failed")
    assert.equal("effects" in failed, false)
})

function invocation(method: string): InvokeCommand {
    return { type: "invoke", request_id: method, actor, method, args: [] }
}
