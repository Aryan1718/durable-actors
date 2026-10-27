import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import type { ActorConnection } from "../../src/actor/socket.js"
import { runWithActorClient } from "../../src/client/client.js"
import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorRuntime } from "../fixtures/actor-runtime.js"
import { assertReply } from "../fixtures/reply.js"

class Destination extends Actor {
    async increment(amount: number): Promise<number> {
        return amount
    }
}

class Relay extends Actor {
    async forward(): Promise<number> {
        const target = Destination.get("destination")
        const count = await target.increment(3)
        await target.broadcast("updated")
        const socket = await target.connect(null)
        socket.close()
        return count
    }
}

test("actors invoke, broadcast, and open sockets through the remote client", async () => {
    const events: string[] = []
    const transport = new RemoteActorClient(
        { controlPlaneUrl: "https://control.test", projectId: "test" },
        {
            telemetry: () => {},
            fetch: async (url, options) => {
                if (String(url).endsWith("/invoke")) {
                    assert.equal(JSON.parse(String(options?.body)).method, "increment")
                    events.push("invoke")
                    return Response.json({ outcome: { type: "completed", result: 3 } })
                }
                if (String(url).endsWith("/socket-effects")) {
                    assert.equal(JSON.parse(String(options?.body)).effects.length, 1)
                    events.push("broadcast")
                    return new Response(null, { status: 204 })
                }
                assert.ok(String(url).endsWith("/find-websocket"))
                events.push("connect")
                return Response.json({ websocketUrl: "wss://host.test/socket" })
            },
            connectWebSocket: async (): Promise<ActorConnection> => ({
                readyState: 1,
                send() {
                    assert.fail("unexpected socket send")
                },
                close() {
                    events.push("close")
                },
                addEventListener() {
                    assert.fail("unexpected socket listener")
                },
                removeEventListener() {
                    assert.fail("unexpected socket listener removal")
                }
            })
        }
    )
    const definition = registerActorClass(Relay, { actorName: "Relay", fields: [] })
    const reply = await runWithActorClient(transport, async () =>
        new ActorRuntime(definition, () => {}).handle({
            type: "invoke",
            request_id: "parent",
            actor: { project_id: "test", actor_name: "Relay", actor_id: "one" },
            method: "forward",
            args: []
        })
    )
    assertReply(reply, { type: "invoked", result: 3 })
    assert.deepEqual(events, ["invoke", "broadcast", "connect", "close"])
})
