import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply } from "../fixtures/reply.js"

class AlarmProbe extends Actor {
    deliveries = 0

    async readAlarm(): Promise<number | null> {
        return this.alarm.get()
    }
    async setAlarm(timestampMs: number): Promise<number | null> {
        this.alarm.set(timestampMs)
        return this.alarm.get()
    }
    async deleteAlarm(): Promise<number | null> {
        this.alarm.delete()
        return this.alarm.get()
    }

    async readDeliveries(): Promise<number> {
        return this.deliveries
    }

    async onAlarm(): Promise<void> {
        this.deliveries++
    }
}

const definition = registerActorClass(AlarmProbe, {
    actorName: "AlarmProbe",
    fields: [{ name: "deliveries", persistence: Persistence.Persisted }]
})

function actorInvoker(runtime: ActorRuntime) {
    return (method: string, args: number[] = []) =>
        runtime.handle({
            type: "invoke",
            request_id: method,
            actor: {
                project_id: "test",
                actor_name: "AlarmProbe",
                actor_id: "one"
            },
            method,
            args
        })
}

function createRuntime() {
    const host = new HostDatabase()
    const runtime = new ActorRuntime(definition, () => {}, undefined, undefined, new DatabaseProxy(host.call))
    return { runtime, host }
}

test("actors can read, replace, and delete their alarm", async context => {
    const { runtime, host } = createRuntime()
    context.after(async () => {
        await runtime.close()
        host.close()
    })
    const invoke = actorInvoker(runtime)
    assertReply(await invoke("readAlarm"), { type: "invoked", result: null })
    assertReply(await invoke("setAlarm", [1_000]), { type: "invoked", result: 1_000 })
    assertReply(await invoke("setAlarm", [2_000]), { type: "invoked", result: 2_000 })
    assertReply(await invoke("deleteAlarm"), { type: "invoked", result: null })
})

test("delivers alarms to onAlarm", async context => {
    const { runtime, host } = createRuntime()
    context.after(async () => {
        await runtime.close()
        host.close()
    })
    assertReply(await actorInvoker(runtime)("onAlarm"), { type: "invoked", result: null })
    assertReply(await actorInvoker(runtime)("readDeliveries"), { type: "invoked", result: 1 })
})
