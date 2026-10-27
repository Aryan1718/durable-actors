import assert from "node:assert/strict"
import { test } from "node:test"

import { connectionRefused, parseActorHostReply } from "../../src/client-runtime/http.js"
import { ActorProtocolError } from "../../src/errors.js"

test("HTTP replies preserve host latency metadata for every outcome", async () => {
    for (const outcome of [
        { type: "completed", result: 7 },
        { type: "failed", code: "actor_error", message: "failed" },
        { type: "not_executed", reason: "host_unavailable" }
    ]) {
        const reply = {
            ...outcome,
            metadata: {
                routingMs: 500,
                durationMs: 12.5,
                queueWaitMs: outcome.type === "not_executed" ? null : 2.5,
                hostState: outcome.type === "completed" ? "cold" : "warm"
            }
        }
        assert.deepEqual(parseActorHostReply(reply), reply)
    }
})

test("HTTP replies reject invalid host latency metadata", async () => {
    for (const metadata of [
        null,
        {},
        { routingMs: -1, durationMs: 12, queueWaitMs: null, hostState: "warm" },
        { routingMs: 500, durationMs: -1, queueWaitMs: null, hostState: "warm" },
        { routingMs: 500, durationMs: "12", queueWaitMs: null, hostState: "warm" },
        { routingMs: 500, durationMs: 12, hostState: "warm" },
        { routingMs: 500, durationMs: 12, queueWaitMs: -1, hostState: "warm" },
        { routingMs: 500, durationMs: 12, queueWaitMs: 13, hostState: "warm" },
        { routingMs: 500, durationMs: 12, queueWaitMs: "2", hostState: "warm" },
        { routingMs: 500, durationMs: 12, queueWaitMs: 2, hostState: "unknown" },
        { routingMs: 500, durationMs: 12, queueWaitMs: 2, hostState: null },
        { routingMs: 500, durationMs: 12, queueWaitMs: 2, hostState: true },
        { routingMs: 500, durationMs: 12, queueWaitMs: 2 }
    ]) {
        assert.throws(() => parseActorHostReply({ type: "completed", result: 7, metadata }), ActorProtocolError)
    }
})

test("HTTP replies require an explicit valid outcome", async () => {
    for (const document of [
        {},
        { type: "completed" },
        { type: "failed", code: 3 },
        { type: "unauthenticated" },
        { type: "not_executed" },
        { type: "not_executed", reason: "" },
        { type: "not_executed", reason: "unknown" },
        { type: "not_executed", reason: 1 }
    ]) {
        assert.throws(() => parseActorHostReply(document), ActorProtocolError)
    }
})

test("connection refusals identify requests that were never dispatched", async () => {
    const refused = () => Object.assign(new Error("refused"), { code: "ECONNREFUSED", syscall: "connect" })
    for (const error of [
        refused(),
        new TypeError("fetch failed", { cause: refused() }),
        new TypeError("fetch failed", { cause: new AggregateError([refused(), refused()]) })
    ]) {
        assert.equal(connectionRefused(error), true)
    }
})

test("unproven transport failures preserve ambiguity", async () => {
    const refused = Object.assign(new Error("refused"), { code: "ECONNREFUSED", syscall: "connect" })
    const cyclic = new Error("cycle")
    cyclic.cause = cyclic
    for (const error of [
        new TypeError("fetch failed"),
        new Error("connect ECONNREFUSED"),
        Object.assign(new Error("refused"), { code: "ECONNREFUSED" }),
        Object.assign(new Error("reset"), { code: "ECONNRESET", syscall: "read", cause: refused }),
        Object.assign(new Error("timeout"), { code: "ETIMEDOUT" }),
        new AggregateError([refused, new Error("connection lost")]),
        Object.assign(new AggregateError([refused, cyclic]), { code: "ECONNREFUSED" }),
        new AggregateError([]),
        cyclic
    ]) {
        assert.equal(connectionRefused(error), false)
    }
})

test("explicit pre-execution rejections preserve their reason", async () => {
    for (const reason of ["host_unavailable", "upstream_not_reached"]) {
        const reply = { type: "not_executed", reason }
        assert.deepEqual(parseActorHostReply(reply), reply)
    }
})
