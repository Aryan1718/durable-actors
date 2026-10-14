import assert from "node:assert/strict"
import { test } from "node:test"
import { Worker } from "node:worker_threads"

import { completeDatabaseCall } from "../../src/host/database-bridge.js"
import type { DatabaseRequest } from "../../src/host/database-bridge.js"

test("worker database calls block until the host replies, without a reply size limit", async context => {
    const worker = new Worker(new URL("../fixtures/database-bridge-worker.js", import.meta.url))
    context.after(() => worker.terminate())
    const text = "x".repeat(256 * 1024)
    const replies = new Promise<unknown[]>((resolve, reject) => {
        const received: unknown[] = []
        worker.on("error", reject)
        worker.on("message", (message: { type: string; request?: DatabaseRequest; value?: unknown }) => {
            if (message.type === "result") {
                received.push(message.value)
                if (received.length === 2) resolve(received)
                return
            }
            const request = message.request!
            if (received.length === 0)
                setTimeout(() =>
                    completeDatabaseCall(request, {
                        result: { type: "rows", columns: ["value"], rows: [[{ type: "text", value: text }]] }
                    })
                )
            else completeDatabaseCall(request, { error: "storage rejected write" })
        })
    })
    assert.deepEqual(await replies, [text.length, "storage rejected write"])
})
