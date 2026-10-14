import { parentPort } from "node:worker_threads"

import { requestDatabaseCall } from "../../src/host/database-bridge.js"

const send = (request: Parameters<Parameters<typeof requestDatabaseCall>[0]>[0]) =>
    parentPort!.postMessage({ type: "call", request }, [request.port])

const result = requestDatabaseCall(send)
parentPort!.postMessage({
    type: "result",
    value: result.type === "rows" ? String((result.rows[0]![0] as { value: string }).value).length : undefined
})
try {
    requestDatabaseCall(send)
} catch (error) {
    parentPort!.postMessage({ type: "result", value: (error as Error).message })
}
