import { MessageChannel, receiveMessageOnPort } from "node:worker_threads"
import type { MessagePort } from "node:worker_threads"

import type { DatabaseResult } from "./database.js"

interface DatabaseRequest {
    readonly signal: SharedArrayBuffer
    readonly port: MessagePort
}

type DatabaseReply = { readonly result: DatabaseResult } | { readonly error: string }

function requestDatabaseCall(send: (request: DatabaseRequest) => void): DatabaseResult {
    const { port1, port2 } = new MessageChannel()
    const signal = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT)
    try {
        send({ signal, port: port2 })
        Atomics.wait(new Int32Array(signal), 0, 0)
        const reply = receiveMessageOnPort(port1)?.message as DatabaseReply | undefined
        if (reply === undefined) throw new Error("actor database reply was lost")
        if ("error" in reply) throw new Error(reply.error)
        return reply.result
    } finally {
        port1.close()
    }
}

function completeDatabaseCall(request: DatabaseRequest, reply: DatabaseReply): void {
    const signal = new Int32Array(request.signal)
    if (Atomics.load(signal, 0) !== 0) return
    request.port.postMessage(reply)
    request.port.close()
    Atomics.store(signal, 0, 1)
    Atomics.notify(signal, 0)
}

export { completeDatabaseCall, requestDatabaseCall }
export type { DatabaseReply, DatabaseRequest }
