interface SqlitePersistenceResult {
    readonly txid: number
    readonly stateVersion?: number
}

type SqlitePersister = (commitId: number, txid: number) => Promise<SqlitePersistenceResult>
type PersistenceReply = { result: SqlitePersistenceResult } | { error: string }

function persistenceBuffer(): SharedArrayBuffer {
    return new SharedArrayBuffer(64 * 1024)
}

function waitForPersistence(buffer: SharedArrayBuffer, timeoutMs = 30_000): SqlitePersistenceResult {
    const header = new Int32Array(buffer, 0, 2)
    if (Atomics.wait(header, 0, 0, timeoutMs) === "timed-out")
        throw new Error("SQLite persistence acknowledgment timed out")
    const length = Atomics.load(header, 1)
    const reply = JSON.parse(new TextDecoder().decode(new Uint8Array(buffer, 8, length))) as PersistenceReply
    if ("error" in reply) throw new Error(reply.error)
    return reply.result
}

function completePersistence(buffer: SharedArrayBuffer, reply: PersistenceReply): void {
    const header = new Int32Array(buffer, 0, 2)
    if (Atomics.load(header, 0) !== 0) return
    let bytes = new TextEncoder().encode(JSON.stringify(reply))
    if (bytes.length > buffer.byteLength - 8)
        bytes = new TextEncoder().encode(
            JSON.stringify({ error: "SQLite persistence acknowledgment exceeded size limit" })
        )
    new Uint8Array(buffer, 8, bytes.length).set(bytes)
    Atomics.store(header, 1, bytes.length)
    Atomics.store(header, 0, 1)
    Atomics.notify(header, 0)
}

export { persistenceBuffer, waitForPersistence, completePersistence }
export type { SqlitePersistenceResult, SqlitePersister }
