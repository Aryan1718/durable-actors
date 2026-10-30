import { createActorTransport } from "durable-actors/backend"
import { startLocalActors } from "durable-actors/dev"
import assert from "node:assert/strict"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"
import { fileURLToPath } from "node:url"

test("durable writes survive crashes while atomic writes roll back, including after a full restart", { timeout: 60000 }, async () => {
    const project = fileURLToPath(new URL("../", import.meta.url))
    const dataDir = await mkdtemp(path.join(tmpdir(), "persistence-crash-test-"))
    const options = { project, entrypoint: "src/actors.ts", dataDir, projectId: "crash-test", quiet: true }
    let runtime
    try {
        runtime = await startLocalActors(options)
        let client = connect(runtime.connection)
        assert.equal(await client.invoke("PersistenceTests", "crash-test", "getCrashMarker", []), "")
        await assert.rejects(client.invoke("PersistenceTests", "crash-test", "writeThenCrash", ["pineapple"]), { code: "actor_error" })
        assert.equal(await client.invoke("PersistenceTests", "crash-test", "getCrashMarker", []), "pineapple")
        await assert.rejects(client.invoke("PersistenceTests", "crash-test", "atomicWriteThenCrash", ["should-not-survive"]), { code: "actor_error" })
        assert.equal(await client.invoke("PersistenceTests", "crash-test", "getCrashMarker", []), "pineapple")
        await runtime.stop()
        runtime = await startLocalActors(options)
        client = connect(runtime.connection)
        assert.equal(await client.invoke("PersistenceTests", "crash-test", "getCrashMarker", []), "pineapple")
    } finally {
        await runtime?.stop()
        await rm(dataDir, { recursive: true, force: true })
    }
})

function connect({ projectId, controlPlaneUrl, apiKey }) {
    return createActorTransport({ projectId, controlPlaneUrl, apiKey })
}
