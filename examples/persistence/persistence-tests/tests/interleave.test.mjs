import { createActorTransport } from "durable-actors/backend"
import { startLocalActors } from "durable-actors/dev"
import assert from "node:assert/strict"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"
import { fileURLToPath } from "node:url"

test("interleaved writes overlap and survive a worker crash and full restart", { timeout: 60000 }, async () => {
    const project = fileURLToPath(new URL("../", import.meta.url))
    const dataDir = await mkdtemp(path.join(tmpdir(), "persistence-interleave-test-"))
    const options = { project, entrypoint: "src/actors.ts", dataDir, projectId: "interleave-test", quiet: true }
    let runtime
    try {
        runtime = await startLocalActors(options)
        let client = connect(runtime.connection)
        assert.deepEqual(await client.invoke("InterleaveTests", "interleave-test", "getEvents", []), [])
        await Promise.all([
            client.invoke("InterleaveTests", "interleave-test", "testInterleave", ["slow", 3000]),
            client.invoke("InterleaveTests", "interleave-test", "testInterleave", ["fast", 100])
        ])
        const events = await client.invoke("InterleaveTests", "interleave-test", "getEvents", [])
        assert.deepEqual([...events].sort(), ["fast:end", "fast:start", "slow:end", "slow:start"])
        assert.ok(events.indexOf("slow:start") < events.indexOf("slow:end"))
        assert.ok(events.indexOf("fast:start") < events.indexOf("fast:end"))
        assert.ok(events.indexOf("fast:start") < events.indexOf("slow:end"), "calls must overlap")
        assert.ok(events.indexOf("slow:start") < events.indexOf("fast:end"), "calls must overlap")
        await assert.rejects(client.invoke("InterleaveTests", "interleave-test", "crash", []), { code: "outcome_unknown" })
        assert.deepEqual(await client.invoke("InterleaveTests", "interleave-test", "getEvents", []), events)
        await runtime.stop()
        runtime = await startLocalActors(options)
        client = connect(runtime.connection)
        assert.deepEqual(await client.invoke("InterleaveTests", "interleave-test", "getEvents", []), events)
    } finally {
        await runtime?.stop()
        await rm(dataDir, { recursive: true, force: true })
    }
})

function connect({ projectId, controlPlaneUrl, apiKey }) {
    return createActorTransport({ projectId, controlPlaneUrl, apiKey })
}
