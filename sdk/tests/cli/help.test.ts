import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))

test("CLI help explains server defaults and explicit source generation without requiring credentials", async () => {
    const { stdout } = await run(process.execPath, [cli, "generate", "--help"], {
        env: { ...process.env, DURABLE_ACTORS_PROJECT_ID: "", DURABLE_ACTORS_API_KEY: "" }
    })
    assert.match(stdout, /--control-plane-url/u)
    assert.match(stdout, /http:\/\/127\.0\.0\.1:7100/u)
    assert.match(stdout, /source file/u)
    assert.match(stdout, /DURABLE_ACTORS_PROJECT_ID/u)
})

test("dev help explains the project directory, entrypoint, and setup", async () => {
    const { stdout } = await run(process.execPath, [cli, "dev", "--help"])
    assert.match(stdout, /current directory/u)
    assert.match(stdout, /src\/actors\.ts/u)
    assert.match(stdout, /DURABLE_ACTORS_PROJECT\b/u)
    assert.match(stdout, /DURABLE_ACTORS_ENTRYPOINT/u)
    assert.match(stdout, /durable-actors init my-project/u)
})

test("inspect help describes the instance selector, JSON output, and connection settings", async () => {
    const { stdout } = await run(process.execPath, [cli, "inspect", "--help"], {
        env: { ...process.env, DURABLE_ACTORS_PROJECT_ID: "", DURABLE_ACTORS_API_KEY: "" }
    })
    assert.match(stdout, /ActorName\/actorId/u)
    assert.match(stdout, /--json/u)
    assert.match(stdout, /--control-plane-url/u)
    assert.match(stdout, /DURABLE_ACTORS_PROJECT_ID/u)
    assert.match(stdout, /http:\/\/127\.0\.0\.1:7100/u)
})
