import { expect, test } from "bun:test"
import { startLocalActors } from "durable-actors/dev"
import { execFile } from "node:child_process"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)

test("separate CLI commands share actor state through the generated client", async () => {
    const project = fileURLToPath(new URL("../", import.meta.url))
    const cli = fileURLToPath(new URL("../../../../sdk/dist/cli.js", import.meta.url))
    const dataDir = await mkdtemp(path.join(tmpdir(), "pi-cli-test-"))
    let runtime: Awaited<ReturnType<typeof startLocalActors>> | undefined
    try {
        await run(process.execPath, [cli, "generate", "tests/fixtures/actors.ts"], { cwd: project })
        runtime = await startLocalActors({
            project,
            entrypoint: "tests/fixtures/actors.ts",
            dataDir,
            projectId: "pi-cli-test",
            apiKey: "pi-cli-test-key",
            startupTimeoutMs: 15_000,
            quiet: true
        })
        const env: NodeJS.ProcessEnv = {
            ...process.env,
            DURABLE_ACTORS_PROJECT_ID: runtime.connection.projectId,
            DURABLE_ACTORS_CONTROL_PLANE_URL: runtime.connection.controlPlaneUrl,
            DURABLE_ACTORS_API_KEY: "pi-cli-test-key"
        }
        delete env.DURABLE_ACTORS_SECRET
        const first = await run(process.execPath, ["src/index.ts", "send", "Alice"], { cwd: project, env })
        const second = await run(process.execPath, ["src/index.ts", "send", "Hello again"], { cwd: project, env })
        expect(first.stdout.trim()).toBe("Alice")
        expect(second.stdout.trim()).toBe("Alice | Hello again")
        const help = await run(process.execPath, ["src/index.ts", "--help"], { cwd: project, env })
        expect(help.stdout).toContain("send <prompt>")
        await expect(run(process.execPath, ["src/index.ts", "send"], { cwd: project, env })).rejects.toMatchObject({
            code: 1,
            stderr: expect.stringContaining("missing required argument 'prompt'")
        })
    } finally {
        await runtime?.stop()
        await rm(dataDir, { recursive: true, force: true })
    }
}, 30_000)
