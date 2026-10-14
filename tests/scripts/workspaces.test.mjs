import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"
import { promisify } from "node:util"

const execute = promisify(execFile)

test("workspace installation links grouped examples alongside generated clients", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "durable-actors-workspaces-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    const { workspaces } = JSON.parse(await readFile(new URL("../../package.json", import.meta.url), "utf8"))
    const projects = ["examples/bank-typescript", "examples/persistence/persistence-tests", "examples/persistence/cli", "examples/pi/pi-durable", "examples/pi/pi-cli"]
    await manifest(root, "", { name: "workspace-test", private: true, workspaces })
    await manifest(root, "sdk", { name: "durable-actors", version: "0.0.0" })
    for (const project of projects) {
        await manifest(root, project, { name: project.replaceAll("/", "-"), dependencies: { "durable-actors": "workspace:*" } })
        await manifest(root, `${project}/generated`, { private: true, type: "module", peerDependencies: {} })
    }

    await execute("bun", ["install", "--ignore-scripts"], { cwd: root })

    for (const project of projects) {
        assert.equal(await realpath(path.join(root, project, "node_modules/durable-actors")), await realpath(path.join(root, "sdk")))
    }
})

async function manifest(root, project, contents) {
    const directory = path.join(root, project)
    await mkdir(directory, { recursive: true })
    await writeFile(path.join(directory, "package.json"), JSON.stringify(contents))
}
