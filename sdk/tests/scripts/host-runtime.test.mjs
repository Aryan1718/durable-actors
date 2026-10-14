import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { copyFile, mkdtemp, readFile, readdir, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { promisify } from "node:util"

import { buildHostRuntime } from "../../scripts/build-host-runtime.mjs"

test("execution package runs actors and recovers SQLite state without the development SDK", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "host-package-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const output = path.join(directory, "sdk")
    await buildHostRuntime(output)
    const manifest = JSON.parse(await readFile(path.join(output, "package.json"), "utf8"))
    assert.equal(manifest.dependencies, undefined)
    assert.equal(manifest.bin, undefined)
    assert.ok((await readdir(path.join(output, "licenses"))).length > 0)
    // Move the smoke program outside the repository so imports cannot fall back to its node_modules.
    const smoke = path.join(directory, "smoke.mjs")
    await copyFile(new URL("../fixtures/image-smoke.mjs", import.meta.url), smoke)
    const { stdout } = await promisify(execFile)(process.execPath, [smoke, output], {
        cwd: directory,
        timeout: 30000
    })
    assert.match(stdout, /database recovery passed/u)
})
