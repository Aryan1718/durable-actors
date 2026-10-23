import assert from "node:assert/strict"
import { createHash } from "node:crypto"
import { mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"
import { c } from "tar"

import { RuntimeInstaller } from "../../src/runtimeInstaller.js"

test("installs the runtime from a verified release and reuses the cache offline", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "ldo-install-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const archive = await fixture(directory)
    const requests: string[] = []
    const download = async (url: string) => {
        requests.push(url)
        if (url.endsWith(".sha256")) return new Response(createHash("sha256").update(archive).digest("hex"))
        let offset = 0
        return new Response(
            new ReadableStream({
                pull(controller) {
                    if (offset === archive.length) return controller.close()
                    const end = Math.min(offset + 17, archive.length)
                    controller.enqueue(archive.subarray(offset, end))
                    offset = end
                }
            })
        )
    }
    const options = {
        version: "1.2.3",
        platform: "darwin",
        arch: "arm64",
        cacheDirectory: path.join(directory, "cache")
    }
    const binary = await new RuntimeInstaller(options, download).install()
    assert.equal(await readFile(binary, "utf8"), "runtime")
    assert.equal(
        await readFile(path.join(path.dirname(binary), "NOTICE.terse-litestream"), "utf8"),
        "NOTICE.terse-litestream"
    )
    assert.equal(await readFile(path.join(path.dirname(binary), "LICENSE.terse-ltx"), "utf8"), "license")
    assert.equal(requests.length, 2)
    assert.deepEqual(requests, [
        "https://github.com/TerseAI/durable-actors/releases/download/v1.2.3/durable-actors-darwin-arm64.tar.gz",
        "https://github.com/TerseAI/durable-actors/releases/download/v1.2.3/durable-actors-darwin-arm64.tar.gz.sha256"
    ])
    const offline = new RuntimeInstaller(options, async () => {
        throw new Error("offline")
    })
    assert.equal(await offline.install(), binary)
})

test("rejects a damaged archive before exposing executables", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "ldo-install-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const installer = new RuntimeInstaller(
        { version: "1.2.3", platform: "linux", arch: "x64", cacheDirectory: directory },
        async url => new Response(url.endsWith(".sha256") ? "0".repeat(64) : "corrupt")
    )
    await assert.rejects(installer.install(), /checksum/i)
    assert.deepEqual(await readdir(path.join(directory, "1.2.3")), [])
})

test("writes release chunks to disk before the download ends and cleans up interrupted downloads", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "ldo-install-stream-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const versionDirectory = path.join(directory, "1.2.3")
    let chunks = 0
    let written = 0
    const installer = new RuntimeInstaller(
        { version: "1.2.3", platform: "linux", arch: "x64", cacheDirectory: directory },
        async () =>
            new Response(
                new ReadableStream({
                    async pull(controller) {
                        if (chunks++ < 16) {
                            controller.enqueue(new Uint8Array(64 * 1024))
                            return
                        }
                        const [staging] = await readdir(versionDirectory)
                        written = (await stat(path.join(versionDirectory, staging, "archive.tar.gz"))).size
                        controller.error(new Error("download interrupted"))
                    }
                })
            )
    )
    await assert.rejects(installer.install(), /download interrupted/)
    assert.ok(written > 0, "the archive must reach disk while the response is still streaming")
    assert.deepEqual(await readdir(versionDirectory), [])
})

test("rejects oversized checksums, cancels the response, and removes staging files", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "ldo-install-limit-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    let cancelled = false
    const installer = new RuntimeInstaller(
        { version: "1.2.3", platform: "linux", arch: "x64", cacheDirectory: directory },
        async url => {
            if (!url.endsWith(".sha256")) return new Response("archive")
            return new Response(
                new ReadableStream({
                    pull(controller) {
                        controller.enqueue(new Uint8Array(512))
                    },
                    cancel() {
                        cancelled = true
                    }
                })
            )
        }
    )
    await assert.rejects(installer.install(), /size limit/)
    assert.equal(cancelled, true)
    assert.deepEqual(await readdir(path.join(directory, "1.2.3")), [])
})

test("HTTP failures do not expose an incomplete runtime", async t => {
    const directory = await mkdtemp(path.join(tmpdir(), "ldo-install-http-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const installer = new RuntimeInstaller(
        { version: "1.2.3", platform: "linux", arch: "x64", cacheDirectory: directory },
        async () => new Response("not found", { status: 404 })
    )
    await assert.rejects(installer.install(), /HTTP 404/)
    assert.deepEqual(await readdir(path.join(directory, "1.2.3")), [])
})

test("unsupported systems get an actionable error without downloading", async () => {
    const installer = new RuntimeInstaller(
        { version: "1.2.3", platform: "win32", arch: "x64", cacheDirectory: "unused" },
        async () => {
            throw new Error("unexpected download")
        }
    )
    await assert.rejects(installer.install(), /WSL/)
})

async function fixture(directory: string): Promise<Buffer> {
    await writeFile(path.join(directory, "durable-actors"), "runtime")
    await writeFile(path.join(directory, "NOTICE.terse-litestream"), "NOTICE.terse-litestream")
    await writeFile(path.join(directory, "LICENSE.terse-litestream"), "license")
    await writeFile(path.join(directory, "LICENSE.terse-ltx"), "license")
    const archive = path.join(directory, "runtime.tar.gz")
    await c({ gzip: true, file: archive, cwd: directory }, [
        "durable-actors",
        "NOTICE.terse-litestream",
        "LICENSE.terse-litestream",
        "LICENSE.terse-ltx"
    ])
    return readFile(archive)
}
