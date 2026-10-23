import { createHash } from "node:crypto"
import { createWriteStream } from "node:fs"
import { chmod, lstat, mkdir, mkdtemp, readFile, rename, rm } from "node:fs/promises"
import { homedir } from "node:os"
import path from "node:path"
import { pipeline } from "node:stream/promises"
import { x } from "tar"

import { actorEnvironment } from "./environment.js"

const executables = ["durable-actors"]
const bundleFiles = [...executables, "LICENSE.terse-litestream", "NOTICE.terse-litestream", "LICENSE.terse-ltx"]
const maximumBytes = 200 * 1024 * 1024

export async function fetchRuntimeExecutablePath(): Promise<string> {
    const environment = actorEnvironment(process.env)
    if (environment.DURABLE_ACTORS_BINARY) return path.resolve(environment.DURABLE_ACTORS_BINARY)
    const { version } = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"))
    return new RuntimeInstaller({
        version,
        platform: process.platform,
        arch: process.arch,
        cacheDirectory: environment.DURABLE_ACTORS_CACHE_DIR ?? path.join(homedir(), ".cache/durable-actors")
    }).install()
}

interface RuntimeOptions {
    version: string
    platform: string
    arch: string
    cacheDirectory: string
}

export class RuntimeInstaller {
    constructor(
        private readonly options: RuntimeOptions,
        private readonly request: (url: string, init: RequestInit) => Promise<Response> = fetch
    ) {}

    async install(): Promise<string> {
        const { version, platform, arch, cacheDirectory } = this.options
        if (!["darwin", "linux"].includes(platform) || !["arm64", "x64"].includes(arch)) {
            throw new Error(`No prebuilt runtime for ${platform}/${arch}. On Windows, use WSL 2 with Ubuntu.`)
        }
        const destination = path.join(cacheDirectory, version, `${platform}-${arch}`)
        const binary = path.join(destination, executables[0])
        if (await completeBundle(destination)) return binary
        await mkdir(path.dirname(destination), { recursive: true })
        const staging = await mkdtemp(`${destination}-`)
        try {
            await this.unpack(staging)
            try {
                await rename(staging, destination)
            } catch (error) {
                if (!(await completeBundle(destination))) throw error
            }
            return binary
        } finally {
            await rm(staging, { recursive: true, force: true })
        }
    }

    private async unpack(directory: string): Promise<void> {
        const { version, platform, arch } = this.options
        const url = `https://github.com/TerseAI/durable-actors/releases/download/v${version}/durable-actors-${platform}-${arch}.tar.gz`
        const file = path.join(directory, "archive.tar.gz")
        const checksum = `${file}.sha256`
        const actual = await this.download(url, file, maximumBytes)
        await this.download(`${url}.sha256`, checksum, 1024)
        const expected = (await readFile(checksum, "utf8")).trim().split(/\s/u)[0]
        if (!/^[a-f0-9]{64}$/u.test(expected) || actual !== expected) {
            throw new Error("Runtime archive checksum does not match; retry the download.")
        }
        await x({
            file,
            cwd: directory,
            strict: true,
            filter: (name, entry) =>
                bundleFiles.includes(name) && "type" in entry && entry.type === "File" && entry.size <= maximumBytes
        })
        await Promise.all([rm(file), rm(checksum)])
        if (!(await completeBundle(directory)))
            throw new Error("Runtime archive is missing a required runtime or license file.")
        await Promise.all(executables.map(name => chmod(path.join(directory, name), 0o755)))
    }

    private async download(url: string, file: string, limit: number): Promise<string> {
        const signal = AbortSignal.timeout(120_000)
        const response = await this.request(url, { signal })
        if (!response.ok || !response.body) {
            await response.body?.cancel()
            throw new Error(
                `Cannot download runtime (HTTP ${response.status}). Check your connection and that this package version has native release assets: ${url}`
            )
        }
        const hash = createHash("sha256")
        await pipeline(
            response.body,
            async function* (chunks) {
                let size = 0
                for await (const chunk of chunks) {
                    size += chunk.byteLength
                    if (size > limit) throw new Error("Runtime download exceeds the size limit.")
                    hash.update(chunk)
                    yield chunk
                }
            },
            createWriteStream(file, { flags: "wx" }),
            { signal }
        )
        return hash.digest("hex")
    }
}

async function completeBundle(directory: string): Promise<boolean> {
    return Promise.all(
        bundleFiles.map(name =>
            lstat(path.join(directory, name))
                .then(stat => stat.isFile() && stat.size > 0)
                .catch(() => false)
        )
    ).then(results => results.every(Boolean))
}
