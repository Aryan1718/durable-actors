import path from "node:path/posix"
import type { Context } from "@earendil-works/chord"
import {
    err,
    ExecutionError,
    FileError,
    LineScanner,
    ok,
    toError,
    type BinaryReader,
    type DirReader,
    type ExecutionEnv,
    type FileErrorCode,
    type FileInfo,
    type FileWatcher,
    type LineScan,
    type Result,
    type ShellExecOptions,
    type ShellExecResult,
    type TextLine,
    type TextLineReader,
} from "@earendil-works/pi-durable/env"
import {
    SandboxFilesystemIsADirectoryError,
    SandboxFilesystemNotADirectoryError,
    SandboxFilesystemNotFoundError,
    SandboxFilesystemPermissionError,
    type FileInfo as ModalFileInfo,
    type Sandbox,
} from "modal"

type FileResult<T> = Promise<Result<T, FileError>>

export class ModalExecutionEnv implements ExecutionEnv {
    readonly id: string

    constructor(private readonly sandbox: Sandbox, public cwd: string) {
        this.id = `modal:${sandbox.sandboxId}`
    }

    async exec(command: string | readonly string[], options: ShellExecOptions | undefined, context: Context): Promise<Result<ShellExecResult, ExecutionError>> {
        const argv = typeof command === "string" ? ["bash", "-lc", command] : [...command]
        try {
            const running = await this.sandbox.exec(argv, {
                workdir: this.resolve(options?.cwd ?? this.cwd),
                env: options?.env,
                timeoutMs: options?.timeout === undefined ? undefined : options.timeout * 1000,
            })
            await Promise.all([
                forward(running.stdout, "stdout", options, context),
                forward(running.stderr, "stderr", options, context),
            ])
            return ok({ exitCode: await running.wait() })
        } catch (error) {
            return err(new ExecutionError("spawn_error", String(error), toError(error)))
        }
    }

    async absolutePath(target: string): FileResult<string> {
        return ok(this.resolve(target))
    }

    async joinPath(parts: string[]): FileResult<string> {
        return ok(path.join(...parts))
    }

    async canonicalPath(target: string): FileResult<string> {
        return this.output(target, ["realpath", "-e", this.resolve(target)])
    }

    async readTextFile(target: string): FileResult<string> {
        return attempt(target, () => this.sandbox.filesystem.readText(this.resolve(target)))
    }

    async readTextLines(target: string, options: { maxLines?: number } | undefined): FileResult<string[]> {
        const text = await this.readTextFile(target)
        if (!text.ok) return text
        return ok(splitLines(text.value).slice(0, options?.maxLines).map(line => line.text))
    }

    async openTextLineReader(target: string): FileResult<TextLineReader> {
        const text = await this.readTextFile(target)
        if (!text.ok) return text
        return ok(new LinesReader(splitLines(text.value)))
    }

    async readBinaryFile(target: string): FileResult<Uint8Array> {
        return attempt(target, () => this.sandbox.filesystem.readBytes(this.resolve(target)))
    }

    async openBinaryReader(target: string, options: { noFollow?: boolean } | undefined, context: Context): FileResult<BinaryReader> {
        const info = await this.fileInfo(target)
        if (!info.ok) return info
        if (info.value.kind === "directory") return err(new FileError("is_directory", "Is a directory", target))
        if (info.value.kind === "symlink" && options?.noFollow) return err(new FileError("invalid", "Is a symbolic link", target))
        const bytes = await this.readBinaryFile(target)
        if (!bytes.ok) return bytes
        return ok(new BytesReader({ ...info.value, size: bytes.value.length }, bytes.value))
    }

    async writeFile(target: string, content: string | Uint8Array): FileResult<void> {
        const remote = this.resolve(target)
        return attempt(target, () => typeof content === "string"
            ? this.sandbox.filesystem.writeText(content, remote)
            : this.sandbox.filesystem.writeBytes(content, remote))
    }

    async appendFile(target: string): FileResult<void> {
        return unsupported(target)
    }

    async truncateFile(target: string): FileResult<void> {
        return unsupported(target)
    }

    async flushFile(): FileResult<void> {
        return ok(undefined)
    }

    async renameFile(source: string, destination: string): FileResult<void> {
        const moved = await this.output(source, ["mv", "-f", this.resolve(source), this.resolve(destination)])
        return moved.ok ? ok(undefined) : moved
    }

    async fileInfo(target: string): FileResult<FileInfo> {
        return attempt(target, async () => toFileInfo(await this.sandbox.filesystem.stat(this.resolve(target))))
    }

    async listDir(target: string): FileResult<FileInfo[]> {
        return attempt(target, async () => (await this.sandbox.filesystem.listFiles(this.resolve(target))).map(toFileInfo))
    }

    async openDirReader(target: string): FileResult<DirReader> {
        const entries = await this.listDir(target)
        if (!entries.ok) return entries
        return ok(new EntriesReader(entries.value))
    }

    async watch(): FileResult<FileWatcher> {
        return unsupported(this.cwd)
    }

    async exists(target: string): FileResult<boolean> {
        const info = await this.fileInfo(target)
        if (info.ok) return ok(true)
        return info.error.code === "not_found" ? ok(false) : info
    }

    async createDir(target: string, options: { recursive?: boolean } | undefined): FileResult<void> {
        return attempt(target, () => this.sandbox.filesystem.makeDirectory(this.resolve(target), { createParents: options?.recursive }))
    }

    async remove(target: string, options: { recursive?: boolean; force?: boolean } | undefined): FileResult<void> {
        const removed = await attempt(target, () => this.sandbox.filesystem.remove(this.resolve(target), { recursive: options?.recursive }))
        return !removed.ok && options?.force && removed.error.code === "not_found" ? ok(undefined) : removed
    }

    async createTempDir(prefix: string | undefined): FileResult<string> {
        return this.output("/tmp", ["mktemp", "-d", `/tmp/${prefix ?? "pi-"}XXXXXX`])
    }

    async createTempFile(options: { prefix?: string; suffix?: string } | undefined): FileResult<string> {
        return this.output("/tmp", ["mktemp", `/tmp/${options?.prefix ?? "pi-"}XXXXXX${options?.suffix ?? ""}`])
    }

    async cleanup(): Promise<void> {}

    private resolve(target: string): string {
        return path.resolve(this.cwd, target)
    }

    private async output(target: string, argv: string[]): FileResult<string> {
        try {
            const running = await this.sandbox.exec(argv)
            const [stdout, stderr, exitCode] = await Promise.all([running.stdout.readText(), running.stderr.readText(), running.wait()])
            if (exitCode === 0) return ok(stdout.trim())
            return err(new FileError(/No such file/.test(stderr) ? "not_found" : "unknown", stderr.trim(), target))
        } catch (error) {
            return err(new FileError("unknown", String(error), target, toError(error)))
        }
    }
}

async function forward(stream: ReadableStream<string>, name: "stdout" | "stderr", options: ShellExecOptions | undefined, context: Context): Promise<void> {
    for await (const chunk of stream) options?.onOutput?.(chunk, context, { stream: name })
}

async function attempt<T>(target: string, operation: () => Promise<T>): FileResult<T> {
    try {
        return ok(await operation())
    } catch (error) {
        return err(new FileError(fileErrorCode(error), String(error), target, toError(error)))
    }
}

function unsupported<T>(target: string): Result<T, FileError> {
    return err(new FileError("not_supported", "Not supported in a Modal sandbox", target))
}

function fileErrorCode(error: unknown): FileErrorCode {
    if (error instanceof SandboxFilesystemNotFoundError) return "not_found"
    if (error instanceof SandboxFilesystemIsADirectoryError) return "is_directory"
    if (error instanceof SandboxFilesystemNotADirectoryError) return "not_directory"
    if (error instanceof SandboxFilesystemPermissionError) return "permission_denied"
    return "unknown"
}

function toFileInfo(info: ModalFileInfo): FileInfo {
    return { name: info.name, path: info.path, kind: info.type, size: info.size, mtimeMs: info.modifiedTime * 1000 }
}

function splitLines(text: string): TextLine[] {
    const parts = text.split("\n")
    const last = parts.pop()!
    const lines = parts.map(part => ({ text: part, terminated: true }))
    return last === "" ? lines : [...lines, { text: last, terminated: false }]
}

class LinesReader implements TextLineReader {
    private next = 0

    constructor(private readonly lines: TextLine[]) {}

    async readLine(): FileResult<TextLine | undefined> {
        return ok(this.lines[this.next++])
    }

    async close(): Promise<void> {}
}

class BytesReader implements BinaryReader {
    constructor(private readonly file: FileInfo, private readonly bytes: Uint8Array) {}

    async info(): FileResult<FileInfo> {
        return ok(this.file)
    }

    async read(offset: number, length: number): FileResult<Uint8Array> {
        return ok(this.bytes.subarray(offset, offset + length))
    }

    async scanLines(options: { startLine: number; endLine?: number }): FileResult<LineScan> {
        const scanner = new LineScanner(options.startLine, options.endLine)
        scanner.push(this.bytes)
        return ok(scanner.finish())
    }

    async close(): Promise<void> {}
}

class EntriesReader implements DirReader {
    private offset = 0

    constructor(private readonly entries: FileInfo[]) {}

    async next(maxEntries: number): FileResult<{ entries: FileInfo[]; done: boolean }> {
        const page = this.entries.slice(this.offset, this.offset + maxEntries)
        this.offset += page.length
        return ok({ entries: page, done: this.offset >= this.entries.length })
    }

    async close(): Promise<void> {}
}
