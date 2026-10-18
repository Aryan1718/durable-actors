import { spawn } from "node:child_process"
import { once } from "node:events"
import { readFileSync, writeFileSync } from "node:fs"
import path from "node:path"
import { loadEnvFile } from "node:process"
import { createInterface } from "node:readline"
import { fileURLToPath } from "node:url"

export class NgrokTunnel {
    constructor(environment, launch = spawn, environmentFile = new EnvironmentFile(".env")) {
        this.environment = { ...environment, NGROK_AUTHTOKEN: environment.NGROK_AUTH_TOKEN || environment.NGROK_AUTHTOKEN }
        this.launch = launch
        this.environmentFile = environmentFile
    }

    async run({ startRuntime = false, configuredEndpoints = false } = {}) {
        const child = this.launch("ngrok", this.arguments(configuredEndpoints), { env: this.environment, stdio: ["ignore", "pipe", "inherit"] })
        const closed = once(child, "close")
        const lines = createInterface({ input: child.stdout })
        let runtime
        let runtimeClosed
        let stopping = false
        let failure
        const stop = signal => {
            this.exitCode ??= signal === "SIGINT" ? 130 : 143
            child.kill(signal)
            runtime?.kill(signal)
        }
        const launchRuntime = () => {
            const cli = fileURLToPath(new URL("../sdk/dist/cli.js", import.meta.url))
            runtime = this.launch(process.execPath, [cli, "start"], { env: this.environment, stdio: "inherit" })
            runtimeClosed = once(runtime, "close").then(
                ([code, signal]) => {
                    if (!stopping) {
                        this.exitCode ??= exitStatus(code, signal)
                        child.kill("SIGTERM")
                    }
                },
                error => {
                    failure = error
                    child.kill("SIGTERM")
                }
            )
        }
        const interrupt = () => stop("SIGINT")
        const terminate = () => stop("SIGTERM")
        process.on("SIGINT", interrupt)
        process.on("SIGTERM", terminate)
        lines.on("line", line => {
            console.log(line)
            if (failure || this.exitCode !== undefined) return
            try {
                this.updateFromLog(line, startRuntime)
                if (startRuntime && this.origin && !runtime) launchRuntime()
            } catch (error) {
                failure = error
                child.kill("SIGTERM")
            }
        })
        try {
            const [code, signal] = await closed
            if (failure) throw failure
            if (startRuntime && !runtime && code === 0) throw new Error("ngrok exited before the tunnel was ready")
            return this.exitCode ?? exitStatus(code, signal)
        } catch (error) {
            if (error.code === "ENOENT") throw new Error("Install the ngrok CLI and make sure ngrok is on PATH: https://ngrok.com/download")
            throw error
        } finally {
            stopping = true
            lines.close()
            runtime?.kill("SIGTERM")
            await runtimeClosed
            process.off("SIGINT", interrupt)
            process.off("SIGTERM", terminate)
        }
    }

    arguments(configuredEndpoints = false) {
        if (configuredEndpoints) return ["start", "--all", "--config", this.environment.NGROK_CONFIG, "--log=stdout", "--log-format=json", "--log-level=info"]
        const bind = this.environment.DURABLE_ACTORS_CONTROL_PLANE_BIND || "127.0.0.1:7100"
        const upstream = new URL(`http://${bind}`)
        if (upstream.pathname !== "/" || upstream.search || upstream.hash || upstream.username || upstream.password || !/^[1-9]\d*$/.test(upstream.port || "80")) {
            throw new Error("DURABLE_ACTORS_CONTROL_PLANE_BIND must be a host:port with a nonzero port")
        }
        if (upstream.hostname === "0.0.0.0") upstream.hostname = "127.0.0.1"
        if (upstream.hostname === "[::]") upstream.hostname = "[::1]"
        const args = ["http", upstream.origin, "--upstream-protocol=http2", "--log=stdout", "--log-format=json", "--log-level=info"]
        const domain = this.environment.NGROK_DOMAIN
        const url = domain ? (domain.includes("://") ? domain : `https://${domain}`) : this.environment.NGROK_URL
        if (url) args.push("--url", httpsOrigin(url))
        if (this.environment.NGROK_CONFIG) args.push("--config", this.environment.NGROK_CONFIG)
        return args
    }

    updateFromLog(line, startRuntime = false) {
        let event
        try {
            event = JSON.parse(line)
        } catch {
            return
        }
        if (event.msg !== "started tunnel" || !event.url?.startsWith("https://")) return
        const origin = httpsOrigin(event.url)
        if (new URL(origin).hostname.endsWith(".internal")) return
        if (origin === this.origin) return
        this.environmentFile.set("DURABLE_ACTORS_CONTROL_PLANE_URL", origin)
        this.environment.DURABLE_ACTORS_CONTROL_PLANE_URL = origin
        this.origin = origin
        const instructions = startRuntime ? "Starting the control plane. Ctrl+C stops both processes." : "Start or restart your control plane to load it. Keep this terminal running."
        console.log(`\nTunnel ready. Saved DURABLE_ACTORS_CONTROL_PLANE_URL to .env. ${instructions}\n\nFor backends outside this directory:\nexport DURABLE_ACTORS_CONTROL_PLANE_URL=${origin}\n`)
    }
}

class EnvironmentFile {
    constructor(filename, filesystem = { readFileSync, writeFileSync }) {
        this.filename = filename
        this.filesystem = filesystem
    }

    set(key, value) {
        try {
            const contents = this.read()
            this.filesystem.writeFileSync(this.filename, replaceEnvironmentValue(contents, key, value), { mode: 0o600 })
        } catch (error) {
            throw new Error(`Could not update ${this.filename}: ${error.code ?? error.message}`)
        }
    }

    read() {
        try {
            return this.filesystem.readFileSync(this.filename, "utf8")
        } catch (error) {
            if (error.code === "ENOENT") return ""
            throw error
        }
    }
}

function replaceEnvironmentValue(contents, key, value) {
    let found = false
    // Match whole assignments so key-like lines inside multiline values remain untouched.
    const assignment = /^([ \t]*(?:export[ \t]+)?)([\w.-]+)([ \t]*=[ \t]*)('[^']*'|"[^"]*"|`[^`]*`|[^#\r\n]*)([ \t]*(?:#[^\r\n]*)?)/gm
    const updated = contents.replace(assignment, (match, prefix, name, equals, previous, suffix) => {
        if (name !== key) return match
        found = true
        return `${prefix}${name}${equals}${value}${previous.match(/[ \t]*$/)[0]}${suffix}`
    })
    if (found) return updated
    const newline = contents.includes("\r\n") ? "\r\n" : "\n"
    const separator = contents && !contents.endsWith("\n") ? newline : ""
    return `${contents}${separator}${key}=${value}${newline}`
}

function httpsOrigin(value) {
    const url = new URL(value)
    if (url.protocol !== "https:" || url.pathname !== "/" || url.search || url.hash || url.username || url.password) {
        throw new Error("NGROK_DOMAIN / NGROK_URL must identify an HTTPS origin, such as https://your-domain.ngrok.app")
    }
    return url.origin
}

function exitStatus(code, signal) {
    return code ?? (signal === "SIGINT" ? 130 : signal === "SIGTERM" ? 143 : 1)
}

function loadEnvironment() {
    try {
        loadEnvFile()
    } catch (error) {
        if (error.code !== "ENOENT") throw error
    }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        loadEnvironment()
        process.exitCode = await new NgrokTunnel(process.env).run({ startRuntime: process.argv.includes("--start") })
    } catch (error) {
        console.error(error.message)
        process.exitCode = 1
    }
}
