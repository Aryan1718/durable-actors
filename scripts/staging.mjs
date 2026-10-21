import { spawn } from "node:child_process"
import { createHash, randomUUID } from "node:crypto"
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import { setTimeout as delay } from "node:timers/promises"
import { fileURLToPath } from "node:url"
import { parseEnv } from "node:util"

import { NgrokTunnel } from "./tunnel.mjs"

export class Staging {
    constructor(root, environment, examplePath, run = runCommand, request = fetch) {
        this.root = root
        this.env = environment
        this.run = run
        this.request = request
        this.examplePath = examplePath
        this.example = path.basename(examplePath)
        this.work = path.join(root, ".durable-actors/staging", this.example)
        this.secrets = Object.fromEntries(secretKeys(environment).map(key => [key, environment[key]]))
        this.secretName = `staging-${this.example}`
        this.context = `gke_${environment.STAGING_GCP_PROJECT}_${environment.STAGING_GKE_LOCATION}_${environment.STAGING_GKE_CLUSTER}`
    }

    async start() {
        await this.preflight()
        await this.prepareInfrastructure()
        const bundle = await this.build()
        await this.serve(bundle)
    }

    async preflight() {
        await readFile(path.join(this.examplePath, "src/actors.ts"))
        console.log("Checking .env.staging credentials, local Postgres, and staging connectivity…")
        const sdk = (await this.gcloud(["info", "--format=value(installation.sdk_root)"])).trim()
        this.env.PATH = `${sdk}/bin${path.delimiter}${this.env.PATH}`
        await this.checkPostgres()
        await this.command("docker", ["info", "--format", "{{.OSType}}"])
        const cluster = JSON.parse(await this.gcloud(["container", "clusters", "describe", this.env.STAGING_GKE_CLUSTER, `--location=${this.env.STAGING_GKE_LOCATION}`, "--format=json"]))
        const kube = JSON.parse(await this.kubectl(["config", "view", "--minify", "-o", "json"]))
        if (new URL(kube.clusters[0].cluster.server).hostname !== cluster.endpoint) throw new Error("Kubeconfig does not point at the staging cluster endpoint")
        const deployment = JSON.parse(await this.kubectl(["-n", this.env.STAGING_PROBE_NAMESPACE, "get", "deployment", this.env.STAGING_PROBE_DEPLOYMENT, "-o", "json"]))
        const selector = Object.entries(deployment.spec.selector.matchLabels)
            .map(([key, value]) => `${key}=${value}`)
            .join(",")
        const pods = JSON.parse(await this.kubectl(["-n", this.env.STAGING_PROBE_NAMESPACE, "get", "pods", "-l", selector, "-o", "json"]))
        const pod = pods.items.find(pod => pod.status?.conditions?.some(condition => condition.type === "Ready" && condition.status === "True"))
        if (!pod) throw new Error("No ready staging pod to check Tailscale connectivity")
        await this.http(`http://${pod.status.podIP}:7100/healthz`)
        for (const bucket of this.buckets()) await this.gcloud(["storage", "buckets", "describe", `gs://${bucket}`, "--format=value(name)"])
        await readFile(this.env.GOOGLE_APPLICATION_CREDENTIALS)
        await mkdir(this.work, { recursive: true })
    }

    async checkPostgres() {
        const url = new URL(this.env.DURABLE_ACTORS_POSTGRES_URL)
        await this.command("psql", ["-X", "-v", "ON_ERROR_STOP=1", "-Atc", "SELECT 1"], {
            env: {
                ...this.env,
                PGHOST: url.hostname,
                PGPORT: url.port || "5432",
                PGDATABASE: url.pathname.slice(1),
                PGUSER: decodeURIComponent(url.username),
                PGPASSWORD: decodeURIComponent(url.password),
                PGPASSFILE: "/dev/null",
                PGCONNECT_TIMEOUT: "5"
            }
        })
    }

    async prepareInfrastructure() {
        console.log("Preparing the staging actor namespace, image registry, and gVisor node pool…")
        const namespace = this.env.DURABLE_ACTORS_GKE_NAMESPACE
        await this.apply([
            { apiVersion: "v1", kind: "Namespace", metadata: { name: namespace, labels: { "terse.ai/sandboxes": "true" } } },
            { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: "sandbox", namespace }, automountServiceAccountToken: false },
            ...(Object.keys(this.secrets).length
                ? [
                      {
                          apiVersion: "v1",
                          kind: "Secret",
                          metadata: { name: this.secretName, namespace, labels: { "terse.ai/customer-secret": "true" } },
                          type: "Opaque",
                          stringData: this.secrets
                      }
                  ]
                : [])
        ])
        const [registry, , repository] = this.env.STAGING_IMAGE_REPOSITORY.split("/")
        const location = registry.replace("-docker.pkg.dev", "")
        const registries = JSON.parse(await this.gcloud(["artifacts", "repositories", "list", `--location=${location}`, "--format=json"]))
        if (!registries.some(item => item.name.endsWith(`/${repository}`))) {
            await this.gcloud(["artifacts", "repositories", "create", repository, `--location=${location}`, "--repository-format=docker", "--quiet"], { stream: true })
        }
        const pools = JSON.parse(await this.gcloud(["container", "node-pools", "list", `--cluster=${this.env.STAGING_GKE_CLUSTER}`, `--location=${this.env.STAGING_GKE_LOCATION}`, "--format=json"]))
        const existing = pools.find(pool => pool.name === this.env.STAGING_NODE_POOL)
        if (existing && existing.config?.sandboxConfig?.type !== "GVISOR") throw new Error("Existing staging test node pool is not configured for gVisor")
        if (!existing) {
            await this.gcloud(
                [
                    "container",
                    "node-pools",
                    "create",
                    this.env.STAGING_NODE_POOL,
                    `--cluster=${this.env.STAGING_GKE_CLUSTER}`,
                    `--location=${this.env.STAGING_GKE_LOCATION}`,
                    `--node-locations=${this.env.DURABLE_ACTORS_GKE_ZONE}`,
                    "--sandbox=type=gvisor",
                    "--machine-type=e2-standard-2",
                    "--image-type=COS_CONTAINERD",
                    "--disk-size=30",
                    "--num-nodes=1",
                    "--enable-autoscaling",
                    "--min-nodes=0",
                    "--max-nodes=1",
                    "--workload-metadata=GKE_METADATA",
                    "--quiet"
                ],
                { stream: true }
            )
        }
    }

    async build() {
        console.log(`Building this checkout's runtime and ${this.example} actor bundle…`)
        await this.command("bun", ["run", "--bun", "--cwd", "sdk", "build"], { stream: true })
        const project = JSON.parse(await readFile(path.join(this.examplePath, "package.json"), "utf8"))
        if (project.dependencies?.["@durable-actors/pi-durable"]) await this.command("bun", ["run", "--bun", "--cwd", "packages/pi-durable", "build"], { stream: true })
        await this.command("cargo", ["build", "--locked", "--bin", "durable-actors"], { stream: true })
        this.env.DURABLE_ACTORS_BINARY = path.join(this.root, "target/debug/durable-actors")
        const contract = JSON.parse(await this.command(process.execPath, [path.join(this.root, "sdk/dist/compiler/deployment-build.js"), this.examplePath, "src/actors.ts", this.work]))
        for (const runtime of ["typescript", "python"]) {
            const key = `DURABLE_ACTORS_${runtime.toUpperCase()}_IMAGE`
            this.env[key] = await this.buildRuntimeImage(runtime)
            await this.saveSetting(key, this.env[key])
        }
        await this.saveSetting("DURABLE_ACTORS_BINARY", this.env.DURABLE_ACTORS_BINARY)
        return { bundle: await this.uploadBundle(), contract, secretRefs: Object.keys(this.secrets).length ? [this.secretName] : [] }
    }

    async buildRuntimeImage(runtime) {
        const plugins = JSON.parse(await this.command("docker", ["info", "--format", "{{json .ClientInfo.Plugins}}"]))
        const buildx = plugins.find(plugin => plugin.Name === "buildx")
        if (!buildx) throw new Error("Install Docker Buildx before running staging")
        const host = JSON.parse(await this.command("docker", ["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"]))
        if (!host.startsWith("unix://")) throw new Error("Staging image builds require a local Docker socket")
        const image = `${this.env.STAGING_IMAGE_REPOSITORY}:staging-${runtime}`
        const dockerConfig = path.join(this.work, "docker")
        const configuration = JSON.stringify({ cliPluginsExtraDirs: [path.dirname(buildx.Path)] })
        await mkdir(dockerConfig, { recursive: true })
        await writeFile(path.join(dockerConfig, "config.json"), configuration, { mode: 0o600 })
        const dockerEnv = { ...this.env, DOCKER_CONFIG: dockerConfig, DOCKER_HOST: host }
        try {
            await this.command("docker", ["buildx", "build", "--target", runtime, "--platform=linux/amd64", "--tag", image, "--load", "."], { env: dockerEnv, stream: true })
            const token = (await this.gcloud(["auth", "print-access-token"])).trim()
            await this.command("docker", ["login", this.env.STAGING_IMAGE_REPOSITORY.split("/")[0], "--username=oauth2accesstoken", "--password-stdin"], { env: dockerEnv, input: token })
            await this.command("docker", ["push", image], { env: dockerEnv, stream: true })
            const digests = JSON.parse(await this.command("docker", ["image", "inspect", image, "--format", "{{json .RepoDigests}}"], { env: dockerEnv }))
            const pinned = digests.find(value => value.startsWith(`${this.env.STAGING_IMAGE_REPOSITORY}@sha256:`))
            if (!pinned || !/@sha256:[a-f0-9]{64}$/.test(pinned)) throw new Error("Image push did not produce an immutable digest")
            return pinned
        } finally {
            await writeFile(path.join(dockerConfig, "config.json"), configuration, { mode: 0o600 })
        }
    }

    async uploadBundle() {
        const code = await readFile(path.join(this.work, "actors.mjs"))
        const object = `durable-actors/v3/artifacts/${randomUUID()}/actors.mjs`
        const uri = `gs://${this.env.DURABLE_ACTORS_ARTIFACT_BUCKET}/${object}`
        await this.gcloud(["storage", "cp", path.join(this.work, "actors.mjs"), uri, "--if-generation-match=0"])
        const saved = JSON.parse(await this.gcloud(["storage", "objects", "describe", uri, "--format=json"]))
        const generation = Number(saved.generation)
        if (!Number.isSafeInteger(generation) || generation <= 0) throw new Error("Invalid artifact generation")
        return { bucket: this.env.DURABLE_ACTORS_ARTIFACT_BUCKET, files: [{ path: "actors.mjs", object, generation, sha256: createHash("sha256").update(code).digest("base64url") }] }
    }

    async serve(deployment) {
        console.log("Starting the tunnel and local control plane with .env.staging…")
        const server = await this.serverOptions()
        const children = new Set()
        const launch = (command, args, options) => {
            const child = spawn(command, args, { ...options, cwd: server.cwd })
            children.add(child)
            child.once("exit", () => children.delete(child))
            return child
        }
        const tunnel = new NgrokTunnel(server.env, launch, {
            set: (key, value) => {
                if (value !== this.env.DURABLE_ACTORS_PUBLIC_URL) throw new Error("ngrok URL differs from .env.staging; update NGROK_DOMAIN and the three URL settings together")
            }
        })
        const serving = tunnel.run({ startRuntime: true, configuredEndpoints: true }).then(code => {
            throw new Error(`Staging processes stopped (exit ${code})`)
        })
        try {
            await Promise.race([this.waitForControlPlane(), serving])
            await Promise.race([this.register(deployment), serving])
            console.log(
                `\nStaging control plane is ready. Copy these into your CLI's .env:\n\nDURABLE_ACTORS_CONTROL_PLANE_URL=${this.env.DURABLE_ACTORS_CONTROL_PLANE_URL}\nDURABLE_ACTORS_PROJECT_ID=${this.env.DURABLE_ACTORS_PROJECT_ID}\nDURABLE_ACTORS_SECRET=${this.env.DURABLE_ACTORS_SECRET}\n\nCtrl+C stops the local tunnel/server. Staging cloud resources remain.\n`
            )
            await serving
        } finally {
            for (const child of children) child.kill("SIGTERM")
            await serving.catch(() => {})
            await rm(server.cwd, { recursive: true, force: true })
        }
    }

    async serverOptions() {
        await mkdir(this.work, { recursive: true })
        const cwd = await mkdtemp(path.join(this.work, "server-"))
        const env = { ...this.env, NGROK_CONFIG: path.join(cwd, "ngrok.yml") }
        const upstream = `http://${this.env.DURABLE_ACTORS_CONTROL_PLANE_BIND}`
        const socketUrl = `https://${new URL(this.env.DURABLE_ACTORS_PUBLIC_URL).hostname.replaceAll(".", "-")}-socket.internal`
        const config = {
            version: "3",
            agent: { web_addr: false },
            endpoints: [
                {
                    name: "control-plane",
                    url: this.env.DURABLE_ACTORS_PUBLIC_URL,
                    upstream: { url: upstream, protocol: "http2" },
                    traffic_policy: {
                        on_http_request: [
                            {
                                expressions: ["req.url.path == '/v1/socket'"],
                                actions: [{ type: "forward-internal", config: { url: socketUrl } }]
                            }
                        ]
                    }
                },
                { name: "socket", url: socketUrl, upstream: { url: upstream, protocol: "http1" } }
            ]
        }
        await writeFile(env.NGROK_CONFIG, JSON.stringify(config, null, 2), { mode: 0o600 })
        return { cwd, env }
    }

    async waitForControlPlane() {
        const origin = `http://${this.env.DURABLE_ACTORS_CONTROL_PLANE_BIND}`
        for (let attempt = 0; attempt < 90; attempt++) {
            try {
                await this.http(`${origin}/healthz`)
                await this.http(`${this.env.DURABLE_ACTORS_CONTROL_PLANE_URL}/healthz`)
                return
            } catch {
                await delay(1000)
            }
        }
        throw new Error("Local control plane did not become ready through ngrok")
    }

    async register(deployment) {
        await this.http(`${this.env.DURABLE_ACTORS_CONTROL_PLANE_URL}/v1/projects/${this.env.DURABLE_ACTORS_PROJECT_ID}/deployment`, {
            method: "PUT",
            headers: { Authorization: `Bearer ${this.env.DURABLE_ACTORS_SECRET}`, "Content-Type": "application/json" },
            body: JSON.stringify(deployment)
        })
    }

    buckets() {
        return [
            this.env.DURABLE_ACTORS_BUCKET,
            this.env.DURABLE_ACTORS_ARTIFACT_BUCKET,
            this.env.DURABLE_ACTORS_ARCHIVE_BUCKET,
            ...JSON.parse(this.env.DURABLE_ACTORS_RAPID_BUCKETS).map(placement => placement.bucket)
        ]
    }

    apply(items) {
        return this.kubectl(["apply", "-f", "-"], { input: JSON.stringify({ apiVersion: "v1", kind: "List", items }) })
    }

    gcloud(args, options) {
        return this.command("gcloud", [...args, `--project=${this.env.STAGING_GCP_PROJECT}`], options)
    }

    kubectl(args, options) {
        return this.command("kubectl", [`--kubeconfig=${this.env.KUBECONFIG}`, `--context=${this.context}`, "--request-timeout=30s", ...args], options)
    }

    command(command, args, options = {}) {
        return this.run(command, args, { cwd: this.root, env: this.env, ...options })
    }

    async http(url, options = {}) {
        const response = await this.request(url, { ...options, redirect: "error", signal: AbortSignal.timeout(15000) })
        if (!response.ok) throw new Error(`${new URL(url).pathname}: HTTP ${response.status}`)
        return response
    }

    async saveSetting(key, value) {
        const filename = path.join(this.root, ".env.staging")
        const contents = await readFile(filename, "utf8")
        const line = `${key}=${value}`
        const pattern = new RegExp(`^${key}=.*$`, "m")
        await writeFile(filename, pattern.test(contents) ? contents.replace(pattern, line) : `${contents.trimEnd()}\n${line}\n`, { mode: 0o600 })
    }
}

export function stagingEnvironment(document, system = process.env) {
    const configured = parseEnv(document)
    for (const key of [
        "STAGING_GCP_PROJECT",
        "STAGING_GKE_CLUSTER",
        "STAGING_GKE_LOCATION",
        "STAGING_NODE_POOL",
        "STAGING_IMAGE_REPOSITORY",
        "STAGING_PROBE_NAMESPACE",
        "STAGING_PROBE_DEPLOYMENT",
        "KUBECONFIG",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "DURABLE_ACTORS_PROJECT_ID",
        "DURABLE_ACTORS_POSTGRES_URL",
        "DURABLE_ACTORS_JWT_SIGNING_KEY",
        "DURABLE_ACTORS_SECRET",
        "DURABLE_ACTORS_CONTROL_PLANE_BIND",
        "DURABLE_ACTORS_CONTROL_PLANE_URL",
        "DURABLE_ACTORS_PUBLIC_URL",
        "DURABLE_ACTORS_BUCKET",
        "DURABLE_ACTORS_ARTIFACT_BUCKET",
        "DURABLE_ACTORS_ARCHIVE_BUCKET",
        "DURABLE_ACTORS_RAPID_BUCKETS",
        "DURABLE_ACTORS_GOOGLE_SERVICE_ACCOUNT",
        "DURABLE_ACTORS_GKE_NAMESPACE",
        "DURABLE_ACTORS_GKE_ZONE",
        "NGROK_DOMAIN",
        "NGROK_AUTH_TOKEN",
        ...secretKeys(configured)
    ])
        if (!configured[key]?.trim()) throw new Error(`Set ${key} in .env.staging`)
    if (!isStagingResource(configured.STAGING_GKE_CLUSTER)) throw new Error("Use a staging cluster")
    const buckets = [
        configured.DURABLE_ACTORS_BUCKET,
        configured.DURABLE_ACTORS_ARTIFACT_BUCKET,
        configured.DURABLE_ACTORS_ARCHIVE_BUCKET,
        ...JSON.parse(configured.DURABLE_ACTORS_RAPID_BUCKETS).map(item => item.bucket)
    ]
    if (buckets.some(bucket => !isStagingResource(bucket))) throw new Error("Only staging bucket destinations are allowed")
    const [registry, project, repository, image, extra] = configured.STAGING_IMAGE_REPOSITORY.split("/")
    if (
        !/^[a-z0-9-]+-docker\.pkg\.dev$/.test(registry) ||
        project !== configured.STAGING_GCP_PROJECT ||
        !isStagingResource(repository) ||
        !/^[a-z0-9][a-z0-9-]*$/.test(image ?? "") ||
        extra !== undefined
    )
        throw new Error("Use a staging image repository in STAGING_GCP_PROJECT")
    if (!isStagingResource(configured.DURABLE_ACTORS_GKE_NAMESPACE) || !isStagingResource(configured.STAGING_PROBE_NAMESPACE) || /(^|-)(prod|production)(-|$)/.test(configured.STAGING_NODE_POOL))
        throw new Error("Use a staging test namespace and node pool")
    if (
        configured.DURABLE_ACTORS_GKE_ZONE !== configured.STAGING_GKE_LOCATION ||
        configured.DURABLE_ACTORS_PERSISTENCE !== "rapid" ||
        configured.DURABLE_ACTORS_SPARE_IDLE !== "0" ||
        configured.DURABLE_ACTORS_PROCESS_ROLE !== "control_plane"
    )
        throw new Error("Expected staging Rapid configuration with on-demand actors")
    if (!/^[A-Za-z0-9._-]{1,64}$/.test(configured.DURABLE_ACTORS_PROJECT_ID)) throw new Error("Invalid staging project ID")
    const postgres = new URL(configured.DURABLE_ACTORS_POSTGRES_URL)
    if (!["127.0.0.1", "localhost", "[::1]"].includes(postgres.hostname)) throw new Error("Staging script requires a local Postgres database")
    const publicUrl = `https://${configured.NGROK_DOMAIN}`
    for (const key of ["DURABLE_ACTORS_CONTROL_PLANE_URL", "DURABLE_ACTORS_PUBLIC_URL"])
        if (configured[key] !== publicUrl) throw new Error(`${key} must match NGROK_DOMAIN in .env.staging`)
    const env = Object.fromEntries(["PATH", "HOME", "USER", "TMPDIR", "LANG", "LC_ALL", "TERM", "SHELL"].filter(key => system[key] !== undefined).map(key => [key, system[key]]))
    Object.assign(env, configured)
    env.CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE = configured.CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE ?? configured.GOOGLE_APPLICATION_CREDENTIALS
    env.NGROK_AUTHTOKEN = configured.NGROK_AUTH_TOKEN
    return env
}

function isStagingResource(name) {
    return typeof name === "string" && /(^|-)(staging|test)(-|$)/.test(name) && !/(^|-)(prod|production)(-|$)/.test(name)
}

function secretKeys(environment) {
    const keys = (environment.STAGING_SECRET_KEYS ?? "")
        .split(",")
        .map(key => key.trim())
        .filter(Boolean)
    if (keys.some(key => !/^[A-Za-z_][A-Za-z0-9_]*$/.test(key))) throw new Error("Invalid STAGING_SECRET_KEYS in .env.staging")
    return keys
}

function runCommand(command, args, { cwd, env, input, stream = false }) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, args, { cwd, env, stdio: ["pipe", stream ? "inherit" : "pipe", "pipe"] })
        let output = ""
        let errors = ""
        child.stdout?.on("data", chunk => {
            output += chunk
        })
        child.stderr.on("data", chunk => {
            if (stream) process.stderr.write(chunk)
            else errors += chunk
        })
        child.once("error", reject)
        child.once("close", code => (code === 0 ? resolve(output) : reject(new Error(`${command} failed (${code})${errors ? `: ${errors.trim()}` : ""}`))))
        child.stdin.on("error", () => {})
        child.stdin.end(input)
    })
}

export function stagingExamplePath(root, example) {
    if (!example || !example.split("/").every(segment => /^[a-z0-9][a-z0-9-]{0,54}$/.test(segment))) throw new Error("Usage: node scripts/staging.mjs <example-folder>")
    return path.join(root, "examples", example)
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        const root = fileURLToPath(new URL("../", import.meta.url))
        const [example, ...extra] = process.argv.slice(2)
        if (extra.length) throw new Error("Usage: node scripts/staging.mjs <example-folder>")
        const examplePath = stagingExamplePath(root, example)
        const env = stagingEnvironment(await readFile(path.join(root, ".env.staging"), "utf8"))
        const launcher = new Staging(root, env, examplePath)
        await launcher.start()
    } catch (error) {
        console.error(error.message)
        process.exitCode = 1
    }
}
