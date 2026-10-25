import { ActorDatabase, ActorResource } from "durable-actors";
import { ModalClient, Probe, Sandbox } from "modal";
import { ModalExecutionEnv } from "./modal-execution-env.js";
import { WORKSPACE } from "../actors.js";
import { ExecutionEnv } from "@earendil-works/pi-durable/env";

const APP_NAME = "pi-durable-sandboxes"
const IMAGE = "node:22"
const PREVIEW_PORT = 3000

const VITE_CONFIG = `export default { server: { host: "0.0.0.0", port: ${PREVIEW_PORT}, allowedHosts: true, hmr: { clientPort: 443 } } }`

export class ModalSandbox implements ActorResource {
    private modal: ModalClient;
    private _sandbox: Sandbox | undefined;
    private resourceKV: ResourceKV

    get sandbox(): Sandbox {
        if (this._sandbox === undefined) {
            throw new Error("Sandbox not created")
        }
        return this._sandbox
    }

    constructor(private readonly db: ActorDatabase) {
        this.modal = new ModalClient({
            tokenId: process.env.MODAL_TOKEN_ID,
            tokenSecret: process.env.MODAL_TOKEN_SECRET,
        });

        this.resourceKV = new ResourceKV(this.db, "modal_sandbox")
    }

    env(): ExecutionEnv {
        return new ModalExecutionEnv(this.sandbox, WORKSPACE)
    }

    async previewUrl(): Promise<string> {
        const tunnels = await this.sandbox.tunnels();
        return tunnels[PREVIEW_PORT].url;
    }

    async snapshotFilesystem(): Promise<void> {
        const image = await this.sandbox.snapshotFilesystem({ ttlMs: null })
        this.resourceKV.set("image_id", image.imageId)
    }

    // Private helpers

    private async createNewSandbox(): Promise<Sandbox> {
        // const imageId = this.resourceKV.get("image_id")
        const imageId = undefined

        // If there is no workspace image, no problem!
        let sandbox: Sandbox = await this.createSandbox(imageId)

        this.resourceKV.set("sandbox_id", sandbox.sandboxId)

        return sandbox
    }

    private async createSandbox(imageId?: string | undefined): Promise<Sandbox> {
        console.log("Creating sandbox with imageId", imageId)

        const app = await this.modal.apps.fromName(APP_NAME, { createIfMissing: true })
        const image = await this.prepareSandboxImage(imageId)

        const sandbox = await this.modal.sandboxes.create(app, image, {
            encryptedPorts: [PREVIEW_PORT],
            command: ["npm", "run", "dev"],
            workdir: WORKSPACE,
            readinessProbe: Probe.withTcp(PREVIEW_PORT),
            timeoutMs: 60 * 60 * 1000,
        })

        console.log("created sandbox \n", sandbox.sandboxId)
        if (imageId === undefined) {
            console.log("no image id, using default image", IMAGE)
        } else {
            console.log("using image id", imageId)
        }

        return sandbox
    }

    private async prepareSandboxImage(imageId?: string | undefined) {
        if (imageId === undefined) {
            return this.modal.images.fromRegistry(IMAGE).dockerfileCommands([
                `WORKDIR ${WORKSPACE}`,
                "RUN npm init -y && npm pkg set type=module scripts.dev=vite && npm install -D vite",
                `RUN printf '%s' '${VITE_CONFIG}' > vite.config.js`,
                `RUN printf '%s' '<!doctype html><div id="app">Nothing here yet</div><script type="module" src="/main.js"></script>' > index.html && touch main.js`,
            ])
        }

        // this is assumed to have the correct code on it already
        return this.modal.images.fromId(imageId)
    }

    // MARK: Lifecycle hooks

    // Check if sandboxId in resourceKV is still active, else create a new sandbox
    async onResume(): Promise<void> {
        console.log("Resume hook fired")
        const sandboxId = this.resourceKV.get("sandbox_id")

        // No sandbox session associated with this actor
        if (sandboxId === undefined) {
            this._sandbox = await this.createNewSandbox()
            return;
        }

        const sandbox = await this.modal.sandboxes.fromId(sandboxId)
        const isRunning = (await sandbox.poll()) === null

        if (!isRunning) {
            this._sandbox = await this.createNewSandbox()
        } else {
            this._sandbox = sandbox
        }

        console.log("Attached sandbox", this._sandbox.sandboxId)
        return;
    }

    async onGracefulShutdown(): Promise<void> {
        console.log("Graceful shutdown hook fired")
        // const image = await this.sandbox.snapshotFilesystem({ ttlMs: null })

        return;
    }
}

class ResourceKV {
    constructor(private readonly db: ActorDatabase, private readonly namespace: string) {
        db.exec("CREATE TABLE IF NOT EXISTS resource_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    }
    get(key: string): string | undefined {
        return this.db.exec<{ value: string }>(
            "SELECT value FROM resource_kv WHERE key = ?", `${this.namespace}:${key}`
        )[0]?.value
    }
    set(key: string, value: string): void {
        this.db.exec(
            "INSERT INTO resource_kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            `${this.namespace}:${key}`, value
        )
    }
}
