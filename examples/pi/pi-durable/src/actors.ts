import { DurablePiAgent } from "@durable-actors/pi-durable"
import { createModels } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { ConversationView, createRegistry } from "@earendil-works/pi-durable"
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { Actor, Resource, Interleave, ActorSocket, Unchecked, unchecked } from "durable-actors"
import { CodingTools } from "@earendil-works/pi-durable/tools"
import { ModalSandbox } from "./modal/SandboxResource.js";
import { Op } from "@earendil-works/chord/delta";

export class PiAgent extends Actor<null, string, Outgoing> {
    @Resource private sandbox = new ModalSandbox(this.db)
    @Resource private readonly agent = new DurablePiAgent(this.db, {
        models,
        registry,
        env: () => this.sandbox.env(),
        agent: { 
            model: { provider: "openai", modelId: "gpt-6-sol" },
            cwd: WORKSPACE,
            instructions: instructions,
        },
    })

    @Interleave
    async prompt(text: string): Promise<string> {
        const watch = await this.agent.root.watch(context)
        watch.start(async (_view, ops) => this.broadcast({ type: "ops", ops: unchecked(ops) }))
        
        try {
            return await this.agent.prompt(text)
        } finally {
            await watch.stop()
            const latest = await this.agent.root.watch(context)
            this.broadcast({ type: "view", view: unchecked(latest.value) })
            await latest.stop()
        }
    }

    // Returns before Pi runs, so every Pi write happens after this invocation ends.
    async submit(text: string): Promise<void> {
        await this.agent.root.submit({ type: "input", content: text }, context)
    }

    async view(): Promise<Unchecked<ConversationView>> {
        const watch = await this.agent.root.watch(context)
        try {
            return unchecked(watch.value)
        } finally {
            await watch.stop()
        }
    }

    async reset() {
        await this.agent.root.reset(undefined, context)
        const watch = await this.agent.root.watch(context)
        this.broadcast({ type: "view", view: unchecked(watch.value) })
        await watch.stop()
    }

    async onConnect(socket: ActorSocket<null, Outgoing>) {
        const watch = await this.agent.root.watch(context)
        socket.send({ type: "view", view: unchecked(watch.value) })
        await watch.stop()
        socket.send({ status: "ready", url: await this.sandbox.previewUrl() })
    }
}

// MARK: Types
type Outgoing =
| { type: "view"; view: Unchecked<ConversationView> }
| { type: "ops"; ops: Unchecked<readonly Op[]> }
| Preview

export type Preview =
| { status: "starting" }
| { status: "ready"; url: string }
| { status: "stopped" }
| { status: "error"; message: string };

export const WORKSPACE = "/workspace"

const instructions = `You are editing a Vite app in ${WORKSPACE}. Its dev server is already running and the user sees it live; edits hot-reload.
Add packages with npm install. Do not change vite.config.js or the dev script.`

const models = createModels()
models.setProvider(openaiProvider())
const registry = createRegistry()
registry.install(CodingTools)
const context = BACKGROUND_CONTEXT