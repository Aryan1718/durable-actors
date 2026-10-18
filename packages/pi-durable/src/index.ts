import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context"
import { contentText } from "@earendil-works/pi-ai"
import { AgentChange, AssistantEntry, Conversation, Harness, type HarnessOptions, type ModelRef } from "@earendil-works/pi-durable"
import type { Actor, ActorDatabase, ActorResource, ActorResourceContext } from "durable-actors"

import { ActorStorage } from "./storage.js"

const context = BACKGROUND_CONTEXT

export type DurablePiAgentOptions = HarnessOptions & {
    readonly agent: AgentChange & { readonly model: ModelRef }
}

export class DurablePiAgent implements ActorResource {
    private _harness?: Harness
    private _root?: Conversation

    get harness(): Harness {
        if (!this._harness) {
            throw new Error("DurablePiAgent has not resumed")
        }
        return this._harness
    }

    get root(): Conversation {
        if (!this._root) {
            throw new Error("DurablePiAgent has not resumed")
        }
        return this._root
    }

    constructor(private readonly db: ActorDatabase, private readonly options: DurablePiAgentOptions) {}

    async prompt(text: string): Promise<string> {
         const job = {
            type: "input",
            content: text,
        } as const;
        
        const submission = await this.root.submit(job, context);
        const settled = await submission.wait(context);

        if (settled.status === "unanswered") 
            throw new Error(`Pi did not answer: ${settled.reason}${typeof settled.detail === "string" ? `: ${settled.detail}` : ""}`)

        if (settled.status !== "done" || settled.type !== "input")
            throw new Error(`Pi did not answer: ${settled.reason}`)

        const answer = await this.root.commit((tx) => tx.entry(AssistantEntry, settled.answer), context);

        if (answer === undefined)
            throw new Error("Pi did not answer")

        if (answer.model === undefined)
            throw new Error("The Pi model did not provide an answer")

        return contentText(answer.model[0].content)
    }

    async onResume(): Promise<void> {
        this._harness = await Harness.open(new ActorStorage(this.db), this.options, context)
        this._root = await this.harness.root(context)
        await this.root.configure(
            this.options.agent,
            context,
        )
        this._harness.resume();
    }

    async onGracefulShutdown(): Promise<void> {
        if (this.harness === undefined) return
        await this.harness.close(context)
        this._harness = undefined
    }
}
