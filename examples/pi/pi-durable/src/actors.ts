import { DurablePiAgent } from "@durable-actors/pi-durable"
import { createModels } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { createRegistry } from "@earendil-works/pi-durable"
import { Actor, Resource } from "durable-actors"

const models = createModels()
models.setProvider(openaiProvider())
const registry = createRegistry()

export class PiAgent extends Actor {
    @Resource private readonly agent = new DurablePiAgent({
        models,
        registry,
    })

    async prompt(text: string): Promise<string> {
        const response = await this.agent.prompt(text, { provider: "openai", modelId: "gpt-6-sol" })
        return response
    }
}
