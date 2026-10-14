# @durable-actors/pi-durable

Run a Pi Durable agent inside a Durable Actor. The actor owns the database; the adapter owns the Pi harness and conversation.

```ts
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
        registry
    })

    async prompt(text: string): Promise<string> {
        return this.agent.prompt(text, { provider: "openai", modelId: "gpt-6-sol" })
    }
}
```

The constructor requires `models` and `registry`. Each `prompt(text, model)` call requires Pi's native `ModelRef` to select its model. Install your extensions and tools in the registry before prompting. Mark the private `DurablePiAgent` field with `@Resource` so the actor runtime manages its lifecycle. Pi's conversation is persisted separately in the actor's SQLite database.

Construction does not access storage. `onResume()` opens the harness and resumes pending work using the persisted conversation configuration. Each `prompt()` configures the root conversation with the supplied model, submits input, waits for a completed answer, and returns its text. Failed prompts reject with Pi's reason and error detail. `onGracefulShutdown()` closes the harness.

## Native Pi access

`agent.harness` exposes Pi's native harness while the resource is resumed. Use it inside an actor method with a Chord context:

```ts
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context"

async reset(): Promise<void> {
    const root = await this.agent.harness.root(BACKGROUND_CONTEXT)
    await root.reset(undefined, BACKGROUND_CONTEXT)
}

async usage() {
    return this.agent.harness.usage(BACKGROUND_CONTEXT)
}
```

Use Pi's APIs for configuration, documents, tools, watches, additional conversations, and tasks directly. Keep native handles and watches within the resource's lifecycle.

The constructor accepts Pi's native `HarnessOptions`. Pass `env`, `settings`, `conversationCreated`, and `onReport` directly. Register integration tools, including Pi-compatible Composio tools, in the supplied Pi registry; they use Pi's normal tool execution without an adapter protocol. Built-in filesystem tools can use an execution environment supplied through `env`.

Use one agent per actor and ordinary serialized actor methods. This initial adapter replays an append-only commit log into Pi's `MemoryStorage` when the resource resumes, so memory and reopen time grow with history. It does not schedule background wakeups. Native watches and events are available through the harness; the `prompt()` convenience returns completed text.

See the [actor and CLI example](../../examples/pi/README.md). This package is under development in this workspace and has not been published.
