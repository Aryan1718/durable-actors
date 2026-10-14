# Pi Durable

The actor project for the [Pi example pair](../README.md). Each prompt to actor `demo` continues the same conversation; the separate [pi-cli](../pi-cli/README.md) sends prompts through a generated client.

## Run locally

Requires Bun 1.4.2+ and an OpenAI API key. From the repository root:

```sh
bun install
bun run --cwd sdk build
bun run --cwd packages/pi-durable build
cd examples/pi/pi-durable
cp .env.example .env
```

Set `OPENAI_API_KEY` in `.env`, then start the actor runtime:

```sh
bun run dev:actors
```

Wait for `Ready`. In another terminal, from the repository root:

```sh
cd examples/pi/pi-cli
cp .env.example .env
bun run generate
bun run start send "Remember that my name is Alice."
bun run start send "What is my name?"
```

Restart the runtime and ask again to check persistence. The example uses OpenAI's `gpt-6-sol`; change the model passed to `agent.prompt()` in [PiAgent](src/actors.ts) to select another model. The runtime uses port `7104`.

## Storage

[PiAgent](src/actors.ts) creates `new DurablePiAgent({ models, registry })` and returns the text from `agent.prompt(text, model)`. The [adapter](../../../packages/pi-durable/README.md) owns storage and restores Pi's transcript and task state from actor-local SQLite during `onResume()`. Each prompt configures the root conversation with its supplied model before submitting input. Its private actor field uses `@Resource` so the runtime resumes it and closes it during graceful shutdown.

For native Pi operations and integrations, use `agent.harness` with a Chord context inside an actor method. Adapter options also accept Pi's native harness options, and integration tools belong in the supplied registry.

This first version keeps the whole commit log in memory and replays it when the resource resumes. Prompts run serially; this example has no tools or streaming. Pending work resumes with the resource after a restart.

## Checks

```sh
bun run test
bun run check
```

Tests exercise persistence using the real Pi harness without a model API key. Pi dependencies are pinned because its API is experimental. See the [upstream Pi Durable docs](https://github.com/earendil-works/pi/tree/main/packages/durable).
