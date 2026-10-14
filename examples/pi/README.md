# Pi actor and CLI

Two small projects: [pi-durable](pi-durable/README.md) runs the actor, and [pi-cli](pi-cli/README.md) sends prompts through a generated client. Actor `demo` keeps the same conversation across calls and restarts.

## Start the actor

Requires Bun 1.4.2+ and an OpenAI API key. From the repository root:

```sh
bun install
bun run --cwd sdk build
bun run --cwd packages/pi-durable build
cd examples/pi/pi-durable
cp .env.example .env
```

Set `OPENAI_API_KEY` in the actor's `.env`, then run:

```sh
bun run dev:actors
```

The actor runtime uses port `7104`.

## Send a prompt

Wait for `Ready`. In a second terminal, from the repository root:

```sh
cd examples/pi/pi-cli
cp .env.example .env
bun run generate
bun run start send "Remember that my name is Alice."
bun run start send "What is my name?"
```

Restart the actor runtime and ask again to check persistence. The CLI needs only the actor runtime URL; the model API key stays in the actor project. Regenerate the client after changing the actor API.

`bun run check` checks each project's types. In `pi-cli`, `bun run test` checks prompt handling and starts a temporary local test actor to exercise the CLI. In `pi-durable`, it runs the adapter's tests. These tests do not call OpenAI.

## Staging

Configure the root `.env.staging` using `.env.staging.example`. Add `OPENAI_API_KEY` there and include `OPENAI_API_KEY` in the comma-separated `STAGING_SECRET_KEYS` list so the actor receives it.

From the repository root:

```sh
bun run staging:pi
```

This builds the SDK, Pi adapter, runtime, and actor bundle, then starts the existing staging flow. When ready, copy the printed connection settings into `examples/pi/pi-cli/.env`. In another terminal:

```sh
cd examples/pi/pi-cli
bun run generate
bun run start send "Remember that my name is Alice."
bun run start send "What is my name?"
```

The OpenAI key stays in staging's actor secret. Ctrl+C stops the local server and tunnel; cloud resources remain.
