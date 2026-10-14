# pi-cli

A separate CLI project for the [Pi Durable actor](../pi-durable/README.md). It imports a generated client and does not load the actor source or Pi libraries.

Follow the [pair's setup instructions](../README.md) to start the actor. From the repository root in another terminal:

```sh
cd examples/pi/pi-cli
cp .env.example .env
bun run generate
bun run start send "Hello"
bun run start --help
```

Each command sends one prompt to `PiAgent` with ID `demo` and prints its reply. Repeated commands continue the same conversation. The CLI reads `DURABLE_ACTORS_CONTROL_PLANE_URL` from `.env`, defaulting here to port `7104`.

`bun run generate` fetches the running actor's contract into `generated/`. Regenerate after changing the actor API. `bun run check` checks types after generation; `bun run test` checks arguments and errors, then starts a temporary local test actor to exercise the CLI without a model API key.
