# Examples

Each example includes an app and a local actor runtime:

| Example | What it demonstrates | Create a project |
| --- | --- | --- |
| [AI chat](ai-chat/README.md) | Streaming AI replies and saved conversations | `bunx durable-actors init my-ai-chat --template ai-chat` |
| [Chatroom](chat/README.md) | Shared chat over WebSockets | `bunx durable-actors init my-chat --template chat` |
| [Collaborative documents](documents/README.md) | Shared document editing | `bunx durable-actors init my-documents --template documents` |

Follow the example's README to install dependencies, copy `.env.example` to `.env`, and start its two processes. AI chat also requires an OpenAI API key.

## Actor and CLI pairs

[Pi](pi/README.md) groups the `pi-durable` actor and `pi-cli` projects. It requires an OpenAI API key in the actor project and uses actor port `7104`.

[Persistence](persistence/README.md) groups the `persistence-tests` actor and its CLI. They exercise immediate writes, atomic rollback, interleaving, and crash recovery on actor port `7103`. No model API key is needed.

[Background work](background/) groups the `background-gym` actor and its CLI. They exercise writes from work that outlives its invocation, idle eviction, and `@Atomic` overlap on actor port `7110`. No model API key is needed.

## Guide examples

The [TypeScript](../docs/reference/typescript-guide.md) and [Python](../docs/reference/python-guide.md) guides build these projects step by step. Each contains the guide's finished code and runs from a repository checkout.

| Example | What it demonstrates | Actor port |
| --- | --- | --- |
| [Bank account (TypeScript)](bank-typescript/README.md) | Fields, WebSockets, SQLite, interleaving, and sandbox overrides | `7111` |
| [Bank account (Python)](bank-python/README.md) | The same actors in Python | `7112` |

Their ports don't overlap with the app examples below, so every example can run at once.

## Run the examples together

Each app needs its own app port and actor runtime port. Set these values in each project's `.env`:

| Example | `PORT` | `DURABLE_ACTORS_PORT` | `DURABLE_ACTORS_CONTROL_PLANE_URL` |
| --- | --- | --- | --- |
| AI chat | `3000` | `7100` | `http://127.0.0.1:7100` |
| Chatroom | `3001` | `7101` | `http://127.0.0.1:7101` |
| Collaborative documents | `3002` | `7102` | `http://127.0.0.1:7102` |

The control-plane URL must match that project's actor runtime port. Leave each project in its own directory so its `.durable-actors/` state is separate. If you configure `DURABLE_ACTORS_DATA_DIR`, use a different directory for each project.

In each project, run `bun run dev:actors`, wait for `Ready`, then run `bun run dev` in a second terminal. Open the corresponding app port above.

If a file/watch limit disables automatic actor reload, the runtime continues serving requests. Restart `bun run dev:actors` after source changes. You can also opt out of watching with `bun run dev:actors --no-watch`.
