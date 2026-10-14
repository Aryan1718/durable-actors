# CLI example

A separate client project connected to the [persistence-tests example](../persistence-tests/). It imports the generated client, with no imports from the actor source.

Start the persistence-tests server first. Then, from the repository root in a second terminal:

```sh
cd examples/persistence/cli
cp .env.example .env
bun run --bun generate
bun run --bun start crash-read
```

All commands use the `PersistenceTests` actor with ID `demo`.

`bun run --bun generate` fetches the running server's contract and writes the client to `generated/`. Regenerate after changing the actor API.

The `.env` points at `http://127.0.0.1:7103`. Change it to connect to a different actor server; Bun loads it when running the CLI. `bun run --bun check` checks the CLI types after generation.

`bun run --bun start immediate-test` clears the marker through an ordinary persisted-property write. `bun run --bun start atomic-test` clears it atomically. With `--fail`, the atomic method throws and its write should roll back.

After deploying the persistence-tests example, run `bun run --bun generate`, then test a write followed by an abrupt worker exit:

```sh
bun run --bun start crash-write pineapple
bun run --bun start crash-read
```

`crash-write` saves the marker and exits the actor worker with code 1, so the CLI command is expected to fail. Because this actor has an interleaved method, the host reports the crash as `outcome_unknown` (durable publication could not be confirmed). `crash-read` must return `pineapple` from the restored actor. Use a new marker for each run so an old value cannot mask a failed write. The event list is unaffected.

Then test the same crash inside an atomic method:

```sh
bun run --bun start atomic-crash-write should-not-survive
bun run --bun start crash-read
```

`atomic-crash-write` also exits the worker with code 1 and must fail with `outcome_unknown`. The read must still return the previous durable marker (`pineapple` above), because the atomic method crashed before committing.

Test overlapping durable writes, then verify they survive a worker crash:

```sh
bun run --bun start interleave-test
bun run --bun start crash-write interleave-check
bun run --bun start events-read
```

`interleave-test` first reads events to cache the actor route, then starts a slow and a fast call on the same actor and prints their persisted events. Both start entries should appear before either end entry, with all four entries present. Arrival order can vary. Each run appends four entries to the existing list. After the expected `crash-write` failure, `events-read` must print the same list.
