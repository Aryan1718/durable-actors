# Persistence test actor

The actor project for the [persistence example pair](../README.md). Start it with `bun run dev`; the sibling [CLI](../cli/README.md) calls it through a generated client.

[PersistenceTests](src/actors.ts) exercises immediate persisted writes, atomic rollback, interleaving, and recovery after worker crashes. `bun run check` checks its types.

To set up the local integration tests, use Bun 1.4.2 or newer and Rust 1.91 or newer. From the repository root:

```sh
bun install --frozen-lockfile
bun run --cwd sdk build
cargo build --locked --bin durable-actors
cd examples/persistence/persistence-tests
cp .env.example .env
```

Keep existing `.env` settings if you already have that file. Set `DURABLE_ACTORS_BINARY=../../../target/debug/durable-actors` to use the runtime built from this checkout. Rebuild the SDK or native runtime after changing their source.

Then, from this directory:

```sh
bun run test
```

The two tests in `tests/` start their own local servers on free ports, use temporary storage, and clean up afterward. They verify crash recovery, atomic rollback, and overlapping writes, including after a full runtime restart. No separate dev server, CLI generation, cloud services, or credentials are needed. To run just one test file, use `bun test --timeout 60000 tests/crash.test.mjs` or `bun test --timeout 60000 tests/interleave.test.mjs`.
