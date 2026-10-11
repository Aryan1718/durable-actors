# Durable Actors for Python

Durable actors and typed clients, backed by the Rust runtime.

[Quickstart](#quickstart) · [Reference](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/python-guide.md) · [Runnable example](https://github.com/TerseAI/durable-actors/blob/main/examples/python/README.md)

## Quickstart

Requires Python 3.11+ and uv. The CLI, SDK, and native runtime are installed as Python packages.

Create a project anywhere. uv installs the SDK, CLI, and matching native runtime wheel from PyPI.

```sh
uvx --from 'durable-actors[cli]' durable-actors init my-actors
cd my-actors
```

Define `src/actors.py`. Every instance field must use `persisted()` or `ephemeral()`. Persisted fields are stored in SQLite. Litestream is built into the runtime; local development persists to the filesystem. Wrap persisted fields with `emitted()` to broadcast saved changes.

```python
from durable_actors import Actor, emitted, persisted

class Counter(Actor):
    count: int = emitted(persisted(0))

    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
```

```sh
uv run durable-actors dev
```

## Call an actor

In another terminal in the same directory, generate the client:

```sh
uv run durable-actors generate
```

Save as `client.py` and run `uv run client.py`:

```python
from generated import actors

counter = actors.Counter.get("one")
print(counter.increment())
```

## Async actors and clients

Define `AsyncActor` methods and socket hooks with `async def`. Calls stay serialized across `await` unless marked `@interleave`. Generated `async_actors` clients work with either actor style:

```python
from durable_actors import AsyncClient
from generated import async_actors

async with AsyncClient() as client:
    print(await async_actors.Counter.get("one", client).increment())
```

Await actor database and connection queries, and `broadcast()`; socket handle methods remain synchronous. Reuse `AsyncClient` for connection pooling. Close async connections and subscriptions with `async with` or `aclose()`.

## Use the client in another application

Copy the entire generated package, including its helper modules, into the application. From that application's root, install the client runtime:

```sh
uv add durable-actors
```

This installs the runtime dependencies automatically. The `[codegen]` extras are only needed to generate or regenerate clients, not to run them.

## Subscribe to state

Callbacks receive complete typed snapshots on a background thread. Keep the process running while listening; the context manager closes the subscription.

```python
from generated import actors

counter = actors.Counter.get("one")
with counter.subscribe(lambda state: print(state.count), on_error=print):
    counter.increment()
    input("Press Enter to stop.\n")
```
