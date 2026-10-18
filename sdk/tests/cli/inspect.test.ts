import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { once } from "node:events"
import { createServer } from "node:http"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { formatInstance, formatInventory, inspectActors, parseActorSelector } from "../../src/cli/inspect.js"

const run = promisify(execFile)
const cli = fileURLToPath(new URL("../../../dist/cli.js", import.meta.url))

const inventory = {
    connectionsComplete: true,
    actors: [
        {
            actorName: "Counter",
            live: 1,
            dormant: 1,
            unknown: 0,
            instances: [
                {
                    actorId: "one",
                    status: "live" as const,
                    connections: [{ id: "sock-a", metadata: { user: "alice" } }]
                },
                {
                    actorId: "two",
                    status: "dormant" as const,
                    connections: [],
                    waiting: [{ id: "queued-1", operation: "increment" }]
                }
            ]
        }
    ]
}

const snapshot = {
    snapshot: {
        stateVersion: 3,
        ownerEpoch: 1,
        requestId: "req-3",
        state: { count: 3 },
        attribution: { operation: "increment", connectionId: null, committedAtMs: 1234, interleaved: false }
    },
    schema: null
}

test("inspect selector is ActorName/actorId", () => {
    assert.deepEqual(parseActorSelector("Counter/one"), { actorName: "Counter", actorId: "one" })
    assert.deepEqual(parseActorSelector("Room.with.dots/state-demo"), {
        actorName: "Room.with.dots",
        actorId: "state-demo"
    })
    for (const selector of ["Counter", "Counter/", "/one", "Counter/one/two", "Counter/one/extra"])
        assert.throws(() => parseActorSelector(selector), /Expected ActorName\/actorId/u)
})

test("inspect inventory text lists instances and an empty project", () => {
    assert.equal(formatInventory({ actors: [], connectionsComplete: true }), "No actor instances.")
    assert.equal(
        formatInventory(inventory),
        ["ACTOR    INSTANCE ID  STATUS", "Counter  one          live", "Counter  two          dormant"].join("\n")
    )
})

test("inspect instance text is status and the latest state JSON", () => {
    const selector = { actorName: "Counter", actorId: "one" }
    const text = formatInstance(selector, inventory.actors[0]!.instances[0]!, {
        ...snapshot,
        schema: { type: "object" }
    })
    assert.equal(
        text,
        ["Actor: Counter", "Instance ID: one", "Status: live", "State:", "{", '  "count": 3', "}"].join("\n")
    )
    assert.doesNotMatch(text, /sock-a|stateVersion|requestId|ownerEpoch|schema|increment/u)
    assert.equal(
        formatInstance(selector, inventory.actors[0]!.instances[0]!, { snapshot: null, schema: null }),
        ["Actor: Counter", "Instance ID: one", "Status: live", "State:", "No persisted snapshot for Counter/one."].join(
            "\n"
        )
    )
})

test("inspectActors prints JSON inventory and warns when connections are incomplete", async () => {
    const incomplete = { ...inventory, connectionsComplete: false }
    const result = await inspectActors(
        { listActors: async () => incomplete, getState: async () => snapshot },
        { json: true }
    )
    assert.equal(result.warning, "Warning: connection inventory is incomplete.")
    assert.deepEqual(JSON.parse(result.text), incomplete)
})

test("inspectActors loads persisted state for a selector", async () => {
    const requests: string[] = []
    const result = await inspectActors(
        {
            listActors: async () => inventory,
            getState: async query => {
                requests.push(query.toString())
                return snapshot
            }
        },
        { selector: "Counter/one" }
    )
    assert.deepEqual(requests, ["actorName=Counter&actorId=one"])
    assert.match(result.text, /Actor: Counter/u)
    assert.match(result.text, /"count": 3/u)
    assert.doesNotMatch(result.text, /stateVersion|requestId|ownerEpoch/u)
    assert.equal(result.warning, undefined)
})

test("inspectActors rejects a selector that is not in inventory", async () => {
    await assert.rejects(
        inspectActors(
            { listActors: async () => inventory, getState: async () => snapshot },
            { selector: "Counter/missing" }
        ),
        /No actor instance Counter\/missing/u
    )
})

test("inspect CLI lists inventory and reads state from the control plane", async t => {
    const requests: { url: string; authorization: string | undefined }[] = []
    const server = createServer((request, response) => {
        requests.push({ url: request.url!, authorization: request.headers.authorization })
        response.setHeader("content-type", "application/json")
        if (request.url === "/v1/projects/local/observe/actors") response.end(JSON.stringify(inventory))
        else if (request.url === "/v1/projects/local/observe/state?actorName=Counter&actorId=one")
            response.end(JSON.stringify(snapshot))
        else response.writeHead(404).end(JSON.stringify({ error: { message: "not found" } }))
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const env = {
        ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("DURABLE_ACTORS_"))),
        DURABLE_ACTORS_CONTROL_PLANE_URL: "http://unreachable.invalid",
        DURABLE_ACTORS_SECRET: "inspect-key"
    }
    const listed = await run(process.execPath, [cli, "inspect", "--control-plane-url", origin], { env })
    assert.match(listed.stdout, /ACTOR\s+INSTANCE ID\s+STATUS/u)
    assert.match(listed.stdout, /Counter\s+one\s+live/u)
    assert.doesNotMatch(listed.stdout, /connections|waiting/u)
    const json = await run(process.execPath, [cli, "inspect", "--json", "--control-plane-url", origin], { env })
    assert.deepEqual(JSON.parse(json.stdout), inventory)
    const detail = await run(process.execPath, [cli, "inspect", "Counter/one", "--control-plane-url", origin], { env })
    assert.match(detail.stdout, /Status: live/u)
    assert.match(detail.stdout, /"count": 3/u)
    assert.doesNotMatch(detail.stdout, /stateVersion|requestId|ownerEpoch|sock-a/u)
    assert.deepEqual(
        requests.map(request => request.url),
        [
            "/v1/projects/local/observe/actors",
            "/v1/projects/local/observe/actors",
            "/v1/projects/local/observe/actors",
            "/v1/projects/local/observe/state?actorName=Counter&actorId=one"
        ]
    )
    assert.ok(requests.every(request => request.authorization === "Bearer inspect-key"))
})

test("inspect CLI reports a missing instance and an empty project", async t => {
    const server = createServer((request, response) => {
        response.setHeader("content-type", "application/json")
        response.end(
            JSON.stringify(
                request.url?.includes("/observe/actors")
                    ? { actors: [], connectionsComplete: true }
                    : { snapshot: null, schema: null }
            )
        )
    })
    t.after(() => server.close())
    server.listen(0, "127.0.0.1")
    await once(server, "listening")
    const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
    const env = {
        ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("DURABLE_ACTORS_"))),
        DURABLE_ACTORS_CONTROL_PLANE_URL: origin
    }
    const empty = await run(process.execPath, [cli, "inspect"], { env })
    assert.equal(empty.stdout.trim(), "No actor instances.")
    await assert.rejects(run(process.execPath, [cli, "inspect", "Counter/one"], { env }), (error: unknown) => {
        const result = error as Error & { code: number; stderr: string; stdout: string }
        assert.equal(result.code, 1)
        assert.equal(result.stdout, "")
        assert.match(result.stderr, /No actor instance Counter\/one/u)
        return true
    })
    await assert.rejects(run(process.execPath, [cli, "inspect", "Counter"], { env }), (error: unknown) => {
        const result = error as Error & { stderr: string }
        assert.match(result.stderr, /Expected ActorName\/actorId/u)
        return true
    })
})
