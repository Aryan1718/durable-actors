import assert from "node:assert/strict"
import { test } from "node:test"

import { RemoteActorClient } from "../../src/client/remoteClient.js"

const options = { projectId: "project", apiKey: "backend-key", controlPlaneUrl: "https://gateway.example" }

test("every invocation uses the configured gateway and caller credential", async () => {
    const requests: { url: string; body: unknown }[] = []
    const client = new RemoteActorClient(options, {
        requestId: () => "request-1",
        fetch: async (url, init) => {
            assert.equal(new Headers(init?.headers).get("authorization"), "Bearer backend-key")
            requests.push({ url: String(url), body: JSON.parse(String(init?.body)) })
            return Response.json({ outcome: { type: "completed", result: 7 } })
        }
    })
    for (const amount of [2, 3]) assert.equal(await client.invoke("Counter", "one", "increment", [amount]), 7)
    assert.deepEqual(
        requests,
        [2, 3].map(amount => ({
            url: "https://gateway.example/v1/projects/project/actors/Counter/one/invoke",
            body: { requestId: "request-1", method: "increment", args: [amount] }
        }))
    )
})

test("only explicit pre-execution rejection retries with the same request ID", async () => {
    const requests: unknown[] = []
    const client = new RemoteActorClient(options, {
        requestId: () => "request-1",
        fetch: async (_url, init) => {
            requests.push(JSON.parse(String(init?.body)))
            return Response.json({
                outcome:
                    requests.length === 1
                        ? { type: "not_executed", reason: "host_unavailable" }
                        : { type: "completed", result: 7 }
            })
        }
    })
    assert.equal(await client.invoke("Counter", "one", "increment", []), 7)
    assert.equal(requests.length, 2)
    assert.deepEqual(requests[0], requests[1])
})
