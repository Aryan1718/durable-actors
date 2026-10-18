import assert from "node:assert/strict"
import { mkdtemp, readFile, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import test from "node:test"

import { Staging } from "../../scripts/staging.mjs"
import { NgrokTunnel } from "../../scripts/tunnel.mjs"

test("staging routes browser sockets over HTTP/1 while retaining HTTP/2 for actor calls", async t => {
    const root = await mkdtemp(path.join(tmpdir(), "staging-tunnel-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    const environment = {
        DURABLE_ACTORS_CONTROL_PLANE_BIND: "127.0.0.1:7200",
        DURABLE_ACTORS_PUBLIC_URL: "https://staging.ngrok.app"
    }
    const staging = new Staging(root, environment, path.join(root, "examples/pi/pi-durable"))
    const server = await staging.serverOptions()
    const config = JSON.parse(await readFile(server.env.NGROK_CONFIG, "utf8"))
    const publicEndpoint = config.endpoints.find(endpoint => endpoint.url === environment.DURABLE_ACTORS_PUBLIC_URL)
    assert.equal(publicEndpoint.upstream.protocol, "http2")
    const [rule] = publicEndpoint.traffic_policy.on_http_request
    assert.deepEqual(rule.expressions, ["req.url.path == '/v1/socket'"])
    const [action] = rule.actions
    assert.equal(action.type, "forward-internal")
    const socketEndpoint = config.endpoints.find(endpoint => endpoint.url === action.config.url)
    assert.equal(socketEndpoint.upstream.protocol, "http1")
    assert.equal(socketEndpoint.upstream.url, "http://127.0.0.1:7200")
})

test("configured staging tunnels start both endpoints in one ngrok agent", () => {
    const tunnel = new NgrokTunnel({ NGROK_CONFIG: "/tmp/staging-ngrok.json" })
    assert.deepEqual(tunnel.arguments(true), ["start", "--all", "--config", "/tmp/staging-ngrok.json", "--log=stdout", "--log-format=json", "--log-level=info"])
})

test("internal socket endpoints do not replace the public control plane origin", () => {
    const saved = []
    const tunnel = new NgrokTunnel({}, undefined, { set: (...entry) => saved.push(entry) })
    tunnel.updateFromLog(JSON.stringify({ msg: "started tunnel", url: "https://staging-socket.internal" }))
    assert.equal(tunnel.origin, undefined)
    assert.deepEqual(saved, [])
})
