import React, { act } from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import type { ActorInventory, ObserverClient, RequestTracePage } from "../src/client.js"

import "./dom.js"

// React DOM and Radix must see the jsdom globals when they load.
const { cleanup, fireEvent, render, waitFor, within } = await import("@testing-library/react")
const { ConsoleApp } = await import("../src/ConsoleApp.js")
const { Overview } = await import("../src/Overview.js")
const { requestSummary, tracesInRange } = await import("../src/overview-data.js")
afterEach(cleanup)

const inventory: ActorInventory = {
    connectionsComplete: true,
    actors: [
        { actorName: "Room", live: 2, dormant: 1, unknown: 0, instances: [{ actorId: "general", status: "live", connections: [{ id: "socket-a", metadata: null }] }] },
        { actorName: "Counter", live: 0, dormant: 0, unknown: 1, instances: [] }
    ]
}
const page: RequestTracePage = {
    epoch: "first",
    cursor: 2,
    capacity: 500,
    evicted: 0,
    dropped: 0,
    records: [
        {
            sequence: 2,
            projectId: "default",
            requestId: "r2",
            hostId: "h",
            sessionId: "s",
            actorName: "Room",
            actorId: "general",
            kind: "method",
            operation: "post",
            connectionId: null,
            startedAtMs: Date.now(),
            durationMs: 30,
            queueWaitMs: 5,
            hostState: "warm",
            outcome: "failed"
        },
        {
            sequence: 1,
            projectId: "default",
            requestId: "r1",
            hostId: "h",
            sessionId: "s",
            actorName: "Room",
            actorId: "general",
            kind: "method",
            operation: "post",
            connectionId: null,
            startedAtMs: Date.now(),
            durationMs: 10,
            queueWaitMs: 0,
            hostState: "warm",
            outcome: "completed"
        }
    ]
}

test("overview lists only actor types, retains partial inventory and traces, and opens an actor", async () => {
    let receive: (value: RequestTracePage) => void = () => {}
    let requestSignal: AbortSignal | undefined
    let selected = ""
    const client: ObserverClient = {
        listActors: async () => ({ ...inventory, connectionsComplete: false }),
        checkConnection: async () => {},
        watchRequests: async (next, signal) => {
            receive = next
            requestSignal = signal
            return new Promise(() => {})
        }
    }
    const view = render(<Overview client={client} onSelectActor={actor => (selected = actor)} />)
    await view.findByRole("button", { name: "Inspect Room" })
    assert.match(view.getByRole("status").textContent!, /Connection inventory incomplete/)
    const types = within(view.getByRole("list", { name: "Actor types" }))
    assert.deepEqual(
        types.getAllByRole("listitem").map(item => item.textContent),
        ["Room", "Counter"]
    )
    assert.equal(view.queryByRole("table"), null)
    assert.equal(view.queryByRole("textbox", { name: "Filter actor classes" }), null)
    assert.equal(view.queryByRole("combobox", { name: "Filter by residency" }), null)
    assert.equal(view.queryByText("Thresholds"), null)
    assert.equal(view.getByLabelText("Actor instances").textContent, "4")
    assert.equal(view.getByLabelText("Open WebSocket connections").textContent, "1")
    assert.equal(view.getByLabelText("Retained requests").textContent, "—")
    await act(async () => receive(page))
    assert.equal(view.getByLabelText("Retained requests").textContent, "2")
    assert.equal(types.getByRole("button", { name: "Inspect Room" }).textContent, "Room")
    assert.match(view.getByText("50%").textContent!, /50%/)
    fireEvent.click(view.getByRole("button", { name: "Inspect Room" }))
    assert.equal(selected, "Room")
    await act(async () => receive({ ...page, epoch: "restart", records: [], cursor: 0 }))
    assert.equal(view.getByLabelText("Retained requests").textContent, "0")
    view.unmount()
    assert.equal(requestSignal?.aborted, true)
})

test("unavailable sources do not masquerade as zero or successful metrics", async () => {
    const client: ObserverClient = {
        listActors: async () => {
            throw new Error("private")
        },
        checkConnection: async () => {}
    }
    const view = render(<Overview client={client} onSelectActor={() => {}} />)
    await waitFor(() => assert.ok(view.getAllByRole("alert").length >= 1))
    assert.equal(view.getByLabelText("Actor instances").textContent, "—")
    assert.equal(view.getByLabelText("Retained requests").textContent, "—")
    assert.equal(view.getByLabelText("Open WebSocket connections").textContent, "—")
    assert.doesNotMatch(view.container.textContent!, /private|100%/)
})

test("request metrics exclude reroutes and time windows exclude older and future traces", () => {
    const records = page.records.map((record, index) => ({ ...record, startedAtMs: index ? 1000 : 61000 }))
    assert.equal(tracesInRange(records, { fromMs: 1000 }, 61000).length, 2)
    assert.equal(tracesInRange(records, { fromMs: 1001 }, 61001).length, 1)
    assert.equal(tracesInRange(records, { fromMs: 0, toMs: 999 }, 999).length, 0)
    assert.equal(tracesInRange(records, {}, 61000).length, 2)
    assert.deepEqual(requestSummary([{ ...records[0]!, outcome: "rerouted" }]), { count: 1, success: null, p95: null })
})

test("console navigation opens the selected actor and real WebSocket metadata", async () => {
    const client: ObserverClient = { listActors: async () => inventory, checkConnection: async () => {} }
    const view = render(<ConsoleApp client={client} toggleTheme={() => {}} />)
    const actorLink = await view.findByRole("button", { name: "Inspect Room" })
    actorLink.focus()
    fireEvent.click(actorLink)
    assert.ok(await view.findByRole("heading", { name: "Room instances" }))
    assert.ok(view.getByRole("heading", { name: "Room", level: 1 }))
    assert.equal(view.queryByRole("table", { name: "Actor instance counts" }), null)
    fireEvent.click(view.getByRole("button", { name: "Actors", exact: true }))
    fireEvent.change(await view.findByRole("combobox", { name: "Search actors and instances" }), { target: { value: "room" } })
    fireEvent.click(view.getByRole("button", { name: "Room", exact: true }))
    assert.ok(view.getByRole("heading", { name: "Room", level: 1 }))
    fireEvent.click(view.getByRole("button", { name: "Actors", exact: true }))
    assert.ok(view.getByRole("table", { name: "Actor instance counts" }))
    assert.equal((view.getByRole("combobox", { name: "Search actors and instances" }) as HTMLInputElement).value, "room")
    fireEvent.click(view.getByRole("button", { name: "WebSockets", exact: true }))
    assert.ok(await view.findByRole("button", { name: "Inspect connection socket-a" }))
    assert.ok(view.getByText("null"))
    fireEvent.change(view.getByRole("combobox", { name: "Filter connections" }), { target: { value: "missing" } })
    assert.ok(view.getByText("No matching connections"))
    fireEvent.click(view.getByRole("button", { name: "Clear filter" }))
    assert.ok(view.getByRole("button", { name: "Inspect connection socket-a" }))
})

test("with saved history the overview reads metrics for the selected time range instead of the live window", async () => {
    const queries: { fromMs?: number; toMs?: number }[] = []
    const client: ObserverClient = {
        listActors: async () => inventory,
        checkConnection: async () => {},
        watchRequests: async () => {
            throw new Error("live stream must not be used when saved history is available")
        },
        getMetrics: async query => {
            queries.push(query)
            return {
                total: { actorName: "", count: 40, success: (100 * 37) / 38, p95: 120.5, queueP95: 8 },
                classes: [{ actorName: "Room", count: 40, success: (100 * 37) / 38, p95: 120.5, queueP95: 8 }]
            }
        }
    }
    const view = render(<Overview client={client} onSelectActor={() => {}} />)
    await waitFor(() => assert.equal(view.getByLabelText("Retained requests").textContent, "40"))
    assert.equal(typeof queries[0]!.fromMs, "number")
    assert.ok(view.getByText("97.37%"))
    assert.ok(view.getByText("120.5 ms"))
    assert.ok(view.getByText("8 ms"))
    const types = within(view.getByRole("list", { name: "Actor types" }))
    assert.deepEqual(
        types.getAllByRole("listitem").map(item => item.textContent),
        ["Room", "Counter"]
    )
    fireEvent.click(view.getByRole("button", { name: "Time range: Last hour" }))
    fireEvent.click(view.getByRole("button", { name: "All retained" }))
    await waitFor(() => assert.equal(queries.length, 2))
    assert.deepEqual(queries[1], {})
    assert.match(view.container.textContent!, /in retained history/u)
})
