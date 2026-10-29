import React, { act } from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import type { ActorInventory, ObserverClient } from "../src/client.js"
import { recentInstances } from "../src/residency.js"

import "./dom.js"

const { cleanup, fireEvent, render, within } = await import("@testing-library/react")
const { ActorObserver, HttpObserverClient } = await import("../src/index.js")
afterEach(cleanup)

const inventory: ActorInventory = {
    connectionsComplete: true,
    actors: [
        {
            actorName: "Room",
            live: 1,
            dormant: 2,
            unknown: 1,
            instances: [
                { actorId: "old-live", status: "live", lastActiveAtMs: 1000, connections: [] },
                { actorId: "never-reported", status: "unknown", connections: [] },
                { actorId: "recent-dormant", status: "dormant", lastActiveAtMs: 5000, connections: [] },
                { actorId: "older-dormant", status: "dormant", lastActiveAtMs: 3000, connections: [] }
            ]
        }
    ]
}
const periods = [
    { actorId: "recent-dormant", status: "live" as const, fromMs: 1000, toMs: 5000 },
    { actorId: "recent-dormant", status: "dormant" as const, fromMs: 5000, toMs: 10000 },
    { actorId: "old-live", status: "unknown" as const, fromMs: 1000, toMs: 10000 }
]

test("recent activity orders every instance ahead of unknown timestamps without preferring live instances", () => {
    const source = inventory.actors[0]!.instances
    assert.deepEqual(
        recentInstances(source).map(instance => instance.actorId),
        ["recent-dormant", "older-dormant", "old-live", "never-reported"]
    )
    assert.equal(source[0]!.actorId, "old-live")
})

test("the instance list and residency lanes share activity order, filters, and instance navigation", async () => {
    let publish: (value: ActorInventory) => void = () => {}
    const client: ObserverClient = {
        checkConnection: async () => {},
        listActors: async () => inventory,
        watchActors: async receive => {
            publish = receive
            receive(inventory)
            await new Promise(() => {})
        },
        listResidency: async query => {
            assert.deepEqual(query, { actorName: "Room", fromMs: 1000, toMs: 10000 })
            return { periods, truncated: false }
        }
    }
    const view = render(<ActorObserver client={client} initialActorName="Room" timeRange={{ kind: "absolute", fromMs: 1000, toMs: 10000 }} />)
    const table = await view.findByRole("table", { name: "Room instances" })
    const rows = () =>
        within(table)
            .getAllByRole("row")
            .slice(1)
            .map(row => within(row).getByRole("button").textContent)
    assert.deepEqual(rows(), ["recent-dormant", "older-dormant", "old-live", "never-reported"])
    assert.equal(
        within(table)
            .getByRole("columnheader", { name: /Last active/ })
            .getAttribute("aria-sort"),
        "descending"
    )
    const timeline = await view.findByRole("group", { name: "Instance residency timeline" })
    assert.deepEqual(
        [...timeline.querySelectorAll(".socket-timeline-label strong")].map(label => label.textContent),
        rows()
    )
    assert.equal(timeline.querySelector("button.socket-timeline-label"), null)
    fireEvent.focus(within(timeline).getByRole("button", { name: /recent-dormant: In memory/ }))
    assert.match(view.getByRole("tooltip").textContent!, /recent-dormant.*In memory.*4 s/)
    fireEvent.change(view.getByRole("combobox", { name: "Instance state" }), { target: { value: "dormant" } })
    assert.deepEqual(rows(), ["recent-dormant", "older-dormant"])
    assert.deepEqual(
        [...timeline.querySelectorAll(".socket-timeline-label strong")].map(label => label.textContent),
        ["recent-dormant", "older-dormant"]
    )
    await act(async () =>
        publish({
            ...inventory,
            actors: [{ ...inventory.actors[0]!, instances: inventory.actors[0]!.instances.map(instance => (instance.actorId === "older-dormant" ? { ...instance, lastActiveAtMs: 9000 } : instance)) }]
        })
    )
    assert.deepEqual(rows(), ["older-dormant", "recent-dormant"])
    fireEvent.click(within(timeline).getByRole("button", { name: /recent-dormant: Dormant/ }))
    assert.ok(view.getByRole("heading", { name: "recent-dormant", level: 1 }))
})

test("residency errors are retryable and a truncated response explains the history limit", async () => {
    let calls = 0
    const client = {
        checkConnection: async () => {},
        listActors: async () => inventory,
        listResidency: async () => {
            if (++calls === 1) throw new Error("private detail")
            return { periods, truncated: true }
        }
    }
    const view = render(<ActorObserver client={client} initialActorName="Room" />)
    await view.findByRole("button", { name: "Retry residency history" })
    assert.doesNotMatch(view.container.textContent!, /private detail/)
    fireEvent.click(view.getByRole("button", { name: "Retry residency history" }))
    await view.findByText(/latest 10,000 reports/)
    assert.ok(view.getByRole("group", { name: "Instance residency timeline" }))
})

test("the residency adapter preserves actor and range filters and rejects invalid periods", async () => {
    const controller = new AbortController()
    const client = new HttpObserverClient("/project/observe", async (url, options) => {
        assert.equal(url, "/project/observe/residency?actorName=Room&fromMs=1000&toMs=10000")
        assert.equal(options?.signal, controller.signal)
        assert.equal(options?.credentials, "same-origin")
        return Response.json({ periods, truncated: false })
    })
    assert.deepEqual(await client.listResidency({ actorName: "Room", fromMs: 1000, toMs: 10000 }, controller.signal), { periods, truncated: false })
    for (const value of [{}, { periods: [{ ...periods[0], toMs: 0 }], truncated: false }, { periods: [{ ...periods[0], status: "bad" }], truncated: false }]) {
        await assert.rejects(new HttpObserverClient("/observe", async () => Response.json(value)).listResidency({ actorName: "Room" }))
    }
})
