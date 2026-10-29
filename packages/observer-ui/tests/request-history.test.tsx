import React, { act } from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import type { RequestHistoryQuery, RequestTracePage } from "../src/client.js"
import { useRequestHistory } from "../src/request-history.js"

import "./dom.js"

const { cleanup, renderHook, waitFor } = await import("@testing-library/react")
afterEach(cleanup)

function page(eventId: string, nextCursor?: string, overrides: Partial<RequestTracePage> = {}): RequestTracePage {
    return {
        epoch: "one",
        cursor: 100,
        capacity: 500,
        evicted: 0,
        dropped: 0,
        records: [
            {
                eventId,
                sequence: 1,
                projectId: "local",
                requestId: eventId,
                hostId: "host",
                sessionId: "session",
                actorName: "Room",
                actorId: "lobby",
                kind: "websocket",
                operation: "onMessage",
                connectionId: "socket",
                startedAtMs: 1000,
                durationMs: 1,
                queueWaitMs: 0,
                hostState: "warm",
                outcome: "completed"
            }
        ],
        nextCursor,
        ...overrides
    }
}

test("history retries the failed page and keeps distinct events with the same sequence", async () => {
    const cursors: (string | undefined)[] = []
    const client = {
        listRequests: async ({ cursor }: RequestHistoryQuery) => {
            cursors.push(cursor)
            if (!cursor) return page("newest", "second")
            if (cursors.length === 2) throw new Error("temporarily unavailable")
            if (cursor === "second") return page("middle", "third", { records: [...page("newest").records, ...page("middle").records] })
            return page("oldest")
        }
    }
    const query = { fromMs: 0 }
    const { result } = renderHook(() => useRequestHistory(client, query))
    await waitFor(() => assert.equal(result.current.failed, true))
    assert.deepEqual(
        result.current.page?.records.map(record => record.eventId),
        ["newest"]
    )
    act(() => result.current.retry())
    await waitFor(() => assert.equal(result.current.page?.records.length, 3))
    assert.equal(result.current.failed, false)
    assert.equal(result.current.loading, false)
    assert.deepEqual(cursors, [undefined, "second", "second", "third"])
    assert.deepEqual(
        result.current.page?.records.map(record => record.eventId),
        ["newest", "middle", "oldest"]
    )
})

test("changing filters aborts pagination and ignores a late older page", async () => {
    let finish!: (page: RequestTracePage) => void
    let olderSignal: AbortSignal | undefined
    const queries: RequestHistoryQuery[] = []
    const client = {
        listRequests: async (query: RequestHistoryQuery, signal?: AbortSignal) => {
            queries.push(query)
            if (query.actorId === "other") return page("filtered")
            if (!query.cursor) return page("newest", "older")
            olderSignal = signal
            return new Promise<RequestTracePage>(resolve => {
                finish = resolve
            })
        }
    }
    const { result, rerender } = renderHook(({ query }) => useRequestHistory(client, query), { initialProps: { query: { actorId: "lobby" } } })
    await waitFor(() => assert.equal(queries.length, 2))
    rerender({ query: { actorId: "other" } })
    await waitFor(() => assert.equal(result.current.page?.records[0]?.eventId, "filtered"))
    assert.equal(olderSignal?.aborted, true)
    await act(async () => finish(page("stale", "never-load")))
    assert.equal(result.current.page?.records[0]?.eventId, "filtered")
    assert.equal(queries.length, 3)
})

test("retention resets discard stale pages and preserve the incomplete-history warning", async () => {
    const client = {
        listRequests: async ({ cursor }: RequestHistoryQuery) => {
            if (!cursor) return page("expired", "reset")
            if (cursor === "reset") return page("retained", "last", { reset: true, epoch: "two" })
            return page("last", undefined, { epoch: "two" })
        }
    }
    const query = {}
    const { result } = renderHook(() => useRequestHistory(client, query))
    await waitFor(() => assert.equal(result.current.loading, false))
    assert.deepEqual(
        result.current.page?.records.map(record => record.eventId),
        ["retained", "last"]
    )
    assert.equal(result.current.page?.reset, true)
})
