import { useEffect, useState } from "react"

import type { ActorInventory, ObserverClient, RequestTracePage } from "./client.js"

export function useInventory(client: ObserverClient) {
    const [attempt, setAttempt] = useState(0)
    const [snapshot, setSnapshot] = useState<{ client: ObserverClient; inventory: ActorInventory; updatedAt: number }>()
    const [loading, setLoading] = useState(true)
    const [failed, setFailed] = useState(false)
    useEffect(() => {
        const controller = new AbortController()
        let timer: ReturnType<typeof setTimeout> | undefined
        let retryDelay = 1_000
        setFailed(false)
        setLoading(true)
        void refresh()
        return () => {
            controller.abort()
            clearTimeout(timer)
        }
        function receive(inventory: ActorInventory) {
            if (controller.signal.aborted) return
            setSnapshot({ client, inventory, updatedAt: Date.now() })
            setFailed(false)
            setLoading(false)
            retryDelay = 1_000
        }
        async function refresh() {
            try {
                if (client.watchActors) {
                    await client.watchActors(receive, controller.signal)
                    if (!controller.signal.aborted) throw new Error("Stream ended")
                } else {
                    setLoading(true)
                    receive(await client.listActors(controller.signal))
                }
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                if (!controller.signal.aborted) {
                    setLoading(false)
                    timer = setTimeout(refresh, client.watchActors ? retryDelay : 5_000)
                    retryDelay = Math.min(retryDelay * 2, 10_000)
                }
            }
        }
    }, [client, attempt])
    return {
        updatedAt: snapshot?.client === client ? snapshot.updatedAt : undefined,
        inventory: snapshot?.client === client ? snapshot.inventory : undefined,
        loading,
        failed,
        retry: () => setAttempt(value => value + 1)
    }
}

export function useRequests(client: Pick<ObserverClient, "watchRequests">, enabled = true) {
    const [page, setPage] = useState<RequestTracePage>()
    const [failed, setFailed] = useState(false)
    const [attempt, setAttempt] = useState(0)
    useEffect(() => setPage(undefined), [client])
    useEffect(() => {
        if (!enabled) return
        let after: string | undefined
        const controller = new AbortController()
        let timer: ReturnType<typeof setTimeout> | undefined
        let delay = 1000
        async function watch() {
            try {
                if (!client.watchRequests) throw new Error("Request traces unavailable")
                await client.watchRequests(
                    incoming => {
                        if (controller.signal.aborted) return
                        after = incoming.resumeCursor
                        setPage(current => mergePages(current, incoming))
                        setFailed(false)
                        delay = 1000
                    },
                    controller.signal,
                    after
                )
                if (!controller.signal.aborted) throw new Error("Request stream disconnected")
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                if (!controller.signal.aborted) {
                    timer = setTimeout(watch, delay)
                    delay = Math.min(delay * 2, 10000)
                }
            }
        }
        setFailed(false)
        void watch()
        return () => {
            controller.abort()
            clearTimeout(timer)
        }
    }, [client, attempt, enabled])
    return { page, failed, retry: () => setAttempt(value => value + 1) }
}

function mergePages(current: RequestTracePage | undefined, incoming: RequestTracePage): RequestTracePage {
    const records = new Map((current?.epoch === incoming.epoch && !incoming.reset && !incoming.snapshot ? current.records : []).map(record => [record.eventId ?? record.sequence, record]))
    for (const record of incoming.records) records.set(record.eventId ?? record.sequence, record)
    return {
        ...incoming,
        reset: incoming.reset || (current?.epoch === incoming.epoch && current.reset),
        records: [...records.values()].sort((a, b) => b.sequence - a.sequence || (b.eventId ?? "").localeCompare(a.eventId ?? "")).slice(0, incoming.capacity)
    }
}

export function usePolledQuery<Q extends object, T>(client: object, query: Q, request: ((query: Q, signal?: AbortSignal) => Promise<T>) | undefined, interval = 10_000) {
    const key = request ? JSON.stringify(query) : undefined
    const [attempt, setAttempt] = useState(0)
    const [result, setResult] = useState<{ client: object; key: string; value: T; updatedAt: number }>()
    const [failed, setFailed] = useState(false)
    const [loading, setLoading] = useState(false)
    useEffect(() => {
        if (!key) return
        const controller = new AbortController()
        let timer: ReturnType<typeof setTimeout> | undefined
        setFailed(false)
        setLoading(true)
        void load()
        return () => {
            controller.abort()
            clearTimeout(timer)
        }
        async function load() {
            try {
                const value = await request!.call(client, JSON.parse(key!) as Q, controller.signal)
                if (controller.signal.aborted) return
                setResult({ client, key: key!, value, updatedAt: Date.now() })
                setFailed(false)
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                if (!controller.signal.aborted) {
                    setLoading(false)
                    timer = setTimeout(load, interval)
                }
            }
        }
    }, [client, request, key, attempt, interval])
    const current = result && result.client === client && result.key === key ? result : undefined
    return { supported: !!request, value: current?.value, updatedAt: current?.updatedAt, loading, failed, retry: () => setAttempt(value => value + 1) }
}
