import { useEffect, useRef, useState } from "react"

import type { ObserverClient, RequestHistoryQuery, RequestTracePage } from "./client.js"

export function useRequestHistory(client: Pick<ObserverClient, "listRequests">, query: RequestHistoryQuery | undefined) {
    const [page, setPage] = useState<RequestTracePage>()
    const [loading, setLoading] = useState(false)
    const [failed, setFailed] = useState(false)
    const load = useRef<(cursor?: string) => void>(() => {})
    useEffect(() => {
        setPage(undefined)
        setFailed(false)
        setLoading(false)
        if (!query) return
        const controller = new AbortController()
        let busy = false
        load.current = (cursor?: string) => {
            if (busy || controller.signal.aborted) return
            busy = true
            setLoading(true)
            setFailed(false)
            void fetchPage(cursor)
        }
        async function fetchPage(cursor?: string) {
            try {
                if (!client.listRequests) throw new Error("History is unavailable")
                do {
                    const incoming = await client.listRequests({ limit: 500, ...query, cursor }, controller.signal)
                    if (controller.signal.aborted) return
                    const append = !!cursor
                    setPage(current => {
                        if (!append || !current || incoming.reset || current.epoch !== incoming.epoch) return incoming
                        const records = new Map(current.records.map(record => [record.eventId ?? record.sequence, record]))
                        for (const record of incoming.records) records.set(record.eventId ?? record.sequence, record)
                        return { ...incoming, reset: current.reset || incoming.reset, records: [...records.values()] }
                    })
                    cursor = incoming.nextCursor ?? undefined
                } while (cursor)
            } catch {
                if (!controller.signal.aborted) setFailed(true)
            } finally {
                busy = false
                if (!controller.signal.aborted) setLoading(false)
            }
        }
        load.current()
        return () => controller.abort()
    }, [client, query])
    return { page, loading, failed, retry: () => load.current(page?.nextCursor ?? undefined) }
}
