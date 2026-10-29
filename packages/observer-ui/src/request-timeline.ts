import type { ActorInventory, RequestTrace } from "./client.js"

export type RequestTimelineLane = Pick<RequestTrace, "actorName" | "actorId" | "kind" | "operation"> & { projectId?: string }

export function requestTimeline(records: RequestTrace[], lanes: RequestTimelineLane[] = []) {
    const ordered = [...records].sort((a, b) => a.startedAtMs - b.startedAtMs || a.sequence - b.sequence || (a.eventId ?? "").localeCompare(b.eventId ?? ""))
    const start = ordered[0]?.startedAtMs ?? 0
    let end = start
    const finishes = new Map<string, number>()
    const calls = ordered.map(record => {
        const key = JSON.stringify([record.projectId, record.actorName, record.actorId])
        const previousEnd = finishes.get(key)
        const finish = record.startedAtMs + record.durationMs
        finishes.set(key, Math.max(previousEnd ?? finish, finish))
        end = Math.max(end, finish)
        return { record, offsetMs: record.startedAtMs - start, gapMs: previousEnd === undefined ? null : record.startedAtMs - previousEnd }
    })
    return { start, span: end - start, calls, rows: methodRows(calls, lanes) }
}

function methodRows(calls: { record: RequestTrace; offsetMs: number; gapMs: number | null }[], lanes: RequestTimelineLane[]) {
    const rows = new Map<string, RequestTimelineLane & { key: string; calls: typeof calls }>()
    const project = calls[0]?.record.projectId ?? lanes.find(lane => lane.projectId)?.projectId ?? ""
    function row(lane: RequestTimelineLane) {
        const { actorName, actorId, kind, operation } = lane
        const projectId = lane.projectId ?? project
        const key = JSON.stringify([projectId, actorName, actorId, kind, operation])
        if (!rows.has(key)) rows.set(key, { key, projectId, actorName, actorId, kind, operation, calls: [] })
        return rows.get(key)!
    }
    for (const lane of lanes) row(lane)
    for (const call of calls) {
        row(call.record).calls.push(call)
    }
    return [...rows.values()].sort((a, b) => a.key.localeCompare(b.key))
}

export function requestTimelineLanes(inventory: ActorInventory | undefined, records: RequestTrace[], actor?: Pick<RequestTrace, "actorName" | "actorId">): RequestTimelineLane[] {
    const lanes: RequestTimelineLane[] = [...records]
    for (const type of inventory?.actors ?? []) {
        if (actor && actor.actorName !== type.actorName) continue
        const instances = actor ? [actor.actorId] : new Set(records.filter(record => record.actorName === type.actorName).map(record => record.actorId))
        for (const actorId of instances) {
            for (const operation of type.methods ?? []) lanes.push({ actorName: type.actorName, actorId, kind: "method", operation })
            if (type.methods) {
                for (const operation of ["onConnect", "onDisconnect", "onMessage"]) lanes.push({ actorName: type.actorName, actorId, kind: "websocket", operation })
            }
        }
    }
    return lanes.filter(lane => !actor || (lane.actorName === actor.actorName && lane.actorId === actor.actorId))
}

export function duration(ms: number): string {
    if (ms >= 60_000) return `${(ms / 60_000).toLocaleString(undefined, { maximumFractionDigits: 2 })} min`
    return ms >= 1000 ? `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 2 })} s` : `${ms.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`
}

export function gapLabel(gapMs: number | null): string {
    if (gapMs === null) return "First in view"
    if (gapMs < 0) return "Overlapping"
    return gapMs === 0 ? "No gap" : `${duration(gapMs)} gap`
}
