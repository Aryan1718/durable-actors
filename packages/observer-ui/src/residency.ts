import type { ActorInstance, ActorResidency } from "./client.js"
import type { ResolvedRange } from "./time-range.js"

export interface ResidencyQuery extends ResolvedRange {
    actorName: string
}

export interface ResidencyPeriod {
    actorId: string
    status: ActorResidency
    fromMs: number
    toMs: number
}

export interface ResidencyHistory {
    periods: ResidencyPeriod[]
    truncated: boolean
}

export function recentInstances(instances: ActorInstance[]): ActorInstance[] {
    return [...instances].sort((a, b) => (b.lastActiveAtMs ?? -1) - (a.lastActiveAtMs ?? -1) || a.actorId.localeCompare(b.actorId))
}

export function parseResidencyHistory(value: unknown): ResidencyHistory {
    if (!value || typeof value !== "object" || !("truncated" in value) || typeof value.truncated !== "boolean" || !("periods" in value) || !Array.isArray(value.periods)) {
        throw new Error("Invalid residency history response.")
    }
    for (const period of value.periods) {
        if (
            !period ||
            typeof period.actorId !== "string" ||
            !["live", "dormant", "unknown"].includes(period.status) ||
            !Number.isSafeInteger(period.fromMs) ||
            !Number.isSafeInteger(period.toMs) ||
            period.fromMs < 0 ||
            period.fromMs >= period.toMs
        )
            throw new Error("Invalid residency period.")
    }
    return value as ResidencyHistory
}

export function activityLabel(time: number | null | undefined, now: number): string {
    if (time == null) return "Unknown"
    const seconds = Math.max(0, Math.floor((now - time) / 1000))
    if (seconds < 5) return "Just now"
    if (seconds < 60) return `${seconds}s ago`
    const minutes = Math.floor(seconds / 60)
    if (minutes < 60) return `${minutes}m ago`
    const hours = Math.floor(minutes / 60)
    return hours < 24 ? `${hours}h ago` : `${Math.floor(hours / 24)}d ago`
}
