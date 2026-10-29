import type { FocusEvent as ReactFocusEvent, MouseEvent as ReactMouseEvent } from "react"

import { InstanceTimeline, InstanceTimelineBar, InstanceTimelineRow, useTimelineHover } from "./InstanceTimeline.js"
import { formatDuration, metadataSummary, sessionDuration } from "./socket-sessions.js"
import type { SocketSession } from "./socket-sessions.js"

export { timelineTicks } from "./InstanceTimeline.js"

export interface SocketTimelineProps {
    sessions: SocketSession[]
    start: number
    end: number
    endLabel?: string
    selected?: SocketSession
    onSelect: (session: SocketSession, trigger: HTMLElement) => void
}

interface Lane {
    key: string
    actorName: string
    actorId: string
    rows: Placed[][]
}

interface Placed {
    session: SocketSession
    from: number
    to: number
    openEnded: boolean
    unknownStart: boolean
}

const maxLanes = 40

export function SocketTimeline({ sessions, start, end, endLabel = "now", selected, onSelect }: SocketTimelineProps) {
    const { hover, show, hide } = useTimelineHover<SocketSession>()
    const lanes = packLanes(sessions, start, end)
    const visible = lanes.slice(0, maxLanes)
    return (
        <InstanceTimeline
            start={start}
            end={end}
            endLabel={endLabel}
            label="Connection timeline"
            onMouseLeave={hide}
            footer={
                lanes.length > maxLanes && (
                    <p className="socket-timeline-more">
                        {(lanes.length - maxLanes).toLocaleString()} more {lanes.length - maxLanes === 1 ? "instance is" : "instances are"} listed in the table below.
                    </p>
                )
            }
            overlay={
                hover && (
                    <div role="tooltip" className={`socket-tooltip${hover.below ? " socket-tooltip-below" : ""}`} style={{ left: hover.left, top: hover.top }}>
                        <TooltipMetadata metadata={hover.item.metadata} />
                        <dl>
                            <dt>Connection</dt>
                            <dd>{shortId(hover.item.connectionId)}</dd>
                            <dt>Instance</dt>
                            <dd>
                                {hover.item.actorName} / {hover.item.actorId}
                            </dd>
                            <dt>Status</dt>
                            <dd>{statusLabel(hover.item.status)}</dd>
                            <dt>Opened</dt>
                            <dd>
                                {hover.item.openedAtMs === null
                                    ? "Before retained history"
                                    : `${hover.item.estimatedStart ? "≈ " : ""}${new Date(hover.item.openedAtMs).toLocaleTimeString([], { hour12: false })}`}
                            </dd>
                            <dt>Duration</dt>
                            <dd>{durationLabel(hover.item, end)}</dd>
                            <dt>Messages</dt>
                            <dd>{hover.item.messages.toLocaleString()}</dd>
                        </dl>
                    </div>
                )
            }
        >
            {scale =>
                visible.map(lane => (
                    <InstanceTimelineRow key={lane.key} actorName={lane.actorName} actorId={lane.actorId} tracks={lane.rows.length}>
                        {lane.rows.flatMap((row, index) =>
                            row.map(placed => (
                                <InstanceTimelineBar
                                    key={placed.session.connectionId}
                                    scale={scale}
                                    from={placed.from}
                                    to={placed.to}
                                    track={index}
                                    className={`socket-bar-${placed.session.status}${placed.openEnded ? " socket-bar-open-ended" : ""}${placed.unknownStart ? " socket-bar-unknown-start" : ""}`}
                                    data-state={selected === placed.session ? "selected" : undefined}
                                    aria-label={barLabel(placed.session, end)}
                                    onClick={event => onSelect(placed.session, event.currentTarget)}
                                    onMouseEnter={(event: ReactMouseEvent<HTMLElement>) => show(placed.session, event.currentTarget)}
                                    onFocus={(event: ReactFocusEvent<HTMLElement>) => show(placed.session, event.currentTarget)}
                                    onBlur={hide}
                                />
                            ))
                        )}
                    </InstanceTimelineRow>
                ))
            }
        </InstanceTimeline>
    )
}

const tooltipEntries = 4

// Metadata identifies the person behind a connection, so it leads the tooltip one entry per line.
function TooltipMetadata({ metadata }: { metadata: unknown }) {
    if (metadata === undefined || metadata === null) return null
    const entries: [string, unknown][] =
        typeof metadata === "object" ? (Array.isArray(metadata) ? metadata.map((value, index) => [String(index), value]) : Object.entries(metadata)) : [["value", metadata]]
    if (!entries.length) return null
    return (
        <dl className="socket-tooltip-metadata" aria-label="Connection metadata">
            {entries.slice(0, tooltipEntries).map(([key, value]) => (
                <div key={key}>
                    <dt>{key}</dt>
                    <dd>{typeof value === "string" ? value : JSON.stringify(value)}</dd>
                </div>
            ))}
            {entries.length > tooltipEntries && <p>+{entries.length - tooltipEntries} more in details</p>}
        </dl>
    )
}

export function packLanes(sessions: SocketSession[], start: number, end: number): Lane[] {
    const lanes = new Map<string, Lane>()
    const placed = sessions
        .map(session => place(session, start, end))
        .filter(item => item.to > start)
        .sort((a, b) => a.from - b.from || a.to - b.to)
    for (const item of placed) {
        const key = `${item.session.actorName}\u0000${item.session.actorId}`
        const lane = lanes.get(key) ?? { key, actorName: item.session.actorName, actorId: item.session.actorId, rows: [] }
        lanes.set(key, lane)
        const row = lane.rows.find(row => row.at(-1)!.to + minimumGap(start, end) <= item.from)
        if (row) row.push(item)
        else lane.rows.push([item])
    }
    return [...lanes.values()].sort((a, b) => latest(b) - latest(a))
}

function place(session: SocketSession, start: number, end: number): Placed {
    const from = session.openedAtMs ?? (session.status === "open" ? start : (session.lastSeenMs ?? start))
    const to = session.status === "closed" ? session.closedAtMs! : session.status === "open" ? end : (session.lastSeenMs ?? from)
    return {
        session,
        from: Math.max(start, from),
        to: Math.max(Math.max(start, from) + minimumGap(start, end), Math.min(end, to)),
        openEnded: session.status !== "closed",
        unknownStart: session.openedAtMs === null
    }
}

function minimumGap(start: number, end: number) {
    return Math.max(1, (end - start) / 160)
}

function latest(lane: Lane) {
    return Math.max(...lane.rows.flat().map(item => item.to))
}

export function statusLabel(status: SocketSession["status"]) {
    return status === "open" ? "Open" : status === "closed" ? "Closed" : status === "unknown" ? "Unknown" : "Lost"
}

export function durationLabel(session: SocketSession, now: number) {
    const duration = sessionDuration(session, now)
    if (!duration) return "Unknown"
    return `${duration.lowerBound ? "≥ " : ""}${formatDuration(duration.ms)}`
}

export function shortId(id: string) {
    return id.length > 14 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id
}

function barLabel(session: SocketSession, now: number) {
    const summary = metadataSummary(session.metadata)
    return `${statusLabel(session.status)} connection ${shortId(session.connectionId)}${summary === null ? "" : ` (${summary})`} on ${session.actorName} ${session.actorId}, ${durationLabel(session, now)}`
}
