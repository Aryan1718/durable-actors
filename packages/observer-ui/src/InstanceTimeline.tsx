import { useState } from "react"
import type { ComponentProps, ReactNode } from "react"

const laneHeight = 18
const lanePadding = 8
const barHeight = 12

interface InstanceTimelineProps {
    start: number
    end: number
    endLabel?: string
    label: string
    children: (scale: TimelineScale) => ReactNode
    footer?: ReactNode
    overlay?: ReactNode
    onMouseLeave?: () => void
}

export function InstanceTimeline({ start, end, endLabel = "now", label, children, footer, overlay, onMouseLeave }: InstanceTimelineProps) {
    const scale = timelineScale(start, end)
    const ticks = timelineTicks(start, end)
    const step = tickStep(scale.span)
    return (
        <div className="socket-timeline" role="group" aria-label={label} onMouseLeave={onMouseLeave}>
            <div className="socket-timeline-axis" aria-hidden="true">
                <span>Instance</span>
                <div>
                    {ticks.map(tick => (
                        <span key={tick} style={{ left: scale.position(tick) }}>
                            {tickLabel(tick, step)}
                        </span>
                    ))}
                    <span className="socket-timeline-now">{endLabel}</span>
                </div>
            </div>
            <div className="socket-timeline-body">
                <div className="socket-timeline-grid" aria-hidden="true">
                    {ticks.map(tick => (
                        <i key={tick} style={{ left: scale.position(tick) }} />
                    ))}
                </div>
                {children(scale)}
                {footer}
            </div>
            {overlay}
        </div>
    )
}

export function InstanceTimelineRow({ actorName, actorId, tracks = 1, children }: { actorName?: string; actorId: string; tracks?: number; children: ReactNode }) {
    return (
        <div className="socket-timeline-row">
            <div className="socket-timeline-label" title={actorName ? `${actorName} / ${actorId}` : actorId}>
                {actorName && <span>{actorName}</span>}
                <strong>{actorId}</strong>
            </div>
            <div className="socket-timeline-lanes" style={{ height: tracks * laneHeight + lanePadding * 2 }}>
                {children}
            </div>
        </div>
    )
}

export function InstanceTimelineBar({ scale, from, to, track = 0, className = "", style, ...props }: ComponentProps<"button"> & { scale: TimelineScale; from: number; to: number; track?: number }) {
    return (
        <button
            {...props}
            type="button"
            className={`socket-bar ${className}`}
            style={{ ...scale.interval(from, to), top: lanePadding + track * laneHeight + (laneHeight - barHeight) / 2, ...style }}
        />
    )
}

export function useTimelineHover<T>() {
    const [hover, setHover] = useState<{ item: T; left: number; top: number; below: boolean }>()
    const show = (item: T, target: HTMLElement) => {
        const bar = target.getBoundingClientRect()
        const frame = target.closest(".socket-timeline")!.getBoundingClientRect()
        const below = bar.top - frame.top < 150
        setHover({
            item,
            left: Math.min(Math.max(bar.left - frame.left + bar.width / 2, 120), frame.width - 120),
            top: below ? bar.bottom - frame.top : bar.top - frame.top,
            below
        })
    }
    return { hover, show, hide: () => setHover(undefined) }
}

type TimelineScale = ReturnType<typeof timelineScale>

function timelineScale(start: number, end: number) {
    const span = Math.max(1, end - start)
    const position = (time: number) => `${((Math.min(Math.max(time, start), end) - start) / span) * 100}%`
    const interval = (from: number, to: number) => ({ left: position(from), width: `${(Math.max(0, Math.min(to, end) - Math.max(from, start)) / span) * 100}%` })
    return { span, position, interval }
}

const minute = 60_000
const hour = 60 * minute
const day = 24 * hour
const tickSteps = [
    1_000,
    2_000,
    5_000,
    10_000,
    15_000,
    30_000,
    minute,
    2 * minute,
    5 * minute,
    10 * minute,
    15 * minute,
    30 * minute,
    hour,
    2 * hour,
    3 * hour,
    6 * hour,
    12 * hour,
    day,
    2 * day,
    7 * day
]

export function timelineTicks(start: number, end: number): number[] {
    const step = tickStep(Math.max(1, end - start))
    const ticks: number[] = []
    for (let tick = Math.floor(start / step) * step + step; tick < end - step * 0.35; tick += step) ticks.push(tick)
    return ticks
}

function tickStep(span: number) {
    return tickSteps.find(step => span / step <= 9) ?? tickSteps.at(-1)!
}

function tickLabel(tick: number, step: number) {
    const date = new Date(tick)
    if (step >= day) return date.toLocaleDateString([], { month: "short", day: "numeric" })
    if (step >= minute) return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false })
    return date.toLocaleTimeString([], { hour12: false })
}
