import { InstanceTimeline, InstanceTimelineBar, InstanceTimelineRow, useTimelineHover } from "./InstanceTimeline.js"
import type { ActorInstance } from "./client.js"
import type { ResidencyPeriod } from "./residency.js"
import { formatDuration } from "./socket-sessions.js"

export interface ResidencyTimelineProps {
    instances: ActorInstance[]
    periods: ResidencyPeriod[]
    start: number
    end: number
    endLabel?: string
    onSelect: (actorId: string) => void
}

export function ResidencyTimeline({ instances, periods, start, end, endLabel = "now", onSelect }: ResidencyTimelineProps) {
    const { hover, show, hide } = useTimelineHover<ResidencyPeriod>()
    const byInstance = new Map<string, ResidencyPeriod[]>()
    for (const period of periods) {
        if (period.toMs <= start || period.fromMs >= end) continue
        const row = byInstance.get(period.actorId) ?? []
        row.push(period)
        byInstance.set(period.actorId, row)
    }
    return (
        <div className="overview-panel socket-timeline-panel residency-history">
            <div className="socket-panel-heading">
                <h3>Residency over time</h3>
                <ul className="socket-legend" aria-label="Residency legend">
                    <li>
                        <span className="socket-swatch residency-swatch-live" />
                        In memory
                    </li>
                    <li>
                        <span className="socket-swatch residency-swatch-dormant" />
                        Dormant
                    </li>
                    <li>
                        <span className="socket-swatch residency-swatch-unknown" />
                        Unknown
                    </li>
                </ul>
            </div>
            <InstanceTimeline
                start={start}
                end={end}
                endLabel={endLabel}
                label="Instance residency timeline"
                onMouseLeave={hide}
                overlay={
                    hover && (
                        <div role="tooltip" className={`socket-tooltip${hover.below ? " socket-tooltip-below" : ""}`} style={{ left: hover.left, top: hover.top }}>
                            {hover.item.actorId} · {periodLabel(hover.item)}
                        </div>
                    )
                }
            >
                {scale =>
                    instances.map(instance => (
                        <InstanceTimelineRow key={instance.actorId} actorId={instance.actorId}>
                            {byInstance.get(instance.actorId)?.length ? (
                                byInstance
                                    .get(instance.actorId)!
                                    .map(period => (
                                        <InstanceTimelineBar
                                            key={`${period.fromMs}-${period.status}`}
                                            scale={scale}
                                            from={period.fromMs}
                                            to={period.toMs}
                                            className={`residency-swatch-${period.status}`}
                                            aria-label={`${instance.actorId}: ${periodLabel(period)}`}
                                            onClick={() => onSelect(instance.actorId)}
                                            onMouseEnter={event => show(period, event.currentTarget)}
                                            onMouseLeave={hide}
                                            onFocus={event => show(period, event.currentTarget)}
                                            onBlur={hide}
                                        />
                                    ))
                            ) : (
                                <span className="residency-no-history">No retained residency</span>
                            )}
                        </InstanceTimelineRow>
                    ))
                }
            </InstanceTimeline>
        </div>
    )
}

function periodLabel(period: ResidencyPeriod): string {
    const status = period.status === "live" ? "In memory" : period.status === "dormant" ? "Dormant" : "Unknown"
    return `${status} · ${new Date(period.fromMs).toLocaleString()} – ${new Date(period.toMs).toLocaleString()} · ${formatDuration(period.toMs - period.fromMs)}`
}
