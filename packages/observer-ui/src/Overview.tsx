import { useEffect, useState } from "react"

import { RefreshCw } from "lucide-react"

import { ConnectionInventoryNotice } from "./ConnectionInventoryNotice.js"
import { TimeRangePicker } from "./TimeRangePicker.js"
import type { ActorInventory, ObserverClient, RequestTracePage } from "./client.js"
import { Button } from "./components/ui/button.js"
import { useInventory, usePolledQuery, useRequests } from "./observer-hooks.js"
import { inventorySummary, tracesInRange } from "./overview-data.js"
import { liveOverviewMetrics } from "./overview-metrics.js"
import type { OverviewMetrics } from "./overview-metrics.js"
import { defaultTimeRange, rangePhrase, resolveRange } from "./time-range.js"
import type { TimeRange } from "./time-range.js"

export interface OverviewProps {
    client: ObserverClient
    onSelectActor: (actorName: string) => void
    timeRange?: TimeRange
    onTimeRangeChange?: (range: TimeRange) => void
}

export function Overview({ client, onSelectActor, timeRange, onTimeRangeChange }: OverviewProps) {
    const actors = useInventory(client)
    const [localRange, setLocalRange] = useState<TimeRange>(defaultTimeRange)
    const range = timeRange ?? localRange
    const setRange = onTimeRangeChange ?? setLocalRange
    const [now, setNow] = useState(Date.now)
    useEffect(() => {
        const timer = setInterval(() => setNow(Date.now()), 10_000)
        return () => clearInterval(timer)
    }, [])
    const resolved = resolveRange(range, now)
    const saved = usePolledQuery(client, resolved, client.getMetrics)
    const requests = useRequests(client, !saved.supported)
    const live = requests.page ? liveOverviewMetrics(tracesInRange(requests.page.records, resolved, Math.max(now, Date.now()))) : undefined
    const metrics: OverviewMetrics | undefined = saved.supported ? saved.value : live
    const failed = saved.supported ? saved.failed : requests.failed
    return (
        <section className="la-observer overview" aria-label="Runtime overview">
            <div className="overview-heading">
                <h1>Durable Actors</h1>
                <div className="overview-controls">
                    <span className="overview-updated">
                        {actors.failed ? (
                            "Inventory reconnecting…"
                        ) : actors.updatedAt ? (
                            <>
                                Last inventory update <time dateTime={new Date(actors.updatedAt).toISOString()}>{new Date(actors.updatedAt).toLocaleTimeString([], { hour12: false })}</time>
                            </>
                        ) : (
                            "Connecting…"
                        )}
                    </span>
                    <TimeRangePicker value={range} onChange={setRange} className="overview-range" />
                    <Button
                        variant="outline"
                        size="icon"
                        aria-label="Refresh overview"
                        onClick={() => {
                            actors.retry()
                            saved.retry()
                            requests.retry()
                        }}
                    >
                        <RefreshCw aria-hidden="true" />
                    </Button>
                </div>
            </div>
            {actors.failed && (
                <p className="overview-alert" role="alert">
                    Inventory unavailable. {actors.inventory ? "Showing the last received counts; they may be out of date." : "Check your connection and access."} Retrying automatically.
                </p>
            )}
            {failed && (
                <p className="overview-alert" role="alert">
                    {saved.supported ? "Request history unavailable." : "Request stream unavailable."} {metrics ? "Showing the last received metrics." : "Request metrics are not available yet."}{" "}
                    Retrying automatically.
                </p>
            )}
            <SummaryCards inventory={actors.inventory} metrics={metrics} range={range} />
            <ConnectionInventoryNotice inventory={actors.inventory} />
            <ActorTypes inventory={actors.inventory} failed={actors.failed} onSelectActor={onSelectActor} />
            <TraceNotices page={saved.supported ? undefined : requests.page} />
        </section>
    )
}

function SummaryCards({ inventory, metrics, range }: { inventory?: ActorInventory; metrics?: OverviewMetrics; range: TimeRange }) {
    const totals = inventory ? inventorySummary(inventory) : undefined
    const total = totals ? totals.live + totals.dormant + totals.unknown : undefined
    const requests = metrics?.total
    return (
        <div className="overview-metrics">
            <section className="overview-metric">
                <h2>Actor instances</h2>
                <div className="overview-value">
                    <strong aria-label="Actor instances">{number(total)}</strong>
                    <span>{inventory ? `${inventory.actors.length} actor ${inventory.actors.length === 1 ? "class" : "classes"}` : "Waiting for inventory"}</span>
                </div>
                <div className="overview-residency" aria-hidden="true">
                    {totals &&
                        Object.entries(totals)
                            .filter(([key]) => key !== "connections")
                            .map(([key, value]) => <span key={key} className={`residency-${key}`} style={{ flexGrow: value }} />)}
                </div>
                <div className="overview-metric-foot">
                    <span>
                        <b>{number(totals?.live)}</b> live
                    </span>
                    <span>
                        <b>{number(totals?.dormant)}</b> dormant
                    </span>
                    {!!totals?.unknown && (
                        <span>
                            <b>{number(totals.unknown)}</b> unknown
                        </span>
                    )}
                </div>
            </section>
            <section className="overview-metric">
                <h2>Requests</h2>
                <div className="overview-value">
                    <strong aria-label="Retained requests">{number(requests?.count)}</strong>
                    <span>{rangePhrase(range)}</span>
                </div>
                <div className="overview-health">
                    <span>
                        <Health value={requests?.success} kind="success" /> success
                    </span>
                    <span>
                        <Health value={requests?.p95} kind="latency" /> p95 latency
                    </span>
                    <span>
                        <b>{milliseconds(requests?.queueP95)}</b> p95 queue wait
                    </span>
                </div>
            </section>
            <section className="overview-metric">
                <h2>WebSocket connections</h2>
                <div className="overview-value">
                    <strong aria-label="Open WebSocket connections">{number(totals?.connections)}</strong>
                    <span>open</span>
                </div>
                <div className="overview-health">
                    <span>Current inventory snapshot</span>
                    <span>Active sockets</span>
                </div>
            </section>
        </div>
    )
}

function ActorTypes({ inventory, failed, onSelectActor }: { inventory?: ActorInventory; failed: boolean; onSelectActor: OverviewProps["onSelectActor"] }) {
    return (
        <section aria-label="Actor types">
            <div className="overview-section-heading">
                <h2>Actor types</h2>
            </div>
            {inventory?.actors.length ? (
                <ul className="overview-actor-list" aria-label="Actor types">
                    {inventory.actors.map(actor => (
                        <li key={actor.actorName}>
                            <Button variant="ghost" type="button" aria-label={`Inspect ${actor.actorName}`} onClick={() => onSelectActor(actor.actorName)}>
                                {actor.actorName}
                            </Button>
                        </li>
                    ))}
                </ul>
            ) : (
                <p role="status">{!inventory ? (failed ? "Inventory unavailable" : "Loading actor types…") : "No actor types deployed yet."}</p>
            )}
        </section>
    )
}

function Health({ value, kind }: { value?: number | null; kind: "success" | "latency" }) {
    if (value == null) return <span title="No retained execution attempts">—</span>
    const level = kind === "success" ? (value < 95 ? "bad" : value < 99 ? "warn" : "good") : value > 100 ? "bad" : "good"
    return (
        <span className={`overview-signal signal-${level}`} title={level === "bad" ? "Critical threshold breached" : level === "warn" ? "Warning threshold breached" : "Within threshold"}>
            {level !== "good" && <span aria-hidden="true">{level === "warn" ? "!" : "×"}</span>}
            {kind === "success" ? `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}%` : milliseconds(value)}
        </span>
    )
}

function TraceNotices({ page }: { page?: RequestTracePage }) {
    if (!page?.dropped && !page?.persistenceFailed && !page?.evicted) return null
    return (
        <div className="overview-data-scope">
            {!!page?.dropped && <p role="alert">{number(page.dropped)} traces were not delivered. Request metrics are incomplete.</p>}
            {page?.persistenceFailed && <p role="alert">Some request events could not be saved. This history may be incomplete.</p>}
            {!!page?.evicted && <p>Earlier traces have expired; these metrics do not represent the full time window.</p>}
        </div>
    )
}

function number(value?: number) {
    return value === undefined ? "—" : value.toLocaleString()
}
function milliseconds(value?: number | null) {
    return value == null ? "—" : `${value.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`
}
