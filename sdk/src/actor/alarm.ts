/** One durable scheduled callback for an actor. */
interface ActorAlarm {
    /** Returns the scheduled Unix timestamp in milliseconds, or `null` when unset. */
    get(): number | null
    /** Replaces the alarm with a Unix timestamp in milliseconds. Past timestamps are immediately due. */
    set(timestampMs: number): void
    /** Clears the scheduled alarm. */
    delete(): void
}

interface ActorAlarmState {
    readonly timestampMs: number | null
}

interface ActorAlarmStorage {
    getAlarmState(): ActorAlarmState
    setAlarm(timestampMs: number): void
    deleteAlarm(): void
}

export type { ActorAlarm, ActorAlarmState, ActorAlarmStorage }
