import { Actor, Atomic, Interleave, Persisted } from "durable-actors"

export class PersistenceTests extends Actor {
    @Persisted private crashMarker = ""
    @Persisted private events: string[] = []

    async testImmediate() {
        this.crashMarker = ""
    }

    async writeThenCrash(marker: string): Promise<void> {
        this.crashMarker = marker
        process.exit(1)
    }

    @Atomic
    async atomicWriteThenCrash(marker: string): Promise<void> {
        this.crashMarker = marker
        process.exit(1)
    }

    async getCrashMarker(): Promise<string> {
        return this.crashMarker
    }

    @Interleave
    async testInterleave(label: string, delayMs: number): Promise<void> {
        this.events = [...this.events, `${label}:start`]
        await new Promise(resolve => setTimeout(resolve, delayMs))
        this.events = [...this.events, `${label}:end`]
    }

    async getEvents(): Promise<string[]> {
        return this.events
    }

    @Atomic
    async testAtomic(shouldFail: boolean) {
        this.crashMarker = ""

        if (shouldFail) throw new Error("Testing atomic rollback")
    }
}
