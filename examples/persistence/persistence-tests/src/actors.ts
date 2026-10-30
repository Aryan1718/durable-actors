import { Actor, Interleave, Persisted } from "durable-actors"

export class PersistenceTests extends Actor {
    @Persisted private crashMarker = ""

    async testImmediate() {
        this.crashMarker = ""
    }

    async writeThenCrash(marker: string): Promise<void> {
        this.crashMarker = marker
        process.exit(1)
    }

    async atomicWriteThenCrash(marker: string): Promise<void> {
        return this.db.transaction(async () => {
            this.crashMarker = marker
            process.exit(1)
        })
    }

    async getCrashMarker(): Promise<string> {
        return this.crashMarker
    }

    async testAtomic(shouldFail: boolean) {
        return this.db.transaction(async () => {
            this.crashMarker = ""

            if (shouldFail) throw new Error("Testing atomic rollback")
        })
    }
}

export class InterleaveTests extends Actor {
    @Persisted private events: string[] = []

    @Interleave
    async testInterleave(label: string, delayMs: number): Promise<void> {
        this.events = [...this.events, `${label}:start`]
        await new Promise(resolve => setTimeout(resolve, delayMs))
        this.events = [...this.events, `${label}:end`]
    }

    async getEvents(): Promise<string[]> {
        return this.events
    }

    async crash(): Promise<void> {
        process.exit(1)
    }
}
