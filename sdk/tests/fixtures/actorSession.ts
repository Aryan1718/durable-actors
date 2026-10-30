import { Actor, Persisted } from "durable-actors"

class SessionCounter extends Actor {
    @Persisted private count = 0

    async increment(amount = 1): Promise<number> {
        this.count += amount
        return this.count
    }

    async incrementAtomic(): Promise<number> {
        return this.db.transaction(async () => {
            return this.incrementTwice()
        })
    }

    async incrementTwice(): Promise<number> {
        this.count++
        this.count++
        return this.count
    }

    async failAfterWrite(): Promise<void> {
        this.count++
        throw new Error("failed after write")
    }

    async failAtomic(): Promise<void> {
        return this.db.transaction(async () => {
            return this.failAfterWrite()
        })
    }

    async read(): Promise<number> {
        return this.count
    }

    async stream(): Promise<number> {
        this.broadcast({ delta: "first" })
        this.broadcast({ delta: "last" })
        return this.count
    }

    async announceThenSpin(): Promise<never> {
        await SessionCounter.get("worker-start-observer").increment(0)
        return this.spinForever()
    }

    async spinForever(): Promise<never> {
        // Deliberately uncooperative code used to verify hard Worker termination.
        while (true) {
            // Keep the loop opaque to optimizers without yielding the Worker event loop.
            void performance.now()
        }
    }
}

export { SessionCounter }
