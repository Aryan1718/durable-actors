import { actors } from "../generated/index.js"

type Gym = ReturnType<typeof actors.BackgroundGym.get>
type GymStatus = Awaited<ReturnType<Gym["status"]>>

const scenarios: Record<string, (gym: Gym) => Promise<boolean>> = {
    // Background steps commit after start() returns.
    async short(gym) {
        await gym.start(3, 1_000)
        const status = await watch(gym, 30_000)
        return check(status, status.jobs[0].done === 3 && status.rejections === 0)
    },
    // Writes survive idle eviction; the status call reactivates the actor.
    async persist(gym) {
        await gym.start(2, 500)
        console.log("waiting 25s without calls so the actor is evicted")
        await sleep(25_000)
        const status = await gym.status()
        return check(status, status.jobs[0].done === 2 && status.activations === 2)
    },
    // Background SQL is rejected while @Atomic holds the transaction, then retries.
    async atomic(gym) {
        await gym.start(30, 100)
        await sleep(500)
        await gym.holdAtomic(2_000)
        const status = await watch(gym, 30_000)
        return check(status, status.jobs[0].done === 30 && status.rejections > 0)
    },
    // Each step outlasts the idle timeout. Fails until background work keeps the actor alive.
    async eviction(gym) {
        await gym.start(3, 15_000)
        console.log("waiting 60s without calls")
        await sleep(60_000)
        const status = await gym.status()
        return check(status, status.jobs[0].done === 3 && status.activations === 1)
    }
}

const [command, ...args] = process.argv.slice(2)
if (command === "start" && args.length === 3) {
    console.log(await actors.BackgroundGym.get(args[0]).start(Number(args[1]), Number(args[2])))
} else if (command === "status" && args.length === 1) {
    console.log(await actors.BackgroundGym.get(args[0]).status())
} else if (command === "hold" && args.length === 2) {
    await actors.BackgroundGym.get(args[0]).holdAtomic(Number(args[1]))
} else if (command === "all" && args.length === 0) {
    const results = []
    for (const name of Object.keys(scenarios)) results.push(await run(name))
    process.exit(results.every(Boolean) ? 0 : 1)
} else if (command !== undefined && command in scenarios && args.length === 0) {
    process.exit((await run(command)) ? 0 : 1)
} else {
    console.error(`Usage: bun run start <${Object.keys(scenarios).join(" | ")} | all> | start <id> <steps> <stepMs> | status <id> | hold <id> <ms>`)
    process.exit(1)
}

async function run(name: string): Promise<boolean> {
    const id = `${name}-${Date.now()}`
    console.log(`\n${name} (actor ${id})`)
    const passed = await scenarios[name](actors.BackgroundGym.get(id))
    console.log(passed ? "PASS" : "FAIL")
    return passed
}

async function watch(gym: Gym, timeoutMs: number): Promise<GymStatus> {
    const deadline = Date.now() + timeoutMs
    for (;;) {
        const status = await gym.status()
        if (status.jobs.every(job => job.done >= job.steps) || Date.now() > deadline) return status
        await sleep(1_000)
    }
}

function check(status: GymStatus, passed: boolean): boolean {
    console.log(status)
    return passed
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
}
