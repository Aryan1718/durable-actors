import { Actor, Atomic, Resource } from "durable-actors"
import type { ActorDatabase, ActorResource } from "durable-actors"

export interface GymJob {
    id: number
    steps: number
    stepMs: number
    done: number
}

export interface GymStatus {
    activations: number
    rejections: number
    lastError: string | null
    jobs: GymJob[]
}

export class BackgroundGym extends Actor {
    @Resource private readonly jobs = new JobRunner(this.db)

    // Returns before any step runs, so every step writes after this invocation ends.
    async start(steps: number, stepMs: number): Promise<number> {
        return this.jobs.start(steps, stepMs)
    }

    async status(): Promise<GymStatus> {
        return this.jobs.status()
    }

    @Atomic
    async holdAtomic(ms: number): Promise<void> {
        this.db.exec("INSERT INTO gym_holds (at) VALUES (?)", Date.now())
        await sleep(ms)
    }
}

// Behaves like Pi's harness: jobs live in SQLite and resume in the background after each activation.
class JobRunner implements ActorResource {
    private readonly running = new Set<number>()
    private stopped = false
    private rejections = 0
    private lastError: string | null = null

    constructor(private readonly db: ActorDatabase) {
        db.exec("CREATE TABLE IF NOT EXISTS gym_jobs (id INTEGER PRIMARY KEY, steps INTEGER NOT NULL, step_ms INTEGER NOT NULL, done INTEGER NOT NULL DEFAULT 0)")
        db.exec("CREATE TABLE IF NOT EXISTS gym_steps (job_id INTEGER NOT NULL, step INTEGER NOT NULL, at INTEGER NOT NULL)")
        db.exec("CREATE TABLE IF NOT EXISTS gym_activations (at INTEGER NOT NULL)")
        db.exec("CREATE TABLE IF NOT EXISTS gym_holds (at INTEGER NOT NULL)")
    }

    start(steps: number, stepMs: number): number {
        const [job] = this.db.exec<{ id: number }>("INSERT INTO gym_jobs (steps, step_ms) VALUES (?, ?) RETURNING id", steps, stepMs)
        this.run(job.id)
        return job.id
    }

    status(): GymStatus {
        const [{ activations }] = this.db.exec<{ activations: number }>("SELECT count(*) AS activations FROM gym_activations")
        return {
            activations,
            rejections: this.rejections,
            lastError: this.lastError,
            jobs: this.db.exec<GymJob>("SELECT id, steps, step_ms AS stepMs, done FROM gym_jobs ORDER BY id")
        }
    }

    async onResume(): Promise<void> {
        this.db.exec("INSERT INTO gym_activations (at) VALUES (?)", Date.now())
        for (const { id } of this.db.exec<{ id: number }>("SELECT id FROM gym_jobs WHERE done < steps")) this.run(id)
    }

    async onGracefulShutdown(): Promise<void> {
        this.stopped = true
    }

    private run(id: number): void {
        if (this.running.has(id)) return
        this.running.add(id)
        void this.steps(id).finally(() => this.running.delete(id))
    }

    private async steps(id: number): Promise<void> {
        while (!this.stopped) {
            try {
                const [job] = this.db.exec<GymJob>("SELECT steps, step_ms AS stepMs, done FROM gym_jobs WHERE id = ?", id)
                if (job === undefined || job.done >= job.steps) return
                await sleep(job.stepMs)
                if (this.stopped) return
                this.db.exec("INSERT INTO gym_steps (job_id, step, at) VALUES (?, ?, ?)", id, job.done + 1, Date.now())
                this.db.exec("UPDATE gym_jobs SET done = done + 1 WHERE id = ?", id)
            } catch (error) {
                // An unhandled rejection would crash the actor Worker and fail every running invocation.
                this.rejections++
                this.lastError = error instanceof Error ? error.message : String(error)
                await sleep(100)
            }
        }
    }
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
}
