import type { ActorDatabase } from "./database.js"

export interface ActorResourceContext {
    db: ActorDatabase
}

export interface ActorResource {
    onResume(): Promise<void>
    onGracefulShutdown(): Promise<void>
}
