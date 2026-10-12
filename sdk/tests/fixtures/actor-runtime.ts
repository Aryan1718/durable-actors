import { ActorRuntime as Runtime } from "../../src/host/actor-runtime.js"
import { SqliteActorDatabase as Database } from "../../src/host/sqlite.js"
import type { SqliteState } from "../../src/host/sqlite.js"

import { commit, commitSync } from "./litestream.js"

export type { SqliteState } from "../../src/host/sqlite.js"
export { SqliteCaptureError } from "../../src/host/sqlite.js"
export { hydrateActorState, snapshotActorState } from "../../src/host/actor-runtime.js"

export class SqliteActorDatabase extends Database {
    private state: SqliteState | undefined

    constructor(connect?: ConstructorParameters<typeof Database>[2]) {
        super(
            () => commit(this.state!),
            () => commitSync(this.state!),
            connect
        )
    }

    override restore(state: SqliteState | undefined): void {
        super.restore(state)
        this.state = state
    }
}

type RuntimeArguments = ConstructorParameters<typeof Runtime>
export class ActorRuntime extends Runtime {
    constructor(
        definition: RuntimeArguments[0],
        allowNextInvocation: RuntimeArguments[1],
        publish?: RuntimeArguments[3],
        connections?: RuntimeArguments[4],
        database: RuntimeArguments[2] = new SqliteActorDatabase()
    ) {
        super(definition, allowNextInvocation, database, publish, connections)
    }
}
