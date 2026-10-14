import type { ActorExecutorReply } from "../../src/host/protocol.js"
import type { DatabaseRequester } from "../../src/host/types.js"
import type { ActorWorker, ActorWorkerSupervisor } from "../../src/host/worker-supervisor.js"
import type { JsonObject } from "../../src/json.js"

import { HostDatabase } from "./host-database.js"

type Arguments = Parameters<ActorWorkerSupervisor["handle"]>
const databases = new WeakMap<object, HostDatabase>()

export function handle(
    host: ActorWorkerSupervisor,
    command: Arguments[0],
    allowNext: Arguments[1],
    publish?: Arguments[3],
    connections?: Arguments[4]
): Promise<ActorExecutorReply> {
    return host.handle(command, allowNext, requester(host), publish, connections)
}

export function execute(
    worker: ActorWorker,
    command: Parameters<ActorWorker["execute"]>[0],
    allowNext: Arguments[1],
    publish?: Arguments[3],
    connections?: Arguments[4]
): Promise<ActorExecutorReply> {
    return worker.execute(command, allowNext, requester(worker), publish, connections)
}

export function hostDatabase(host: object): HostDatabase {
    let database = databases.get(host)
    if (database === undefined) databases.set(host, (database = new HostDatabase()))
    return database
}

export function seed(host: object, fields: JsonObject): void {
    const database = hostDatabase(host)
    database.call({ op: "replace_fields", fields })
    database.call({ op: "commit" })
}

function requester(host: object): DatabaseRequester {
    const database = hostDatabase(host)
    return async call => database.call(call)
}
