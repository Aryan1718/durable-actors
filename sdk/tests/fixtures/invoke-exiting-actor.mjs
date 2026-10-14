import { pathToFileURL } from "node:url"

import { ActorWorkerSupervisor } from "../../dist/host/worker-supervisor.js"

import { HostDatabase } from "./host-database.ts"

const supervisor = new ActorWorkerSupervisor({ actorEntrypointUrl: pathToFileURL(process.argv[2]).href })
try {
    const database = new HostDatabase()
    const reply = await supervisor.handle(
        {
            type: "invoke",
            request_id: "exit",
            actor: { project_id: "local", actor_name: "Exiting", actor_id: "one" },
            method: "stop",
            args: []
        },
        () => {},
        async call => database.call(call)
    )
    console.log(JSON.stringify(reply))
} finally {
    supervisor.close()
}
