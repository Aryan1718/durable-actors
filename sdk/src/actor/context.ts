import { ActorDefinitionError, ActorValidationError } from "../errors.js"

import type { ActorAlarm, ActorAlarmStorage } from "./alarm.js"
import type { ActorDatabase } from "./database.js"
import { isActorInvocation } from "./database.js"
import { validateActorComponent } from "./identity.js"

declare const actorContextBrand: unique symbol

interface ActorContext {
    readonly [actorContextBrand]: never
}

class RuntimeActorContext implements ActorContext {
    declare readonly [actorContextBrand]: never
    readonly actorId: string
    readonly persistedFields = new Map<string, PropertyDescriptor>()
    readonly #database: ActorDatabase | undefined
    readonly #alarm: ActorAlarm | undefined
    #actor: object | undefined
    #constructing = false

    private constructor(actorId: string, database?: ActorDatabase & ActorAlarmStorage) {
        this.actorId = validateActorComponent("actor ID", actorId)
        this.#database = database
        this.#alarm = database && alarm(database)
    }

    static runtime(actorId: string, database: ActorDatabase & ActorAlarmStorage): RuntimeActorContext {
        return new RuntimeActorContext(actorId, database)
    }

    static reference(actorId: string): RuntimeActorContext {
        return new RuntimeActorContext(actorId)
    }

    static bind(context: unknown, actor: object): RuntimeActorContext {
        if (typeof context !== "object" || context === null || !(#actor in context) || context.#actor !== undefined)
            throw new ActorDefinitionError("actors are constructed by the actor runtime")
        context.#actor = actor
        return context
    }

    construct(actorClass: Function): object {
        this.#constructing = true
        try {
            const actor = Reflect.construct(actorClass, [this])
            if (this.#actor !== actor)
                throw new ActorDefinitionError("actor constructors must call super with their ActorContext")
            return actor
        } finally {
            this.#constructing = false
        }
    }

    get database(): ActorDatabase {
        if (this.#database === undefined)
            throw new ActorDefinitionError("actor database is unavailable outside the actor runtime")
        return this.#database
    }

    get alarm(): ActorAlarm {
        if (this.#alarm === undefined)
            throw new ActorDefinitionError("actor alarm is unavailable outside the actor runtime")
        return this.#alarm
    }

    assertAccess(): void {
        if (this.#constructing || (this.#actor !== undefined && isActorInvocation(this.#actor))) return
        throw new ActorDefinitionError("persisted actor fields cannot change outside its invocation")
    }
}

function alarm(storage: ActorAlarmStorage): ActorAlarm {
    return {
        get: () => storage.getAlarmState().timestampMs,
        set: timestampMs => {
            if (!Number.isSafeInteger(timestampMs))
                throw new ActorValidationError("actor alarm timestamp must be a safe integer")
            storage.setAlarm(timestampMs)
        },
        delete: () => storage.deleteAlarm()
    }
}

export { RuntimeActorContext }
export type { ActorContext }
