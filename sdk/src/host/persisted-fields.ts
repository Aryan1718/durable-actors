import { assertActorDatabaseAccess } from "../actor/database.js"
import { Persistence } from "../actor/schema.js"
import type { ActorSchema } from "../actor/schema.js"
import { cloneJsonObject } from "../json.js"

import type { ActorDatabaseStorage } from "./sqlite.js"

const bindings = new WeakMap<object, Map<string, PropertyDescriptor>>()

function bindPersistedFields(instance: object, schema: ActorSchema, database: ActorDatabaseStorage): void {
    const fields = new Map<string, PropertyDescriptor>()
    for (const field of schema.fields) {
        if (field.persistence !== Persistence.Persisted || !Object.hasOwn(instance, field.name)) continue
        let value: unknown = Reflect.get(instance, field.name)
        const descriptor: PropertyDescriptor = {
            configurable: true,
            enumerable: true,
            get: () => value,
            set: next => {
                assertActorDatabaseAccess(instance)
                const state = cloneJsonObject({ [field.name]: next }, "actor state")
                database.persistField(field.name, state[field.name])
                value = next
            }
        }
        Object.defineProperty(instance, field.name, descriptor)
        fields.set(field.name, descriptor)
    }
    bindings.set(instance, fields)
}

function isPersistedField(instance: object, name: string, descriptor: PropertyDescriptor): boolean {
    const bound = bindings.get(instance)?.get(name)
    return bound !== undefined && bound.get === descriptor.get && bound.set === descriptor.set
}

export { bindPersistedFields, isPersistedField }
