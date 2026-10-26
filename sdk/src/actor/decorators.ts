import { ActorDefinitionError } from "../errors.js"

import { ActorResource } from "./resource.js"

/** Saves all of the method's writes together when it succeeds, or none if it throws. */
function Atomic(_value: Function, context: ClassMethodDecoratorContext): void {
    if (context.kind !== "method" || context.static || context.private || typeof context.name !== "string")
        throw new ActorDefinitionError("@Atomic requires a public instance async method")
}

/**
 * Lets other calls run during awaits. State may change while waiting.
 * Disables error rollback for the whole actor class.
 * @experimental
 */
function Interleave(_value: Function, context: ClassMethodDecoratorContext): void {
    if (context.kind !== "method" || context.static || context.private || typeof context.name !== "string")
        throw new ActorDefinitionError("@Interleave requires a public instance async method")
}

/** Saves each assignment; reassign objects and arrays to save changes. `@Atomic` methods save on success. */
function Persisted(_value: undefined, context: ClassFieldDecoratorContext): void {
    validateField("Persisted", context)
    if (context.private) throw new ActorDefinitionError("@Persisted cannot decorate a JavaScript private field")
}

/** Marks a temporary field that may reset between calls. */
function Ephemeral(_value: undefined, context: ClassFieldDecoratorContext): void {
    validateField("Ephemeral", context)
}

/** Sends browser state updates for a public `@Persisted` field. */
function Emittable(_value: undefined, context: ClassFieldDecoratorContext): void {
    validateField("Emittable", context)
    if (context.private) throw new ActorDefinitionError("@Emittable requires a public field")
}

/** Marks a property as being a Resource. Includes them in Actor Lifecycle */
function Resource<This, Value extends ActorResource>(
    _value: undefined,
    context: ClassFieldDecoratorContext<This, Value>
): void {
    validateField("Resource", context)
}

function validateField(name: string, context: ClassFieldDecoratorContext): void {
    if (context.kind !== "field" || context.static || typeof context.name !== "string")
        throw new ActorDefinitionError(`@${name} requires an instance field with a string name`)
}

export { Atomic, Emittable, Ephemeral, Interleave, Persisted, Resource }
