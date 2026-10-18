import type { JSONSchema7 } from "json-schema"
import ts from "typescript"

import { Persistence } from "../actor/schema.js"
import type { ActorSchema } from "../actor/schema.js"
import type { SocketContract } from "../wire/contract.js"

import { assertJsonType, jsonSchema } from "./json-schema.js"

function socketContract(
    checker: ts.TypeChecker,
    actor: ts.ClassDeclaration,
    schema: ActorSchema,
    unchecked: ts.Symbol | undefined
): SocketContract {
    const instance = checker.getTypeAtLocation(actor)
    const base = instance.getBaseTypes()![0] as ts.TypeReference
    const [metadata, incoming, outgoing] = checker.getTypeArguments(base)
    const types: Record<string, ts.Type> = { Metadata: metadata!, Incoming: incoming!, Outgoing: outgoing! }
    const optional = new Set<string>()
    const state: JSONSchema7 = { type: "object", properties: Object.create(null), required: [] }
    for (const field of schema.fields.filter(
        field => field.persistence === Persistence.Persisted && !field.private && !field.visibility
    )) {
        const property = instance.getProperty(field.name)!
        const name = `Field_${field.name}`
        types[name] = checker.getTypeOfSymbolAtLocation(property, actor)
        state.properties![field.name] = { $ref: `#/definitions/${pointer(name)}` }
        if (property.flags & ts.SymbolFlags.Optional) optional.add(name)
        else state.required!.push(field.name)
    }
    for (const [name, type] of Object.entries(types))
        assertJsonType(checker, unchecked, type, `${schema.actorName}.${name}`, optional.has(name))
    const generated = jsonSchema(checker, unchecked, types)
    const definitions = { ...generated.definitions, State: state }
    return {
        version: 1,
        actorName: schema.actorName,
        schema: { $schema: "http://json-schema.org/draft-07/schema#", definitions },
        emittable: schema.fields.filter(field => field.emittable).map(field => field.name)
    }
}

function pointer(value: string): string {
    return value.replaceAll("~", "~0").replaceAll("/", "~1")
}

export { socketContract }
