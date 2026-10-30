import type { JSONSchema7, JSONSchema7Definition } from "json-schema"
import ts from "typescript"
import { JsonSchemaGenerator, getDefaultArgs } from "typescript-json-schema"

import { ActorDefinitionError } from "../errors.js"

function jsonSchema(
    checker: ts.TypeChecker,
    unchecked: ts.Symbol | undefined,
    types: Record<string, ts.Type>
): JSONSchema7 {
    const { allTypes, namesByType } = collectSchemaTypes(checker, unchecked, types)
    const generator = new JsonSchemaGenerator([], allTypes, {}, {}, checker, {
        ...getDefaultArgs(),
        required: true,
        strictNullChecks: true,
        rejectDateType: true,
        ref: true,
        aliasRef: false,
        topRef: false,
        defaultProps: false
    })
    const names = Object.keys(allTypes).filter(
        name => !(allTypes[name].flags & (ts.TypeFlags.Never | ts.TypeFlags.Undefined))
    )
    const generated = generator.getSchemaForSymbols(names) as JSONSchema7
    const definitions: Record<string, JSONSchema7Definition> = { ...generated.definitions }
    for (const [name, type] of Object.entries(allTypes)) {
        if (type.flags & ts.TypeFlags.Never) definitions[name] = false
        if (type.flags & ts.TypeFlags.Undefined) definitions[name] = { anyOf: [false] }
    }
    const seen = new Set<JSONSchema7>()
    for (const [name, type] of Object.entries(allTypes))
        preserveTypes(checker, unchecked, type, definitions[name], definitions, namesByType, seen)
    return { $schema: "http://json-schema.org/draft-07/schema#", definitions }
}

function collectSchemaTypes(checker: ts.TypeChecker, unchecked: ts.Symbol | undefined, roots: Record<string, ts.Type>) {
    const allTypes = { ...roots }
    const namesByType = new Map(Object.entries(roots).map(([name, type]) => [type, name]))
    const seen = new Set<ts.Type>()
    const register = (type: ts.Type) => {
        if (!namesByType.has(type)) {
            const name = `Type:${namesByType.size}`
            namesByType.set(type, name)
            allTypes[name] = type
        }
        visit(type)
    }
    const visit = (type: ts.Type) => {
        if (seen.has(type) || isUnchecked(type, unchecked)) return
        seen.add(type)
        if (type.isUnionOrIntersection()) {
            type.types.forEach(register)
            return
        }
        if (isArray(checker, type)) {
            checker.getTypeArguments(type as ts.TypeReference).forEach(visit)
            return
        }
        if (!(type.flags & ts.TypeFlags.Object)) return
        for (const property of type.getProperties()) {
            const declaration = property.valueDeclaration ?? property.declarations?.[0]
            if (declaration) visit(checker.getTypeOfSymbolAtLocation(property, declaration))
        }
        const indexType = checker.getIndexTypeOfType(type, ts.IndexKind.String)
        if (indexType) register(indexType)
    }
    Object.values(roots).forEach(visit)
    return { allTypes, namesByType }
}

function preserveTypes(
    checker: ts.TypeChecker,
    unchecked: ts.Symbol | undefined,
    type: ts.Type,
    schema: JSONSchema7Definition | undefined,
    definitions: Record<string, JSONSchema7Definition>,
    namesByType: ReadonlyMap<ts.Type, string>,
    seen: Set<JSONSchema7>
): void {
    if (!schema || typeof schema === "boolean" || seen.has(schema)) return
    seen.add(schema)
    if (isUnchecked(type, unchecked)) {
        for (const key of Object.keys(schema)) delete schema[key as keyof JSONSchema7]
        return
    }
    const reference = (child: ts.Type) => ({
        $ref: `#/definitions/${namesByType.get(child)!.replaceAll("~", "~0").replaceAll("/", "~1")}`
    })
    if (
        type.isUnion() &&
        type.types.some(
            member =>
                member.flags &
                (ts.TypeFlags.Object |
                    ts.TypeFlags.Intersection |
                    ts.TypeFlags.TemplateLiteral |
                    ts.TypeFlags.Undefined)
        )
    ) {
        // Rebuild branches from compiler types: the schema library coalesces primitive unions and loses their correspondence.
        for (const key of ["$ref", "type", "enum", "const", "anyOf", "oneOf"] as const) delete schema[key]
        schema.anyOf = type.types.filter(member => !(member.flags & ts.TypeFlags.Undefined)).map(reference)
    } else if (schema.$ref) {
        const key = decodeURIComponent(schema.$ref.slice("#/definitions/".length))
            .replaceAll("~1", "/")
            .replaceAll("~0", "~")
        preserveTypes(checker, unchecked, type, definitions[key], definitions, namesByType, seen)
        return
    }
    const title = sourceTypeName(type)
    if (title) schema.title ??= title
    if (type.isUnion()) return
    if (type.isIntersection() && schema.allOf) {
        schema.allOf = type.types.map(reference)
        return
    }
    preserveChildren(checker, type, schema, (child, definition) =>
        preserveTypes(checker, unchecked, child, definition, definitions, namesByType, seen)
    )
    const indexType = checker.getIndexTypeOfType(type, ts.IndexKind.String)
    if (indexType && type.flags & ts.TypeFlags.Object && !isArray(checker, type))
        schema.additionalProperties = reference(indexType)
}

function preserveChildren(
    checker: ts.TypeChecker,
    type: ts.Type,
    schema: JSONSchema7,
    visit: (type: ts.Type, schema: JSONSchema7Definition | undefined) => void
): void {
    if (isArray(checker, type)) {
        const elements = checker.getTypeArguments(type as ts.TypeReference)
        if (Array.isArray(schema.items))
            schema.items.forEach((item, index) => {
                if (elements[index]) visit(elements[index], item)
            })
        else if (elements[0]) visit(elements[0], schema.items)
        return
    }
    if (!(type.flags & ts.TypeFlags.Object)) return
    for (const property of type.getProperties()) {
        const declaration = property.valueDeclaration ?? property.declarations?.[0]
        if (declaration)
            visit(checker.getTypeOfSymbolAtLocation(property, declaration), schema.properties?.[property.name])
    }
}

function isArray(checker: ts.TypeChecker, type: ts.Type): boolean {
    return checker.isArrayType(type) || checker.isTupleType(type) || type.getSymbol()?.name === "ReadonlyArray"
}

function sourceTypeName(type: ts.Type): string | undefined {
    const symbol = type.aliasSymbol ?? type.getSymbol()
    if (symbol?.declarations?.some(declaration => declaration.getSourceFile().hasNoDefaultLib)) return undefined
    return symbol?.declarations?.some(
        declaration =>
            ts.isInterfaceDeclaration(declaration) ||
            ts.isTypeAliasDeclaration(declaration) ||
            ts.isEnumDeclaration(declaration)
    )
        ? symbol.name
        : undefined
}

function assertJsonType(
    checker: ts.TypeChecker,
    unchecked: ts.Symbol | undefined,
    type: ts.Type,
    label: string,
    optional = false,
    seen = new Set<ts.Type>()
): void {
    if (type.flags & ts.TypeFlags.Undefined) {
        if (optional) return
        throw new ActorDefinitionError(
            `${label} must be JSON-compatible; undefined is only allowed for optional properties`
        )
    }
    if (type.isUnionOrIntersection()) {
        for (const member of type.types) assertJsonType(checker, unchecked, member, label, optional, seen)
        return
    }
    if (seen.has(type) || isUnchecked(type, unchecked)) return
    seen.add(type)
    if (
        type.flags &
        (ts.TypeFlags.StringLike |
            ts.TypeFlags.NumberLike |
            ts.TypeFlags.BooleanLike |
            ts.TypeFlags.Null |
            ts.TypeFlags.Unknown |
            ts.TypeFlags.Never)
    )
        return
    if (checker.isArrayType(type) || checker.isTupleType(type) || type.getSymbol()?.name === "ReadonlyArray") {
        for (const element of checker.getTypeArguments(type as ts.TypeReference))
            assertJsonType(checker, unchecked, element, label, false, seen)
        return
    }
    if (
        !(type.flags & ts.TypeFlags.Object) ||
        type.getCallSignatures().length ||
        type.getConstructSignatures().length ||
        (type.getSymbol()?.flags ?? 0) & ts.SymbolFlags.Class
    )
        throw new ActorDefinitionError(
            `${label} must be JSON-compatible; unsupported type ${checker.typeToString(type)}`
        )
    for (const property of type.getProperties()) {
        if (property.name.startsWith("__@"))
            throw new ActorDefinitionError(`${label} must be JSON-compatible; symbol properties are not supported`)
        // Mapped-type properties (Record, Drizzle's $inferSelect) have no declaration of their own.
        const declaration = property.valueDeclaration ?? property.declarations?.[0]
        assertJsonType(
            checker,
            unchecked,
            declaration ? checker.getTypeOfSymbolAtLocation(property, declaration) : checker.getTypeOfSymbol(property),
            `${label}.${property.name}`,
            !!(property.flags & ts.SymbolFlags.Optional),
            seen
        )
    }
    for (const index of checker.getIndexInfosOfType(type))
        assertJsonType(checker, unchecked, index.type, label, true, seen)
}

function isUnchecked(type: ts.Type, unchecked: ts.Symbol | undefined): boolean {
    return unchecked !== undefined && type.getSymbol() === unchecked
}

export { assertJsonType, jsonSchema, sourceTypeName }
