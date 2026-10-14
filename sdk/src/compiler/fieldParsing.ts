import ts from "typescript"

import { definitionDiagnostic } from "./actor-compiler.js"
import { ActorFieldSchema, AnnotationKind, DecoratorResult, DecoratorUse, ParsedMember, Persistence } from "./types.js"

export type FieldValidation = { readonly field: ActorFieldSchema } | { readonly diagnostics: readonly ts.Diagnostic[] }

export function validateField(actorName: string, member: ParsedMember): FieldValidation {
    if (!ts.isPropertyDeclaration(member.node)) return { diagnostics: [] }

    const node = member.node
    const nodeName = fieldName(node.name)

    if (nodeName === undefined)
        return {
            diagnostics: [
                definitionDiagnostic(
                    node.name,
                    `actor field ${actorName}.${node.name.getText()} must have a literal string name`
                )
            ]
        }

    if (fieldDenyList.includes(nodeName))
        return { diagnostics: [definitionDiagnostic(node.name, `actor field ${nodeName} is reserved.`)] }

    if (member.diagnostics.length > 0) return { diagnostics: [...member.diagnostics] }

    const annotations = member.annotations.filter(annotation => annotation.kind === AnnotationKind.Persistence)
    const emission = member.annotations.filter(annotation => annotation.kind === AnnotationKind.Emission)
    const resource = member.annotations.filter(annotation => annotation.kind === AnnotationKind.Resource)
    const isResource = resource.length === 1 || (resource.length === 0 && member.resource)

    const persistence = annotations.length === 0 ? Persistence.Ephemeral : annotations[0]?.mode

    const isPersistenceValid = (persistence !== undefined && annotations.length === 1) || isResource
    const isEmissionValid = emission.length <= 1 && (emission.length === 0 || persistence === Persistence.Persisted)
    const isResourceValid =
        resource.length <= 1 && (!isResource || (persistence !== Persistence.Persisted && emission.length === 0))

    if (!isPersistenceValid) {
        return {
            diagnostics: [
                definitionDiagnostic(
                    node.name,
                    `actor field ${actorName}.${node.name.getText()} must declare exactly one of @Persisted or @Ephemeral`,
                    annotations.map(annotation => annotation.node)
                )
            ]
        }
    }

    if (!isEmissionValid) {
        return {
            diagnostics: [
                definitionDiagnostic(node, "@Emittable requires a public @Persisted field and cannot be repeated")
            ]
        }
    }

    if (!isResourceValid) {
        return {
            diagnostics: [definitionDiagnostic(node, "@Resource cannot be @Persisted or @Emittable")]
        }
    }

    // if the field is a resource, it must be ephemeral
    const finalPersistence = isResource ? Persistence.Ephemeral : persistence

    const isPrivate = hasModifier(node, ts.SyntaxKind.PrivateKeyword)
    const isProtected = hasModifier(node, ts.SyntaxKind.ProtectedKeyword)

    const field: ActorFieldSchema = {
        name: nodeName,
        persistence: finalPersistence,
        private: ts.isPrivateIdentifier(node.name),
        visibility: isPrivate ? "private" : isProtected ? "protected" : undefined,
        emittable: emission.length === 1,
        resource: isResource
    }

    return { field }
}

const fieldDenyList = ["db"]

export function readPersistence(use: DecoratorUse, mode: Persistence): DecoratorResult {
    const name = mode === Persistence.Persisted ? "Persisted" : "Ephemeral"
    const node = use.target
    if (
        !ts.isPropertyDeclaration(node) ||
        hasModifier(node, ts.SyntaxKind.StaticKeyword) ||
        hasModifier(node, ts.SyntaxKind.AccessorKeyword)
    )
        return invalid(use, `@${name} requires an instance field`)
    if (use.called) return invalid(use, `use @${name} without parentheses or arguments`)
    if (hasModifier(node, ts.SyntaxKind.DeclareKeyword))
        return invalid(use, `@${name} requires a runtime field, not a declare field`)
    if (fieldName(node.name) === undefined) return invalid(use, `@${name} requires a field with a literal string name`)
    if (mode === Persistence.Persisted && ts.isPrivateIdentifier(node.name))
        return invalid(use, "@Persisted cannot decorate a JavaScript private field; use @Ephemeral")
    return { annotations: [{ kind: AnnotationKind.Persistence, mode, node: use.node }], diagnostics: [] }
}

export function readEmission(use: DecoratorUse): DecoratorResult {
    const node = use.target
    if (
        !ts.isPropertyDeclaration(node) ||
        hasModifier(node, ts.SyntaxKind.StaticKeyword) ||
        hasModifier(node, ts.SyntaxKind.AccessorKeyword)
    )
        return invalid(use, "@Emittable requires an instance field")
    if (use.called) return invalid(use, "use @Emittable without parentheses or arguments")
    if (
        ts.isPrivateIdentifier(node.name) ||
        hasModifier(node, ts.SyntaxKind.PrivateKeyword) ||
        hasModifier(node, ts.SyntaxKind.ProtectedKeyword)
    )
        return invalid(use, "@Emittable requires a public persisted field")
    return { annotations: [{ kind: AnnotationKind.Emission, node: use.node }], diagnostics: [] }
}

function fieldName(name: ts.PropertyName): string | undefined {
    const isLiteralName =
        ts.isIdentifier(name) || ts.isPrivateIdentifier(name) || ts.isStringLiteral(name) || ts.isNumericLiteral(name)
    if (isLiteralName) return name.text

    const isComputedLiteralName =
        ts.isComputedPropertyName(name) && (ts.isStringLiteral(name.expression) || ts.isNumericLiteral(name.expression))
    if (isComputedLiteralName) return name.expression.text
}

export function hasModifier(node: ts.Node, kind: ts.SyntaxKind): boolean {
    return ts.canHaveModifiers(node) && (ts.getModifiers(node)?.some(modifier => modifier.kind === kind) ?? false)
}

function invalid(use: DecoratorUse, message: string): DecoratorResult {
    return { annotations: [], diagnostics: [definitionDiagnostic(use.node, message)] }
}
