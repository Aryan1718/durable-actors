import ts from "typescript"

import { definitionDiagnostic } from "../actor-compiler.js"
import { AnnotationKind } from "../types.js"
import type { DecoratorResult, DecoratorUse, ParsedActor } from "../types.js"

function readAtomicity(use: DecoratorUse): DecoratorResult {
    const node = use.target
    if (use.called) return invalidAtomicity(use)
    if (!ts.isMethodDeclaration(node) || !node.body) return invalidAtomicity(use)
    if (!hasModifier(node, ts.SyntaxKind.AsyncKeyword)) return invalidAtomicity(use)
    if (hasModifier(node, ts.SyntaxKind.StaticKeyword)) return invalidAtomicity(use)
    if (hasModifier(node, ts.SyntaxKind.PrivateKeyword) || hasModifier(node, ts.SyntaxKind.ProtectedKeyword))
        return invalidAtomicity(use)
    if (!ts.isIdentifier(node.name) && !ts.isStringLiteral(node.name)) return invalidAtomicity(use)

    return { annotations: [{ kind: AnnotationKind.Atomicity, node: use.node }], diagnostics: [] }
}

function validateAtomicity(actor: ParsedActor) {
    const methods: string[] = []
    const diagnostics: ts.Diagnostic[] = []
    for (const member of actor.members) {
        const annotations = member.annotations.filter(annotation => annotation.kind === AnnotationKind.Atomicity)
        if (annotations.length > 1) diagnostics.push(definitionDiagnostic(member.node, "@Atomic cannot be repeated"))
        if (annotations.length === 1) methods.push((member.node.name as ts.Identifier | ts.StringLiteral).text)
    }
    return { methods, diagnostics }
}

function hasModifier(node: ts.MethodDeclaration, kind: ts.SyntaxKind): boolean {
    return ts.getModifiers(node)?.some(modifier => modifier.kind === kind) ?? false
}

function invalidAtomicity(use: DecoratorUse): DecoratorResult {
    return {
        annotations: [],
        diagnostics: [
            definitionDiagnostic(
                use.target,
                "@Atomic requires a public instance async method; use it without parentheses"
            )
        ]
    }
}

export { readAtomicity, validateAtomicity }
