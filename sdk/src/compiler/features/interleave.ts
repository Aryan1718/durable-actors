import ts from "typescript"

import { definitionDiagnostic } from "../actor-compiler.js"
import { AnnotationKind } from "../types.js"
import type { DecoratorResult, DecoratorUse, ParsedActor } from "../types.js"

function readInterleave(use: DecoratorUse): DecoratorResult {
    const node = use.target
    if (use.called) return invalidInterleave(use)
    if (!ts.isMethodDeclaration(node) || !node.body) return invalidInterleave(use)
    if (!hasModifier(node, ts.SyntaxKind.AsyncKeyword)) return invalidInterleave(use)
    if (hasModifier(node, ts.SyntaxKind.StaticKeyword)) return invalidInterleave(use)
    if (hasModifier(node, ts.SyntaxKind.PrivateKeyword) || hasModifier(node, ts.SyntaxKind.ProtectedKeyword))
        return invalidInterleave(use)
    if (!ts.isIdentifier(node.name) && !ts.isStringLiteral(node.name)) return invalidInterleave(use)

    return { annotations: [{ kind: AnnotationKind.Interleave, node: use.node }], diagnostics: [] }
}

function validateInterleave(actor: ParsedActor) {
    const methods: string[] = []
    const diagnostics: ts.Diagnostic[] = []
    for (const member of actor.members) {
        const annotations = member.annotations.filter(annotation => annotation.kind === AnnotationKind.Interleave)
        if (annotations.length > 1)
            diagnostics.push(definitionDiagnostic(member.node, "@Interleave cannot be repeated"))
        if (annotations.length === 1) methods.push((member.node.name as ts.Identifier | ts.StringLiteral).text)
    }
    return { methods, diagnostics }
}

function hasModifier(node: ts.MethodDeclaration, kind: ts.SyntaxKind): boolean {
    return ts.getModifiers(node)?.some(modifier => modifier.kind === kind) ?? false
}

function invalidInterleave(use: DecoratorUse): DecoratorResult {
    return {
        annotations: [],
        diagnostics: [
            definitionDiagnostic(
                use.target,
                "@Interleave requires a public instance async method; use it without parentheses"
            )
        ]
    }
}

export { readInterleave, validateInterleave }
