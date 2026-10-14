import ts from "typescript"

import { definitionDiagnostic } from "../actor-compiler.js"
import { hasModifier, validateField } from "../fieldParsing.js"
import type { ActorFieldSchema, ParsedActor } from "../types.js"

function validatePersistence(actor: ParsedActor) {
    const fields: ActorFieldSchema[] = []
    const diagnostics: ts.Diagnostic[] = []
    for (const member of actor.members) {
        const node = member.node
        if (ts.isConstructorDeclaration(node)) {
            for (const parameter of node.parameters) {
                if (ts.isParameterPropertyDeclaration(parameter, node))
                    diagnostics.push(
                        definitionDiagnostic(
                            parameter,
                            "actor parameter properties must become explicitly decorated class fields"
                        )
                    )
            }
        }
        if (!ts.isPropertyDeclaration(node) || hasModifier(node, ts.SyntaxKind.StaticKeyword)) continue
        const result = validateField(actor.name, member)
        if ("field" in result) fields.push(result.field)
        else diagnostics.push(...result.diagnostics)
    }
    return { fields, diagnostics }
}

export { validatePersistence }
