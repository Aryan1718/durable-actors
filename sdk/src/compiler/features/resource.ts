import ts from "typescript"

import { definitionDiagnostic } from "../actor-compiler.js"
import { AnnotationKind, DecoratorResult, DecoratorUse, SdkSymbols } from "../types.js"

function readResource(use: DecoratorUse, sdk: SdkSymbols, checker: ts.TypeChecker): DecoratorResult {
    if (sdk.ActorResource === undefined) {
        return { annotations: [], diagnostics: [definitionDiagnostic(use.node, "@Resource requires a resource field")] }
    }

    const fieldType = checker.getTypeAtLocation(use.target)
    const resourceType = checker.getDeclaredTypeOfSymbol(sdk.ActorResource)

    if (!checker.isTypeAssignableTo(fieldType, resourceType)) {
        return {
            annotations: [],
            diagnostics: [definitionDiagnostic(use.node, "@Resource fields must implement ActorResource")]
        }
    }

    return { annotations: [{ kind: AnnotationKind.Resource, node: use.node }], diagnostics: [] }
}

export { readResource }
