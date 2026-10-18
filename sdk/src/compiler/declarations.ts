import { generateDtsBundle } from "dts-bundle-generator"
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs"
import os from "node:os"
import path from "node:path"
import ts from "typescript"

import { ActorDefinitionError } from "../errors.js"
import type { ActorApi, TypeScriptContract } from "../wire/public-contract.js"

class DeclarationCompiler {
    constructor(private readonly system: ts.System = ts.sys) {}

    compile(
        program: ts.Program,
        actors: readonly ts.ClassDeclaration[],
        contracts: readonly ActorApi[]
    ): TypeScriptContract {
        const directory = mkdtempSync(path.join(os.tmpdir(), "actor-declarations-"))
        try {
            return this.bundle(directory, program, actors, contracts)
        } finally {
            rmSync(directory, { recursive: true, force: true })
        }
    }

    private bundle(
        directory: string,
        program: ts.Program,
        actors: readonly ts.ClassDeclaration[],
        contracts: readonly ActorApi[]
    ) {
        const packages = new Map<string, { directory: string; version: string }>()
        const entry = this.writeActors(directory, program, actors, contracts, packages)
        const config = writeBundleConfig(directory, program.getCompilerOptions(), packages)
        const [declarations] = generateDtsBundle(
            [
                {
                    filePath: entry,
                    libraries: { inlinedLibraries: ["durable-actors"] },
                    output: { noBanner: true, exportReferencedTypes: false, sortNodes: true }
                }
            ],
            { preferredConfigPath: config }
        )
        const dependencies = Object.fromEntries(
            externalModules(declarations)
                .map(specifier => {
                    const name = packageName(specifier)
                    const pkg =
                        packages.get(name) ??
                        findPackage(specifier, program.getRootFileNames()[0], program.getCompilerOptions(), this.system)
                    return [name, pkg.version]
                })
                .sort(([left], [right]) => left.localeCompare(right))
        )
        return { declarations, dependencies }
    }

    private writeActors(
        directory: string,
        program: ts.Program,
        actors: readonly ts.ClassDeclaration[],
        contracts: readonly ActorApi[],
        packages: Map<string, { directory: string; version: string }>
    ): string {
        const files = emitDeclarations(program, this.system)
        const imports: string[] = []
        const properties: string[] = []
        for (const [index, contract] of contracts.entries()) {
            const actor = actors.find(actor => actor.name!.text === contract.actorName)!
            const source = actor.getSourceFile()
            const emitted = files.get(source.fileName)!
            const projected = projectActor(emitted, actor, contract, program.getTypeChecker())
            const file = path.join(directory, `actor-${index}.d.ts`)
            const code = resolveImports(
                eraseUnchecked(projected.code),
                source.fileName,
                program.getCompilerOptions(),
                this.system,
                packages
            )
            writeFileSync(file, code)
            imports.push(`import type { ${projected.name} as Actor${index} } from "./actor-${index}.js"`)
            properties.push(`${JSON.stringify(contract.actorName)}: Actor${index}`)
        }
        const entry = path.join(directory, "index.d.ts")
        writeFileSync(entry, `${imports.join("\n")}\nexport interface ActorTypes { ${properties.join(";\n")} }`)
        return entry
    }
}

function writeBundleConfig(
    directory: string,
    options: ts.CompilerOptions,
    packages: ReadonlyMap<string, { directory: string }>
): string {
    for (const [name, pkg] of packages) {
        const link = path.join(directory, "node_modules", name)
        mkdirSync(path.dirname(link), { recursive: true })
        symlinkSync(pkg.directory, link, "dir")
    }
    const config = path.join(directory, "tsconfig.json")
    writeFileSync(
        config,
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                strict: true,
                skipLibCheck: true,
                baseUrl: options.baseUrl ?? options.pathsBasePath,
                paths: options.paths,
                exactOptionalPropertyTypes: options.exactOptionalPropertyTypes ?? false,
                types: []
            }
        })
    )
    writeFileSync(path.join(directory, "package.json"), '{"type":"module"}')
    return config
}

function emitDeclarations(program: ts.Program, system: ts.System) {
    const options = {
        ...program.getCompilerOptions(),
        noEmit: false,
        declaration: true,
        declarationMap: false,
        emitDeclarationOnly: true,
        outDir: undefined,
        outFile: undefined,
        incremental: false,
        composite: false
    }
    const host = ts.createCompilerHost(options)
    host.readFile = file => program.getSourceFile(file)?.text ?? system.readFile(file)
    host.fileExists = system.fileExists
    const emitter = ts.createProgram(program.getRootFileNames(), options, host, program)
    const files = new Map<string, string>()
    const result = emitter.emit(
        undefined,
        (_file, text, _bom, _error, sources) => {
            if (sources?.length === 1) files.set(sources[0].fileName, text)
        },
        undefined,
        true
    )
    if (result.diagnostics.length)
        throw new ActorDefinitionError(
            result.diagnostics.map(d => ts.flattenDiagnosticMessageText(d.messageText, "\n")).join("\n")
        )
    return files
}

function projectActor(text: string, actor: ts.ClassDeclaration, contract: ActorApi, checker: ts.TypeChecker) {
    const source = ts.createSourceFile("actor.d.ts", text, ts.ScriptTarget.Latest, true)
    const declaration = source.statements.find(
        statement => ts.isClassDeclaration(statement) && statement.name?.text === actor.name!.text
    ) as ts.ClassDeclaration
    const instance = checker.getTypeAtLocation(actor)
    const types = checker.getTypeArguments(instance.getBaseTypes()![0] as ts.TypeReference)
    const declaredTypes = declaration.heritageClauses?.find(clause => clause.token === ts.SyntaxKind.ExtendsKeyword)
        ?.types[0].typeArguments
    const name = availableName(source, `$${contract.actorName}Contract`)
    const properties = ["Metadata", "Incoming", "Outgoing"].map((kind, index) =>
        ts.factory.createPropertySignature(
            undefined,
            kind,
            undefined,
            declaredTypes?.[index] ??
                checker.typeToTypeNode(
                    types[index],
                    undefined,
                    ts.NodeBuilderFlags.NoTruncation | ts.NodeBuilderFlags.UseFullyQualifiedType
                )!
        )
    )
    properties.push(
        ts.factory.createPropertySignature(undefined, "State", undefined, publicState(declaration, contract))
    )
    properties.push(
        ts.factory.createPropertySignature(undefined, "Methods", undefined, publicMethods(declaration, contract))
    )
    const root = ts.factory.createInterfaceDeclaration(
        [ts.factory.createModifier(ts.SyntaxKind.ExportKeyword)],
        name,
        undefined,
        undefined,
        properties
    )
    return {
        name,
        code: ts.createPrinter().printFile(ts.factory.updateSourceFile(source, [...source.statements, root]))
    }
}

function publicState(declaration: ts.ClassDeclaration, contract: ActorApi): ts.TypeLiteralNode {
    const state = declaration.members.filter(
        member =>
            ts.isPropertyDeclaration(member) &&
            Object.hasOwn(
                (contract.socket.schema.definitions!.State as { properties: object }).properties,
                propertyName(member.name)
            )
    ) as ts.PropertyDeclaration[]
    return ts.factory.createTypeLiteralNode(
        state.map(member =>
            ts.factory.createPropertySignature(
                member.modifiers?.filter(m => m.kind === ts.SyntaxKind.ReadonlyKeyword),
                member.name,
                member.questionToken,
                member.type
            )
        )
    )
}

function publicMethods(declaration: ts.ClassDeclaration, contract: ActorApi): ts.TypeLiteralNode {
    const members = declaration.members.filter(
        member =>
            ts.isMethodDeclaration(member) &&
            contract.rpc.methods.some(method => method.name === propertyName(member.name))
    ) as ts.MethodDeclaration[]
    return ts.factory.createTypeLiteralNode(
        members
            .sort((a, b) => propertyName(a.name).localeCompare(propertyName(b.name)))
            .map(member =>
                ts.factory.createMethodSignature(
                    undefined,
                    member.name,
                    member.questionToken,
                    member.typeParameters,
                    member.parameters,
                    member.type
                )
            )
    )
}

function availableName(source: ts.SourceFile, proposed: string): string {
    let name = proposed
    while (source.text.includes(name)) name += "_"
    return name
}

function propertyName(name: ts.PropertyName): string {
    if (ts.isComputedPropertyName(name) && ts.isStringLiteral(name.expression)) return name.expression.text
    return ts.isIdentifier(name) || ts.isStringLiteral(name) || ts.isNumericLiteral(name) ? name.text : name.getText()
}

function eraseUnchecked(text: string): string {
    const source = ts.createSourceFile("actor.d.ts", text, ts.ScriptTarget.Latest, true)
    const used = new Set<string>()
    const collect = (node: ts.Node): void => {
        if (ts.isImportDeclaration(node) || isUnchecked(node)) return
        if (ts.isIdentifier(node)) used.add(node.text)
        ts.forEachChild(node, collect)
    }
    collect(source)
    const result = ts.transform(source, [
        context => root =>
            ts.visitNode(root, function visit(node: ts.Node): ts.VisitResult<ts.Node | undefined> {
                if (isUnchecked(node)) return ts.factory.createKeywordTypeNode(ts.SyntaxKind.UnknownKeyword)
                if (ts.isImportSpecifier(node) && !used.has(node.name.text)) return undefined
                const visited = ts.visitEachChild(node, visit, context)
                return ts.isImportDeclaration(visited) && isEmptyImport(visited) ? undefined : visited
            }) as ts.SourceFile
    ])
    const code = ts.createPrinter().printFile(result.transformed[0])
    result.dispose()
    return code
}

// TODO: Make this handle import renames. import { type Unchecked as Opaque } from "durable-actors" will fail
function isUnchecked(node: ts.Node): boolean {
    const name = ts.isTypeReferenceNode(node) ? node.typeName : ts.isImportTypeNode(node) ? node.qualifier : undefined
    return name !== undefined && ts.isIdentifier(name) && name.text === "Unchecked"
}

function isEmptyImport(declaration: ts.ImportDeclaration): boolean {
    const clause = declaration.importClause
    const bindings = clause?.namedBindings
    return !!clause && !clause.name && !!bindings && ts.isNamedImports(bindings) && !bindings.elements.length
}

function resolveImports(
    text: string,
    location: string,
    options: ts.CompilerOptions,
    system: ts.System,
    packages: Map<string, { directory: string; version: string }>
): string {
    const source = ts.createSourceFile("actor.d.ts", text, ts.ScriptTarget.Latest, true)
    const result = ts.transform(source, [
        context => root =>
            ts.visitNode(root, function visit(node: ts.Node): ts.VisitResult<ts.Node> {
                if (ts.isStringLiteral(node) && isModuleSpecifier(node)) {
                    const resolved = ts.resolveModuleName(node.text, location, options, system).resolvedModule
                    if (!resolved) throw new ActorDefinitionError(`Cannot resolve public type dependency ${node.text}`)
                    if (node.text.startsWith(".") || path.isAbsolute(node.text) || !resolved.isExternalLibraryImport)
                        return ts.factory.createStringLiteral(resolved.resolvedFileName)
                    const name = packageName(node.text)
                    packages.set(name, packageAt(resolved.resolvedFileName, name, system))
                }
                return ts.visitEachChild(node, visit, context)
            }) as ts.SourceFile
    ])
    const code = ts.createPrinter().printFile(result.transformed[0])
    result.dispose()
    return code
}

function isModuleSpecifier(node: ts.StringLiteral): boolean {
    const parent = node.parent
    return (
        ((ts.isImportDeclaration(parent) || ts.isExportDeclaration(parent)) && parent.moduleSpecifier === node) ||
        (ts.isLiteralTypeNode(parent) && ts.isImportTypeNode(parent.parent))
    )
}

function externalModules(code: string): string[] {
    const source = ts.createSourceFile("types.d.ts", code, ts.ScriptTarget.Latest, true)
    const names = new Set(
        source.typeReferenceDirectives.map(ref =>
            ref.fileName.startsWith("@types/") ? ref.fileName : `@types/${ref.fileName}`
        )
    )
    const visit = (node: ts.Node) => {
        if (ts.isStringLiteral(node) && isModuleSpecifier(node)) {
            if (node.text.startsWith(".") || path.isAbsolute(node.text))
                throw new ActorDefinitionError(`Unbundled public type dependency ${node.text}`)
            if (!node.text.startsWith("node:")) names.add(node.text)
        }
        ts.forEachChild(node, visit)
    }
    visit(source)
    return [...names].sort()
}

function packageName(specifier: string): string {
    return specifier
        .split("/")
        .slice(0, specifier.startsWith("@") ? 2 : 1)
        .join("/")
}

function findPackage(specifier: string, location: string, options: ts.CompilerOptions, system: ts.System) {
    const resolved = ts.resolveModuleName(specifier, location, options, system).resolvedModule
    if (!resolved) throw new ActorDefinitionError(`Cannot resolve public type dependency ${specifier}`)
    return packageAt(resolved.resolvedFileName, packageName(specifier), system)
}

function packageAt(file: string, name: string, system: ts.System) {
    for (let directory = path.dirname(file); ; directory = path.dirname(directory)) {
        const manifest = system.readFile(path.join(directory, "package.json"))
        if (manifest) {
            const json = JSON.parse(manifest)
            if (json.name === name || directory.endsWith(`${path.sep}${name}`)) {
                if (typeof json.version !== "string")
                    throw new ActorDefinitionError(`Public type dependency ${name} must declare a version`)
                return { directory, version: json.version as string }
            }
        }
        if (directory === path.dirname(directory))
            throw new ActorDefinitionError(`Cannot find package metadata for ${name}`)
    }
}

export { DeclarationCompiler }
