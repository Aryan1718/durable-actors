import { Ajv } from "ajv"
import assert from "node:assert/strict"
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import ts from "typescript"

import { ActorCompiler } from "../../src/compiler/actor-compiler.js"
import { generateClient } from "../../src/compiler/generators/client-generator.js"
import { parsePublicContract } from "../../src/compiler/validate-public-contract.js"

test("mapped dictionaries preserve their value schemas and generated index signatures", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Metadata = Record<string, { value: string }>
        export class Room extends Actor<{}, never, never> {
            async echo(value: Metadata): Promise<Metadata> { return value }
        }
    `)
    const { rpc } = project.contract().actors[0]
    const validate = new Ajv().compile({ ...rpc.schema, ...rpc.methods[0].parameters[0].type })
    assert.equal(validate({ provider: { value: "ok" } }), true)
    assert.equal(validate({ provider: 1 }), false)
    assert.equal(validate({ provider: { value: 1 } }), false)
    await project.check(`
        type Metadata = Record<string, { value: string }>
        declare const metadata: Metadata
        const result: Metadata = await room.echo(metadata)
        result.provider.value.toUpperCase()
        // @ts-expect-error dictionary values retain their structure
        await room.echo({ provider: 1 })
    `)
})

test("nested dictionaries preserve union branches and recursive values", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Node = { value: string; children: Record<string, Node> }
        type Part = { kind: "tree"; nodes: Record<string, Node> } | { kind: "text"; text: string }
        export class Room extends Actor<{}, Part, Part> {
            async echo(value: Part[]): Promise<Part[]> { return value }
        }
    `)
    await project.check(`
        type Node = { value: string; children: Record<string, Node> }
        type Part = { kind: "tree"; nodes: Record<string, Node> } | { kind: "text"; text: string }
        declare const parts: Part[]
        const result: Part[] = await room.echo(parts)
        const incoming: actors.Room.Incoming = parts[0]
        declare const outgoing: actors.Room.Outgoing
        const sent: Part = outgoing
        // @ts-expect-error recursive dictionary entries retain their value type
        await room.echo([{ kind: "tree", nodes: { root: { value: "ok", children: { leaf: 42 } } } }])
        // @ts-expect-error union branches retain their required properties
        await room.echo([{ kind: "text", nodes: {} }])
    `)
})

test("intersections preserve dictionary values and required properties", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Message = { id: string } & { metadata: Record<string, { requestId: string }> }
        export class Room extends Actor<{}, Message, Message> {
            async echo(value: Message): Promise<Message> { return value }
        }
    `)
    await project.check(`
        type Message = { id: string } & { metadata: Record<string, { requestId: string }> }
        declare const message: Message
        const result: Message = await room.echo(message)
        const incoming: actors.Room.Incoming = message
        declare const outgoing: actors.Room.Outgoing
        const sent: Message = outgoing
        // @ts-expect-error both sides of the intersection are required
        await room.echo({ metadata: {} })
        // @ts-expect-error the dictionary retains its value type
        await room.echo({ id: "one", metadata: { provider: { requestId: 42 } } })
    `)
})

test("unknown stays unconstrained in methods, properties, dictionaries, sockets and public state", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Message = { metadata?: unknown; values: Record<string, unknown> }
        export class Room extends Actor<unknown, Message, unknown> {
            @Persisted messages: Message[] = []
            @Persisted value: unknown = null
            async echo(value: unknown): Promise<unknown> { return value }
            async optional(value?: unknown): Promise<unknown> { return value ?? null }
            async message(value: Message): Promise<Message> { return value }
        }
    `)
    await project.check(`
        declare const value: unknown
        await room.echo(value)
        await room.optional()
        await room.optional(value)
        await room.message({ metadata: value, values: { anything: value } })
        const metadata: actors.Room.Metadata = value
        const outgoing: actors.Room.Outgoing = value
        const incoming: actors.Room.Incoming = { metadata: value, values: { anything: value } }
        const state: actors.Room.State = { messages: [incoming], value }
        // @ts-expect-error unknown results require narrowing
        const object: Record<string, unknown> = await room.echo(value)
        // @ts-expect-error unknown properties require narrowing
        const text: string = (await room.message(incoming)).metadata
        // @ts-expect-error unknown dictionary values require narrowing
        const entry: string = (await room.message(incoming)).values.anything
        // @ts-expect-error unknown public state requires narrowing
        const stored: string = state.value
    `)
})

test("unknown survives nested containers, unions, intersections and recursion", async t => {
    const project = await createProject(t)
    const types = `
        type Tree = { payload: unknown; children: Record<string, Tree> }
        type Message = { id: string } & (
            | { kind: "tree"; root: Tree }
            | { kind: "raw"; values: unknown[]; pair: [string, unknown] }
        )
    `
    await project.generate(`
        ${types}
        export class Room extends Actor<{}, Message, Message> {
            async echo(value: Message): Promise<Message> { return value }
        }
    `)
    await project.check(`
        ${types}
        declare const value: unknown
        declare const message: Message
        const result: Message = await room.echo(message)
        await room.echo({ id: "one", kind: "raw", values: [value], pair: ["label", value] })
        await room.echo({ id: "two", kind: "tree", root: { payload: value, children: {} } })
        const incoming: actors.Room.Incoming = message
        declare const outgoing: actors.Room.Outgoing
        const sent: Message = outgoing
        if (result.kind === "raw") {
            // @ts-expect-error array elements require narrowing
            const item: string = result.values[0]
            // @ts-expect-error tuple elements require narrowing
            const item2: string = result.pair[1]
        }
        // @ts-expect-error the intersection still requires an id
        await room.echo({ kind: "raw", values: [], pair: ["label", value] })
    `)
})

test("template literal types survive unions, tuples, dictionaries and JSON transport", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Kind = \`\${string}.\${string}\`
        type Part = { kind: "text"; text: string } | { kind: Kind; data: unknown }
        type Combined = ({ kind: Kind } & { metadata: unknown }) | ({ kind: "text" } & { text: string })
        export class Room extends Actor<{}, Part, Part> {
            @Persisted parts: Part[] = []
            async echo(value: Kind): Promise<Kind> { return value }
            async part(value: Part): Promise<Part> { return value }
            async tuple(value: [Kind, \`item-\${number}\`]) { return value }
            async dictionary(value: Record<string, Kind>) { return value }
            async mixed(value: Kind | "system") { return value }
            async integer(value: \`id-\${bigint}\`) { return value }
            async intersect(value: { kind: Kind } & { metadata: Record<string, unknown> }) { return value }
            async combined(value: Combined): Promise<Combined> { return value }
        }
    `)
    await project.check(`
        type Kind = \`\${string}.\${string}\`
        type Part = { kind: "text"; text: string } | { kind: Kind; data: unknown }
        type Combined = ({ kind: Kind } & { metadata: unknown }) | ({ kind: "text" } & { text: string })
        declare const part: Part
        const result: Kind = await room.echo("custom.part")
        const returned: Part = await room.part(part)
        const tuple: [Kind, \`item-\${number}\`] = await room.tuple(["custom.part", "item-2"])
        const dictionary: Record<string, Kind> = await room.dictionary({ kind: "custom.part" })
        const mixed: Kind | "system" = await room.mixed("system")
        const integer: \`id-\${bigint}\` = await room.integer("id-42")
        const intersection: { kind: Kind; metadata: Record<string, unknown> } = await room.intersect({ kind: "custom.part", metadata: {} })
        const combined: Combined = await room.combined({ kind: "custom.part", metadata: null })
        const incoming: actors.Room.Incoming = part
        declare const outgoing: actors.Room.Outgoing
        const sent: Part = outgoing
        const state: actors.Room.State = { parts: [part] }
        const stored: Part[] = state.parts
        // @ts-expect-error arbitrary strings do not satisfy the template
        await room.echo("invalid")
        // @ts-expect-error union branches retain template constraints
        await room.part({ kind: "invalid", data: null })
    `)
})

test("recursive JSON dictionaries retain undefined without widening other uses", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Json = null | boolean | number | string | Json[] | JsonObject
        interface JsonObject { [key: string]: Json | undefined }
        type Payload = { kind: "json"; value: JsonObject } | { kind: "text"; value: string }
        export class Room extends Actor<{}, Payload, Payload> {
            async echo(value: JsonObject): Promise<JsonObject> { return value }
            async json(value: Json): Promise<Json> { return value }
            async payload(value: Payload): Promise<Payload> { return value }
            async omitted(value: Record<string, undefined>) { return value }
        }
    `)
    await project.check(`
        type Json = null | boolean | number | string | Json[] | JsonObject
        interface JsonObject { [key: string]: Json | undefined }
        declare const value: JsonObject
        const result: JsonObject = await room.echo(value)
        const omitted: Record<string, undefined> = await room.omitted({ absent: undefined })
        // @ts-expect-error only undefined values are allowed
        await room.omitted({ present: 42 })
        await room.echo({ missing: undefined, nested: { missing: undefined }, items: [{ missing: undefined }] })
        const json: Json = await room.json(value)
        await room.payload({ kind: "json", value })
        const incoming: actors.Room.Incoming = { kind: "json", value }
        // @ts-expect-error undefined is not a top-level JSON value
        await room.json(undefined)
        // @ts-expect-error undefined is not an array element
        await room.json([undefined])
        // @ts-expect-error dictionary lookups can be undefined
        const present: Json = (await room.echo(value)).missing
    `)
})

test("unchecked values skip JSON checks and accept any JSON on RPC and sockets", async t => {
    const project = await createProject(t)
    await project.generate(`
        import { type Unchecked, unchecked } from "durable-actors"
        declare const brand: unique symbol
        type Opaque = { readonly [brand]: string }
        export class Room extends Actor<{}, never, { value: Unchecked<Opaque> }> {
            async latest(): Promise<Unchecked<readonly Opaque[]>> { return unchecked([]) }
        }
    `)
    const { rpc, socket } = project.contract().actors[0]
    const result = rpc.methods[0].result
    assert.equal(result.kind, "value")
    const latest = new Ajv().compile({ ...rpc.schema, ...(result.kind === "value" ? result.type : {}) })
    assert.equal(latest([{ any: "shape" }]), true)
    assert.equal(latest("text"), true)
    const outgoing = new Ajv().compile({ ...socket.schema, $ref: "#/definitions/Outgoing" })
    assert.equal(outgoing({ value: [1, 2] }), true)
    assert.equal(outgoing({ value: null }), true)
    assert.equal(outgoing({}), false)
})

test("declarations preserve readonly types across RPC, sockets and public state", async t => {
    const project = await createProject(t)
    await project.generate(`
        type Values = readonly string[]
        export class Room extends Actor<Values, Values, Values> {
            @Persisted values: Values = []
            async echo(value: Values): Promise<Values> { return value }
        }
    `)
    assert.ok("typescript" in project.contract())
    await project.check(`
        declare const value: readonly string[]
        const result = await room.echo(value)
        const metadata: actors.Room.Metadata = value
        const incoming: actors.Room.Incoming = value
        const outgoing: actors.Room.Outgoing = value
        const state: actors.Room.State = { values: value }
        // @ts-expect-error readonly results cannot be mutated
        result.push("invalid")
        // @ts-expect-error readonly state remains readonly
        state.values.push("invalid")
    `)
})

async function createProject(t: { after(fn: () => Promise<void>): void }) {
    const root = await mkdtemp(path.join(os.tmpdir(), "type-preservation-"))
    t.after(() => rm(root, { recursive: true, force: true }))
    await mkdir(path.join(root, "node_modules"))
    const location = fileURLToPath(new URL("../../", import.meta.url))
    const sdk = location.endsWith(`${path.sep}.test-dist${path.sep}`) ? path.dirname(location.slice(0, -1)) : location
    await symlink(sdk, path.join(root, "node_modules/durable-actors"))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    let contract: ReturnType<ActorCompiler["compileContract"]>
    return {
        contract: () => contract,
        async generate(source: string) {
            const entrypoint = path.join(root, "actors.ts")
            await writeFile(entrypoint, `import { Actor, Persisted } from "durable-actors"\n${source}`)
            contract = parsePublicContract(JSON.parse(JSON.stringify(new ActorCompiler().compileContract(entrypoint))))
            await rm(entrypoint)
            await generateClient(contract, path.join(root, "generated"))
        },
        async check(source: string) {
            const consumer = path.join(root, "consumer.ts")
            await writeFile(
                consumer,
                `import { actors } from "./generated/index.js"\nconst room = actors.Room.get("one")\n${source}`
            )
            const program = ts.createProgram([consumer], {
                strict: true,
                noEmit: true,
                skipLibCheck: false,
                target: ts.ScriptTarget.ES2022,
                module: ts.ModuleKind.NodeNext
            })
            assert.deepEqual(
                ts
                    .getPreEmitDiagnostics(program)
                    .map(diagnostic => ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n")),
                []
            )
        }
    }
}
