import { type Loader, type Plugin, build } from "esbuild"
import { resolve } from "import-meta-resolve"
import { realpathSync } from "node:fs"
import { mkdir, writeFile } from "node:fs/promises"
import path from "node:path"
import { fileURLToPath, pathToFileURL } from "node:url"

import { ACTOR_ARTIFACT_VERSION } from "../actor/schema.js"

import { ActorCompiler } from "./actor-compiler.js"
import type { CompilerOptions } from "./types.js"

async function buildActor(entrypoint: string, outfile: string, options: CompilerOptions & { local?: boolean } = {}) {
    const compilation = new ActorCompiler().compileDeployment(entrypoint, options)
    await bundleActor(entrypoint, outfile, compilation, options)
    return compilation.contract
}

async function bundleActor(
    entrypoint: string,
    outfile: string,
    compilation: ReturnType<ActorCompiler["compileDeployment"]>,
    options: CompilerOptions & { local?: boolean } = {}
) {
    if (!outfile.endsWith(".mjs")) throw new Error("actor build output must use the .mjs extension")
    const source = path.resolve(entrypoint)
    const { schemas, sources } = compilation
    const result = await build({
        stdin: {
            contents: `import * as actors from ${JSON.stringify(source)};
                export { actors };
                export const schemas = ${JSON.stringify(schemas)};
                export const version = ${ACTOR_ARTIFACT_VERSION};`,
            resolveDir: path.dirname(source),
            sourcefile: "actor-artifact.ts",
            loader: "ts"
        },
        outfile,
        tsconfig: options.configFile,
        bundle: true,
        external: options.local
            ? [fileURLToPath(new URL("../*", import.meta.url))]
            : ["durable-actors", "durable-actors/*"],
        plugins: [analyzedSources(sources), bundledSdkAdapters(), ...(options.local ? [localSdkImports()] : [])],
        loader: { ".sql": "text" },
        sourcemap: options.local ? "inline" : false,
        platform: "node",
        format: "esm",
        target: "es2022",
        keepNames: true,
        write: false,
        logLevel: "silent"
    })
    await mkdir(path.dirname(outfile), { recursive: true })
    await writeFile(outfile, result.outputFiles[0]!.contents)
}

function analyzedSources(sources: ReadonlyMap<string, string>): Plugin {
    const files = new Map([...sources].map(([file, contents]) => [realpathSync(file), contents]))
    const loaders: Record<string, Loader> = {
        ".ts": "ts",
        ".tsx": "tsx",
        ".mts": "ts",
        ".cts": "ts",
        ".js": "js",
        ".jsx": "jsx",
        ".mjs": "js",
        ".cjs": "js",
        ".json": "json"
    }
    return {
        name: "analyzed-actor-sources",
        setup(builder) {
            builder.onLoad({ filter: /\.[cm]?[jt]sx?$|\.json$/ }, args => {
                const contents = files.get(args.path)
                const loader = loaders[path.extname(args.path)]
                return contents === undefined || loader === undefined
                    ? undefined
                    : { contents, loader, resolveDir: path.dirname(args.path) }
            })
        }
    }
}

// ORM adapters import the actor's own ORM package, so they bundle with the actor instead of resolving from the host.
const BUNDLED_SDK_ADAPTERS = /^durable-actors\/drizzle$/

function bundledSdkAdapters(): Plugin {
    return {
        name: "bundled-sdk-adapters",
        setup(builder) {
            builder.onResolve({ filter: BUNDLED_SDK_ADAPTERS }, args => ({
                path: fileURLToPath(resolve(args.path, pathToFileURL(path.join(args.resolveDir, "index.js")).href))
            }))
        }
    }
}

function localSdkImports(): Plugin {
    return {
        name: "local-sdk-imports",
        setup(builder) {
            builder.onResolve({ filter: /^durable-actors(?:\/|$)/ }, async args => {
                if (args.pluginData === "resolving-sdk") return undefined
                const resolved = await builder.resolve(args.path, {
                    kind: args.kind,
                    resolveDir: args.resolveDir,
                    pluginData: "resolving-sdk"
                })
                return { path: resolved.path, external: true, errors: resolved.errors }
            })
        }
    }
}

export { buildActor, bundleActor }
