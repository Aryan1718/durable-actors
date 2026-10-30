import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { createRequire } from "node:module"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import { installSdk } from "../fixtures/installed-sdk.js"

const run = promisify(execFile)

test("actors bundle durable-actors/drizzle with their own drizzle-orm and drizzle-kit migrations", async t => {
    const project = await mkdtemp(path.join(tmpdir(), "drizzle-build-"))
    t.after(() => rm(project, { recursive: true, force: true }))
    const sdk = await installSdk(project)
    await symlink(drizzleOrm(), path.join(project, "node_modules/drizzle-orm"), "dir")
    await writeProject(project)

    const { stdout } = await run(
        "bun",
        [path.join(sdk, "dist/compiler/deployment-build.js"), project, "actors.ts", path.join(project, "build")],
        { timeout: 30_000 }
    )
    assert.equal(JSON.parse(stdout).actors[0].actorName, "Library")
    const artifact = path.join(project, "build/actors.mjs")
    const bundle = await readFile(artifact, "utf8")
    assert.deepEqual(bundle.match(/^import .* from "[^"]+";$/gm), [
        'import { Actor } from "durable-actors";',
        'import { createHash } from "node:crypto";'
    ])
    assert.match(bundle, /CREATE TABLE `authors`/)

    const script = fileURLToPath(new URL("../fixtures/invoke-drizzle-actor.js", import.meta.url))
    const invoked = await run("bun", [script, sdk, artifact], { timeout: 30_000 })
    assert.match(invoked.stdout, /drizzle invocation passed/)
})

function drizzleOrm(): string {
    const require = createRequire(fileURLToPath(new URL("../../../package.json", import.meta.url)))
    return path.dirname(require.resolve("drizzle-orm/package.json"))
}

async function writeProject(project: string): Promise<void> {
    await mkdir(path.join(project, "drizzle/meta"), { recursive: true })
    await writeFile(
        path.join(project, "drizzle/0000_init.sql"),
        "CREATE TABLE `authors` (`id` integer PRIMARY KEY, `name` text NOT NULL);\n--> statement-breakpoint\n" +
            "CREATE TABLE `books` (`id` integer PRIMARY KEY, `author_id` integer NOT NULL REFERENCES `authors`(`id`), `title` text NOT NULL UNIQUE);"
    )
    await writeFile(
        path.join(project, "drizzle/meta/_journal.json"),
        JSON.stringify({
            version: "7",
            dialect: "sqlite",
            entries: [{ idx: 0, version: "6", when: 1700000000000, tag: "0000_init", breakpoints: true }]
        })
    )
    await writeFile(
        path.join(project, "drizzle/migrations.js"),
        "import journal from './meta/_journal.json';\nimport m0000 from './0000_init.sql';\n\nexport default {\n  journal,\n  migrations: {\n    m0000\n  }\n}\n"
    )
    await writeFile(
        path.join(project, "drizzle/migrations.d.ts"),
        'import type { DrizzleMigrations } from "durable-actors/drizzle"\ndeclare const migrations: DrizzleMigrations\nexport default migrations\n'
    )
    await writeFile(
        path.join(project, "actors.ts"),
        `
import { Actor, type ActorContext } from "durable-actors"
import { drizzle, migrate } from "durable-actors/drizzle"
import { eq } from "drizzle-orm"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"

import migrations from "./drizzle/migrations.js"

const authors = sqliteTable("authors", { id: integer("id").primaryKey(), name: text("name").notNull() })
const books = sqliteTable("books", {
    id: integer("id").primaryKey(),
    authorId: integer("author_id").notNull(),
    title: text("title").notNull().unique()
})

export class Library extends Actor {
    constructor(context: ActorContext) {
        super(context)
        migrate(this.db, migrations)
    }

    async add(author: string, title: string): Promise<number | null> {
        const orm = drizzle(this.db)
        try {
            return await orm.transaction(async tx => {
                const [written] = await tx.insert(authors).values({ name: author }).returning({ id: authors.id })
                const [book] = await tx.insert(books).values({ authorId: written!.id, title }).returning({ id: books.id })
                return book!.id
            })
        } catch {
            return null
        }
    }

    async list(): Promise<{ authorId: number; bookId: number; title: string }[]> {
        return drizzle(this.db)
            .select({ authorId: authors.id, bookId: books.id, title: books.title })
            .from(books)
            .innerJoin(authors, eq(authors.id, books.authorId))
    }
}
`
    )
    await writeFile(
        path.join(project, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                moduleResolution: "NodeNext",
                strict: true,
                skipLibCheck: true,
                types: []
            },
            include: ["actors.ts", "drizzle/migrations.d.ts"]
        })
    )
}
