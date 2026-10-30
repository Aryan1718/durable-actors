import { TransactionRollbackError, eq, relations, sql } from "drizzle-orm"
import { blob, integer, sqliteTable, text } from "drizzle-orm/sqlite-core"
import assert from "node:assert/strict"
import { test } from "node:test"

import { runWithActorDatabase } from "../../src/actor/database.js"
import { drizzle, migrate } from "../../src/drizzle.js"
import { DatabaseProxy } from "../../src/host/database.js"
import { HostDatabase } from "../fixtures/host-database.js"

const users = sqliteTable("users", {
    id: integer("id").primaryKey(),
    name: text("name").notNull().unique(),
    admin: integer("admin", { mode: "boolean" }).notNull().default(false),
    avatar: blob("avatar", { mode: "buffer" })
})
const posts = sqliteTable("posts", {
    id: integer("id").primaryKey(),
    userId: integer("user_id")
        .notNull()
        .references(() => users.id),
    title: text("title").notNull()
})
const usersRelations = relations(users, ({ many }) => ({ posts: many(posts) }))
const postsRelations = relations(posts, ({ one }) => ({
    author: one(users, { fields: [posts.userId], references: [users.id] })
}))
const schema = { users, posts, usersRelations, postsRelations }

const uniqueViolation = (error: unknown) => /UNIQUE constraint failed/.test(String((error as Error).cause ?? error))

const SCHEMA_SQL = [
    "CREATE TABLE users (id integer PRIMARY KEY, name text NOT NULL UNIQUE, admin integer NOT NULL DEFAULT 0, avatar blob)",
    "CREATE TABLE posts (id integer PRIMARY KEY, user_id integer NOT NULL REFERENCES users(id), title text NOT NULL)"
]

async function withOrm(
    operation: (orm: ReturnType<typeof drizzle<typeof schema>>, host: HostDatabase) => Promise<void>
) {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    try {
        await runWithActorDatabase({}, async () => {
            for (const statement of SCHEMA_SQL) database.exec(statement)
            await operation(drizzle(database, { schema }), host)
        })
        database.commit()
    } finally {
        host.close()
    }
}

test("joined selects keep columns that share a name", async () => {
    await withOrm(async orm => {
        await orm.insert(users).values({ id: 1, name: "ada" })
        await orm.insert(posts).values({ id: 10, userId: 1, title: "engines" })
        assert.deepEqual(
            await orm
                .select({ userId: users.id, postId: posts.id, title: posts.title })
                .from(users)
                .innerJoin(posts, eq(posts.userId, users.id)),
            [{ userId: 1, postId: 10, title: "engines" }]
        )
        assert.deepEqual(await orm.select().from(users).innerJoin(posts, eq(posts.userId, users.id)), [
            {
                users: { id: 1, name: "ada", admin: false, avatar: null },
                posts: { id: 10, userId: 1, title: "engines" }
            }
        ])
    })
})

test("relational queries load nested relations", async () => {
    await withOrm(async orm => {
        await orm.insert(users).values([
            { id: 1, name: "ada", admin: true },
            { id: 2, name: "grace" }
        ])
        await orm.insert(posts).values([
            { id: 10, userId: 1, title: "engines" },
            { id: 11, userId: 1, title: "notes" }
        ])
        assert.deepEqual(
            await orm.query.users.findMany({
                columns: { name: true, admin: true },
                with: { posts: { columns: { title: true }, orderBy: posts.id } },
                orderBy: users.id
            }),
            [
                { name: "ada", admin: true, posts: [{ title: "engines" }, { title: "notes" }] },
                { name: "grace", admin: false, posts: [] }
            ]
        )
        assert.deepEqual(
            await orm.query.posts.findFirst({ where: eq(posts.id, 11), with: { author: { columns: { name: true } } } }),
            {
                id: 11,
                userId: 1,
                title: "notes",
                author: { name: "ada" }
            }
        )
    })
})

test("column modes and returning round-trip through the actor database", async () => {
    await withOrm(async orm => {
        const avatar = Buffer.from([0, 1, 255])
        const [inserted] = await orm.insert(users).values({ id: 1, name: "ada", admin: true, avatar }).returning()
        assert.deepEqual(inserted, { id: 1, name: "ada", admin: true, avatar })
        await orm.update(users).set({ admin: false }).where(eq(users.id, 1))
        assert.deepEqual(await orm.select({ admin: users.admin }).from(users).get(), { admin: false })
        assert.deepEqual(await orm.delete(users).where(eq(users.id, 1)).returning({ id: users.id }), [{ id: 1 }])
    })
})

test("raw SQL follows the sqlite-proxy contract and binds booleans", async () => {
    await withOrm(async orm => {
        await orm.run(sql`INSERT INTO users (id, name, admin) VALUES (${1}, ${"ada"}, ${true})`)
        assert.deepEqual(await orm.all(sql`SELECT id, name, admin FROM users`), [[1, "ada", 1]])
        assert.deepEqual(await orm.get(sql`SELECT name FROM users WHERE id = ${1}`), ["ada"])
        assert.deepEqual(await orm.values(sql`SELECT 1 AS id, 2 AS id`), [[1, 2]])
    })
})

test("transactions roll back only their own writes, nest, and honor tx.rollback()", async () => {
    await withOrm(async orm => {
        await orm.insert(users).values({ id: 1, name: "ada" })
        await assert.rejects(
            orm.transaction(async tx => {
                await tx.insert(users).values({ id: 2, name: "grace" })
                await tx.insert(users).values({ id: 3, name: "ada" })
            }),
            uniqueViolation
        )
        await orm.transaction(async tx => {
            await tx.insert(users).values({ id: 4, name: "linus" })
            await tx
                .transaction(async nested => {
                    await nested.insert(users).values({ id: 5, name: "nested" })
                    nested.rollback()
                })
                .catch((error: unknown) => assert.ok(error instanceof TransactionRollbackError))
        })
        assert.deepEqual(
            (await orm.select({ name: users.name }).from(users).orderBy(users.id)).map(row => row.name),
            ["ada", "linus"]
        )
    })
})

test("batches apply every statement or none", async () => {
    await withOrm(async orm => {
        const [inserted, selected] = await orm.batch([
            orm.insert(users).values({ id: 1, name: "ada" }).returning({ id: users.id }),
            orm.select({ name: users.name }).from(users)
        ])
        assert.deepEqual(inserted, [{ id: 1 }])
        assert.deepEqual(selected, [{ name: "ada" }])
        await assert.rejects(
            orm.batch([
                orm.insert(users).values({ id: 2, name: "grace" }),
                orm.insert(users).values({ id: 3, name: "ada" })
            ]),
            uniqueViolation
        )
        assert.deepEqual(await orm.select({ id: users.id }).from(users), [{ id: 1 }])
    })
})

test("migrations apply drizzle-kit output once, in journal order", async () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    const journal = {
        version: "7",
        dialect: "sqlite",
        entries: [
            { idx: 0, version: "6", when: 1700000000000, tag: "0000_users", breakpoints: true },
            { idx: 1, version: "6", when: 1700000001000, tag: "0001_posts", breakpoints: true }
        ]
    }
    const migrations = {
        m0000: "CREATE TABLE `users` (`id` integer PRIMARY KEY);\n--> statement-breakpoint\nCREATE INDEX `users_id` ON `users` (`id`);",
        m0001: "CREATE TABLE `posts` (`id` integer PRIMARY KEY);"
    }
    try {
        await runWithActorDatabase({}, async () => {
            migrate(database, { journal: { ...journal, entries: journal.entries.slice(0, 1) }, migrations })
            migrate(database, { journal: { ...journal, entries: journal.entries.slice(0, 1) }, migrations })
            migrate(database, { journal, migrations })
        })
        database.commit()
        assert.deepEqual(
            host.query(
                "SELECT name FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' AND name NOT GLOB '__*' ORDER BY name"
            ),
            [{ name: "posts" }, { name: "users" }, { name: "users_id" }]
        )
        assert.deepEqual(
            host.query("SELECT created_at FROM __drizzle_migrations ORDER BY id").map(row => Number(row.created_at)),
            [1700000000000, 1700000001000]
        )
        assert.throws(
            () =>
                migrate(database, {
                    journal: { ...journal, entries: [{ ...journal.entries[1]!, idx: 2, when: 1700000002000 }] },
                    migrations
                }),
            /Missing migration m0002 for journal entry 0001_posts/
        )
    } finally {
        host.close()
    }
})

test("runtimes without db.query or db.transaction are rejected with an upgrade message", () => {
    const olderRuntime = { exec: () => [] } as unknown as Parameters<typeof drizzle>[0]
    assert.throws(() => drizzle(olderRuntime), /durable-actors\/drizzle needs db\.query and db\.transaction.*runtime/)
})
