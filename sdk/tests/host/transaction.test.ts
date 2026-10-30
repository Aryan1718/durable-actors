import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { runWithActorDatabase } from "../../src/actor/database.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply } from "../fixtures/reply.js"

test("a failed transaction rolls back only its own writes", async () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    await runWithActorDatabase({}, async () => {
        database.exec("CREATE TABLE entries (value INTEGER)")
        database.exec("INSERT INTO entries VALUES (1)")
        await assert.rejects(
            database.transaction(async () => {
                database.exec("INSERT INTO entries VALUES (2)")
                throw new Error("capacity")
            }),
            /capacity/
        )
        database.exec("INSERT INTO entries VALUES (3)")
    })
    database.commit()
    assert.deepEqual(host.query("SELECT value FROM entries ORDER BY value"), [{ value: 1n }, { value: 3n }])
    host.close()
})

test("transactions nest", async () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    await runWithActorDatabase({}, async () => {
        database.exec("CREATE TABLE entries (value INTEGER)")
        const result = await database.transaction(async () => {
            database.exec("INSERT INTO entries VALUES (1)")
            await database
                .transaction(async () => {
                    database.exec("INSERT INTO entries VALUES (2)")
                    throw new Error("inner")
                })
                .catch(() => {})
            database.exec("INSERT INTO entries VALUES (3)")
            return "outer"
        })
        assert.equal(result, "outer")
    })
    database.commit()
    assert.deepEqual(host.query("SELECT value FROM entries ORDER BY value"), [{ value: 1n }, { value: 3n }])
    host.close()
})

test("immediate writes inside a transaction commit when it is released", async () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    await runWithActorDatabase({}, async () => {
        database.exec("CREATE TABLE entries (value INTEGER)")
        const committed = host.commits
        await database.transaction(async () => {
            database.exec("INSERT INTO entries VALUES (1)")
            database.exec("INSERT INTO entries VALUES (2)")
            assert.equal(host.commits, committed)
        })
        assert.equal(host.commits, committed + 1)
        await database
            .transaction(async () => {
                database.exec("INSERT INTO entries VALUES (3)")
                throw new Error("abort")
            })
            .catch(() => {})
    })
    database.rollback()
    assert.deepEqual(host.query("SELECT value FROM entries ORDER BY value"), [{ value: 1n }, { value: 2n }])
    host.close()
})

test("a failed transaction restores @Persisted fields and the call still succeeds", async t => {
    const host = new HostDatabase()
    class Wallet extends Actor {
        balance = 10

        async spend(amount: number) {
            return this.db.transaction(async () => {
                await this.db
                    .transaction(async () => {
                        this.balance -= amount
                        if (this.balance < 0) throw new Error("insufficient funds")
                    })
                    .catch(() => {})
                return this.balance
            })
        }
    }
    const runtime = new ActorRuntime(
        registerActorClass(Wallet, {
            actorName: "Wallet",
            fields: [{ name: "balance", persistence: Persistence.Persisted }]
        }),
        () => {},
        undefined,
        undefined,
        new DatabaseProxy(host.call)
    )
    t.after(() => runtime.close(true))
    const spend = (amount: number) =>
        runtime.handle({
            type: "invoke",
            request_id: `spend-${amount}`,
            actor: { project_id: "test", actor_name: "Wallet", actor_id: "one" },
            method: "spend",
            args: [amount]
        })
    assertReply(await spend(4), { type: "invoked", result: 6 })
    assertReply(await spend(50), { type: "invoked", result: 6 })
    assert.deepEqual(host.fields(), { balance: 6 })
})

test("interleaved actors cannot open transactions", async t => {
    class Feed extends Actor {
        async post() {
            await this.db.transaction(async () => {})
        }
    }
    const runtime = new ActorRuntime(
        registerActorClass(Feed, { actorName: "Feed", fields: [], interleaveMethods: ["post"] }),
        () => {}
    )
    t.after(() => runtime.close(true))
    const reply = await runtime.handle({
        type: "invoke",
        request_id: "post",
        actor: { project_id: "test", actor_name: "Feed", actor_id: "one" },
        method: "post",
        args: []
    })
    assert.equal(reply.type, "failed")
    assert.match((reply as { message: string }).message, /uses @Interleave/)
})
