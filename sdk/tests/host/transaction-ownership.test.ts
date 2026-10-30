import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime, DatabaseProxy } from "../fixtures/actor-runtime.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { assertReply } from "../fixtures/reply.js"

function deferred() {
    let resolve!: () => void
    const promise = new Promise<void>(done => (resolve = done))
    return { promise, resolve }
}

test("overlapping sibling transactions are rejected before they can write", async () => {
    const host = new HostDatabase()
    const db = new DatabaseProxy(host.call)
    try {
        db.exec("CREATE TABLE items (name TEXT)")
        const finish = deferred()
        const first = db.transaction(async () => {
            db.exec("INSERT INTO items VALUES ('failed')")
            await finish.promise
            throw new Error("first failed")
        })
        const failed = assert.rejects(first, /first failed/)
        await assert.rejects(
            db.transaction(async () => {
                db.exec("INSERT INTO items VALUES ('overlapping')")
            }),
            /in use by a db.transaction/
        )
        assert.throws(() => db.exec("SELECT * FROM items"), /in use by a db.transaction/)
        assert.throws(() => db.persistField("count", 99), /in use by a db.transaction/)
        finish.resolve()
        await failed
        await db.transaction(async () => db.exec("INSERT INTO items VALUES ('kept')"))
        assert.deepEqual(db.exec("SELECT name FROM items"), [{ name: "kept" }])
    } finally {
        host.close()
    }
})

test("nested sibling transactions cannot close one another's savepoints", async () => {
    const host = new HostDatabase()
    const db = new DatabaseProxy(host.call)
    try {
        db.exec("CREATE TABLE items (name TEXT)")
        await db.transaction(async () => {
            db.exec("INSERT INTO items VALUES ('outer')")
            const finish = deferred()
            const nested = db.transaction(async () => {
                db.exec("INSERT INTO items VALUES ('inner')")
                await finish.promise
            })
            await assert.rejects(
                db.transaction(async () => {}),
                /in use by a db.transaction/
            )
            finish.resolve()
            await nested
        })
        assert.deepEqual(db.exec("SELECT name FROM items"), [{ name: "outer" }, { name: "inner" }])
    } finally {
        host.close()
    }
})

test("unawaited nested transactions roll back and cannot write after their parent exits", async () => {
    const host = new HostDatabase()
    const db = new DatabaseProxy(host.call)
    const finish = deferred()
    let child!: Promise<unknown>
    try {
        db.exec("CREATE TABLE items (name TEXT)")
        await assert.rejects(
            db.transaction(async () => {
                db.exec("INSERT INTO items VALUES ('outer')")
                child = db.transaction(async () => {
                    db.exec("INSERT INTO items VALUES ('inner')")
                    await finish.promise
                    db.exec("INSERT INTO items VALUES ('late')")
                })
            }),
            /await nested db.transaction/
        )
        const failed = assert.rejects(child, /in use by a db.transaction/)
        finish.resolve()
        await failed
        assert.deepEqual(db.exec("SELECT name FROM items"), [])
        await db.transaction(async () => db.exec("INSERT INTO items VALUES ('next')"))
        assert.deepEqual(db.exec("SELECT name FROM items"), [{ name: "next" }])
    } finally {
        host.close()
    }
})

test("detached work cannot use a transaction after it has committed", async () => {
    const host = new HostDatabase()
    const db = new DatabaseProxy(host.call)
    const finish = deferred()
    let detached!: Promise<unknown>
    try {
        db.exec("CREATE TABLE items (name TEXT)")
        await db.transaction(async () => {
            db.exec("INSERT INTO items VALUES ('kept')")
            detached = finish.promise.then(() => db.exec("INSERT INTO items VALUES ('late')"))
        })
        const failed = assert.rejects(detached, /in use by a db.transaction/)
        finish.resolve()
        await failed
        assert.deepEqual(db.exec("SELECT name FROM items"), [{ name: "kept" }])
    } finally {
        host.close()
    }
})

test("transactions support awaits and fetch before committing", async () => {
    const host = new HostDatabase()
    const db = new DatabaseProxy(host.call)
    try {
        db.exec("CREATE TABLE items (name TEXT)")
        const commits = host.commits
        await db.transaction(async () => {
            db.exec("INSERT INTO items VALUES ('first')")
            const response = await fetch("data:text/plain,second")
            db.exec("INSERT INTO items VALUES (?)", await response.text())
            assert.equal(host.commits, commits)
        })
        assert.equal(host.commits, commits + 1)
        assert.deepEqual(db.exec("SELECT name FROM items"), [{ name: "first" }, { name: "second" }])
    } finally {
        host.close()
    }
})

test("rollback restores assigned fields without replacing untouched objects", async t => {
    const host = new HostDatabase()
    class OwnershipWallet extends Actor {
        balance = 10
        settings = { enabled: true }
        async spend() {
            const settings = this.settings
            this.settings.enabled = false
            await assert.rejects(
                this.db.transaction(async () => {
                    this.balance = 5
                    throw new Error("cancel")
                }),
                /cancel/
            )
            assert.equal(this.settings, settings)
            assert.equal(this.settings.enabled, false)
            return this.balance
        }
    }
    const runtime = new ActorRuntime(
        registerActorClass(OwnershipWallet, {
            actorName: "OwnershipWallet",
            fields: ["balance", "settings"].map(name => ({ name, persistence: Persistence.Persisted }))
        }),
        () => {},
        undefined,
        undefined,
        new DatabaseProxy(host.call)
    )
    t.after(async () => {
        await runtime.close(true)
        host.close()
    })
    assertReply(
        await runtime.handle({
            type: "invoke",
            request_id: "spend",
            actor: { project_id: "test", actor_name: "OwnershipWallet", actor_id: "one" },
            method: "spend",
            args: []
        }),
        { type: "invoked", result: 10 }
    )
})

test("transaction commands travel as raw SQL over exec", async () => {
    const host = new HostDatabase()
    const db = new DatabaseProxy(host.call)
    try {
        await db.transaction(async () => {
            await db.transaction(async () => {})
        })
        assert.deepEqual(
            host.calls.map(call => call.op),
            ["exec", "exec", "exec", "exec"]
        )
        assert.deepEqual(
            host.calls.map(call => (call.op === "exec" ? call.sql : "")),
            ["BEGIN", "SAVEPOINT actor_transaction_2", "RELEASE actor_transaction_2", "COMMIT"]
        )
    } finally {
        host.close()
    }
})

test("a SQLite rollback of the whole transaction invalidates every nested scope", async () => {
    const host = new HostDatabase()
    const db = new DatabaseProxy(host.call)
    try {
        db.exec("CREATE TABLE items (name TEXT UNIQUE)")
        await assert.rejects(
            db.transaction(async () => {
                db.exec("INSERT INTO items VALUES ('outer')")
                await assert.rejects(
                    db.transaction(async () => {
                        db.exec("INSERT OR ROLLBACK INTO items VALUES ('outer')")
                    })
                )
                assert.throws(() => db.exec("INSERT INTO items VALUES ('escaped')"), /in use by a db.transaction/)
            }),
            /await nested db.transaction/
        )
        assert.deepEqual(db.exec("SELECT * FROM items"), [])
        await db.transaction(async () => db.exec("INSERT INTO items VALUES ('next')"))
        assert.deepEqual(db.exec("SELECT * FROM items"), [{ name: "next" }])
    } finally {
        host.close()
    }
})
