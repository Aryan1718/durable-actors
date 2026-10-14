import assert from "node:assert/strict"
import { test } from "node:test"

import { runWithActorDatabase } from "../../src/actor/database.js"
import { DatabaseCommitError, DatabaseProxy } from "../../src/host/database.js"
import type { DatabaseCall, DatabaseResult } from "../../src/host/database.js"
import { HostDatabase } from "../fixtures/host-database.js"

test("values keep their SQLite storage classes across the host boundary", () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    database.exec("CREATE TABLE value_types (value)")
    const values = [null, 9007199254740993n, 7, 1.5, "text", new Uint8Array([0, 1, 255])]
    for (const value of values) database.exec("INSERT INTO value_types VALUES (?)", value)
    assert.deepEqual(database.exec("SELECT value, typeof(value) AS type FROM value_types ORDER BY rowid"), [
        { value: null, type: "null" },
        { value: 9007199254740993n, type: "integer" },
        { value: 7, type: "integer" },
        { value: 1.5, type: "real" },
        { value: "text", type: "text" },
        { value: new Uint8Array([0, 1, 255]), type: "blob" }
    ])
    assert.deepEqual((host.calls[1] as Extract<DatabaseCall, { op: "exec" }>).bindings, [{ type: "null" }])
    assert.deepEqual((host.calls[2] as Extract<DatabaseCall, { op: "exec" }>).bindings, [
        { type: "integer", value: "9007199254740993" }
    ])
    host.close()
})

test("immediate mode asks the host to commit each write", async () => {
    const host = new HostDatabase()
    const database = new DatabaseProxy(host.call)
    await runWithActorDatabase({}, "immediate", async () => {
        database.exec("CREATE TABLE entries (value INTEGER)")
        database.persistField("count", 1)
    })
    await runWithActorDatabase({}, "atomic", async () => {
        database.persistField("count", 2)
        database.persistField("count", undefined)
    })
    assert.deepEqual(
        host.calls.map(call => ("immediate" in call ? [call.op, call.immediate] : [call.op])),
        [
            ["exec", true],
            ["persist_field", true],
            ["persist_field", false],
            ["delete_field", false]
        ]
    )
    assert.equal(host.commits, 2)
    database.rollback()
    assert.deepEqual(database.fields(), { count: 1 })
    host.close()
})

test("host errors surface to actor code and commit failures are distinguishable", () => {
    const database = new DatabaseProxy((call: DatabaseCall): DatabaseResult => {
        throw new Error(call.op === "commit" ? "replication failed" : "SQLite runtime table names are reserved")
    })
    assert.throws(() => database.exec("SELECT * FROM __terse_fields"), /SQLite runtime table names are reserved/)
    assert.throws(() => database.commit(), DatabaseCommitError)
})

test("unexpected host results are rejected", () => {
    const database = new DatabaseProxy(() => ({ type: "done" }))
    assert.throws(() => database.exec("SELECT 1"), /unexpected actor database result/)
})
