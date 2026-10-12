import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorProtocolError } from "../../src/errors.js"
import { parseActorSessionServerMessage } from "../../src/host/protocol.js"

test("rejects malformed actor session messages", () => {
    assert.throws(
        () =>
            parseActorSessionServerMessage(
                '{"type":"command","message_id":1,"command":{"type":"invoke","request_id":false}}'
            ),
        ActorProtocolError
    )
})

test("parses SQLite persistence acknowledgments", () => {
    for (const fields of [{ txid: 42 }, { txid: 42, state_version: 7 }, { error: "durable write failed" }]) {
        const message = { type: "sqlite_persisted", message_id: 1, commit_id: 2, ...fields }
        assert.deepEqual(parseActorSessionServerMessage(JSON.stringify(message)), message)
    }
})

test("rejects incomplete SQLite persistence acknowledgments", () => {
    for (const fields of [{}, { state_version: 7 }, { txid: 0, state_version: 7 }]) {
        assert.throws(
            () =>
                parseActorSessionServerMessage(
                    JSON.stringify({
                        type: "sqlite_persisted",
                        message_id: 1,
                        commit_id: 2,
                        ...fields
                    })
                ),
            ActorProtocolError
        )
    }
})
