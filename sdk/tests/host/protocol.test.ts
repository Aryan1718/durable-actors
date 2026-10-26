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

test("parses database results", () => {
    for (const fields of [
        {
            result: {
                type: "rows",
                columns: ["value"],
                rows: [
                    [{ type: "integer", value: "9007199254740993" }, { type: "blob", value: "AAH/" }, { type: "null" }]
                ]
            }
        },
        { result: { type: "fields", fields: { count: 1 } } },
        { result: { type: "alarm", timestamp_ms: 1_000 } },
        { result: { type: "done" } },
        { error: "storage rejected write" }
    ]) {
        const message = { type: "database_result", call_id: 1, ...fields }
        assert.deepEqual(parseActorSessionServerMessage(JSON.stringify(message)), message)
    }
})

test("rejects ambiguous or malformed database results", () => {
    for (const fields of [
        {},
        { result: { type: "done" }, error: "both" },
        { result: { type: "rows", columns: ["value"], rows: [[{ type: "integer", value: "1.5" }]] } }
    ]) {
        assert.throws(
            () => parseActorSessionServerMessage(JSON.stringify({ type: "database_result", call_id: 1, ...fields })),
            ActorProtocolError
        )
    }
})
