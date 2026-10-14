import assert from "node:assert/strict"

import type { ActorExecutorReply } from "../../src/host/protocol.js"

export function assertReply(actual: ActorExecutorReply, expected: object): void {
    assert.deepEqual(actual, expected)
}
