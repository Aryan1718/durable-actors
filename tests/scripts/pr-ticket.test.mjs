import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import test from "node:test"

import { releaseCategory, ticketFromBranch } from "../../scripts/check-pr-ticket.mjs"

test("accepts a single DUR ticket in lit, contributor, and release branches", () => {
    for (const branch of ["olivier/DUR-2-fix", "codex/DUR-42-release-0.9.0", "DUR-123", "fix/dur-15-reconnect"]) {
        assert.match(ticketFromBranch(branch), /^DUR-[1-9]\d*$/)
    }
})

test("rejects missing, wrong-team, malformed, or ambiguous ticket identifiers", () => {
    for (const branch of [undefined, "", "fix/reconnect", "codex/release-0.9.0", "thomas/TER-765-fix", "DUR-0-fix", "DUR-01-fix", "NOTDUR-42-fix", "DUR-42oops", "DUR-1-and-DUR-2"]) {
        assert.throws(() => ticketFromBranch(branch), /exactly one DUR ticket/)
    }
})

test("CLI fails invalid PR branches and reports valid ticket identifiers", () => {
    const script = new URL("../../scripts/check-pr-ticket.mjs", import.meta.url)
    for (const [branch, status] of [
        ["fix/no-ticket", 1],
        ["codex/DUR-42-fix", 0]
    ]) {
        const result = spawnSync(process.execPath, [script.pathname], { env: { ...process.env, PR_HEAD_REF: branch, PR_LABELS: '["release:fix"]' }, encoding: "utf8" })
        assert.equal(result.status, status)
        assert.match(status === 0 ? result.stdout : result.stderr, status === 0 ? /DUR-42/ : /exactly one DUR ticket/)
    }
})

test("requires exactly one release category while allowing unrelated labels", () => {
    for (const category of ["feature", "fix", "skip"]) {
        assert.equal(releaseCategory(["help wanted", `release:${category}`]), category)
    }
    for (const labels of [[], ["bug"], ["release:feature", "release:fix"], ["release:skip", "release:fix"]]) {
        assert.throws(() => releaseCategory(labels), /exactly one release label/)
    }
})

test("CLI blocks a tracked branch without a release category", () => {
    const script = new URL("../../scripts/check-pr-ticket.mjs", import.meta.url)
    const result = spawnSync(process.execPath, [script.pathname], { env: { ...process.env, PR_HEAD_REF: "codex/DUR-42-fix", PR_LABELS: "[]" }, encoding: "utf8" })
    assert.equal(result.status, 1)
    assert.match(result.stderr, /exactly one release label/)
})
