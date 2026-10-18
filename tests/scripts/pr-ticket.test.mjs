import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import test from "node:test"

import { releaseCategory, ticketFromPullRequest } from "../../scripts/check-pr-ticket.mjs"

test("accepts a single DUR ticket in lit, contributor, and release branches", () => {
    for (const branch of ["olivier/DUR-2-fix", "codex/DUR-42-release-0.9.0", "DUR-123", "fix/dur-15-reconnect"]) {
        assert.match(ticketFromPullRequest(branch, ""), /^DUR-[1-9]\d*$/)
    }
})

test("rejects missing, wrong-team, malformed, or ambiguous ticket identifiers", () => {
    for (const branch of [undefined, "", "fix/reconnect", "codex/release-0.9.0", "thomas/TER-765-fix", "DUR-0-fix", "DUR-01-fix", "NOTDUR-42-fix", "DUR-42oops", "DUR-1-and-DUR-2"]) {
        assert.throws(() => ticketFromPullRequest(branch, ""), /exactly one DUR ticket/)
    }
})

test("accepts a ticket from the PR description when the branch has none", () => {
    for (const body of ["Fixes DUR-7", "Some context.\n\ncloses dur-7\nCloses #12", "Resolves DUR-7 and fixes DUR-7"]) {
        assert.equal(ticketFromPullRequest("fix/cli-inspect", body), "DUR-7")
    }
    assert.equal(ticketFromPullRequest("olivier/DUR-7-fix", "Fixes DUR-7"), "DUR-7")
})

test("rejects descriptions with no, conflicting, or template-only ticket references", () => {
    for (const [branch, body] of [
        ["fix/cli-inspect", null],
        ["fix/cli-inspect", "Related to DUR-7"],
        ["fix/cli-inspect", "Fixes DUR-7\nFixes DUR-8"],
        ["olivier/DUR-7-fix", "Fixes DUR-8"],
        ["fix/cli-inspect", "<!-- Include Fixes DUR-123 and Closes #<GitHub issue> below. -->"]
    ]) {
        assert.throws(() => ticketFromPullRequest(branch, body), /exactly one DUR ticket/)
    }
})

test("CLI fails PRs without a ticket and reports valid ticket identifiers", () => {
    const script = new URL("../../scripts/check-pr-ticket.mjs", import.meta.url)
    for (const [branch, body, status] of [
        ["fix/no-ticket", "", 1],
        ["codex/DUR-42-fix", "", 0],
        ["fix/no-ticket", "Fixes DUR-42", 0]
    ]) {
        const result = spawnSync(process.execPath, [script.pathname], { env: { ...process.env, PR_HEAD_REF: branch, PR_BODY: body, PR_LABELS: '["release:fix"]' }, encoding: "utf8" })
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
