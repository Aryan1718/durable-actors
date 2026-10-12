import assert from "node:assert/strict"
import test from "node:test"

import { formatReleaseNotes, notesTarget } from "../../scripts/release-notes.mjs"

test("preserves categorized changes and links with only the two requested sections", () => {
    const generated =
        "## What's Changed\n### Features\n* Add actor streaming by @sam in https://github.com/TerseAI/durable-actors/pull/10\n### Fixes\n* Fix reconnect by @oli in https://github.com/TerseAI/durable-actors/pull/11\n\n## New Contributors\n* @sam made their first contribution\n\n**Full Changelog**: https://github.com/TerseAI/durable-actors/compare/v0.8.0...v0.9.0"
    assert.equal(
        formatReleaseNotes(generated),
        "## Features\n\n- Add actor streaming ([#10](https://github.com/TerseAI/durable-actors/pull/10))\n\n## Fixes\n\n- Fix reconnect ([#11](https://github.com/TerseAI/durable-actors/pull/11))\n"
    )
})

test("keeps an empty section without inventing changes", () => {
    assert.equal(
        formatReleaseNotes("## What's Changed\n### Fixes\n* Fix reconnect by @oli in https://github.com/TerseAI/durable-actors/pull/11\n"),
        "## Features\n\n## Fixes\n\n- Fix reconnect ([#11](https://github.com/TerseAI/durable-actors/pull/11))\n"
    )
})

test("preserves attribution-like text inside titles and supports bot authors", () => {
    assert.match(
        formatReleaseNotes("### Fixes\n* Handle requests by @sam in actor logs by @dependabot[bot] in https://github.com/TerseAI/durable-actors/pull/12\n"),
        /- Handle requests by @sam in actor logs \(\[#12\]\(https:\/\/github.com\/TerseAI\/durable-actors\/pull\/12\)\)/
    )
})

test("rejects changes without a recognizable PR link", () => {
    assert.throws(() => formatReleaseNotes("### Features\n* Add streaming\n"), /PR link/)
})

test("unclassified changes block release notes instead of silently disappearing", () => {
    assert.throws(() => formatReleaseNotes("## What's Changed\n### Uncategorized\n* Reconnect (#11)\n"), /Classify.*Reconnect/)
    assert.throws(() => formatReleaseNotes("## What's Changed\n* Missing category (#11)\n"), /Classify/)
})

test("prepares notes from origin/main against the latest stable release", () => {
    const head = "a".repeat(40)
    assert.deepEqual(
        notesTarget({
            head,
            releases: [
                { tag_name: "v0.9.0", draft: false, prerelease: false },
                { tag_name: "v0.10.0", draft: true, prerelease: false },
                { tag_name: "v0.8.0", draft: false, prerelease: false },
                { tag_name: "v1.0.0-rc.1", draft: false, prerelease: true },
                { tag_name: "nightly", draft: false, prerelease: false }
            ]
        }),
        { previous: "v0.9.0", tag: head, sha: head }
    )
})

test("refuses notes when main is not a commit or no stable release exists", () => {
    assert.throws(() => notesTarget({ head: "main", releases: [{ tag_name: "v0.9.0", draft: false, prerelease: false }] }), /commit/)
    assert.throws(() => notesTarget({ head: "b".repeat(40), releases: [] }), /stable release/)
})
