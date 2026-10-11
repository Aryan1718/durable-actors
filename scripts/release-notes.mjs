import { execFileSync } from "node:child_process"
import { resolve } from "node:path"
import { fileURLToPath } from "node:url"

export function formatReleaseNotes(body) {
    const sections = { Features: [], Fixes: [] }
    let section
    for (const line of body.split(/\r?\n/)) {
        const heading = /^#{2,3} (.+)$/.exec(line)
        if (heading) section = heading[1]
        const bullet = /^[*-] (.+)$/.exec(line)
        if (!bullet || section === "New Contributors") continue
        if (!Object.hasOwn(sections, section)) throw new Error(`Classify this PR with release:feature, release:fix, or release:skip before releasing: ${bullet[1]}`)
        sections[section].push(releaseBullet(bullet[1]))
    }
    return (
        Object.entries(sections)
            .map(([title, lines]) => [`## ${title}`, lines.join("\n")].filter(Boolean).join("\n\n"))
            .join("\n\n") + "\n"
    )
}

function releaseBullet(change) {
    const match = /^(.+) by @\S+ in (https:\/\/github\.com\/TerseAI\/durable-actors\/pull\/([1-9]\d*))$/.exec(change)
    if (!match) throw new Error(`Could not read the PR link from this generated change: ${change}`)
    return `- ${match[1]} ([#${match[3]}](${match[2]}))`
}

function main() {
    const [previous, tag, sha, ...extra] = process.argv.slice(2)
    if (!previous || !tag || !sha || extra.length) throw new Error("Usage: node scripts/release-notes.mjs PREVIOUS_TAG NEW_TAG TARGET_SHA")
    const generated = execFileSync(
        "gh",
        [
            "api",
            "repos/TerseAI/durable-actors/releases/generate-notes",
            "-f",
            `tag_name=${tag}`,
            "-f",
            `target_commitish=${sha}`,
            "-f",
            `previous_tag_name=${previous}`,
            "-f",
            "configuration_file_path=.github/release.yml"
        ],
        { encoding: "utf8", timeout: 60000 }
    )
    process.stdout.write(formatReleaseNotes(JSON.parse(generated).body))
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        main()
    } catch (error) {
        console.error(error.message)
        process.exitCode = 1
    }
}
