import { resolve } from "node:path"
import { fileURLToPath } from "node:url"

export function ticketFromPullRequest(branch, body) {
    const tickets = new Set([...branchTickets(branch), ...descriptionTickets(body)].map(ticket => ticket.toUpperCase()))
    if (tickets.size !== 1) throw new Error("The PR must reference exactly one DUR ticket in its branch, for example codex/DUR-123-fix-reconnect, or in its description as Fixes DUR-123. A maintainer can add the ticket ID to the description.")
    return [...tickets][0]
}

function branchTickets(branch) {
    const tickets = [...(branch ?? "").matchAll(/(?:^|[/-])(DUR-[1-9]\d*)(?=$|[/-])/gi)].map(match => match[1])
    if (tickets.length > 1) throw new Error("The source branch must contain exactly one DUR ticket, for example codex/DUR-123-fix-reconnect.")
    return tickets
}

// The PR template's guidance comment contains an example "Fixes DUR-123".
function descriptionTickets(body) {
    const text = (body ?? "").replace(/<!--[\s\S]*?-->/g, "")
    return [...text.matchAll(/\b(?:fixes|closes|resolves)\s+(DUR-[1-9]\d*)\b/gi)].map(match => match[1])
}

export function releaseCategory(labels) {
    const categories = labels.filter(label => ["release:feature", "release:fix", "release:skip"].includes(label))
    if (categories.length !== 1) throw new Error("Choose exactly one release label: release:feature, release:fix, or release:skip. A maintainer can apply it for external contributors.")
    return categories[0].slice("release:".length)
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        console.log(`Ticket identifier: ${ticketFromPullRequest(process.env.PR_HEAD_REF, process.env.PR_BODY)}`)
        console.log(`Release category: ${releaseCategory(JSON.parse(process.env.PR_LABELS ?? "[]"))}`)
    } catch (error) {
        console.error(error.message)
        process.exitCode = 1
    }
}
