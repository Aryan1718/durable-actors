import { resolve } from "node:path"
import { fileURLToPath } from "node:url"

export function ticketFromBranch(branch) {
    const tickets = [...(branch ?? "").matchAll(/(?:^|[/-])(DUR-[1-9]\d*)(?=$|[/-])/gi)]
    if (tickets.length !== 1) throw new Error("The source branch must contain exactly one DUR ticket, for example codex/DUR-123-fix-reconnect. Ask a maintainer for the ticket ID if needed.")
    return tickets[0][1].toUpperCase()
}

export function releaseCategory(labels) {
    const categories = labels.filter(label => ["release:feature", "release:fix", "release:skip"].includes(label))
    if (categories.length !== 1) throw new Error("Choose exactly one release label: release:feature, release:fix, or release:skip. A maintainer can apply it for external contributors.")
    return categories[0].slice("release:".length)
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
    try {
        console.log(`Ticket identifier: ${ticketFromBranch(process.env.PR_HEAD_REF)}`)
        console.log(`Release category: ${releaseCategory(JSON.parse(process.env.PR_LABELS ?? "[]"))}`)
    } catch (error) {
        console.error(error.message)
        process.exitCode = 1
    }
}
