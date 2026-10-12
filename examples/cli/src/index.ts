import { actors } from "../generated/index.js"

const [command, ...args] = process.argv.slice(2)
const suite = actors.PersistenceTests.get("demo")
if (command === "atomic-test" && (args.length === 0 || (args.length === 1 && args[0] === "--fail"))) {
    await suite.testAtomic(args[0] === "--fail")
    console.log("Crash marker cleared.")
} else if (command === "immediate-test" && args.length === 0) {
    await suite.testImmediate()
} else if (command === "crash-write" && args.length === 1 && args[0].trim()) {
    await suite.writeThenCrash(args[0])
} else if (command === "atomic-crash-write" && args.length === 1 && args[0].trim()) {
    await suite.atomicWriteThenCrash(args[0])
} else if (command === "crash-read" && args.length === 0) {
    console.log(await suite.getCrashMarker())
} else if (command === "interleave-test" && args.length === 0) {
    await suite.getEvents()
    await Promise.all([suite.testInterleave("slow", 3000), suite.testInterleave("fast", 100)])
    console.log(await suite.getEvents())
} else if (command === "events-read" && args.length === 0) {
    console.log(await suite.getEvents())
} else {
    console.error("Usage: bun run --bun start atomic-test [--fail] | immediate-test | crash-write <marker> | atomic-crash-write <marker> | crash-read | interleave-test | events-read")
    process.exit(1)
}
