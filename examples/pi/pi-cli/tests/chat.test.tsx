import { afterEach, expect, test } from "bun:test"
import { cleanup, render } from "ink-testing-library"
import { setTimeout as delay } from "node:timers/promises"

import { ChatView } from "../src/ChatView.js"
import type { StreamingAgent } from "../src/chat.js"

afterEach(cleanup)

test("chat shows partial replies while waiting for completion", async () => {
    let update!: (text: string) => void
    let finish!: (text: string) => void
    const messages: string[] = []
    const agent: StreamingAgent = {
        ready: Promise.resolve(), closed: new Promise(() => {}), connected: true,
        prompt(text, onText) {
            messages.push(text)
            update = onText
            return new Promise(resolve => { finish = resolve })
        },
        close() {}
    }
    const app = render(<ChatView agent={agent} />)
    await delay(30)
    app.stdin.write("Hello")
    await delay(30)
    app.stdin.write("\r")
    await delay(30)
    expect(messages).toEqual(["Hello"])
    expect(app.lastFrame()).toContain("Thinking...")
    update("A streamed")
    await delay(30)
    expect(app.lastFrame()).toContain("A streamed")
    finish("A streamed reply")
    await delay(30)
    expect(app.frames.join("\n")).toContain("A streamed reply")
})

test("chat shows a failed turn and accepts the next prompt", async () => {
    let calls = 0
    const agent: StreamingAgent = {
        ready: Promise.resolve(), closed: new Promise(() => {}), connected: true,
        async prompt() { if (++calls === 1) throw new Error("Model unavailable"); return "Recovered" },
        close() {}
    }
    const app = render(<ChatView agent={agent} />)
    await delay(30)
    for (const text of ["Hello", "Again"]) {
        app.stdin.write(text)
        await delay(30)
        app.stdin.write("\r")
        await delay(30)
    }
    expect(app.frames.join("\n")).toContain("Model unavailable")
    expect(app.frames.join("\n")).toContain("Recovered")
})
