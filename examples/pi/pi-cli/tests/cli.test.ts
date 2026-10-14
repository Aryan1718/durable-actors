import { expect, test } from "bun:test"

import { PiCli, type PromptAgent } from "../src/cli.js"

function setup(agent: PromptAgent) {
    const replies: string[] = []
    const command = new PiCli(agent, text => replies.push(text)).createCommand()
    for (const item of [command, ...command.commands]) item.exitOverride().configureOutput({ writeOut: () => {}, writeErr: () => {} })
    return { command, replies }
}

test("send forwards the prompt and prints the actor's reply", async () => {
    const messages: string[] = []
    const { command, replies } = setup({
        async prompt(text) {
            messages.push(text)
            return "Hello Alice"
        }
    })
    await command.parseAsync(["send", "  Hello Alice  "], { from: "user" })
    expect(messages).toEqual(["Hello Alice"])
    expect(replies).toEqual(["Hello Alice"])
})

test("send requires a nonempty prompt before contacting the actor", async () => {
    let calls = 0
    for (const args of [["send"], ["send", "  "]]) {
        const { command } = setup({
            async prompt() {
                calls++
                return "unused"
            }
        })
        await expect(command.parseAsync(args, { from: "user" })).rejects.toMatchObject({ exitCode: 1 })
    }
    expect(calls).toBe(0)
})

test("send surfaces failures from the actor", async () => {
    const failure = new Error("Model request failed")
    const { command, replies } = setup({
        async prompt() {
            throw failure
        }
    })
    await expect(command.parseAsync(["send", "Hello"], { from: "user" })).rejects.toBe(failure)
    expect(replies).toEqual([])
})
