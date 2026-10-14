import { expect, test } from "bun:test"

import { SocketChat } from "../src/socket.js"

class TestSocket extends EventTarget {
    readyState = 0
    sent: string[] = []
    send(message: string) { this.sent.push(message) }
    close() { this.readyState = 3; this.dispatchEvent(new Event("close")) }
    open() { this.readyState = 1; this.dispatchEvent(new Event("open")) }
    receive(data: unknown) { this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(data) })) }
}

test("receives partial replies before the final WebSocket message", async () => {
    const socket = new TestSocket()
    const chat = new SocketChat(socket)
    socket.open()
    const updates: string[] = []
    const reply = chat.prompt("Hello", text => updates.push(text))
    await Promise.resolve()
    expect(socket.sent).toEqual([JSON.stringify("Hello")])
    socket.receive({ type: "text", text: "Hel" })
    socket.receive({ type: "text", text: "Hello" })
    expect(updates).toEqual(["Hel", "Hello"])
    socket.receive({ type: "done", text: "Hello Alice" })
    expect(await reply).toBe("Hello Alice")
    chat.close()
})

test("a model error rejects the current turn and leaves the connection usable", async () => {
    const socket = new TestSocket()
    const chat = new SocketChat(socket)
    socket.open()
    const reply = chat.prompt("Hello", () => {})
    await Promise.resolve()
    socket.receive({ type: "error", message: "Model unavailable" })
    await expect(reply).rejects.toThrow("Model unavailable")
    expect(chat.connected).toBe(true)
    chat.close()
})

test("disconnecting during a reply rejects the waiting turn", async () => {
    const socket = new TestSocket()
    const chat = new SocketChat(socket)
    socket.open()
    const reply = chat.prompt("Hello", () => {})
    await Promise.resolve()
    socket.close()
    await expect(reply).rejects.toThrow("Connection closed")
    await chat.closed
    expect(chat.connected).toBe(false)
})
