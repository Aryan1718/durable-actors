import type { StreamingAgent } from "./chat.js"

interface ChatSocket extends Pick<EventTarget, "addEventListener"> {
    readonly readyState: number
    send(text: string): void
    close(): void
}

interface PendingReply {
    update(text: string): void
    resolve(text: string): void
    reject(error: Error): void
}

export class SocketChat implements StreamingAgent {
    readonly ready: Promise<void>
    readonly closed: Promise<void>
    private pending: PendingReply | undefined

    constructor(private readonly socket: ChatSocket) {
        this.ready = new Promise((resolve, reject) => {
            socket.addEventListener("open", () => resolve())
            socket.addEventListener("error", () => reject(new Error("Could not connect to the actor")))
            socket.addEventListener("close", () => reject(new Error("Connection closed")))
        })
        this.closed = new Promise(resolve => {
            socket.addEventListener("close", () => { this.fail(new Error("Connection closed")); resolve() })
        })
        socket.addEventListener("message", event => this.receive((event as MessageEvent).data))
        socket.addEventListener("error", () => { this.fail(new Error("Actor connection failed")); socket.close() })
    }

    get connected(): boolean {
        return this.socket.readyState === 1
    }

    async prompt(text: string, update: (text: string) => void): Promise<string> {
        await this.ready
        if (!this.connected) throw new Error("Connection closed")
        if (this.pending) throw new Error("A reply is already in progress")
        return new Promise((resolve, reject) => {
            this.pending = { update, resolve, reject }
            try { this.socket.send(JSON.stringify(text)) }
            catch (error) { this.fail(error instanceof Error ? error : new Error(String(error))) }
        })
    }

    close(): void {
        this.fail(new Error("Chat closed"))
        this.socket.close()
    }

    private receive(data: unknown): void {
        if (!this.pending) return
        try {
            const event: unknown = JSON.parse(String(data))
            if (!event || typeof event !== "object" || !("type" in event)) return
            if ((event.type === "text" || event.type === "done") && "text" in event && typeof event.text === "string") {
                if (event.type === "text") this.pending.update(event.text)
                else { const pending = this.pending; this.pending = undefined; pending.resolve(event.text) }
            } else if (event.type === "error" && "message" in event && typeof event.message === "string") {
                this.fail(new Error(event.message))
            }
        } catch {
            this.fail(new Error("Could not read the actor's reply"))
        }
    }

    private fail(error: Error): void {
        const pending = this.pending
        this.pending = undefined
        pending?.reject(error)
    }
}
