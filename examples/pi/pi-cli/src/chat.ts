export interface StreamingAgent {
    readonly ready: Promise<void>
    readonly closed: Promise<void>
    readonly connected: boolean
    prompt(text: string, update: (text: string) => void): Promise<string>
    close(): void
}

