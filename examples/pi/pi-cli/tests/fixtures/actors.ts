import { Actor, Persisted } from "durable-actors"

export class PiAgent extends Actor {
    @Persisted private messages: string[] = []

    async prompt(text: string): Promise<string> {
        this.messages = [...this.messages, text]
        return this.messages.join(" | ")
    }
}
