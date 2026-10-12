import { Actor, Persisted } from "durable-actors"
import type { ActorSocket } from "durable-actors"

export class ChatRoom extends Actor<Member, string, ChatMessage[]> {
    @Persisted history: ChatMessage[] = []

    async onConnect(socket: ActorSocket<Member, ChatMessage[]>) {
        socket.send(this.history)
    }

    async onMessage(socket: ActorSocket<Member, ChatMessage[]>, text: string) {
        this.history = [...this.history, { name: socket.metadata.name, text }]
        this.broadcast(this.history)
    }
}

type Member = { name: string }
type ChatMessage = { name: string; text: string }
