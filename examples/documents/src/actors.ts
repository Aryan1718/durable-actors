import { fromBase64, toBase64 } from "lib0/buffer"
import { Actor, Persisted } from "durable-actors"
import type { ActorSocket } from "durable-actors"
import * as Y from "yjs"

export class Workspace extends Actor<null, DocumentInfo, DocumentInfo[]> {
    @Persisted documents: DocumentInfo[] = [{ id: "welcome", title: "Welcome" }]

    async onConnect(socket: ActorSocket<null, DocumentInfo[]>) {
        socket.send(this.documents)
    }

    async onMessage(_socket: ActorSocket<null, DocumentInfo[]>, document: DocumentInfo) {
        if (!this.documents.some(item => item.id === document.id)) this.documents = [...this.documents, document]
        this.broadcast(this.documents)
    }
}

export class Document extends Actor<null, string, string> {
    @Persisted content = emptyDocument()

    async onConnect(socket: ActorSocket<null, string>) {
        socket.send(this.content)
    }

    async onMessage(_socket: ActorSocket<null, string>, update: string) {
        const document = new Y.Doc()
        try {
            Y.applyUpdate(document, fromBase64(this.content))
            Y.applyUpdate(document, fromBase64(update))
            this.content = toBase64(Y.encodeStateAsUpdate(document))
            this.broadcast(this.content)
        } finally {
            document.destroy()
        }
    }
}

function emptyDocument() {
    const document = new Y.Doc()
    // Seed on the server so simultaneous first joins share the same paragraph.
    document.getXmlFragment("default").push([new Y.XmlElement("paragraph")])
    const content = toBase64(Y.encodeStateAsUpdate(document))
    document.destroy()
    return content
}

type DocumentInfo = { id: string; title: string }
