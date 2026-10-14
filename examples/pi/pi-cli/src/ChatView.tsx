import { useEffect, useRef, useState } from "react"
import { Box, Static, Text, useApp } from "ink"
import TextInput from "ink-text-input"
import stripAnsi from "strip-ansi"

import type { StreamingAgent } from "./chat.js"

interface Turn {
    id: number
    role: "header" | "you" | "pi" | "error"
    text: string
}

export function ChatView({ agent }: { agent: StreamingAgent }) {
    const { exit } = useApp()
    const [turns, setTurns] = useState<Turn[]>([{ id: 0, role: "header", text: "Pi chat" }])
    const [input, setInput] = useState("")
    const [live, setLive] = useState("")
    const [busy, setBusy] = useState(false)
    const [connected, setConnected] = useState(agent.connected)
    const [disconnected, setDisconnected] = useState(false)
    const inFlight = useRef(false)

    useEffect(() => {
        let active = true
        void agent.ready.then(() => { if (active) setConnected(true) }).catch(error => {
            if (active) { append("error", error instanceof Error ? error.message : String(error)); setDisconnected(true) }
        })
        void agent.closed.then(() => { if (active) { setConnected(false); setDisconnected(true) } })
        return () => { active = false }
    }, [agent])

    async function submit(value: string): Promise<void> {
        const text = value.trim()
        if (text === "/exit") { exit(); return }
        if (!text || inFlight.current || !connected) return
        inFlight.current = true
        setInput("")
        setBusy(true)
        setLive("")
        append("you", text)
        try {
            const answer = await agent.prompt(text, text => setLive(cleanText(text)))
            append("pi", answer)
        } catch (error) {
            append("error", error instanceof Error ? error.message : String(error))
        } finally {
            inFlight.current = false
            setBusy(false)
            setLive("")
        }
    }

    function append(role: Turn["role"], text: string): void {
        setTurns(turns => [...turns, { id: turns.length, role, text: cleanText(text) }])
    }

    return <Box flexDirection="column" paddingX={1}>
        <Static items={turns}>{turn => <TranscriptTurn key={turn.id} turn={turn} />}</Static>
        {busy && <Box flexDirection="column" marginBottom={1}>
            <Text bold>pi</Text>
            <Box paddingLeft={2}><Text>{live || <Text dimColor>Thinking...</Text>}</Text></Box>
        </Box>}
        {connected && !busy
            ? <Box gap={1}><Text bold>you &gt;</Text><Box flexGrow={1}><TextInput value={input} onChange={setInput} onSubmit={submit} placeholder="Send a message" /></Box></Box>
            : <Text dimColor>{disconnected ? "Connection closed. Ctrl+C to leave." : busy ? "Replying... Ctrl+C to close." : "Connecting..."}</Text>}
    </Box>
}

function TranscriptTurn({ turn }: { turn: Turn }) {
    if (turn.role === "header") return <Box flexDirection="column" marginBottom={1}>
        <Text bold>{turn.text}</Text>
        <Text dimColor>Conversation demo · Enter to send · /exit to leave</Text>
    </Box>
    return <Box flexDirection="column" marginBottom={1}>
        <Text bold color={turn.role === "error" ? "red" : undefined}>{turn.role === "error" ? "Reply failed" : turn.role}</Text>
        <Box paddingLeft={2}><Text>{turn.text}</Text></Box>
    </Box>
}

function cleanText(text: string): string {
    return stripAnsi(text).replace(/[\u0000-\u0008\u000b-\u001f\u007f]/g, "")
}
