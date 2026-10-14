import { Command, InvalidArgumentError } from "commander"

export interface PromptAgent {
    prompt(text: string): Promise<string>
}

export class PiCli {
    constructor(
        private readonly agent: PromptAgent,
        private readonly output: (text: string) => void
    ) {}

    createCommand(): Command {
        const program = new Command().name("pi-cli").description("Send a prompt to the Pi Durable actor")
        program
            .command("send")
            .description("Send a prompt and print the reply")
            .argument("<prompt>", "Message to send", parsePrompt)
            .action(async (text: string) => {
                this.output(await this.agent.prompt(text))
            })
        return program
    }
}

function parsePrompt(value: string): string {
    const text = value.trim()
    if (!text) throw new InvalidArgumentError("Prompt must not be empty")
    return text
}
