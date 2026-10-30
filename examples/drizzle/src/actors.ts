import { Actor, type ActorContext, Ephemeral, Persisted } from "durable-actors"
import { drizzle, migrate } from "durable-actors/drizzle"
import { eq } from "drizzle-orm"

import migrations from "../drizzle/migrations.js"
import * as schema from "./schema.js"

type Author = typeof schema.authors.$inferSelect
type Book = typeof schema.books.$inferSelect

export class Library extends Actor {
    @Persisted private added = 0
    @Ephemeral private readonly orm = drizzle(this.db, { schema })

    constructor(context: ActorContext) {
        super(context)
        migrate(this.db, migrations)
    }

    async addBook(author: string, title: string): Promise<Book | null> {
        try {
            return await this.orm.transaction(async tx => {
                const [writer] = await tx
                    .insert(schema.authors)
                    .values({ name: author })
                    .onConflictDoUpdate({ target: schema.authors.name, set: { name: author } })
                    .returning()
                const [book] = await tx.insert(schema.books).values({ authorId: writer!.id, title }).returning()
                this.added += 1
                return book!
            })
        } catch {
            return null
        }
    }

    async addedCount(): Promise<number> {
        return this.added
    }

    async byAuthor(name: string): Promise<(Author & { books: Book[] }) | null> {
        const author = await this.orm.query.authors.findFirst({
            where: eq(schema.authors.name, name),
            with: { books: true }
        })
        return author ?? null
    }

    async catalog(): Promise<{ author: string; title: string }[]> {
        return this.orm
            .select({ author: schema.authors.name, title: schema.books.title })
            .from(schema.books)
            .innerJoin(schema.authors, eq(schema.books.authorId, schema.authors.id))
            .orderBy(schema.books.id)
    }
}
