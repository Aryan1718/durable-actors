import { relations } from "drizzle-orm"
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core"

export const authors = sqliteTable("authors", {
    id: integer().primaryKey({ autoIncrement: true }),
    name: text().notNull().unique()
})

export const books = sqliteTable("books", {
    id: integer().primaryKey({ autoIncrement: true }),
    authorId: integer()
        .notNull()
        .references(() => authors.id),
    title: text().notNull().unique()
})

export const authorsRelations = relations(authors, ({ many }) => ({ books: many(books) }))

export const booksRelations = relations(books, ({ one }) => ({
    author: one(authors, { fields: [books.authorId], references: [authors.id] })
}))
