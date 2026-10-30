import { actors } from "./generated/index.js"

const library = actors.Library.get("demo")
console.log("Added:", await library.addBook("Ursula K. Le Guin", "The Dispossessed"))
console.log("Duplicate title rolls back:", await library.addBook("Someone Else", "The Dispossessed"))
console.log("By author:", await library.byAuthor("Ursula K. Le Guin"))
console.log("Catalog:", await library.catalog())
console.log("Books added:", await library.addedCount())
