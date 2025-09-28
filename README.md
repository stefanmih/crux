# Crux

Crux is a small in-memory document store written in Java. It exposes a feature-rich
command line interface (CLI) that lets you insert, query, transform, and persist
schemaless entities without needing to define a schema up front.

## Build and test

The project uses the Gradle wrapper. The commands below download the correct
Gradle version automatically.

```bash
./gradlew test
```

## Running the CLI

Build the project and then launch the `Main` class.

```bash
./gradlew build
java -cp build/classes/java/main:$(find ~/.gradle -name 'gson-*.jar' | head -n1) com.crux.Main
```

The CLI reads commands from standard input until you type `exit`. Each command
prints either a confirmation message or the requested JSON output. Any
validation error results in a line starting with `error:` followed by the
failure message.

## Command reference

Every command understood by the CLI is documented below along with its
arguments, behaviour, and notable edge cases. Commands are case-insensitive.

### `add entity {json} [vector [n1 n2 ...]]`

* **Purpose:** Insert a new entity into the store.
* **JSON body:** Required. Must be valid JSON representing the fields of the entity.
  * If the JSON omits an `id`, Crux generates a UUID automatically.
  * The inserted record is immediately indexed, versioned, and persisted.
* **Vector clause (optional):** Appending `vector [n1 n2 ...]` sets the special
  `vector` field to a list of floating-point numbers used by `find similar`.
  * Numbers can be integers or decimals separated by whitespace.
  * If you omit the bracket contents (`vector []`), an empty vector is stored.
* **Output:** `inserted <id>` where `<id>` is the final entity identifier.

Example:

```text
add entity {"name":"Ada","skills":["math","code"]} vector [0.2 1.5 0.9]
```

### `delete entity <id>`

* **Purpose:** Remove an entity and drop it from all internally tracked sets.
* **Behaviour:** If the entity exists it is deleted from the in-memory store,
  indices, history, and persistence log. Missing IDs produce `deleted <id>` even
  though no record is removed (the command is idempotent).

### `update entities where <filter> set {json}`

* **Purpose:** Partially update every entity matching a filter expression.
* **Filter:** Uses the syntax described in [Filter expressions](#filter-expressions).
* **JSON body:** Merged into each matching entity. Only the provided fields are
  overwritten; absent properties remain untouched.
* **Output:** `updated <n>` where `<n>` is the number of entities affected.

Example:

```text
update entities where status == "active" set {"status":"archived","archivedAt":"2024-01-01"}
```

### `get entities using filter <filter>`

* **Purpose:** Retrieve all entities satisfying a filter.
* **Output:** A JSON array of entity documents. When no results match, prints `[]`.
* **Notes:** Returned JSON reflects the latest state (including partial updates).

### `get field <path> from <id>`

* **Purpose:** Extract a single nested field or array element.
* **Path syntax:** Use dot notation. Array indices are numeric path segments.
  * Example: `address.street` or `scores.0`.
* **Output:** The selected value encoded as JSON, or `null` if the entity or path
  is missing.

### `get some [N]`

* **Purpose:** Inspect a limited sample of stored entities.
* **Arguments:** Optional `[N]` (square brackets included) specifies how many
  documents to print; defaults to 5.
* **Output:** JSON array containing up to `N` entities in insertion order.

### `generate <N>`

* **Purpose:** Quickly populate the store with synthetic data.
* **Behaviour:** Inserts `N` entities that look like
  `{"id":uuid,"value":<0-999>,"vector":[r1,r2,r3]}`.
* **Use cases:** Useful for trying out similarity search and testing queries.
* **Output:** `generated <N>`.

### `find similar <id> [N]`

* **Purpose:** Retrieve the entities whose `vector` fields are most similar to a
  reference entity.
* **Arguments:**
  * `<id>` – Identifier of the reference entity. It must contain a numeric
    `vector` field.
  * Optional `[N]` – Maximum number of neighbours to return (default 5).
* **Similarity metric:** Cosine similarity between vectors of equal length.
  Entities without vectors or with mismatched dimensions are ignored.
* **Output:** JSON array of the neighbouring entities ordered from most to least
  similar.

### `show history <id>`

* **Purpose:** Inspect the complete change history of an entity.
* **Output:** JSON array of historical snapshots ordered chronologically.
  Each snapshot includes metadata maintained by the versioning subsystem.

### `create transform function { expr -> field; ... }`

* **Purpose:** Define a reusable transformation that can be applied to entity
  sets.
* **Body format:** Inside the braces provide one or more semicolon-separated
  clauses that map a value expression to the destination field path.
  * Example clause: `&name -> originalName;` copies the source name into
    `originalName`.
  * Value expressions support the same syntax as filter value expressions,
    including numeric operations and field references.
* **Effect:** Stores the compiled function until the next call to
  `create transform function` replaces it.
* **Output:** `transform function created`.

### `apply transform function from set <source> to <target>`

* **Purpose:** Execute the currently defined transform function against a named
  set of entity IDs, storing the results in another set.
* **Sets:**
  * The CLI maintains in-memory sets of IDs. The default `all` set tracks every
    entity ever inserted (excluding deletions).
  * `<source>` must reference an existing set; `<target>` is created if needed.
* **Execution:** For each entity in `<source>`, the transform function is
  evaluated to construct a brand new entity with a freshly generated `id`. New
  entities are inserted, added to the `all` set, and recorded in `<target>`.
* **Output:** `transformed <n>` showing how many new entities were created.

Example:

```text
create transform function {
  &id -> sourceId;
  &vector -> vector;
  &value * 2 -> doubledValue
}
apply transform function from set all to doubles
```

### `persist snapshot`

* **Purpose:** Force the store to write the current state to disk.
* **Behaviour:** Delegates to the persistence layer which saves a snapshot under
  the `data/` directory relative to the working directory.
* **Output:** `snapshot saved` on success.

### `help`

* **Purpose:** Print a short summary of all available commands.

### `exit`

* **Purpose:** Terminate the CLI session. No persistence work is triggered beyond
  what previous commands have already performed.

## Filter expressions

Filters power the `update` and `get entities using filter` commands. They are
parsed by a recursive-descent parser that supports:

* **Comparison operators:** `==`, `=`, `!=`, `>`, `>=`, `<`, `<=`, `contains`,
  and `like`.
  * `contains` performs a case-insensitive substring search on string fields.
  * `like` accepts SQL-style wildcards (`%` and `_`) and matches case-insensitively.
* **Logical operators:** `and`, `or`, `not`, and parentheses for grouping.
* **Inline JSON:** A literal object such as `{ "status": "active" }` expands to a
  conjunction of equality comparisons for each property.
* **Value expressions:**
  * Numeric literals (integers or decimals), strings enclosed in single or double
    quotes, booleans, and raw identifiers.
  * Field references using `&field.path` to pull values from the entity currently
    being evaluated.
  * Arithmetic with `+`, `-`, `*`, `/`, including nested parentheses and unary minus.
* **Nested fields:** Use dot notation (`profile.age`) and array indices
  (`scores.0`) on either side of a comparison.

Example filters:

```text
age >= 30 and status == "active"
score > &benchmark.average
name contains "ann"
tags like "%beta%"
{"category":"book","inStock":true}
```

When a comparison cannot be evaluated (for example due to missing fields or
type mismatches) the expression simply evaluates to `false` for that entity.

## Working with sets and history

* Every insertion automatically adds the new entity ID to the `all` set. Deleting
  an entity removes it from every set.
* The `show history` command leverages the built-in versioning system to list all
  previous states of an entity, enabling audit and time-travel scenarios.

## Persistence and snapshots

Crux records every insert/update/delete in an append-only log under the `data/`
directory. Restarting the CLI replays this log and restores entity history. Use
`persist snapshot` to force a full snapshot of the current state, which speeds up
subsequent startups by avoiding a long replay.
