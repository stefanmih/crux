# Crux

Crux is a small in-memory document store written in Java. It exposes a simple
command line interface that lets you insert, query and update schemaless
entities.

## Build and test

```
./gradlew test
```

## Running the CLI

Build the project and then run the `Main` class. For example:

```
./gradlew build
java -cp build/classes/java/main:$(find ~/.gradle -name 'gson-*.jar' | head -n1) com.crux.Main
```

Once started you can interact with the store using the commands below. Type
`help` to print the list at any time and `exit` to quit.

## Commands

* `add entity {json} [vector [n1 n2 ...]]` – insert a new entity. If an `id`
  is not provided one will be generated automatically.
* `delete entity ID` – remove an entity.
* `update entities where <filter> set {json}` – partially update all entities
  matching the given filter expression.
* `get entities using filter <filter>` – return a JSON array of all matching
  entities.
* `get field <path> from <id>` – extract a single field using dot notation.
* `get some [N]` – print up to `N` entities from the store (default `5`).
* `show history <id>` – display the stored history for an entity.
* `help` – show the command summary.
* `exit` – terminate the program.

### Filter expressions

Filters use the syntax `<field> <operator> <value expression>`. Value
expressions may contain numbers, strings, booleans or references to other
fields using the `&fieldName` notation. The supported comparison operators are
`==`, `!=`, `>`, `>=`, `<`, `<=` and `=` (alias for equality).

Examples:

```
get entities using filter age >= 30
update entities where name == "Bob" set {"age":26}
```
