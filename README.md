# qubr

A query builder for Go with a querying and data mapping mechanism. The SQL is not hidden from the caller, and we embrace
understanding of what is going on under the hood. Instead of engineering a solution that poorly fits everyone, qubr's
goal is to fit a specific purpose of providing a good toolkit of useful, simple 80%+ use case helpers, and a seamless
way around not having to use these tools for that other ~20% of the time.

## Example

Many SQL queries are just simple select queries, so this will be your bread and butter.

```go
package main

import (
	"database/sql"
	"fmt"
	"github.com/aliics/qubr"
)

type user struct {
	ID    uint64
	Email string
}

func main() {
	// Your SQL database connection.
	db, err := sql.Open("driverName", "sourceName")
	if err != nil {
		panic(err)
	}

	// SELECT "ID", "Email"
	// FROM "user" 
	// WHERE "Email" = '100'
	// LIMIT 100;
	users, err := qubr.Select[user]().
		Where(qubr.Equal("Email", "ollie@example.org")).
		Limit(100).
		Query(db) // Maps the resulting rows to a "user".
	if err != nil {
		panic(err)
	}

	fmt.Printf("users found: %v\n", users)
}

```

There are functions for `DELETE FROM`, `INSERT INTO`, and `UPDATE ... SET` statements as well. If you cannot neatly
construct your queries using these functions, we also support using the `QueryContext` function and providing a query.
Like with `Query` in the example, the rows will be mapped to your struct.
