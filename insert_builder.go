package qubr

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// InsertBuilder is a QueryBuilder for building SQL INSERT queries.
// Utilizing the related Insert functions, you can construct these queries.
// Example:
//
//	result, err := Insert[User]().
//		Values(User{ID: 42, Name: "Alex"}).
//		ExecContext(ctx, db) // Or BuildQuery to use the raw SQL.
//	if err != nil {
//		return err
//	}
type InsertBuilder[T any] struct {
	into tableName

	literalValues []T

	err error
}

// Insert will construct a new InsertBuilder, and the table name will be set based on the type given.
func Insert[T any]() InsertBuilder[T] {
	return InsertBuilder[T]{
		into: tableName{forType: reflect.TypeFor[T]()},
	}
}

// Into will explicitly set the table name. This cannot be called more once.
func (b InsertBuilder[T]) Into(tableName string) InsertBuilder[T] {
	if b.into.schema != "" && b.into.tableName != "" {
		b.err = ErrTableNameAlreadySet
		return b
	}

	t, err := newTableNameFromString(tableName)
	if err != nil {
		b.err = err
		return b
	}

	b.into = *t
	return b
}

// Values will represent the values to be inserted into your table. Each struct given being a row, and its fields
// being the columns.
func (b InsertBuilder[T]) Values(t ...T) InsertBuilder[T] {
	b.literalValues = t
	return b
}

// BuildQuery will construct the SQL query InsertBuilder is currently representing.
// User input will utilize placeholders, and the values of the input will be in the 2nd return value, args.
// If there was an issue in the construction of InsertBuilder, then the 3rd return value, err will not non-nil.
//
// The resulting query should look something like:
//
//	INSERT INTO "table" VALUES (?, ?, ?);
func (b InsertBuilder[T]) BuildQuery() (query string, args []any, err error) {
	if b.err != nil {
		return "", nil, b.err
	}

	tableName := b.into.String()

	// Create a set of placeholders (?,...)... for each "literalValue", and append the actual values to args.
	// End result should look like: VALUES (?,?),(?,?)
	var values string
	{
		if len(b.literalValues) == 0 {
			return "", nil, ErrNoInsertValues
		}

		// Determine the number of settable fields on the struct.
		insertType := reflect.TypeFor[T]()
		var numExported int
		for i := range insertType.NumField() {
			f := insertType.Field(i)
			if !f.IsExported() {
				continue
			}

			numExported++
		}

		sb := strings.Builder{}
		sb.WriteString(" VALUES ")
		for i, v := range b.literalValues {
			insertValue := reflect.ValueOf(v)

			// (?,?)
			placeholders := strings.TrimSuffix(strings.Repeat("?, ", numExported), ", ") // Remove trailing comma.
			sb.WriteString(fmt.Sprintf("(%s)", placeholders))

			for i := range numExported {
				args = append(args, insertValue.Field(i).Interface())
			}

			if i < len(b.literalValues)-1 {
				sb.WriteString(", ")
			}
		}

		values = sb.String()
	}

	return fmt.Sprintf("INSERT INTO %s%s;", tableName, values), args, nil
}

// Exec wraps InsertBuilder.ExecContext, which will execute the insert query represented by the InsertBuilder.
func (b InsertBuilder[T]) Exec(db *sql.DB) (sql.Result, error) {
	return b.ExecContext(context.Background(), db)
}

// ExecContext will execute the insert query represented by the InsertBuilder.
// This will execute using the provided sql.DB, and the response is simply passed back.
func (b InsertBuilder[T]) ExecContext(ctx context.Context, db *sql.DB) (sql.Result, error) {
	query, args, err := b.BuildQuery()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}
