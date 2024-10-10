package qubr

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

type DeleteBuilder[T any] struct {
	from tableName

	fieldOperationTree fieldOperationTree

	limit *uint64

	err error
}

// Delete will construct a new DeleteBuilder, and the table name will be set based on the type given.
func Delete[T any]() DeleteBuilder[T] {
	return DeleteBuilder[T]{
		from: tableName{forType: reflect.TypeFor[T]()},
	}
}

// From will explicitly set the table name. This cannot be called more once.
func (b DeleteBuilder[T]) From(tableName string) DeleteBuilder[T] {
	if b.from.schema != "" && b.from.tableName != "" {
		b.err = ErrTableNameAlreadySet
		return b
	}
	t, err := newTableNameFromString(tableName)
	if err != nil {
		b.err = err
		return b
	}

	b.from = *t
	return b
}

// Where will apply a FieldOperation as the initial comparison operator of a where clause.
// Where cannot be called more than once, use DeleteBuilder.And or DeleteBuilder.Or for further filtering.
func (b DeleteBuilder[T]) Where(op FieldOperation) DeleteBuilder[T] {
	if b.fieldOperationTree != emptyFieldOperationTree {
		b.err = ErrDoubleWhereClause
		return b
	}

	b.fieldOperationTree.op = op

	return b
}

// And will apply an AND to the existing where clause. DeleteBuilder.Where must be called before this.
func (b DeleteBuilder[T]) And(op FieldOperation) DeleteBuilder[T] {
	err := appendToFieldOperationTree(&b.fieldOperationTree, func(next *fieldOperationTree) {
		next.and = &fieldOperationTree{op: op}
	})
	if err != nil {
		b.err = err
	}
	return b
}

// Or will apply an OR to the existing where clause. DeleteBuilder.Where must be called before this.
func (b DeleteBuilder[T]) Or(op FieldOperation) DeleteBuilder[T] {
	err := appendToFieldOperationTree(&b.fieldOperationTree, func(next *fieldOperationTree) {
		next.or = &fieldOperationTree{op: op}
	})
	if err != nil {
		b.err = err
	}
	return b
}

// Limit will apply a limit to the select statement. Limiting the number of rows resulting from your table.
// This cannot be called more than once.
func (b DeleteBuilder[T]) Limit(n uint64) DeleteBuilder[T] {
	if b.limit != nil {
		// This was probably not set intentionally by the caller, and it's sort of undefined behaviour.
		// Let's help them out with a useful error.
		b.err = ErrLimitAlreadySet
		return b
	}

	b.limit = &n
	return b
}

// BuildQuery will construct the SQL query DeleteBuilder is currently representing.
// User input will utilize placeholders, and the values of the input will be in the 2nd return value, args.
// If there was an issue in the construction of DeleteBuilder, then the 3rd return value, err will not non-nil.
//
// The resulting query should look something like:
//
//	DELETE FROM "schema"."table" WHERE "field1" = ? LIMIT ?;
func (b DeleteBuilder[T]) BuildQuery() (query string, args []any, err error) {
	if b.err != nil {
		return "", nil, b.err
	}

	tableName := b.from.String()

	whereClause, whereArgs := b.fieldOperationTree.buildQuery()
	args = append(args, whereArgs...)

	var limit string
	if b.limit != nil {
		limit = " LIMIT ?"
		args = append(args, *b.limit)
	}

	return fmt.Sprintf("DELETE FROM %s%s%s;", tableName, whereClause, limit), args, nil
}

// Exec wraps DeleteBuilder.ExecContext, which will execute the delete query represented by the DeleteBuilder.
func (b DeleteBuilder[T]) Exec(db *sql.DB) (sql.Result, error) {
	return b.ExecContext(context.Background(), db)
}

// ExecContext will execute the delete query represented by DeleteBuilder.
// This will execute using the provided sql.DB, and the response is simply passed back.
func (b DeleteBuilder[T]) ExecContext(ctx context.Context, db *sql.DB) (sql.Result, error) {
	query, args, err := b.BuildQuery()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}
