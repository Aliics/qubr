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

func Delete[T any]() DeleteBuilder[T] {
	return DeleteBuilder[T]{
		from: tableName{tableName: reflect.TypeFor[T]().Name()},
	}
}

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

func (b DeleteBuilder[T]) Where(op FieldOperation) DeleteBuilder[T] {
	if b.fieldOperationTree != emptyFieldOperationTree {
		b.err = ErrDoubleWhereClause
		return b
	}

	b.fieldOperationTree.op = op

	return b
}

func (b DeleteBuilder[T]) And(op FieldOperation) DeleteBuilder[T] {
	err := appendToFieldOperationTree(&b.fieldOperationTree, func(next *fieldOperationTree) {
		next.and = &fieldOperationTree{op: op}
	})
	if err != nil {
		b.err = err
	}
	return b
}

func (b DeleteBuilder[T]) Or(op FieldOperation) DeleteBuilder[T] {
	err := appendToFieldOperationTree(&b.fieldOperationTree, func(next *fieldOperationTree) {
		next.or = &fieldOperationTree{op: op}
	})
	if err != nil {
		b.err = err
	}
	return b
}

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

func (b DeleteBuilder[T]) Exec(db *sql.DB) (sql.Result, error) {
	return b.ExecContext(context.Background(), db)
}

func (b DeleteBuilder[T]) ExecContext(ctx context.Context, db *sql.DB) (sql.Result, error) {
	query, args, err := b.BuildQuery()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}
