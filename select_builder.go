package qubr

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type SelectBuilder[T any] struct {
	from tableName

	fieldOperationTree fieldOperationTree

	limit *uint64

	err error
}

func Select[T any]() SelectBuilder[T] {
	return SelectBuilder[T]{
		from: tableName{tableName: reflect.TypeFor[T]().Name()},
	}
}

func (b SelectBuilder[T]) From(tableName string) SelectBuilder[T] {
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

func (b SelectBuilder[T]) Where(op FieldOperation) SelectBuilder[T] {
	if b.fieldOperationTree != emptyFieldOperationTree {
		b.err = ErrDoubleWhereClause
		return b
	}

	b.fieldOperationTree.op = op

	return b
}

func (b SelectBuilder[T]) And(op FieldOperation) SelectBuilder[T] {
	return b.appendToFieldOperationTree(func(next *fieldOperationTree) {
		next.and = &fieldOperationTree{op: op}
	})
}

func (b SelectBuilder[T]) Or(op FieldOperation) SelectBuilder[T] {
	return b.appendToFieldOperationTree(func(next *fieldOperationTree) {
		next.or = &fieldOperationTree{op: op}
	})
}

func (b SelectBuilder[T]) Limit(n uint64) SelectBuilder[T] {
	if b.limit != nil {
		// This was probably not set intentionally by the caller, and it's sort of undefined behaviour.
		// Let's help them out with a useful error.
		b.err = ErrLimitAlreadySet
		return b
	}

	b.limit = &n
	return b
}

func (b SelectBuilder[T]) BuildQuery() (query string, args []any, err error) {
	if b.err != nil {
		return "", nil, b.err
	}

	var fields string
	{
		// Struct field names are how we determine the select.
		selectType := reflect.TypeFor[T]()
		numField := selectType.NumField()

		sb := strings.Builder{}
		for i := range numField {
			sb.WriteString(`"` + selectType.Field(i).Name + `"`)
			if i < numField-1 {
				// Last field won't need a comma.
				sb.WriteRune(',')
			}
		}

		fields = sb.String()
	}

	tableName := b.from.String()

	// The WHERE clause present. Includes building the AND/OR nodes.
	// Since this is not obviously sized, we are going to use a strings.Builder for efficiency.
	var whereClause string
	if b.fieldOperationTree != emptyFieldOperationTree {
		sb := strings.Builder{}

		sb.WriteString(" WHERE ")

		query, data := b.fieldOperationTree.op.QueryData()
		sb.WriteString(query)
		args = append(args, data)

		// Walk down the tree for each "and" and "or" branch.
		// Write the op node out once discovered. Otherwise, we are done.

		next := b.fieldOperationTree
		for {
			if next.and == nil && next.or == nil {
				// Nothing left to be written.
				break
			} else if next.and != nil {
				next = *next.and
				sb.WriteString(" AND ")
			} else if next.or != nil {
				next = *next.or
				sb.WriteString(" OR ")
			}

			// Both branches will append data the same way.
			query, data := next.op.QueryData()
			sb.WriteString(query)
			args = append(args, data)
		}

		whereClause = sb.String()
	}

	var limit string
	if b.limit != nil {
		limit = " LIMIT ?"
		args = append(args, *b.limit)
	}

	return fmt.Sprintf("SELECT %s FROM %s%s%s;", fields, tableName, whereClause, limit), args, nil
}

func (b SelectBuilder[T]) Query(db *sql.DB) ([]T, error) {
	return b.QueryContext(context.Background(), db)
}

func (b SelectBuilder[T]) QueryContext(ctx context.Context, db *sql.DB) ([]T, error) {
	query, args, err := b.BuildQuery()
	if err != nil {
		return nil, err
	}

	return QueryContext[T](ctx, db, query, args...)
}

func (b SelectBuilder[T]) GetOne(db *sql.DB) (*T, error) {
	return b.GetOneContext(context.Background(), db)
}

func (b SelectBuilder[T]) GetOneContext(ctx context.Context, db *sql.DB) (*T, error) {
	ts, err := b.QueryContext(ctx, db)
	if err != nil {
		return nil, err
	}

	if len(ts) == 0 {
		return nil, ErrNoRows
	}

	return &ts[0], nil
}

func (b SelectBuilder[T]) appendToFieldOperationTree(assign func(next *fieldOperationTree)) SelectBuilder[T] {
	if b.fieldOperationTree == emptyFieldOperationTree {
		b.err = ErrMissingWhereClause
		return b
	}

	// Starting from our top node, "where", we walk down either the "and" or "or" branch.
	// Once we reach the bottom, we assign.

	next := &b.fieldOperationTree
	for {
		if next.and != nil {
			next = next.and
			continue
		}
		if next.or != nil {
			next = next.or
			continue
		}

		// We reached the bottom.
		assign(next)

		break
	}

	return b
}
