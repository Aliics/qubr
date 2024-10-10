package qubr

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// SelectBuilder is a QueryBuilder for building SQL SELECT queries.
// Utilizing the related Select functions, you can construct these queries.
// Example:
//
//	query, args, err := Select[User]().
//		Where(Equal("ID", 42)).
//		BuildQuery() // Or QueryContext to run perform the query.
//	 if err != nil {
//	 	return err
//	 }
type SelectBuilder[T any] struct {
	from tableName

	fieldOperationTree fieldOperationTree

	limit *uint64

	err error
}

// Select will construct a new SelectBuilder, and the table name will be set based on the type given.
func Select[T any]() SelectBuilder[T] {
	return SelectBuilder[T]{
		from: tableName{forType: reflect.TypeFor[T]()},
	}
}

// From will explicitly set the table name. This cannot be called more once.
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

// Where will apply a FieldOperation as the initial comparison operator of a where clause.
// Where cannot be called more than once, use SelectBuilder.And or SelectBuilder.Or for further filtering.
func (b SelectBuilder[T]) Where(op FieldOperation) SelectBuilder[T] {
	if b.fieldOperationTree != emptyFieldOperationTree {
		b.err = ErrDoubleWhereClause
		return b
	}

	b.fieldOperationTree.op = op

	return b
}

// And will apply an AND to the existing where clause. SelectBuilder.Where must be called before this.
func (b SelectBuilder[T]) And(op FieldOperation) SelectBuilder[T] {
	err := appendToFieldOperationTree(&b.fieldOperationTree, func(next *fieldOperationTree) {
		next.and = &fieldOperationTree{op: op}
	})
	if err != nil {
		b.err = err
	}
	return b
}

// Or will apply an OR to the existing where clause. SelectBuilder.Where must be called before this.
func (b SelectBuilder[T]) Or(op FieldOperation) SelectBuilder[T] {
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

	// "X","Y"
	var fields string
	{
		// Struct field names are how we determine the select.
		selectType := reflect.TypeFor[T]()
		numField := selectType.NumField()

		sb := strings.Builder{}
		for i := range numField {
			f := selectType.Field(i)
			if !f.IsExported() {
				continue
			}

			sb.WriteString(fmt.Sprintf(`"%s", `, f.Name))
		}

		fields = strings.TrimSuffix(sb.String(), ", ")
	}

	tableName := b.from.String()

	whereClause, whereArgs := b.fieldOperationTree.buildQuery()
	args = append(args, whereArgs...)

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
