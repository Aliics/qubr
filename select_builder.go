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
//	users, err := Select[User]().
//		Where(Equal("ID", 42)).
//		QueryContext(ctx, db) // Or BuildQuery to use the raw SQL.
//	if err != nil {
//		return err
//	}
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

// BuildQuery will construct the SQL query SelectBuilder is currently representing.
// User input will utilize placeholders, and the values of the input will be in the 2nd return value, args.
// If there was an issue in the construction of SelectBuilder, then the 3rd return value, err will not non-nil.
//
// The resulting query should look something like:
//
//	SELECT "field1", "field2" FROM "schema"."table" WHERE "field1" = ? LIMIT ?;
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

// Query wraps SelectBuilder.QueryContext, this will use the query represented by SelectBuilder.
// The row results are all mapped to T.
func (b SelectBuilder[T]) Query(db *sql.DB) ([]T, error) {
	return b.QueryContext(context.Background(), db)
}

// QueryContext will use the query represented by the SelectBuilder, utilizing the sql.DB provided.
// The results are all mapped to T.
func (b SelectBuilder[T]) QueryContext(ctx context.Context, db *sql.DB) ([]T, error) {
	query, args, err := b.BuildQuery()
	if err != nil {
		return nil, err
	}

	return QueryContext[T](ctx, db, query, args...)
}

// GetOne wraps SelectBuilder.GetOneContext, this will use the query represented by SelectBuilder.
// There is the expectation that at least one result is returned. The first result will be mapped to T.
func (b SelectBuilder[T]) GetOne(db *sql.DB) (*T, error) {
	return b.GetOneContext(context.Background(), db)
}

// GetOneContext will use the query represented by the SelectBuilder, utilizing the sql.DB provided.
// There is the expectation that at least one result is returned. The first result will be mapped to T.
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
