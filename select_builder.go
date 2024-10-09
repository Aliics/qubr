package qubr

import (
	"context"
	"database/sql"
	"errors"
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

func (s SelectBuilder[T]) From(tableName string) SelectBuilder[T] {
	if s.from.schema != "" && s.from.tableName != "" {
		s.err = ErrTableNameAlreadySet
		return s
	}

	parts := strings.Split(tableName, ".")
	if tableName == "" || len(parts) > 2 {
		s.err = ErrInvalidTableName{tableName}
		return s
	}

	if len(parts) > 1 {
		// Schema was provided in tableName, it comes before the actual table name.
		s.from.schema = parts[0]
	}

	// Whether a schema is provided or not, the table name is always the last part.
	s.from.tableName = parts[len(parts)-1]

	return s
}

func (s SelectBuilder[T]) Where(op FieldOperation) SelectBuilder[T] {
	if s.fieldOperationTree != emptyFieldOperationTree {
		s.err = ErrDoubleWhereClause
		return s
	}

	s.fieldOperationTree.op = op

	return s
}

func (s SelectBuilder[T]) And(op FieldOperation) SelectBuilder[T] {
	return s.appendToFieldOperationTree(func(next *fieldOperationTree) {
		next.and = &fieldOperationTree{op: op}
	})
}

func (s SelectBuilder[T]) Or(op FieldOperation) SelectBuilder[T] {
	return s.appendToFieldOperationTree(func(next *fieldOperationTree) {
		next.or = &fieldOperationTree{op: op}
	})
}

func (s SelectBuilder[T]) Limit(n uint64) SelectBuilder[T] {
	if s.limit != nil {
		// This was probably not set intentionally by the caller, and it's sort of undefined behaviour.
		// Let's help them out with a useful error.
		s.err = ErrLimitAlreadySet
		return s
	}

	s.limit = &n
	return s
}

func (s SelectBuilder[T]) BuildQuery() (query string, args []any, err error) {
	if s.err != nil {
		return "", nil, s.err
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

	var tableName string
	{
		if s.from.schema != "" {
			tableName = `"` + s.from.schema + `".`
		}
		tableName += `"` + s.from.tableName + `"`
	}

	// The WHERE clause present. Includes building the AND/OR nodes.
	// Since this is not obviously sized, we are going to use a strings.Builder for efficiency.
	var whereClause string
	if s.fieldOperationTree != emptyFieldOperationTree {
		sb := strings.Builder{}

		sb.WriteString(" WHERE ")

		query, data := s.fieldOperationTree.op.QueryData()
		sb.WriteString(query)
		args = append(args, data)

		// Walk down the tree for each "and" and "or" branch.
		// Write the op node out once discovered. Otherwise, we are done.

		next := s.fieldOperationTree
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
	if s.limit != nil {
		limit = " LIMIT ?"
		args = append(args, *s.limit)
	}

	return fmt.Sprintf("SELECT %s FROM %s%s%s;", fields, tableName, whereClause, limit), args, nil
}

func (s SelectBuilder[T]) Query(db *sql.DB) ([]T, error) {
	return s.QueryContext(context.Background(), db)
}

func (s SelectBuilder[T]) QueryContext(ctx context.Context, db *sql.DB) ([]T, error) {
	query, args, err := s.BuildQuery()
	if err != nil {
		return nil, err
	}

	return QueryContext[T](ctx, db, query, args...)
}

func (s SelectBuilder[T]) GetOne(db *sql.DB) (*T, error) {
	return s.GetOneContext(context.Background(), db)
}

func (s SelectBuilder[T]) GetOneContext(ctx context.Context, db *sql.DB) (*T, error) {
	ts, err := s.QueryContext(ctx, db)
	if err != nil {
		return nil, err
	}

	if len(ts) == 0 {
		return nil, ErrNoRows
	}

	return &ts[0], nil
}

func (s SelectBuilder[T]) appendToFieldOperationTree(assign func(next *fieldOperationTree)) SelectBuilder[T] {
	if s.fieldOperationTree == emptyFieldOperationTree {
		s.err = ErrMissingWhereClause
		return s
	}

	// Starting from our top node, "where", we walk down either the "and" or "or" branch.
	// Once we reach the bottom, we assign.

	next := &s.fieldOperationTree
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

	return s
}

var (
	ErrTableNameAlreadySet = errors.New("table name has already been set")

	ErrDoubleWhereClause  = errors.New("where clause is already present")
	ErrMissingWhereClause = errors.New("where clause is not yet present")

	ErrLimitAlreadySet = errors.New("limit value has already been set")

	ErrNoRows = errors.New("get resulted in no rows")
)

// ErrInvalidTableName occurs when a string provided cannot be used as a table name.
type ErrInvalidTableName struct {
	Name string
}

func (e ErrInvalidTableName) Error() string {
	return fmt.Sprintf(`"%s" is not a valid table name`, e.Name)
}
