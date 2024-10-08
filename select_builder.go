package qube

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type SelectBuilder[T any] struct {
	ofType T

	from struct {
		schema    string
		tableName string
	}

	fieldOperationTree fieldOperationTree

	limit *uint64

	err error
}

func Select[T any](ofType T) SelectBuilder[T] {
	return SelectBuilder[T]{
		ofType: ofType,
		err:    ErrInvalidTableName{""}, // We need to set that the table has no name for the initial state.
	}
}

func (s SelectBuilder[T]) From(tableName string) SelectBuilder[T] {
	parts := strings.Split(tableName, ".")
	if tableName == "" || len(parts) > 2 {
		s.err = ErrInvalidTableName{tableName}
		return s
	}

	s.err = nil

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

func (s SelectBuilder[T]) Build() (string, error) {
	if s.err != nil {
		return "", s.err
	}

	var fields string
	{
		// Struct field names are how we determine the select.
		selectType := reflect.TypeOf(s.ofType)
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
		sb.WriteString(s.fieldOperationTree.op.String())

		// Walk down the tree for each "and" and "or" branch.
		// Write the op node out once discovered. Otherwise, we are done.

		next := s.fieldOperationTree
		for {
			if next.and != nil {
				next = *next.and
				sb.WriteString(" AND ")
				sb.WriteString(next.op.String())
				continue
			}
			if next.or != nil {
				next = *next.or
				sb.WriteString(" OR ")
				sb.WriteString(next.op.String())
				continue
			}

			// Nothing left to be written.

			break
		}

		whereClause = sb.String()
	}

	var limit string
	if s.limit != nil {
		limit = fmt.Sprintf(" LIMIT %d", *s.limit)
	}

	return fmt.Sprintf("SELECT %s FROM %s%s%s;", fields, tableName, whereClause, limit), nil
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
	ErrDoubleWhereClause  = errors.New("where clause is already present")
	ErrMissingWhereClause = errors.New("where clause is not yet present")

	ErrLimitAlreadySet = errors.New("limit value has already been set")
)

// ErrInvalidTableName occurs when a string provided cannot be used as a table name.
type ErrInvalidTableName struct {
	Name string
}

func (e ErrInvalidTableName) Error() string {
	return fmt.Sprintf(`"%s" is not a valid table name`, e.Name)
}
