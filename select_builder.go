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

	err error
}

func Select[T any](ofType T) SelectBuilder[T] {
	return SelectBuilder[T]{
		ofType: ofType,
	}
}

func (s SelectBuilder[T]) From(tableName string) SelectBuilder[T] {
	parts := strings.Split(tableName, ".")
	if len(parts) == 0 || len(parts) > 2 {
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
	if s.fieldOperationTree == emptyFieldOperationTree {
		s.err = ErrMissingWhereClause
		return s
	}

	// Starting from our top node, "where", we walk down either the "and" or "or" branch.
	// Once we reach the bottom, we assign "and".

	next := s.fieldOperationTree
	for {
		if next.and != nil {
			next = *next.and
			continue
		}
		if next.or != nil {
			next = *next.or
			continue
		}

		// We reached the bottom.
		next.and = &fieldOperationTree{op: op}

		break
	}

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

	return fmt.Sprintf("SELECT %s FROM %s;", fields, tableName), nil
}

var (
	ErrDoubleWhereClause  = errors.New("where clause is already present")
	ErrMissingWhereClause = errors.New("where clause is not yet present")
)

// ErrInvalidTableName occurs when a string provided cannot be used as a table name.
type ErrInvalidTableName struct {
	Name string
}

func (e ErrInvalidTableName) Error() string {
	return fmt.Sprintf(`"%s" is not a valid table name`, e.Name)
}
