package qubr

import (
	"fmt"
	"strings"
)

type FieldOperation struct {
	Operator Operator

	FieldName string
	ValueRaw  any
}

func (f FieldOperation) QueryData() (string, any) {
	return fmt.Sprintf(`"%s"%s?`, f.FieldName, f.Operator), f.ValueRaw
}

type Operator uint8

const (
	OperatorEqual Operator = iota
	OperatorNotEqual
	OperatorGreaterThan
	OperatorLessThan
	OperatorGreaterThanOrEqual
	OperatorLessThanOrEqual
)

func (o Operator) String() string {
	var s string
	switch o {
	case OperatorEqual:
		s = "="
	case OperatorNotEqual:
		s = "!="
	case OperatorGreaterThan:
		s = ">"
	case OperatorLessThan:
		s = "<"
	case OperatorGreaterThanOrEqual:
		s = ">="
	case OperatorLessThanOrEqual:
		s = "<="
	}
	return s
}

type fieldOperationTree struct {
	op FieldOperation

	or  *fieldOperationTree
	and *fieldOperationTree
}

var emptyFieldOperationTree = fieldOperationTree{}

func Equal(field string, v any) FieldOperation {
	return FieldOperation{OperatorEqual, field, v}
}

func NotEqual(field string, v any) FieldOperation {
	return FieldOperation{OperatorNotEqual, field, v}
}

func GreaterThan(field string, v any) FieldOperation {
	return FieldOperation{OperatorGreaterThan, field, v}
}

func LessThan(field string, v any) FieldOperation {
	return FieldOperation{OperatorLessThan, field, v}
}

func GreaterThanOrEqual(field string, v any) FieldOperation {
	return FieldOperation{OperatorGreaterThanOrEqual, field, v}
}

func LessThanOrEqual(field string, v any) FieldOperation {
	return FieldOperation{OperatorLessThanOrEqual, field, v}
}

func (t fieldOperationTree) BuildQuery() (string, []any) {
	if t == emptyFieldOperationTree {
		return "", nil
	}

	var args []any

	// The WHERE clause is present. Includes building the AND/OR nodes.
	// Since this is not obviously sized, we are going to use a strings.Builder for efficiency.
	sb := strings.Builder{}

	sb.WriteString(" WHERE ")

	query, data := t.op.QueryData()
	sb.WriteString(query)
	args = append(args, data)

	// Walk down the tree for each "and" and "or" branch.
	// Write the op node out once discovered. Otherwise, we are done.

	next := t
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

	return sb.String(), args
}

func appendToFieldOperationTree(opTree *fieldOperationTree, assign func(next *fieldOperationTree)) error {
	if opTree == nil || *opTree == emptyFieldOperationTree {
		return ErrMissingWhereClause
	}

	// Starting from our top node, "where", we walk down either the "and" or "or" branch.
	// Once we reach the bottom, we assign.

	next := opTree
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

	return nil
}
