package qubr

import (
	"fmt"
	"strings"
)

// FieldOperation represents some field comparison operation utilizing an Operator.
// It is not recommended to construct a FieldOperation directly, instead, use one of the constructing functions like
// Equal or In.
type FieldOperation struct {
	Operator Operator

	FieldName string
	ValueRaw  any
}

func (f FieldOperation) queryData() (string, []any) {
	var (
		placeholders string
		args         []any
	)
	{
		argArr, isArr := f.ValueRaw.([]any)

		if !isArr && f.Operator != OperatorIn && f.Operator != OperatorNotIn {
			// Our "ValueRaw" is not an array of any, and it's not some kind of in operator.
			placeholders = "?"
			args = []any{f.ValueRaw}
		} else {
			// (?,?,?)
			points := strings.Repeat("?, ", len(argArr))
			placeholders = fmt.Sprintf("(%s)", strings.TrimSuffix(points, ", "))
			args = argArr
		}
	}

	return fmt.Sprintf(`"%s" %s %s`, f.FieldName, f.Operator, placeholders), args
}

// Operator is a type representing one of the various comparison operators in ANSI SQL (ISO 9075).
type Operator uint8

const (
	OperatorEqual Operator = iota
	OperatorNotEqual
	OperatorGreaterThan
	OperatorLessThan
	OperatorGreaterThanOrEqual
	OperatorLessThanOrEqual
	OperatorIn
	OperatorNotIn
)

func (o Operator) String() string {
	var s string
	switch o {
	case OperatorEqual:
		s = "="
	case OperatorNotEqual:
		s = "<>"
	case OperatorGreaterThan:
		s = ">"
	case OperatorLessThan:
		s = "<"
	case OperatorGreaterThanOrEqual:
		s = ">="
	case OperatorLessThanOrEqual:
		s = "<="
	case OperatorIn:
		s = "IN"
	case OperatorNotIn:
		s = "NOT IN"
	}
	return s
}

type fieldOperationTree struct {
	op FieldOperation

	or  *fieldOperationTree
	and *fieldOperationTree
}

var emptyFieldOperationTree = fieldOperationTree{}

// Equal is a wrapper for constructing a FieldOperation with an OperatorEqual passed in.
// Equivalent SQL will be:
//
//	"field" = ?
func Equal(field string, v any) FieldOperation {
	return FieldOperation{OperatorEqual, field, v}
}

// NotEqual is a wrapper for constructing a FieldOperation with an OperatorNotEqual passed in.
// Equivalent SQL will be:
//
//	"field" != ?
func NotEqual(field string, v any) FieldOperation {
	return FieldOperation{OperatorNotEqual, field, v}
}

// GreaterThan is a wrapper for constructing a FieldOperation with an OperatorGreaterThan passed in.
// Equivalent SQL will be:
//
//	"field" > ?
func GreaterThan(field string, v any) FieldOperation {
	return FieldOperation{OperatorGreaterThan, field, v}
}

// LessThan is a wrapper for constructing a FieldOperation with an OperatorLessThan passed in.
// Equivalent SQL will be:
//
//	"field" < ?
func LessThan(field string, v any) FieldOperation {
	return FieldOperation{OperatorLessThan, field, v}
}

// GreaterThanOrEqual is a wrapper for constructing a FieldOperation with an OperatorGreaterThanOrEqual passed in.
// Equivalent SQL will be:
//
//	"field" >= ?
func GreaterThanOrEqual(field string, v any) FieldOperation {
	return FieldOperation{OperatorGreaterThanOrEqual, field, v}
}

// LessThanOrEqual is a wrapper for constructing a FieldOperation with an OperatorLessThanOrEqual passed in.
// Equivalent SQL will be:
//
//	"field" <= ?
func LessThanOrEqual(field string, v any) FieldOperation {
	return FieldOperation{OperatorLessThanOrEqual, field, v}
}

// IsTrue is a wrapper for constructing a FieldOperation with an OperatorIsTrue passed in.
// Since it's just a boolean comparison, we utilize Equal to do this.
// Equivalent SQL will be:
//
//	"field" = TRUE
func IsTrue(field string) FieldOperation {
	return Equal(field, true)
}

// IsFalse is a wrapper for constructing a FieldOperation with an OperatorIsFalse passed in.
// Since it's just a boolean comparison, we utilize Equal to do this.
// Equivalent SQL will be:
//
//	"field" = FALSE
func IsFalse(field string) FieldOperation {
	return Equal(field, false)
}

// In is a wrapper for constructing a FieldOperation with an OperatorIn passed in.
// Equivalent SQL will be:
//
//	"field" IN (?, ...)
func In(field string, values ...any) FieldOperation {
	return FieldOperation{OperatorIn, field, values}
}

// NotIn is a wrapper for constructing a FieldOperation with an OperatorNotIn passed in.
// Equivalent SQL will be:
//
//	"field" NOT IN (?, ...)
func NotIn(field string, values ...any) FieldOperation {
	return FieldOperation{OperatorNotIn, field, values}
}

// buildQuery will construct a where clause for SQL queries.
func (t fieldOperationTree) buildQuery() (string, []any) {
	if t == emptyFieldOperationTree {
		return "", nil
	}

	var args []any

	// The WHERE clause is present. Includes building the AND/OR nodes.
	// Since this is not obviously sized, we are going to use a strings.Builder for efficiency.
	sb := strings.Builder{}

	sb.WriteString(" WHERE ")

	query, data := t.op.queryData()
	sb.WriteString(query)
	args = append(args, data...)

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
		query, data := next.op.queryData()
		sb.WriteString(query)
		args = append(args, data...)
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
