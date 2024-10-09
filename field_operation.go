package qubr

import (
	"fmt"
)

type FieldOperation struct {
	Operator Operator

	FieldName string
	ValueRaw  any
}

func (f FieldOperation) QueryData() (string, any) {
	return fmt.Sprintf(`"%s" %s ?`, f.FieldName, f.Operator), f.ValueRaw
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
