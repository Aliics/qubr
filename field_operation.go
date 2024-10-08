package qube

import "fmt"

type FieldOperation struct {
	Operator Operator

	FieldName string
	ValueRaw  string
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

type fieldOperationTree struct {
	op FieldOperation

	or  *fieldOperationTree
	and *fieldOperationTree
}

var emptyFieldOperationTree = fieldOperationTree{}

type sqlComparable interface {
	bool |
		int | int8 | int16 | int32 | int64 | // Integers.
		uint | uint8 | uint16 | uint32 | uint64 | // Unsigned integers.
		float32 | float64 | // Floats.
		string | []byte // String variants.
}

func comparableToString[c sqlComparable](v c) string {
	var raw string
	switch x := any(v).(type) {
	case string, []byte:
		raw = fmt.Sprintf("'%s'", x)
	default:
		raw = fmt.Sprintf("%v", v)
	}

	return raw
}

func Equal[c sqlComparable](field string, v c) FieldOperation {
	return FieldOperation{OperatorEqual, field, comparableToString(v)}
}

func NotEqual[c sqlComparable](field string, v c) FieldOperation {
	return FieldOperation{OperatorNotEqual, field, comparableToString(v)}
}

func GreaterThan[c sqlComparable](field string, v c) FieldOperation {
	return FieldOperation{OperatorGreaterThan, field, comparableToString(v)}
}

func LessThan[c sqlComparable](field string, v c) FieldOperation {
	return FieldOperation{OperatorLessThan, field, comparableToString(v)}
}

func GreaterThanOrEqual[c sqlComparable](field string, v c) FieldOperation {
	return FieldOperation{OperatorGreaterThanOrEqual, field, comparableToString(v)}
}

func LessThanOrEqual[c sqlComparable](field string, v c) FieldOperation {
	return FieldOperation{OperatorLessThanOrEqual, field, comparableToString(v)}
}
