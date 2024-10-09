package qubr

import (
	"errors"
	"fmt"
)

var (
	ErrTableNameAlreadySet = errors.New("table name has already been set")

	ErrDoubleWhereClause  = errors.New("where clause is already present")
	ErrMissingWhereClause = errors.New("where clause is not yet present")

	ErrLimitAlreadySet = errors.New("limit value has already been set")

	ErrNoInsertValues = errors.New("insert statement has no insert values")

	ErrNoRows = errors.New("get resulted in no rows")
)

// ErrInvalidTableName occurs when a string provided cannot be used as a table name.
type ErrInvalidTableName struct {
	Name string
}

func (e ErrInvalidTableName) Error() string {
	return fmt.Sprintf(`"%s" is not a valid table name`, e.Name)
}
