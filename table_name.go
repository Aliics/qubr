package qubr

import (
	"fmt"
	"reflect"
	"strings"
)

type tableName struct {
	forType   reflect.Type
	schema    string
	tableName string
}

func newTableNameFromString(s string) (*tableName, error) {
	parts := strings.Split(s, ".")
	if s == "" || len(parts) > 2 {
		return nil, ErrInvalidTableName{s}
	}

	var t tableName
	if len(parts) > 1 {
		// Schema was provided in tableName, it comes before the actual table name.
		t.schema = parts[0]
	}

	// Whether a schema is provided or not, the table name is always the last part.
	t.tableName = parts[len(parts)-1]

	return &t, nil
}

func (t tableName) String() string {
	if t.schema == "" && t.tableName == "" {
		return `"` + t.forType.Name() + `"`
	}

	if t.schema != "" {
		return fmt.Sprintf(`"%s"."%s"`, t.schema, t.tableName)
	}

	return `"` + t.tableName + `"`
}
