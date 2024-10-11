package qubr

import "reflect"

func structFieldName(field reflect.StructField) string {
	if dbName, ok := field.Tag.Lookup("db"); ok {
		return dbName
	}
	return field.Name
}
