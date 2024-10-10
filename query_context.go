package qubr

import (
	"context"
	"database/sql"
	"reflect"
)

// QueryContext is a wrapper for sql.DB's QueryContext function.
// The rows are mapped to T, where each field of T is a column in the row.
func QueryContext[T any](ctx context.Context, db *sql.DB, query string, args ...any) ([]T, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	selectType := reflect.TypeFor[T]()

	// No way to determine the number of rows, other than by simply scanning one-by-one.
	var mapped []T
	for rows.Next() {
		mappedValue := reflect.ValueOf(new(T)).Elem()

		numField := selectType.NumField()

		// Create pointers for "Scan" to populate row values onto a temporary "values" array.
		numExported := 0 // Our end number of rows may not actually be equal to "numField" due to unexported fields.
		values := make([]any, numField)
		for i := range numField {
			f := mappedValue.Field(i)
			if !f.CanSet() {
				continue
			}

			field := f.Interface()
			values[numExported] = &field

			numExported++
		}

		// Pull out the row values.
		if err = rows.Scan(values[:numExported]...); err != nil {
			return nil, err
		}

		// Set the row values onto a new "T", field by field.
		for i := range numExported {
			mappedValue.Field(i).Set(reflect.ValueOf(*values[i].(*any)))
		}

		// Finally, we have our new element
		mapped = append(mapped, mappedValue.Interface().(T))
	}

	return mapped, nil
}
