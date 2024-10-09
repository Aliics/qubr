package qubr

import (
	"context"
	"database/sql"
	"reflect"
)

func QueryContext[T any](ctx context.Context, db *sql.DB, query string) ([]T, error) {
	rows, err := db.QueryContext(ctx, query)
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
		values := make([]any, numField)
		for i := range numField {
			field := mappedValue.Field(i).Interface()
			values[i] = &field
		}

		// Pull out the row values.
		if err = rows.Scan(values...); err != nil {
			return nil, err
		}

		// Set the row values onto a new "T", field by field.
		for i := range numField {
			mappedValue.Field(i).Set(reflect.ValueOf(*values[i].(*any)))
		}

		// Finally, we have our new element
		mapped = append(mapped, mappedValue.Interface().(T))
	}

	return mapped, nil
}
