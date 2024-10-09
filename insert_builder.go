package qubr

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type InsertBuilder[T any] struct {
	into tableName

	literalValues []T

	err error
}

func Insert[T any]() InsertBuilder[T] {
	return InsertBuilder[T]{
		into: tableName{tableName: reflect.TypeFor[T]().Name()},
	}
}

func (b InsertBuilder[T]) Into(tableName string) InsertBuilder[T] {
	if b.into.schema != "" && b.into.tableName != "" {
		b.err = ErrTableNameAlreadySet
		return b
	}

	t, err := newTableNameFromString(tableName)
	if err != nil {
		b.err = err
		return b
	}

	b.into = *t
	return b
}

func (b InsertBuilder[T]) Values(t ...T) InsertBuilder[T] {
	b.literalValues = t
	return b
}

func (b InsertBuilder[T]) BuildQuery() (query string, args []any, err error) {
	if b.err != nil {
		return "", nil, b.err
	}

	tableName := b.into.String()

	// Create a set of placeholders (?,...)... for each "literalValue", and append the actual values to args.
	// End result should look like: VALUES (?,?),(?,?)
	var values string
	{
		if len(b.literalValues) == 0 {
			return "", nil, ErrNoInsertValues
		}

		values += " VALUES "
		for i, v := range b.literalValues {
			insertValue := reflect.ValueOf(v)
			numField := insertValue.NumField()

			// (?,?)
			placeholders := strings.TrimSuffix(strings.Repeat("?,", numField), ",") // Remove trailing comma.
			values += fmt.Sprintf("(%s)", placeholders)

			for i := range numField {
				args = append(args, insertValue.Field(i).Interface())
			}

			if i < len(b.literalValues)-1 {
				values += ","
			}
		}
	}

	return fmt.Sprintf("INSERT INTO %s%s;", tableName, values), args, nil
}

func (b InsertBuilder[T]) Exec(db *sql.DB) (sql.Result, error) {
	return b.ExecContext(context.Background(), db)
}

func (b InsertBuilder[T]) ExecContext(ctx context.Context, db *sql.DB) (sql.Result, error) {
	query, args, err := b.BuildQuery()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}
