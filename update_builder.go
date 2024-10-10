package qubr

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type UpdateBuilder[T any] struct {
	from tableName

	literalValue *T

	fieldOperationTree fieldOperationTree

	err error
}

func Update[T any]() UpdateBuilder[T] {
	return UpdateBuilder[T]{
		from: tableName{tableName: reflect.TypeFor[T]().Name()},
	}
}

func (b UpdateBuilder[T]) Into(tableName string) UpdateBuilder[T] {
	if b.from.schema != "" && b.from.tableName != "" {
		b.err = ErrTableNameAlreadySet
		return b
	}
	t, err := newTableNameFromString(tableName)
	if err != nil {
		b.err = err
		return b
	}

	b.from = *t
	return b
}

func (b UpdateBuilder[T]) SetStruct(t T) UpdateBuilder[T] {
	b.literalValue = &t
	return b
}

func (b UpdateBuilder[T]) Where(op FieldOperation) UpdateBuilder[T] {
	if b.fieldOperationTree != emptyFieldOperationTree {
		b.err = ErrDoubleWhereClause
		return b
	}

	b.fieldOperationTree.op = op

	return b
}

func (b UpdateBuilder[T]) And(op FieldOperation) UpdateBuilder[T] {
	err := appendToFieldOperationTree(&b.fieldOperationTree, func(next *fieldOperationTree) {
		next.and = &fieldOperationTree{op: op}
	})
	if err != nil {
		b.err = err
	}
	return b
}

func (b UpdateBuilder[T]) Or(op FieldOperation) UpdateBuilder[T] {
	err := appendToFieldOperationTree(&b.fieldOperationTree, func(next *fieldOperationTree) {
		next.or = &fieldOperationTree{op: op}
	})
	if err != nil {
		b.err = err
	}
	return b
}

func (b UpdateBuilder[T]) BuildQuery() (query string, args []any, err error) {
	if b.err != nil {
		return "", nil, b.err
	}

	tableName := b.from.String()

	// SET "X" = ?, "Y" = ?
	var setStmt string
	{
		if b.literalValue == nil {
			return "", nil, ErrNoSetStatement
		}

		sb := strings.Builder{}
		sb.WriteString(" SET ")

		// We can calculate the set statement query and the args in one pass.
		// We don't need to keep a reference to which index we are on, so passing over exported fields is fine.

		insertType := reflect.TypeFor[T]()
		insertValue := reflect.ValueOf(*b.literalValue)
		numField := insertValue.NumField()
		for i := range numField {
			fieldType := insertType.Field(i)
			if !fieldType.IsExported() {
				continue
			}

			sb.WriteString(fmt.Sprintf(`"%s"=?,`, fieldType.Name))
			args = append(args, insertValue.Field(i).Interface())
		}

		setStmt = strings.TrimSuffix(sb.String(), ",")
	}

	whereClause, whereArgs := b.fieldOperationTree.BuildQuery()
	args = append(args, whereArgs...)

	return fmt.Sprintf("UPDATE %s%s%s;", tableName, setStmt, whereClause), args, nil
}

func (b UpdateBuilder[T]) Exec(db *sql.DB) (sql.Result, error) {
	return b.ExecContext(context.Background(), db)
}

func (b UpdateBuilder[T]) ExecContext(ctx context.Context, db *sql.DB) (sql.Result, error) {
	query, args, err := b.BuildQuery()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}
