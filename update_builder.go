package qubr

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// UpdateBuilder is a QueryBuilder for building SQL UPDATE queries.
// Utilizing the related Update functions, you can construct these queries.
// Example:
//
//	result, err := Update[User]().
//		SetStruct(User{ID: 42, Name: "Alex"}).
//		Where(Equal("ID", 42)).
//		ExecContext(ctx, db) // Or BuildQuery to use the raw SQL.
//	if err != nil {
//		return err
//	}
type UpdateBuilder[T any] struct {
	from tableName

	literalValue *T

	fieldOperationTree fieldOperationTree

	err error
}

// Update will construct a new UpdateBuilder, and the table name will be set based on the type given.
func Update[T any]() UpdateBuilder[T] {
	return UpdateBuilder[T]{
		from: tableName{forType: reflect.TypeFor[T]()},
	}
}

// Into will explicitly set the table name. This cannot be called more once.
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

// Where will apply a FieldOperation as the initial comparison operator of a where clause.
// Where cannot be called more than once, use UpdateBuilder.And or UpdateBuilder.Or for further filtering.
func (b UpdateBuilder[T]) Where(op FieldOperation) UpdateBuilder[T] {
	if b.fieldOperationTree != emptyFieldOperationTree {
		b.err = ErrDoubleWhereClause
		return b
	}

	b.fieldOperationTree.op = op

	return b
}

// And will apply an AND to the existing where clause. UpdateBuilder.Where must be called before this.
func (b UpdateBuilder[T]) And(op FieldOperation) UpdateBuilder[T] {
	err := appendToFieldOperationTree(&b.fieldOperationTree, func(next *fieldOperationTree) {
		next.and = &fieldOperationTree{op: op}
	})
	if err != nil {
		b.err = err
	}
	return b
}

// Or will apply an OR to the existing where clause. UpdateBuilder.Where must be called before this.
func (b UpdateBuilder[T]) Or(op FieldOperation) UpdateBuilder[T] {
	err := appendToFieldOperationTree(&b.fieldOperationTree, func(next *fieldOperationTree) {
		next.or = &fieldOperationTree{op: op}
	})
	if err != nil {
		b.err = err
	}
	return b
}

// BuildQuery will construct the SQL query UpdateBuilder is currently representing.
// User input will utilize placeholders, and the values of the input will be in the 2nd return value, args.
// If there was an issue in the construction of UpdateBuilder, then the 3rd return value, err will not non-nil.
//
// The resulting query should look something like:
//
//	UPDATE "table" SET "field1" = ?, "field2" = ? WHERE "field1" = ?;
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
			f := insertType.Field(i)
			if !f.IsExported() {
				continue
			}

			sb.WriteString(fmt.Sprintf(`"%s" = ?, `, structFieldName(f)))
			args = append(args, insertValue.Field(i).Interface())
		}

		setStmt = strings.TrimSuffix(sb.String(), ", ")
	}

	whereClause, whereArgs := b.fieldOperationTree.buildQuery()
	args = append(args, whereArgs...)

	return fmt.Sprintf("UPDATE %s%s%s;", tableName, setStmt, whereClause), args, nil
}

// Exec wraps UpdateBuilder.ExecContext, which will execute the update query represented by the UpdateBuilder.
func (b UpdateBuilder[T]) Exec(db *sql.DB) (sql.Result, error) {
	return b.ExecContext(context.Background(), db)
}

// ExecContext will execute the update query represented by the UpdateBuilder.
// This will execute using the provided sql.DB, and the response is simply passed back.
func (b UpdateBuilder[T]) ExecContext(ctx context.Context, db *sql.DB) (sql.Result, error) {
	query, args, err := b.BuildQuery()
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, query, args...)
}
