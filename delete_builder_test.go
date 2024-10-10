package qubr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDeleteAll(t *testing.T) {
	type food struct {
		Name       string
		Kilojoules float64
	}

	query, args, err := Delete[food]().
		From("pantry").
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `DELETE FROM "pantry";`, query)
	assert.Empty(t, args)
}

func TestDeleteWithSimpleFilter(t *testing.T) {
	type food struct {
		Name       string
		Kilojoules float64
	}

	query, args, err := Delete[food]().
		Where(LessThan("Kilojoules", 415)).
		And(NotEqual("Name", "mold")).
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `DELETE FROM "food" WHERE "Kilojoules"<? AND "Name"!=?;`, query)
	assert.Equal(t, []any{415, "mold"}, args)
}

func TestDeleteWhereDoubleUp(t *testing.T) {
	type food struct {
		Name       string
		Kilojoules float64
	}

	_, _, err := Delete[food]().
		Where(LessThan("kilojoules", 415)).
		Where(NotEqual("name", "mold")).
		BuildQuery()

	assert.ErrorIs(t, ErrDoubleWhereClause, err)
}

func TestDeleteBigLimit(t *testing.T) {
	type food struct {
		Name       string
		Kilojoules float64
	}

	query, args, err := Delete[food]().
		From("store").
		Limit(2938910).
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `DELETE FROM "store" LIMIT ?;`, query)
	assert.Equal(t, []any{uint64(2938910)}, args)
}

func TestDeleteLimitAlreadySet(t *testing.T) {
	type food struct {
		Name       string
		Kilojoules float64
	}

	_, _, err := Delete[food]().
		Limit(1).
		Limit(2).
		BuildQuery()

	assert.ErrorIs(t, ErrLimitAlreadySet, err)
}

func TestDeleteAndExec(t *testing.T) {
	type food struct {
		Name       string
		Kilojoules float64
	}

	db := SetupTestDatabase(
		t,
		`CREATE TABLE "food" ("Name" TEXT, "Kilojoules" FLOAT);`,
		`INSERT INTO "food" VALUES('donut', 875)`,
		`INSERT INTO "food" VALUES('spaghetti', 1234)`,
		`INSERT INTO "food" VALUES('tic tac', 12)`,
	)

	result, err := Delete[food]().
		Where(LessThanOrEqual("Kilojoules", 15)).
		Exec(db)

	assert.NoError(t, err)
	affected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}
