package qubr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUpdate(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64
	}

	query, args, err := Update[bunny]().
		SetStruct(bunny{"king oliver", 30}).
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "bunny" SET "Name"=?,"EarLength"=?;`, query)
	assert.Equal(t, []any{"king oliver", 30.0}, args)
}

func TestUpdateWithSimpleFilter(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64
	}

	query, args, err := Update[bunny]().
		SetStruct(bunny{"king oliver", 30}).
		Where(Equal("Name", "mr. oliver")).
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "bunny" SET "Name"=?,"EarLength"=? WHERE "Name"=?;`, query)
	assert.Equal(t, []any{"king oliver", 30.0, "mr. oliver"}, args)
}

func TestUpdateWithWhereDoubleUp(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64
	}

	_, _, err := Update[bunny]().
		SetStruct(bunny{"king oliver", 30}).
		Where(Equal("Name", "mr. oliver")).
		Where(Equal("Name", "mr. oliver")).
		BuildQuery()

	assert.ErrorIs(t, ErrDoubleWhereClause, err)
}

func TestUpdateWithNoSetter(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64
	}

	_, _, err := Update[bunny]().
		Where(Equal("Name", "mr. oliver")).
		BuildQuery()

	assert.ErrorIs(t, ErrNoSetStatement, err)
}

func TestUpdateAndExec(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64
	}

	db := SetupTestDatabase(
		t,
		`CREATE TABLE "bunny_kingdom" ("Name" TEXT, "EarLength" FLOAT);`,
		`INSERT INTO "bunny_kingdom" VALUES ('oliver', 20);`,
	)

	result, err := Update[bunny]().
		Into("bunny_kingdom").
		SetStruct(bunny{"king oliver", 30}).
		Where(Equal("Name", "oliver")).
		Exec(db)

	assert.NoError(t, err)
	affected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}
