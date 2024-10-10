package qubr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInsert(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64
	}

	query, args, err := Insert[bunny]().
		Values(
			bunny{"oliver", 20},
			bunny{"king ollie", 30},
		).
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "bunny" VALUES (?,?),(?,?);`, query)
	assert.Equal(t, []any{"oliver", 20.0, "king ollie", 30.0}, args)
}

func TestInsertWithUnexported(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64

		age int64
	}

	query, args, err := Insert[bunny]().
		Values(
			bunny{"oliver", 20, 0},
			bunny{"king ollie", 30, 0},
		).
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "bunny" VALUES (?,?),(?,?);`, query)
	assert.Equal(t, []any{"oliver", 20.0, "king ollie", 30.0}, args)
}

func TestInsertNoValues(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64
	}

	_, _, err := Insert[bunny]().
		BuildQuery()

	assert.ErrorIs(t, ErrNoInsertValues, err)
}

func TestInsertAndExec(t *testing.T) {
	type bunny struct {
		Name           string
		TummyWhiteness int64

		age int64
	}

	db := SetupTestDatabase(t, `CREATE TABLE "bunnies" ("Name" TEXT, "TummyWhiteness" INT);`)

	result, err := Insert[bunny]().
		Into("bunnies").
		Values(
			bunny{"oliver", 1000, 0},
			bunny{"king ollie", 1500, 0},
		).
		Exec(db)

	assert.NoError(t, err)
	affected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), affected)
}
