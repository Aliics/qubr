package qubr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelectAll(t *testing.T) {
	type bunny struct {
		name      string
		earLength float64
	}

	query, args, err := Select[bunny]().
		From("bunnies").
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `SELECT "name","earLength" FROM "bunnies";`, query)
	assert.Empty(t, args)
}

func TestSelectDefaultTableName(t *testing.T) {
	type bunny struct {
		name      string
		earLength float64
	}

	query, args, err := Select[bunny]().
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `SELECT "name","earLength" FROM "bunny";`, query)
	assert.Empty(t, args)
}

func TestSelectWithSimpleFilter(t *testing.T) {
	type bunny struct {
		name      string
		earLength float64
		ageMonths uint16
	}

	query, args, err := Select[bunny]().
		From("bunnies").
		Where(GreaterThan("earLength", 10)).
		And(NotEqual("name", "")).
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `SELECT "name","earLength","ageMonths" FROM "bunnies" WHERE "earLength" > ? AND "name" != ?;`, query)
	assert.Equal(t, []any{10, ""}, args)
}

func TestSelectWhereDoubleUp(t *testing.T) {
	type bunny struct {
		name      string
		earLength float64
		ageMonths uint16
	}

	_, _, err := Select[bunny]().
		From("bunnies").
		Where(GreaterThan("earLength", 10)).
		Where(NotEqual("name", "")).
		BuildQuery()

	assert.ErrorIs(t, ErrDoubleWhereClause, err)
}

func TestSelectBigLimit(t *testing.T) {
	type donut struct {
		filled    bool
		sprinkled bool
	}

	query, args, err := Select[donut]().
		From("donuts").
		Limit(2938910).
		BuildQuery()

	assert.NoError(t, err)
	assert.Equal(t, `SELECT "filled","sprinkled" FROM "donuts" LIMIT ?;`, query)
	assert.Equal(t, []any{uint64(2938910)}, args)
}

func TestSelectLimitAlreadySet(t *testing.T) {
	type donut struct {
		filled    bool
		sprinkled bool
	}

	_, _, err := Select[donut]().
		From("donuts").
		Limit(1).
		Limit(2).
		BuildQuery()

	assert.ErrorIs(t, ErrLimitAlreadySet, err)
}

func TestSelectAndQueryContext(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64
	}

	db := SetupTestDatabase(
		t,
		`CREATE TABLE "bunnies" ("Name" TEXT, "EarLength" FLOAT);`,
		`INSERT INTO "bunnies" VALUES('ollie', 15)`,
	)

	bunnies, err := Select[bunny]().
		From("bunnies").
		Query(db)

	assert.NoError(t, err)
	assert.Equal(t, []bunny{{"ollie", 15}}, bunnies)
}

func TestSelectAndGetOneContext(t *testing.T) {
	type bunny struct {
		Name      string
		EarLength float64
		IsMortal  bool
	}

	db := SetupTestDatabase(
		t,
		`CREATE TABLE "bunny" ("Name" TEXT, "EarLength" FLOAT, "IsMortal" BOOLEAN);`,
		`INSERT INTO "bunny" VALUES('ollie', 15, TRUE)`,
		`INSERT INTO "bunny" VALUES('oliver', 20, TRUE)`,
		`INSERT INTO "bunny" VALUES('king ollie', 30.57, TRUE)`,
		`INSERT INTO "bunny" VALUES('ollie the omniscient', 25000, FALSE)`,
	)

	longEaredBunny, err := Select[bunny]().
		Where(GreaterThan("EarLength", 20)).
		And(Equal("IsMortal", false)).
		GetOne(db)

	assert.NoError(t, err)
	assert.Equal(t, &bunny{"ollie the omniscient", 25000, false}, longEaredBunny)
}
