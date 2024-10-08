package qube

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelectAll(t *testing.T) {
	type bunny struct {
		name      string
		earLength float64
	}

	sql, err := Select(bunny{"ollie", 15}).
		From("bunnies").
		Build()

	assert.NoError(t, err)
	assert.Equal(t, `SELECT "name","earLength" FROM "bunnies";`, sql)
}

func TestSelectWithoutTableName(t *testing.T) {
	type bunny struct {
		name      string
		earLength float64
	}

	_, err := Select(bunny{"ollie", 15}).
		Build()

	assert.ErrorIs(t, ErrInvalidTableName{""}, err)
}

func TestSelectWithSimpleFilter(t *testing.T) {
	type bunny struct {
		name      string
		earLength float64
		ageMonths uint16
	}

	sql, err := Select(bunny{"ollie", 15, 72}).
		From("bunnies").
		Where(GreaterThan("earLength", 10)).
		And(NotEqual("name", "")).
		Build()

	assert.NoError(t, err)
	assert.Equal(t, `SELECT "name","earLength","ageMonths" FROM "bunnies" WHERE "earLength" > 10 AND "name" != '';`, sql)
}

func TestSelectWhereDoubleUp(t *testing.T) {
	type bunny struct {
		name      string
		earLength float64
		ageMonths uint16
	}

	_, err := Select(bunny{"ollie", 15, 72}).
		From("bunnies").
		Where(GreaterThan("earLength", 10)).
		Where(NotEqual("name", "")).
		Build()

	assert.ErrorIs(t, ErrDoubleWhereClause, err)
}

func TestSelectBigLimit(t *testing.T) {
	type donut struct {
		filled    bool
		sprinkled bool
	}

	sql, err := Select(donut{true, true}).
		From("donuts").
		Limit(2938910).
		Build()

	assert.NoError(t, err)
	assert.Equal(t, `SELECT "filled","sprinkled" FROM "donuts" LIMIT 2938910;`, sql)
}

func TestSelectLimitAlreadySet(t *testing.T) {
	type donut struct {
		filled    bool
		sprinkled bool
	}

	_, err := Select(donut{true, true}).
		From("donuts").
		Limit(1).
		Limit(2).
		Build()

	assert.ErrorIs(t, ErrLimitAlreadySet, err)
}
