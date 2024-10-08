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
