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
