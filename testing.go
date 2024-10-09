package qube

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func SetupTestDatabase(t *testing.T, setupQueries ...string) *sql.DB {
	// Create a temp file so that the sqlite file is not populating random directories.
	f, err := os.CreateTemp("", "qube-test-data")
	assert.NoError(t, err, "could not create database temp file")
	db, err := sql.Open("sqlite3", f.Name())
	assert.NoError(t, err, "could not connect to sqlite3")

	// Run the provided queries as a setup step.
	for _, query := range setupQueries {
		_, err = db.Exec(query)
		assert.NoError(t, err, "failed to run setup queries")
	}

	return db
}
