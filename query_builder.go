package qubr

type QueryBuilder interface {
	BuildQuery() (query string, args []any, err error)
}
