package qubr

type QueryBuilder interface {
	BuildQuery() (string, error)
}
