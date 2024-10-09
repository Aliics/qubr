package qube

type QueryBuilder interface {
	BuildQuery() (string, error)
}
