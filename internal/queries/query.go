package queries

import clientv2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"

// Query represents a SELECT or INSERT query.
type Query interface {
	// This method returns the actual query to be run.
	Query() (string, error)

	// This is useful for getting client and object information.
	Set(client *clientv2.SQLClient, obj any)
}
