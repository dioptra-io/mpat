package queries

// Represents a Query
type Query interface {
	// This method returns the actual query to be run.
	Query() (string, error)
}
