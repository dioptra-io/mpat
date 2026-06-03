package store

import (
	"bytes"
	"fmt"
	"text/template"

	_ "embed"
)

const (
	defaultDatabase = "mpat"
)

//go:embed sql/results_create.sql
var resultsDDLTemplate string

// CreateResultsSQL renders the results.sql template for the given destination table.
func CreateResultsSQL(dest DatabaseTable) (string, error) {
	tmpl, err := template.New("results").Parse(resultsDDLTemplate)
	if err != nil {
		return "", fmt.Errorf("store: failed to parse results DDL template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, dest); err != nil {
		return "", fmt.Errorf("store: failed to render results DDL template: %w", err)
	}

	return buf.String(), nil
}
