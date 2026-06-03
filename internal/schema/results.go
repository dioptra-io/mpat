package schema

import (
	_ "embed"
)

//go:embed templates/fies.sql
var resultsDDLTemplate string

type ResultsSchema struct{}

func (s ResultsSchema) SchemaName() string {
	return "results"
}

func (s ResultsSchema) DDL(database, table string) string {
	str, err := renderDDLTemplate(resultsDDLTemplate, database, table)
	if err != nil {
		panic(err)
	}
	return str
}

func (s ResultsSchema) Columns() ([]Column, error) {
	return parseColumnsFromDDLTemplate(resultsDDLTemplate)
}
