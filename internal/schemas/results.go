package schemas

import (
	_ "embed"
)

//go:embed templates/fies.sql
var resultsDDL string

type ResultsSchema struct{}

func (s ResultsSchema) SchemaName() string {
	return "results"
}

func (s ResultsSchema) DDL(database, table string) string {
	str, err := renderDDLTemplate(resultsDDL, database, table)
	if err != nil {
		panic(err)
	}
	return str
}

func (s ResultsSchema) Columns() ([]Column, error) {
	return parseColumns(resultsDDL)
}
