package schema

import (
	_ "embed"
)

//go:embed templates/fies.sql
var fiesDDLTemplate string

type FiesSchema struct{}

func (s FiesSchema) SchemaName() string {
	return "fies"
}

func (s FiesSchema) DDL(database, table string) string {
	str, err := renderDDLTemplate(fiesDDLTemplate, database, table)
	if err != nil {
		panic(err)
	}
	return str
}

func (s FiesSchema) Columns() ([]Column, error) {
	return parseColumnsFromDDLTemplate(fiesDDLTemplate)
}
