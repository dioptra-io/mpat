package schema

import (
	_ "embed"
)

//go:embed templates/fies.tmpl
var fiesDDLTemplate string

type FIEsSchema struct{}

func (s FIEsSchema) SchemaName() string {
	return "fies"
}

func (s FIEsSchema) DDL(database, table string) string {
	str, err := renderDDLTemplate(fiesDDLTemplate, database, table)
	if err != nil {
		panic(err)
	}
	return str
}

func (s FIEsSchema) Columns() ([]Column, error) {
	return parseColumnsFromDDLTemplate(fiesDDLTemplate)
}
