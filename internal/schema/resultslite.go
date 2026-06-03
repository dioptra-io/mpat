package schema

import (
	_ "embed"
)

//go:embed templates/resultslite.tmpl
var resultsliteDDLTemplate string

type ResultsLiteSchema struct{}

func (s ResultsLiteSchema) SchemaName() string {
	return "resultslite"
}

func (s ResultsLiteSchema) DDL(database, table string) string {
	str, err := renderDDLTemplate(resultsliteDDLTemplate, database, table)
	if err != nil {
		panic(err)
	}
	return str
}

func (s ResultsLiteSchema) Columns() ([]Column, error) {
	return parseColumnsFromDDLTemplate(resultsliteDDLTemplate)
}
