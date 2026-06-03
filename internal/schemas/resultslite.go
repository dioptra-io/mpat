package schemas

import (
	_ "embed"
)

//go:embed templates/resultslite.sql
var resultsliteDDLTemplate string

type ResultsliteSchema struct{}

func (s ResultsliteSchema) SchemaName() string {
	return "resultslite"
}

func (s ResultsliteSchema) DDL(database, table string) string {
	str, err := renderDDLTemplate(resultsliteDDLTemplate, database, table)
	if err != nil {
		panic(err)
	}
	return str
}

func (s ResultsliteSchema) Columns() ([]Column, error) {
	return parseColumnsFromDDLTemplate(resultsliteDDLTemplate)
}
