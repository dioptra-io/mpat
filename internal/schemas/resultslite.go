package schemas

import (
	_ "embed"
)

//go:embed templates/resultslite.sql
var resultsliteDDL string

type ResultsliteSchema struct{}

func (s ResultsliteSchema) SchemaName() string {
	return "resultslite"
}

func (s ResultsliteSchema) DDL(database, table string) string {
	str, err := renderDDLTemplate(resultsliteDDL, database, table)
	if err != nil {
		panic(err)
	}
	return str
}

func (s ResultsliteSchema) Columns() ([]Column, error) {
	return parseColumns(resultsliteDDL)
}
