package schema

import (
	_ "embed"
)

//go:embed templates/ripeprefixes.tmpl
var ripePrefixesDDLTemplate string

// RipePrefixesSchema describes the structure of the ripeprefixes table,
// which stores IP prefixes originated by ASes as observed by the RIPE RIS system.
type RipePrefixesSchema struct{}

func (s RipePrefixesSchema) SchemaName() string {
	return "ripeprefixes"
}

func (s RipePrefixesSchema) DDL(database, table string) string {
	str, err := renderDDLTemplate(ripePrefixesDDLTemplate, database, table)
	if err != nil {
		panic(err)
	}
	return str
}

func (s RipePrefixesSchema) Columns() ([]Column, error) {
	return parseColumnsFromDDLTemplate(ripePrefixesDDLTemplate)
}
