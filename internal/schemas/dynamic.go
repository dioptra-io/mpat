package schemas

import (
	"fmt"
	clickhouse "github.com/AfterShip/clickhouse-sql-parser/parser"
	"strings"
)

type DynamicSchema struct {
	name        string
	ddlTemplate string
}

func NewDynamicSchema(ddl string) (*DynamicSchema, error) {
	p := clickhouse.NewParser(ddl)
	stmts, err := p.ParseStmts()
	if err != nil {
		return nil, fmt.Errorf("schema: failed to parse table identifier: %w", err)
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("schema: no statements found in DDL")
	}
	ct, ok := stmts[0].(*clickhouse.CreateTable)
	if !ok {
		return nil, fmt.Errorf("schema: expected CREATE TABLE statement, got %T", stmts[0])
	}
	if ct.Name == nil {
		return nil, fmt.Errorf("schema: CREATE TABLE has no name")
	}

	var database, table string
	if ct.Name.Database != nil {
		database = ct.Name.Database.Name
	}
	table = ct.Name.Table.Name

	name := table
	if database != "" {
		name = database + "." + table
	}

	oldRef := fmt.Sprintf("%s.%s", database, table)
	ddlTemplate := strings.Replace(ddl, oldRef, "{{.Database}}.{{.Table}}", 1)

	// Add IF NOT EXISTS if not already present.
	if !ct.IfNotExists {
		ddlTemplate = strings.Replace(ddlTemplate, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	}

	return &DynamicSchema{
		name:        name,
		ddlTemplate: ddlTemplate,
	}, nil
}

func (s DynamicSchema) SchemaName() string {
	return s.name
}

func (s DynamicSchema) DDL(database, table string) string {
	str, err := renderDDLTemplate(s.ddlTemplate, database, table)
	if err != nil {
		panic(err)
	}
	return str
}

func (s DynamicSchema) Columns() ([]Column, error) {
	return parseColumnsFromDDLTemplate(s.ddlTemplate)
}
