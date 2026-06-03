package store

import (
	"fmt"

	clickhouse "github.com/AfterShip/clickhouse-sql-parser/parser"
)

type Column struct {
	Name         string
	Type         string
	Materialized bool
}

type TableSchema struct {
	Database string
	Table    string
	Columns  []Column
}

// BaseColumns returns only non-materialized columns.
func (s TableSchema) BaseColumns() []Column {
	cols := make([]Column, 0, len(s.Columns))
	for _, c := range s.Columns {
		if !c.Materialized {
			cols = append(cols, c)
		}
	}
	return cols
}

// IsSubsetOf returns true if every base column in s exists in other
// with exactly the same name and type.
func (s TableSchema) IsSubsetOf(other TableSchema) bool {
	index := make(map[string]string, len(other.Columns))
	for _, c := range other.BaseColumns() {
		index[c.Name] = c.Type
	}
	for _, c := range s.BaseColumns() {
		otherType, ok := index[c.Name]
		if !ok || otherType != c.Type {
			return false
		}
	}
	return true
}

// ParseCreateQuery parses a CREATE TABLE DDL string and returns a TableSchema.
// Returns an error if the input is not a valid CREATE TABLE statement.
func ParseCreateQuery(ddl string) (TableSchema, error) {
	p := clickhouse.NewParser(ddl)
	stmts, err := p.ParseStmts()
	if err != nil {
		return TableSchema{}, fmt.Errorf("sqlparser: failed to parse DDL: %w", err)
	}
	if len(stmts) == 0 {
		return TableSchema{}, fmt.Errorf("sqlparser: no statements found in DDL")
	}

	ct, ok := stmts[0].(*clickhouse.CreateTable)
	if !ok {
		return TableSchema{}, fmt.Errorf("sqlparser: expected CREATE TABLE statement, got %T", stmts[0])
	}
	if ct.TableSchema == nil {
		return TableSchema{}, fmt.Errorf("sqlparser: CREATE TABLE has no schema")
	}

	// Extract database and table name.
	var database, table string
	if ct.Name != nil {
		if ct.Name.Database != nil {
			database = ct.Name.Database.Name
		}
		table = ct.Name.Table.Name
	}

	// Extract columns.
	columns := make([]Column, 0, len(ct.TableSchema.Columns))
	for _, col := range ct.TableSchema.Columns {
		colDef, ok := col.(*clickhouse.ColumnDef)
		if !ok {
			continue
		}
		columns = append(columns, Column{
			Name:         colDef.Name.Ident.Name,
			Type:         clickhouse.Format(colDef.Type),
			Materialized: colDef.MaterializedExpr != nil,
		})
	}

	return TableSchema{
		Database: database,
		Table:    table,
		Columns:  columns,
	}, nil
}
