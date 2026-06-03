package schemas

import (
	"bytes"
	"fmt"
	"text/template"

	clickhouse "github.com/AfterShip/clickhouse-sql-parser/parser"
)

type Column struct {
	Name         string
	Type         string
	Materialized bool
}

// Schema describes the structure of a ClickHouse table. When registering a new
// schema it is important to add the template options .Database' and '.Table'.
type Schema interface {
	// SchemaName returns the canonical name of the schema (e.g. "fies", "results").
	SchemaName() string

	// DDL returns the CREATE TABLE DDL template string for this schema.
	DDL(database, table string) string

	// Columns returns the list of columns defined in this schema,
	// parsed from the DDL. Materialized columns are included.
	Columns() ([]Column, error)
}

// IsSubsetOf returns true if every base column in a exists in b
// with exactly the same name and type.
func IsSubsetOf(a, b Schema, includeMaterialized bool) (bool, error) {
	aCols, err := a.Columns()
	if err != nil {
		return false, err
	}
	bCols, err := b.Columns()
	if err != nil {
		return false, err
	}
	index := make(map[string]string)
	for _, col := range bCols {
		if includeMaterialized || !col.Materialized {
			index[col.Name] = col.Type
		}
	}
	for _, col := range aCols {
		if !includeMaterialized && col.Materialized {
			continue
		}
		if index[col.Name] != col.Type {
			return false, nil
		}
	}
	return true, nil
}

// AreEquivalent returns true if a and b have the same set of columns with the
// same types, regardless of order. Pass includeMaterialized to control whether
// materialized columns are included in the comparison.
func AreEquivalent(a, b Schema, includeMaterialized bool) (bool, error) {
	aSubsetB, err := IsSubsetOf(a, b, includeMaterialized)
	if err != nil {
		return false, err
	}
	bSubsetA, err := IsSubsetOf(b, a, includeMaterialized)
	if err != nil {
		return false, err
	}
	return aSubsetB && bSubsetA, nil
}

// columnsFromDDL renders the DDL template with dummy values and parses
// the resulting CREATE TABLE statement to extract column definitions.
func parseColumns(ddlTemplate string) ([]Column, error) {
	ddl, err := renderDDLTemplate(ddlTemplate, "database", "table") // placeholder values
	if err != nil {
		return nil, fmt.Errorf("schema: failed to render DDL template: %w", err)
	}

	p := clickhouse.NewParser(ddl)
	stmts, err := p.ParseStmts()
	if err != nil {
		return nil, fmt.Errorf("sqlparser: failed to parse DDL: %w", err)
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("sqlparser: no statements found in DDL")
	}
	ct, ok := stmts[0].(*clickhouse.CreateTable)
	if !ok {
		return nil, fmt.Errorf("sqlparser: expected CREATE TABLE statement, got %T", stmts[0])
	}
	if ct.TableSchema == nil {
		return nil, fmt.Errorf("sqlparser: CREATE TABLE has no schema")
	}
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
	return columns, nil
}

func renderDDLTemplate(templateString, database, table string) (string, error) {
	t, err := template.New("schema").Parse(templateString)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, map[string]any{
		"Database": database,
		"Table":    table,
	}); err != nil {
		return "", err
	}
	return buf.String(), nil
}
