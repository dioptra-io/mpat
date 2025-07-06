package queries

import (
	"fmt"
	"strings"

	"github.com/dioptra-io/ufuk-research/cmd/orm"
)

type BasicInsertQuery struct {
	TableName string
	Database  string
	Object    any
}

func (q *BasicInsertQuery) Query() (string, error) {
	query := `
INSERT INTO 
    %s.%s
(%s)
VALUES
    (%s)
;` // end of the query

	fieldNames, err := orm.GetInsertableFieldJSONTags(q.Object)
	if err != nil {
		return "", err
	}

	placeholders := make([]string, 0, len(fieldNames))
	for i := 0; i < len(fieldNames); i++ {
		placeholders = append(placeholders, "?")
	}

	return fmt.Sprintf(
		query,
		q.Database,
		q.TableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "),
	), nil
}

type BasicInsertStartQuery struct {
	TableName string
	Database  string
}

func (q *BasicInsertStartQuery) Query() (string, error) {
	query := `INSERT INTO %s.%s FORMAT CSVWithNames` // end of the query

	return fmt.Sprintf(
		query,
		q.Database,
		q.TableName,
	), nil
}

type InsertFromScores struct {
	TableNameToInsert string
	TableNameToSelect string
	Database          string
}

func (q *InsertFromScores) Query() (string, error) {
	query := `INSERT INTO %s.%s 
SELECT
	near_addr AS addr,
	uniqExact(probe_dst_prefix) AS route_score
FROM %s.%s 
GROUP BY 
	near_addr 
ORDER BY 
	route_score ASC

;` // end of the query

	return fmt.Sprintf(
		query,
		q.Database,
		q.TableNameToInsert,
		q.Database,
		q.TableNameToSelect,
	), nil
}

type InsertFromUniquePrefixes struct {
	TableNameToInsert string
	TableNameToSelect string
	Database          string
}

func (q *InsertFromUniquePrefixes) Query() (string, error) {
	query := `INSERT INTO %s.%s 
SELECT
	DISTINCT probe_dst_prefix
FROM %s.%s 
ORDER BY probe_dst_prefix
;` // end of the query

	return fmt.Sprintf(
		query,
		q.Database,
		q.TableNameToInsert,
		q.Database,
		q.TableNameToSelect,
	), nil
}
