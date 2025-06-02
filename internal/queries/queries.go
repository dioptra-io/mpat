package queries

import (
	"fmt"
	"strings"

	"github.com/dioptra-io/ufuk-research/cmd/orm"
	v2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"
)

// Query represents a SELECT or INSERT query.
type Query interface {
	// This method returns the actual query to be run.
	Query() (string, error)

	// This is useful for getting client and object information.
	Set(client *v2.SQLClient, obj any)
}

type BasicSelectQuery struct {
	TableNames []string
	client     *v2.SQLClient
	object     any
}

func (q *BasicSelectQuery) Query() (string, error) {
	query := `
SELECT
    %s
FROM
    merge('%s', '(%s)')
;` // end of the query

	fieldNames, err := orm.GetFieldJSONTags(q.object)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		query,
		fieldNames,
		q.client.Database(),
		strings.Join(q.TableNames, ")|("),
	), nil
}

func (q *BasicSelectQuery) Set(client *v2.SQLClient, obj any) {
	q.client = client
	q.object = obj
}

type BasicInsertQuery struct {
	TableName string
	client    *v2.SQLClient
	object    any
}

func (q *BasicInsertQuery) Query() (string, error) {
	query := `
INSERT INTO 
    %s.%s
(%s)
VALUES
    (%s)
;` // end of the query

	fieldNames, err := orm.GetInsertableFieldJSONTags(q.object)
	fmt.Printf("err23e: %v\n", err)
	if err != nil {
		return "", err
	}
	placeholders := make([]string, 0, len(fieldNames))
	for i := 0; i < len(fieldNames); i++ {
		placeholders = append(placeholders, "?")
	}

	return fmt.Sprintf(
		query,
		q.client.Database(),
		q.TableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "),
	), nil
}

func (q *BasicInsertQuery) Set(client *v2.SQLClient, obj any) {
	q.client = client
	q.object = obj
}
