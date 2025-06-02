package queries

import (
	"fmt"
	"strings"

	"github.com/dioptra-io/ufuk-research/cmd/orm"
	clientv2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"
)

type BasicInsertQuery struct {
	TableName string
	client    *clientv2.SQLClient
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

func (q *BasicInsertQuery) Set(client *clientv2.SQLClient, obj any) {
	q.client = client
	q.object = obj
}
