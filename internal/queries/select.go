package queries

import (
	"fmt"
	"strings"

	"github.com/dioptra-io/ufuk-research/cmd/orm"
	clientv2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"
)

type BasicSelectQuery struct {
	TableNames []string
	client     *clientv2.SQLClient
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
		strings.Join(fieldNames, ", "),
		q.client.Database(),
		strings.Join(q.TableNames, ")|("),
	), nil
}

func (q *BasicSelectQuery) Set(client *clientv2.SQLClient, obj any) {
	q.client = client
	q.object = obj
}
