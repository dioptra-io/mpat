package queries

import (
	"fmt"

	clientv2 "github.com/dioptra-io/ufuk-research/pkg/client/v2"
)

type BasicDeleteQuery struct {
	TableName       string
	AddCheckIfExist bool
	client          *clientv2.SQLClient
	object          any
}

func (q *BasicDeleteQuery) Query() (string, error) {
	query := `
DROP TABLE 
    %s %s.%s
;`

	existsCheck := ""
	if q.AddCheckIfExist {
		existsCheck = "IF EXISTS"
	}

	return fmt.Sprintf(
		query,
		existsCheck,
		q.client.Database(),
		q.TableName,
	), nil
}

func (q *BasicDeleteQuery) Set(client *clientv2.SQLClient, obj any) {
	q.client = client
	q.object = obj
}
