package queries

import (
	"fmt"
)

type BasicDeleteQuery struct {
	TableName       string
	AddCheckIfExist bool
	Database        string
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
		q.Database,
		q.TableName,
	), nil
}
