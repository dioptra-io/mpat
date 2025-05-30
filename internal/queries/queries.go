package queries

import (
	"fmt"
	"strings"
)

func SelectFromTables(database string, tableNames []string) string {
	// quotedTableNames := make([]string, 0, len(tableNames))
	// for i := 0; i < len(tableNames); i++ {
	// 	quotedTableNames[i] = regexp.QuoteMeta(tableNames[i])
	// }
	joinedString := strings.Join(tableNames, "|")
	return fmt.Sprintf("SELECT * FROM merge('%s', '%s') LIMIT 1", database, joinedString)
}
