package queries

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/dioptra-io/ufuk-research/cmd/orm"
)

type BasicSelectQuery struct {
	TableNames []string
	Database   string
	Object     any
}

func (q *BasicSelectQuery) Query() (string, error) {
	query := `
SELECT
    %s
FROM
    merge('%s', '%s')
;` // end of the query

	fieldNames, err := orm.GetFieldJSONTags(q.Object)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		query,
		strings.Join(fieldNames, ", "),
		q.Database,
		buildRegexFromTableNames(q.TableNames),
	), nil
}

type BasicSelectStartQuery struct {
	TableNames []string
	Database   string
}

func (q *BasicSelectStartQuery) Query() (string, error) {
	query := `SELECT * FROM merge('%s', '%s') FORMAT CSVWithNames` // end of the query

	return fmt.Sprintf(
		query,
		q.Database,
		buildRegexFromTableNames(q.TableNames),
	), nil
}

type GrouppedSelectQuery struct {
	TableNames []string
	Database   string
	Object     any
}

func (q *GrouppedSelectQuery) Query() (string, error) {
	query := `
SELECT
    %s
FROM
    merge('%s', '(%s)')
;` // end of the query

	panic("not implemented")
	// TODO change this
	fieldNames, err := orm.GetFieldJSONTags(q.Object)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		query,
		strings.Join(fieldNames, ", "),
		q.Database,
		strings.Join(q.TableNames, ")|("),
	), nil
}

func buildRegexFromTableNames(names []string) string {
	// Ensure duplicates are removed and sorted (optional but nice)
	set := make(map[string]struct{})
	for _, name := range names {
		set[name] = struct{}{}
	}

	var escaped []string
	for name := range set {
		// Escape special regex characters if needed
		escaped = append(escaped, regexp.QuoteMeta(name))
	}

	sort.Strings(escaped) // optional for consistent ordering

	// Join with `|` for alternation
	return "(" + strings.Join(escaped, "|") + ")"
}
