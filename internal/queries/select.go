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

type GrouppedForwardingDecisionSelectQuery struct {
	TableName string
	Database  string
	Limit     int
	Offset    int
}

func (q *GrouppedForwardingDecisionSelectQuery) Query() (string, error) {
	// This should agree with the Scan function
	query := `
WITH
    cityHash64(probe_protocol, probe_src_addr, probe_dst_addr, probe_src_port, probe_dst_port) AS flowhash
SELECT 
    groupArray(probe_ttl) AS probe_ttls, 
    groupArray(reply_src_addr) AS reply_src_addrs, 
    groupArray(probe_dst_addr) AS probe_dst_addrs,
    groupArray(flowhash) AS flowhashes,
    uniqExact(flowhash) AS num_distinct_flowhashes,
    probe_src_addr, 
    probe_dst_prefix
FROM %s.%s
GROUP BY probe_src_addr, probe_dst_prefix
;` // end of the query

	return fmt.Sprintf(
		query,
		q.Database,
		q.TableName,
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
