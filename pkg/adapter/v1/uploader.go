package v1

import (
	"database/sql"

	apiv1 "dioptra-io/ufuk-research/api/v1"
	"dioptra-io/ufuk-research/pkg/adapter"
	"dioptra-io/ufuk-research/pkg/client"
	"dioptra-io/ufuk-research/pkg/query"
)

type RouteRecordUploader struct {
	adapter.UploaderChan[apiv1.RouteNextHop]

	chunkSize  int
	sqlAdapter client.DBClient
	tableName  string
}

func NewRouteRecordUploader(sqlAdapter client.DBClient, chunkSize int, tableName string, resetTable bool) (*RouteRecordUploader, error) {
	// Drop the table on the research if the flag forceTableReset is set.
	if resetTable {
		if _, err := sqlAdapter.Exec(query.DropTable(tableName, true)); err != nil {
			return nil, err
		}
	}

	// Create the table if not exists
	if _, err := sqlAdapter.Exec(query.CreateRoutesTable(tableName, true)); err != nil {
		return nil, err
	}
	return &RouteRecordUploader{
		chunkSize:  chunkSize,
		sqlAdapter: sqlAdapter,
		tableName:  tableName,
	}, nil
}

func (u *RouteRecordUploader) Upload(streamCh <-chan apiv1.RouteNextHop, errCh <-chan error) (<-chan bool, <-chan error) {
	// Unbuffered channels
	doneCh2 := make(chan bool)
	errCh2 := make(chan error)

	go func() {
		defer close(doneCh2)
		defer close(errCh2)

		numAccumulated := 0

		stmt, tx, err := u.prepare()
		if err != nil {
			errCh2 <- err
			return
		}

		next := true
		for next {
			select {
			case nextHop, ok := <-streamCh:
				if ok {
					// Add it to the query
					u.accumulate(stmt, nextHop)
					numAccumulated += 1

					// Flush
					if numAccumulated == u.chunkSize {
						err = u.commit(tx)
						if err != nil {
							errCh2 <- err
							return
						}

						stmt, tx, err = u.prepare()
						if err != nil {
							errCh2 <- err
							return
						}
						numAccumulated = 0
					}
				} else {
					next = false
				}
			case err, ok := <-errCh:
				if ok {
					errCh2 <- err
					return
				} else {
					next = false
				}
			}
		}

		err = u.commit(tx)
		if err != nil {
			errCh2 <- err
			return
		}

		doneCh2 <- true
	}()

	return doneCh2, errCh2
}

func (u *RouteRecordUploader) prepare() (*sql.Stmt, *sql.Tx, error) {
	tx, err := u.sqlAdapter.Begin()
	if err != nil {
		return nil, nil, err
	}
	stmt, err := tx.Prepare(query.InsertRoutes(u.tableName))
	if err != nil {
		return nil, nil, err
	}
	return stmt, tx, nil
}

func (u *RouteRecordUploader) accumulate(stmt *sql.Stmt, record apiv1.RouteNextHop) error {
	if _, err := stmt.Exec(
		record.IPAddr,
		record.NextAddr,
		record.FirstCaptureTimestamp,
		record.ProbeSrcAddr,
		record.ProbeDstAddr,
		record.ProbeSrcPort,
		record.ProbeDstPort,
		record.ProbeProtocol,
		record.IsDestinationHostReply,
		record.IsDestinationPrefixReply,
		record.ReplyICMPType,
		record.ReplyICMPCode,
		record.ReplySize,
		record.RTT,
		record.TimeExceededReply); err != nil {
		return err
	}
	return nil
}

func (u *RouteRecordUploader) commit(tx *sql.Tx) error {
	err := tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
