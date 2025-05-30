package v2

import (
	"context"
	"errors"

	apiv2 "github.com/dioptra-io/ufuk-research/api/v2"
	"github.com/dioptra-io/ufuk-research/internal/queries"
)

// This is a high level client that contains the analysis and bussiness logic. It uses
// the sqlClient for performing hogh level operations.
type MPATClient struct {
	sqlClient *SQLClient
	arkClient *ArkClient
}

func NewMPATClient(sqlClient *SQLClient, arkClient *ArkClient) *MPATClient {
	return &MPATClient{
		sqlClient: sqlClient,
		arkClient: arkClient,
	}
}

func (c *MPATClient) StreamIrisResultTableRows(ctx context.Context, tableNames []string, bufferSize int, errCh chan<- error) (<-chan *apiv2.IrisResultsTableRow, error) {
	if c.sqlClient == nil {
		return nil, errors.New("sql client is not defined in mpat client")
	}
	query := queries.SelectFromTables(c.sqlClient.Database(), tableNames)
	outCh := make(chan *apiv2.IrisResultsTableRow, bufferSize)

	go func() {
		defer close(outCh)

		rows, err := c.sqlClient.QueryContext(ctx, query)
		if err != nil {
			errCh <- err
			return
		}
		defer rows.Close()
		for rows.Next() {
			r, err := apiv2.ScanIrisResultsTableRow(rows)
			if err != nil {
				errCh <- err
				return
			}

			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
			case outCh <- r:
				// Successfully sent
			}
		}

		if err := rows.Err(); err != nil {
			errCh <- err
			return
		}
	}()
	return outCh, nil
}
