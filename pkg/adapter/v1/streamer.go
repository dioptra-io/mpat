package v1

import (
	apiv1 "dioptra-io/ufuk-research/api/v1"
	"dioptra-io/ufuk-research/pkg/adapter"
	"dioptra-io/ufuk-research/pkg/client"
	"dioptra-io/ufuk-research/pkg/query"
)

type RouteTraceChunkStreamer struct {
	bufferSize int
	sqlAdapter client.DBClient
	tableNames []string
}

func NewRouteTraceChunkSreamer(sqlAdapter client.DBClient, bufferSize int, tableNames []string) adapter.StreamerChan[apiv1.RouteTraceChunk] {
	return &RouteTraceChunkStreamer{
		bufferSize: bufferSize,
		sqlAdapter: sqlAdapter,
		tableNames: tableNames,
	}
}

func (p RouteTraceChunkStreamer) Stream() (<-chan apiv1.RouteTraceChunk, <-chan error) {
	routeTraceCh := make(chan apiv1.RouteTraceChunk, p.bufferSize)
	errCh := make(chan error)

	go func() {
		defer close(routeTraceCh)
		defer close(errCh)

		rows, err := p.sqlAdapter.Query(query.SelectRoutes(p.tableNames))
		if err != nil {
			errCh <- err
			return
		}
		defer rows.Close()

		for rows.Next() {
			var routeTrace apiv1.RouteTraceChunk

			if err := rows.Scan(
				&routeTrace.ProbeDstAddr,
				&routeTrace.ProbeSrcAddr,
				&routeTrace.ProbeDstPort,
				&routeTrace.ProbeSrcPort,
				&routeTrace.ProbeProtocol,
				&routeTrace.ProbeTTLs,
				&routeTrace.CaptureTimestamps,
				&routeTrace.ReplySrcAddrs,
				&routeTrace.DestinationHostReplies,
				&routeTrace.DestinationPrefixReplies,
				&routeTrace.ReplyICMPTypes,
				&routeTrace.ReplyICMPCodes,
				&routeTrace.ReplySizes,
				&routeTrace.RTTs,
				&routeTrace.TimeExceededReplies,
			); err != nil {
				errCh <- err
				return
			}

			routeTraceCh <- routeTrace
		}

		if err := rows.Err(); err != nil {
			errCh <- err
			return
		}
	}()

	return routeTraceCh, errCh
}

// Helper function to convert []interface{} to []uint8
func interfaceToUint8Slice(input []interface{}) []uint8 {
	var result []uint8
	for _, v := range input {
		result = append(result, v.(uint8))
	}
	return result
}

// Helper function to convert []interface{} to []string
func interfaceToStringSlice(input []interface{}) []string {
	var result []string
	for _, v := range input {
		result = append(result, v.(string))
	}
	return result
}
