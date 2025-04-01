package pipeline

import (
	"context"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
	clientv1 "github.com/dioptra-io/ufuk-research/pkg/client/v1"
)

type RoutesTableChunkInfo struct {
	Info     *apiv1.ResultsTableInfo
	ChunkId  int
	Uploaded bool
}

// 3 stages:
//  1. Convert the TableName to RouteTrace using client and groupby
//  2. Convert RouteTraces to RouteInfo using the match algorithm
//  3. Insert the RouteInfo to database
type RoutesPipeline struct {
	sourceClient *clientv1.SQLClient

	numParallelDownloads int
	maxUploadRate        int
	maxRetries           int

	elements []apiv1.TableName

	errCh chan error
}

func NewRoutesPipeline(
	sourceClient *clientv1.SQLClient,
	elements []apiv1.TableName, // result tables
	numParallelDownloads, maxUploadRate, maxRetries int,
) (*RoutesPipeline, <-chan error, error) {
	errCh := make(chan error, numParallelDownloads*2)
	return &RoutesPipeline{
		sourceClient: sourceClient,

		numParallelDownloads: numParallelDownloads,
		maxUploadRate:        maxUploadRate,
		maxRetries:           maxRetries,

		elements: elements,

		errCh: errCh,
	}, errCh, nil
}

func (p *RoutesPipeline) Start(ctx context.Context) error {
	return nil
}

func (p *RoutesPipeline) Close() {
	close(p.errCh)
}

func (p *RoutesPipeline) Output() <-chan ResultsTableChunkInfo {
	return nil
}
