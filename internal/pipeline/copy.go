package pipeline

import (
	"github.com/ubombar/go-pipeline/pkg/stage"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
	clientv1 "github.com/dioptra-io/ufuk-research/pkg/client/v1"
)

type tableChunkerStage struct{}

type CopyPipeline struct {
	sourceClient         clientv1.SQLClient
	destinationClient    clientv1.SQLClient
	numParallelDownloads int
	chunkSize            int

	tableChunker stage.Stager[apiv1.ResultsTableInfo, apiv1.ResultsTableChunkInfo]
}

func NewCopyPipeline(sourceClient, destinationClient clientv1.SQLClient, numParallelDownloads, chunkSize int) (*CopyPipeline, error) {
	return &CopyPipeline{
		sourceClient:      sourceClient,
		destinationClient: destinationClient,
	}, nil
}
