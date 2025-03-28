package pipeline

import (
	"context"
	"math"
	"time"

	"github.com/ubombar/go-pipeline/pkg/stage"

	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
	clientv1 "github.com/dioptra-io/ufuk-research/pkg/client/v1"
	"github.com/dioptra-io/ufuk-research/pkg/config"
	"github.com/dioptra-io/ufuk-research/pkg/query"
	"github.com/dioptra-io/ufuk-research/pkg/util"
)

var logger = util.GetLogger()

type ResultsTableChunkInfo struct {
	Info     *apiv1.ResultsTableInfo
	ChunkId  int
	Uploaded bool
}

type CopyPipeline struct {
	sourceClient      *clientv1.SQLClient
	destinationClient *clientv1.SQLClient

	numParallelDownloads int
	chunkSize            int
	maxUploadRate        int
	maxRetries           int

	elements []apiv1.ResultsTableInfo

	errCh           chan error
	tableChunkerCh  chan ResultsTableChunkInfo
	chunkUploaderCh chan ResultsTableChunkInfo
}

func NewCopyPipeline(
	sourceClient, destinationClient *clientv1.SQLClient,
	elements []apiv1.ResultsTableInfo,
	numParallelDownloads, chunkSize, maxUploadRate, maxRetries int,
) (*CopyPipeline, <-chan error, error) {
	errCh := make(chan error, numParallelDownloads*2)
	return &CopyPipeline{
		sourceClient:      sourceClient,
		destinationClient: destinationClient,

		numParallelDownloads: numParallelDownloads,
		chunkSize:            chunkSize,
		maxUploadRate:        maxUploadRate,
		maxRetries:           maxRetries,

		elements: elements,

		errCh:           errCh,
		tableChunkerCh:  make(chan ResultsTableChunkInfo), // making this unbuffered would ensure table ordering
		chunkUploaderCh: make(chan ResultsTableChunkInfo, numParallelDownloads),
	}, errCh, nil
}

func (p *CopyPipeline) Start(ctx context.Context) error {
	stage.StartWorkersFromArray(ctx, 1, p.elements, p.tableChunkerCh, p.errCh, p.tableInfoToChunkInfo, func(ctx context.Context) {})
	stage.StartWorkersFromChannel(ctx, p.numParallelDownloads, p.tableChunkerCh, p.chunkUploaderCh, p.errCh, p.chunkUploaderFn, func(ctx context.Context) {})
	return nil
}

func (p *CopyPipeline) Close() {
	close(p.errCh)
}

func (p *CopyPipeline) Output() <-chan ResultsTableChunkInfo {
	return p.chunkUploaderCh
}

func (p *CopyPipeline) tableInfoToChunkInfo(ctx context.Context, info apiv1.ResultsTableInfo) ([]ResultsTableChunkInfo, error) {
	numChunks := int(math.Ceil(float64(info.NumRows) / float64(p.chunkSize)))
	chunkInfo := make([]ResultsTableChunkInfo, numChunks)

	for i := 0; i < numChunks; i++ {
		chunkInfo[i] = ResultsTableChunkInfo{
			Info:     &info,
			ChunkId:  i,
			Uploaded: false,
		}
	}

	time.Sleep(time.Millisecond * 1000)

	return chunkInfo, nil
}

func (p *CopyPipeline) chunkUploaderFn(ctx context.Context, chunk ResultsTableChunkInfo) ([]ResultsTableChunkInfo, error) {
	limit := p.chunkSize
	offset := p.chunkSize * chunk.ChunkId

	dataFormat := "Native"
	selectQuery := query.SelectLimitOffsetFormat(chunk.Info.TableName, limit, offset, dataFormat)
	insertQuery := query.InsertFormat(chunk.Info.TableName, dataFormat)

	for i := 1; i <= p.maxRetries; i++ {
		if err := p.downloadAndUpload(selectQuery, insertQuery); err != nil {
			if i == p.maxRetries {
				logger.Panicf("Failed to upload chunk %d for table %s after %d trys: %v.\n", chunk.ChunkId, chunk.Info.TableName, i, err)
				return []ResultsTableChunkInfo{}, err
			} else {
				logger.Warnf("Failed to upload chunk %d for table %s in attempt %d, retrying: %v.\n", chunk.ChunkId, chunk.Info.TableName, i, err)
			}
			time.Sleep(util.ExponentialBackoff(i, config.DefaultExponentialBackupCap))
		} else {
			chunk.Uploaded = true
			break
		}
	}

	return []ResultsTableChunkInfo{chunk}, nil
}

func (p *CopyPipeline) downloadAndUpload(selectQuery, insertQuery string) error {
	downloader, err := p.sourceClient.Download(selectQuery)
	if err != nil {
		return err
	}

	uploader, err := p.destinationClient.Upload(insertQuery, downloader)
	if err != nil {
		panic(err)
	}

	defer downloader.Close()
	defer uploader.Close()

	return nil
}
