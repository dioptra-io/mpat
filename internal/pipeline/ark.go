package pipeline

// This part is not implemented yet.
// import (
// 	"context"
// 	"errors"
// 	"net"
// 	"slices"
//
// 	apiv1 "github.com/dioptra-io/ufuk-research/api/v1"
// 	clientv1 "github.com/dioptra-io/ufuk-research/pkg/client/v1"
// 	"github.com/dioptra-io/ufuk-research/pkg/process"
// 	"github.com/dioptra-io/ufuk-research/pkg/query"
// 	"github.com/dioptra-io/ufuk-research/pkg/util"
// )
//
// type WartFileInfo struct {
// 	WartFile *apiv1.WartFile
// }
//
// type f struct {
// 	WartFile *apiv1.WartFile
// }

// type RoutesPipeline struct {
// 	cfg          RoutesPipelineConfig
// 	sourceClient *clientv1.SQLClient
//
// 	errCh chan error
//
// 	tableNameCh       chan apiv1.TableName
// 	routeTraceCh      chan RouteTraceInfo
// 	routeInfoCh       chan RouteInfo
// 	chunkUploadInfoCh chan ChunkUploadInfo
//
// 	tableNameToRouteTraceProcessor      *process.LinearProcess[apiv1.TableName, RouteTraceInfo]
// 	routeTraceToRouteInfoProcessor      *process.LinearProcess[RouteTraceInfo, RouteInfo]
// 	routeInfoToChunkUploadInfoProcessor *process.LinearProcess[RouteInfo, ChunkUploadInfo]
// }
//
// type RoutesPipelineConfig struct {
// 	NumWorkers       int
// 	NumUploaders     int
// 	NumMaxRetries    int
// 	MaxUploadRate    int
// 	UploadChunkSize  int
// 	SkipDuplicateIPs bool
// }
//
// func NewRoutesPipeline(sourceClient *clientv1.SQLClient, RouteTableNames []apiv1.TableName, cfg RoutesPipelineConfig) (*RoutesPipeline, error) {
// 	// ensure given tables are route tables
// 	for i := 0; i < len(RouteTableNames); i++ {
// 		if RouteTableNames[i].Type() != apiv1.RoutesTable {
// 			return nil, ErrGivenTablesAreNotRoute
// 		}
// 	}
//
// 	// check upload rate for negativity
// 	if cfg.MaxUploadRate != 0 {
// 		logger.Warnln("Current implementation of routes pipeline does not support rate limiting.")
// 	}
//
// 	// Create channels
// 	errCh := make(chan error, cfg.NumWorkers*2) // make sure to consume this to prevent blocking
// 	tableNameCh := process.SliceToChannel(RouteTableNames)
// 	routeTraceCh := make(chan RouteTraceInfo, cfg.NumWorkers)
// 	routeInfoCh := make(chan RouteInfo, cfg.NumWorkers)
// 	chunkUploadInfoCh := make(chan ChunkUploadInfo, cfg.NumWorkers)
//
// 	close(tableNameCh)
//
// 	// Create processors
// 	tableNameToRouteTraceProcessor := &process.LinearProcess[apiv1.TableName, RouteTraceInfo]{
// 		InCh:       tableNameCh,
// 		OutCh:      routeTraceCh,
// 		ErrCh:      errCh,
// 		NumWorkers: 1, // to consume one table at a time
// 	}
// 	routeTraceToRouteInfoProcessor := &process.LinearProcess[RouteTraceInfo, RouteInfo]{
// 		InCh:       routeTraceCh,
// 		OutCh:      routeInfoCh,
// 		ErrCh:      errCh,
// 		NumWorkers: cfg.NumWorkers,
// 	}
// 	routeInfoToChunkUploadInfoProcessor := &process.LinearProcess[RouteInfo, ChunkUploadInfo]{
// 		InCh:       routeInfoCh,
// 		OutCh:      chunkUploadInfoCh,
// 		ErrCh:      errCh,
// 		NumWorkers: cfg.NumUploaders,
// 	}
// 	// Create pipeline
// 	routesPipeline := &RoutesPipeline{
// 		cfg:          cfg,
// 		sourceClient: sourceClient,
//
// 		// channels
// 		errCh:             errCh,
// 		tableNameCh:       tableNameCh,
// 		routeTraceCh:      routeTraceCh,
// 		routeInfoCh:       routeInfoCh,
// 		chunkUploadInfoCh: chunkUploadInfoCh,
//
// 		// processors
// 		tableNameToRouteTraceProcessor:      tableNameToRouteTraceProcessor,
// 		routeTraceToRouteInfoProcessor:      routeTraceToRouteInfoProcessor,
// 		routeInfoToChunkUploadInfoProcessor: routeInfoToChunkUploadInfoProcessor,
// 	}
//
// 	// Connect run functions
// 	tableNameToRouteTraceProcessor.Run = routesPipeline.runTableNameToRouteTraceProcessor
// 	routeTraceToRouteInfoProcessor.Run = routesPipeline.runRouteTraceToRouteInfoProcessor
// 	routeInfoToChunkUploadInfoProcessor.Run = routesPipeline.runRouteInfoToChunkUploadInfoProcessor
//
// 	return routesPipeline, nil
// }
//
// func (p *RoutesPipeline) Start(ctx context.Context) error {
// 	if err := p.tableNameToRouteTraceProcessor.Start(ctx); err != nil {
// 		return err
// 	}
// 	if err := p.routeTraceToRouteInfoProcessor.Start(ctx); err != nil {
// 		return err
// 	}
// 	if err := p.routeInfoToChunkUploadInfoProcessor.Start(ctx); err != nil {
// 		return err
// 	}
// 	return nil
// }
//
// func (p *RoutesPipeline) Close() {
// 	close(p.errCh)
// }
//
// func (p *RoutesPipeline) ErrCh() <-chan error {
// 	return p.errCh
// }
//
// func (p *RoutesPipeline) OutCh() <-chan ChunkUploadInfo {
// 	return p.chunkUploadInfoCh
// }
//
// func (p *RoutesPipeline) runTableNameToRouteTraceProcessor(ctx context.Context, inCh <-chan apiv1.TableName, outch chan<- RouteTraceInfo) error {
// 	for tableName := range inCh {
// 		if ok := process.ContextValid(ctx); !ok {
// 			return nil
// 		}
//
// 		resultsTableName, err := tableName.Convert(apiv1.ResultsTable)
// 		if err != nil {
// 			return err
// 		}
//
// 		logger.Debugf("Trying to fetch the routetraces for table: %v.\n", resultsTableName)
//
// 		// process table name
// 		rows, err := p.sourceClient.Query(query.SelectRoutes(string(resultsTableName)))
// 		if err != nil {
// 			return err
// 		}
//
// 		for rows.Next() {
// 			// make this resilient
// 			if err := rows.Err(); err != nil {
// 				rows.Close()
// 				return err
// 			}
//
// 			var routeTrace apiv1.RouteTrace
// 			if err := routeTrace.Scan(rows); err != nil {
// 				rows.Close()
// 				return err
// 			}
//
// 			routeTraceInfo := RouteTraceInfo{
// 				Data:      &routeTrace,
// 				TableName: tableName,
// 			}
//
// 			if ok := process.Push(ctx, outch, p.errCh, routeTraceInfo); !ok {
// 				rows.Close()
// 				return nil
// 			}
// 		}
//
// 	}
// 	return nil
// }
//
// func (p *RoutesPipeline) runRouteTraceToRouteInfoProcessor(ctx context.Context, inCh <-chan RouteTraceInfo, outch chan<- RouteInfo) error {
// 	for routeTraceInfo := range inCh {
// 		if ok := process.ContextValid(ctx); !ok {
// 			return nil
// 		}
// 		routeTrace := routeTraceInfo.Data
//
// 		// perform the matching algorithm
// 		minTTL, maxTTL := slices.Min(routeTrace.ProbeTTLs), slices.Max(routeTrace.ProbeTTLs)
// 		ttlIndexMap := make(map[uint8][]int, maxTTL-minTTL+1)      // maps probeTTL -> index
// 		ttlAddressMap := make(map[uint8][]net.IP, maxTTL-minTTL+1) // maps probeTTL -> address
// 		routeHops := make([]*apiv1.RouteHop, 0, maxTTL-minTTL+1)
//
// 		// make sure the ip address is not duplicated
// 		for i := 0; i < routeTrace.Length(); i++ {
// 			currentTTL := routeTrace.ProbeTTLs[i]
// 			currentAddress := routeTrace.ReplySrcAddrs[i]
//
// 			// If we want to add all of the addresses, then the size of this would be exponentially large
// 			if p.cfg.SkipDuplicateIPs && util.ContainsIP(ttlAddressMap[currentTTL], currentAddress) {
// 				continue
// 			}
//
// 			// If skip duplicate IPs is true then the maps only contain unique ip addresses
// 			ttlAddressMap[currentTTL] = append(ttlAddressMap[currentTTL], currentAddress)
// 			ttlIndexMap[currentTTL] = append(ttlIndexMap[currentTTL], i)
// 		}
//
// 		// this is the loop that matches all routes
// 		for ttl := minTTL; ttl < maxTTL; ttl++ { // iterate over TTL and TTL+1
// 			nearIndicies := ttlIndexMap[ttl]
// 			farIndicies := ttlIndexMap[ttl+1]
//
// 			for _, nearIndex := range nearIndicies { // Get the indicies for near
// 				for _, farIndex := range farIndicies { // Get the indicies for far
// 					routeHop := &apiv1.RouteHop{
// 						// Most important data.
// 						IPAddr:   routeTrace.ReplySrcAddrs[nearIndex],
// 						NextAddr: routeTrace.ReplySrcAddrs[farIndex],
//
// 						// Additionalt metadata.
// 						FirstCaptureTimestamp: routeTrace.CaptureTimestamps[nearIndex],
//
// 						// Flowid
// 						ProbeSrcAddr:  routeTrace.ProbeSrcAddr,
// 						ProbeDstAddr:  routeTrace.ProbeDstAddr,
// 						ProbeSrcPort:  routeTrace.ProbeSrcPort,
// 						ProbeDstPort:  routeTrace.ProbeDstPort,
// 						ProbeProtocol: routeTrace.ProbeProtocol,
//
// 						// These are the other info might me useful with the next hop row
// 						IsDestinationHostReply:   routeTrace.DestinationHostReplies[nearIndex],
// 						IsDestinationPrefixReply: routeTrace.DestinationPrefixReplies[nearIndex],
// 						ReplyICMPType:            routeTrace.ReplyICMPTypes[nearIndex],
// 						ReplyICMPCode:            routeTrace.ReplyICMPCodes[nearIndex],
// 						ReplySize:                routeTrace.ReplySizes[nearIndex],
// 						RTT:                      routeTrace.RTTs[nearIndex],
// 						TimeExceededReply:        routeTrace.TimeExceededReplies[nearIndex],
// 					}
// 					routeHops = append(routeHops, routeHop)
// 				}
// 			}
// 		}
//
// 		tableName := routeTraceInfo.TableName
// 		routeInfo := RouteInfo{
// 			TableName: tableName,
// 			Data:      routeHops,
// 		}
//
// 		// push it into the outCh
// 		if ok := process.Push(ctx, outch, p.errCh, routeInfo); !ok {
// 			return nil
// 		}
//
// 	}
// 	return nil
// }
//
// func (p *RoutesPipeline) runRouteInfoToChunkUploadInfoProcessor(ctx context.Context, inCh <-chan RouteInfo, outch chan<- ChunkUploadInfo) error {
// 	// buffer for each table name, optimizable
// 	routeInfoBuffer := make(map[apiv1.TableName][]*apiv1.RouteHop, p.cfg.UploadChunkSize)
//
// 	for routeInfo := range inCh {
// 		if ok := process.ContextValid(ctx); !ok {
// 			return nil
// 		}
//
// 		// Chunk and upload
// 		if len(routeInfoBuffer[routeInfo.TableName]) < p.cfg.UploadChunkSize { // not time to ship
// 			routeInfoBuffer[routeInfo.TableName] = append(routeInfoBuffer[routeInfo.TableName], routeInfo.Data...)
// 		} else {
// 			numRowsUploaded := len(routeInfoBuffer[routeInfo.TableName])
// 			if err := p.sourceClient.UploadRouteInfos(string(routeInfo.TableName), routeInfoBuffer[routeInfo.TableName]); err != nil {
// 				return err
// 			}
// 			routeInfoBuffer[routeInfo.TableName] = routeInfoBuffer[routeInfo.TableName][:0] // reset the slice while keeping capacity
//
// 			chunkUploadInfo := ChunkUploadInfo{
// 				TableName: routeInfo.TableName,
// 				NumRows:   numRowsUploaded,
// 			}
//
// 			if ok := process.Push(ctx, outch, p.errCh, chunkUploadInfo); !ok {
// 				return nil
// 			}
// 		}
//
// 	}
//
// 	// Upload the remeaning chunks
// 	for tableName, routeInfos := range routeInfoBuffer {
// 		if ok := process.ContextValid(ctx); !ok {
// 			return nil
// 		}
// 		remeaningRows := len(routeInfos)
//
// 		if err := p.sourceClient.UploadRouteInfos(string(tableName), routeInfos); err != nil {
// 			return err
// 		}
//
// 		chunkUploadInfo := ChunkUploadInfo{
// 			TableName: tableName,
// 			NumRows:   remeaningRows,
// 		}
//
// 		if ok := process.Push(ctx, outch, p.errCh, chunkUploadInfo); !ok {
// 			return nil
// 		}
// 	}
//
// 	return nil
// }
