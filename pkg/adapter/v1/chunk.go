package v1

import (
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	adapterv1 "dioptra-io/ufuk-research/pkg/adapter"
	apiv1 "dioptra-io/ufuk-research/pkg/api/v1"
	"dioptra-io/ufuk-research/pkg/client"
)

type RouteTraceChunkProcessor struct {
	sqlAdapter      client.DBClient
	routesTableName string
	bufferSize      int
	numWorkers      int
}

func NewRouteTraceChunkProcessor(bufferSize int, numWorkers int) adapterv1.ProcessorChan[apiv1.RouteTraceChunk, apiv1.RouteNextHop] {
	return &RouteTraceChunkProcessor{
		bufferSize: bufferSize,
		numWorkers: numWorkers,
	}
}

// This is a quite complex method that processes the objects with mutliple workers.
func (p *RouteTraceChunkProcessor) Process(streamCh <-chan apiv1.RouteTraceChunk, errCh <-chan error) (<-chan apiv1.RouteNextHop, <-chan error) {
	var wg sync.WaitGroup
	workerLimiter := make(chan struct{}, p.numWorkers)
	streamCh2 := make(chan apiv1.RouteNextHop, p.bufferSize)

	// If we have failures on all of the workers, we don't want the err channel to block them
	errCh2 := make(chan error, p.numWorkers)

	go func() {
		defer close(workerLimiter)
		defer close(streamCh2)
		defer close(errCh2)

		next := true
		for next {
			select {
			case rtrace, ok := <-streamCh:
				if ok {
					// This waits for the other workers to finish if the number of workers is reacted.
					// This is outside for not spawning the go routine before the slot is available.
					workerLimiter <- struct{}{}
					wg.Add(1)

					// To prevent race condition copy the value
					rtraceCopy := rtrace

					go func() {
						defer func() {
							// Release the slot
							<-workerLimiter
							wg.Done()
						}()
						// Run the worker
						p.process(&rtraceCopy, streamCh2, errCh2)
					}()
				} else {
					next = false
				}
			case err, ok := <-errCh:
				if ok {
					errCh2 <- err
					return
				}
			}
		}

		wg.Wait()
	}()
	return streamCh2, errCh2
}

func (p *RouteTraceChunkProcessor) process(nh *apiv1.RouteTraceChunk, streamCh2 chan apiv1.RouteNextHop, errCh2 chan error) {
	// defer func() {
	// 	if r := recover(); r != nil {
	// 		errCh2 <- fmt.Errorf("an error occured on the RouteTraceChunkProcessor: %v", r)
	// 	}
	// }()
	rtcMap := newRouteTraceChunkMap(nh)
	numInserted := 0
	numSkipped := 0

	for i := 0; i < nh.Length(); i++ {
		inserted, err := rtcMap.Insert(nh, i)
		if err != nil {
			errCh2 <- err
			return
		}
		if inserted {
			numInserted += 1
		} else {
			numSkipped += 1
		}
	}

	// We know that the eleents on the last TTL does not have 'next hop'. This
	// is why we skip them for next hop computation.
	for currentTTL := rtcMap.minTTL; currentTTL < rtcMap.maxTTL; currentTTL++ {
		currentGroup, err := rtcMap.GetByTTL(currentTTL)
		if err != nil {
			errCh2 <- err
			return
		}
		nextGroup, err := rtcMap.GetByTTL(currentTTL + 1)
		if err != nil {
			errCh2 <- err
			return
		}

		// Get cross of each element. We ignore the random load balancers here.
		for _, currentElement := range currentGroup {
			for _, nextElement := range nextGroup {
				// Add each corss to the output stream
				streamCh2 <- apiv1.RouteNextHop{
					// timestamp data
					FirstCaptureTimestamp: currentElement.CaptureTimestamp,
					// nexthop info
					IPAddr:   currentElement.ReplySrcAddr,
					NextAddr: nextElement.ReplySrcAddr,
					// Flowid
					ProbeSrcAddr:  nh.ProbeSrcAddr,
					ProbeDstAddr:  nh.ProbeDstAddr,
					ProbeSrcPort:  nh.ProbeSrcPort,
					ProbeDstPort:  nh.ProbeDstPort,
					ProbeProtocol: nh.ProbeProtocol,

					// Add other useful information
					IsDestinationHostReply:   currentElement.IsDestinationHostReply,
					IsDestinationPrefixReply: currentElement.IsDestinationPrefixReply,
					ReplyICMPType:            currentElement.ReplyICMPType,
					ReplyICMPCode:            currentElement.ReplyICMPCode,
					ReplySize:                currentElement.ReplySize,
					RTT:                      currentElement.RTT,
					TimeExceededReply:        currentElement.TimeExceededRepliy,
				}
			}
		}

	}
}

// For each reposnse
type routeTraceChunkElement struct {
	// This is the important one
	ReplySrcAddr net.IP

	// Additional data
	CaptureTimestamp         time.Time
	IsDestinationHostReply   uint8
	IsDestinationPrefixReply uint8
	ReplyICMPType            uint8
	ReplyICMPCode            uint8
	ReplySize                uint16
	RTT                      uint16
	TimeExceededRepliy       uint8
}

// This is a data structure for easing the computation
type routeTraceChunkMap struct {
	minTTL uint8
	maxTTL uint8
	length uint8

	// This is the data strcuture we will store the traces.
	ds [][]routeTraceChunkElement
}

// Create a helper data structure for operations etc.
func newRouteTraceChunkMap(nh *apiv1.RouteTraceChunk) *routeTraceChunkMap {
	minTTL := slices.Min(nh.ProbeTTLs)
	maxTTL := slices.Max(nh.ProbeTTLs)
	length := maxTTL - minTTL + 1

	// Initialize dss
	ds := make([][]routeTraceChunkElement, length) // we can optimize this?
	for i := 0; i < int(length); i++ {
		ds[i] = make([]routeTraceChunkElement, 0)
	}

	return &routeTraceChunkMap{
		minTTL: minTTL,
		maxTTL: maxTTL,
		length: length,
		ds:     ds,
	}
}

func (m *routeTraceChunkMap) Length() int {
	return int(m.length)
}

func (m *routeTraceChunkMap) TTLToMapIndex(probeTTL uint8) (uint8, error) {
	mapIndex := probeTTL - m.minTTL
	// Since it is uint8 if it is smaller then 0 it becomes positive. This might introduce a wierd
	// behavior, if the length if larger than 255 then this might pass as normal. This is why we check
	// it like this.
	if probeTTL < m.minTTL || mapIndex > uint8(m.Length()) {
		return 0, errors.New("TTLToMapIndex received a probeTTL that is out of bounds")
	}
	return probeTTL - m.minTTL, nil
}

// This inserts to the probeTTL index and it inserts for unique replySrcAddr. If there is already an element
// then insertion returns false. This means that the first captureTimestamp is registered.
func (m *routeTraceChunkMap) Insert(nh *apiv1.RouteTraceChunk, i int) (bool, error) {
	mapIndex, err := m.TTLToMapIndex(nh.ProbeTTLs[i])
	if err != nil {
		return false, err
	}

	// Check if the given ip address already exists here.
	for i := 0; i < len(m.ds[mapIndex]); i++ {
		if m.ds[mapIndex][i].ReplySrcAddr.Equal(nh.ReplySrcAddrs[i]) {
			return false, nil
		}
	}
	m.ds[mapIndex] = append(m.ds[mapIndex], routeTraceChunkElement{
		CaptureTimestamp: nh.CaptureTimestamps[i],
		ReplySrcAddr:     nh.ReplySrcAddrs[i],
		// other info
		IsDestinationHostReply:   nh.DestinationHostReplies[i],
		IsDestinationPrefixReply: nh.DestinationPrefixReplies[i],
		ReplyICMPType:            nh.ReplyICMPTypes[i],
		ReplyICMPCode:            nh.ReplyICMPCodes[i],
		ReplySize:                nh.ReplySizes[i],
		RTT:                      nh.RTTs[i],
		TimeExceededRepliy:       nh.TimeExceededReplies[i],
	})
	return true, nil
}

func (m *routeTraceChunkMap) GetByTTL(probeTTL uint8) ([]routeTraceChunkElement, error) {
	mapIndex, err := m.TTLToMapIndex(probeTTL)
	if err != nil {
		return nil, err
	}
	return m.ds[mapIndex], nil
}
