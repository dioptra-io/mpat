package pipeline

import (
	"context"
	"slices"

	"golang.org/x/sync/errgroup"

	apiv3 "github.com/dioptra-io/ufuk-research/api/v3"
)

type forwardingDecisionProcessor struct {
	G          *errgroup.Group // rename
	bufferSize int
	workers    int
	ctx        context.Context
}

func NewForwardingDecisionProcessor(ctx context.Context, numWorkers, bufferSize int) *forwardingDecisionProcessor {
	g, ctx := errgroup.WithContext(ctx)
	return &forwardingDecisionProcessor{
		bufferSize: bufferSize,
		workers:    numWorkers,
		G:          g,
		ctx:        ctx,
	}
}

func (p *forwardingDecisionProcessor) Start(ingestCh <-chan *apiv3.GrouppedForwardingDecisionResultsRow) <-chan *apiv3.ForwardingDecisionRow {
	outCh := make(chan *apiv3.ForwardingDecisionRow, p.bufferSize)

	for i := 0; i < p.workers; i++ {
		p.G.Go(func() error {
			for item := range ingestCh {
				err := processForwardingDecision(p.ctx, item, outCh)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	go func() {
		_ = p.G.Wait()
		close(outCh)
	}()

	return outCh
}

func processForwardingDecision(ctx context.Context, item *apiv3.GrouppedForwardingDecisionResultsRow, output chan<- *apiv3.ForwardingDecisionRow) error {
	minTTL, maxTTL := slices.Min(item.ProbeTTLs), slices.Max(item.ProbeTTLs)
	ttlToIndexMap := make(map[uint8][]int, maxTTL-minTTL+1)

	for i := 0; i < len(item.ProbeTTLs); i++ {
		currentTTL := item.ProbeTTLs[i]
		currentAddress := item.ReplySrcAddrs[i]

		for _, j := range ttlToIndexMap[currentTTL] {
			otherAddress := item.ReplySrcAddrs[j]

			if otherAddress.Equal(currentAddress) {
				continue
			}
		}

		ttlToIndexMap[currentTTL] = append(ttlToIndexMap[currentTTL], i)
	}

	for ttl := minTTL; ttl <= maxTTL; ttl++ {
		for _, nearIndex := range ttlToIndexMap[ttl] {
			for _, farIndex := range ttlToIndexMap[ttl+1] {
				forwardingDecisionRow := &apiv3.ForwardingDecisionRow{
					// CaptureTimestamp: item.CaptureTimestamps[nearIndex],
					NearRound:      item.Rounds[nearIndex],
					NearAddr:       item.ReplySrcAddrs[nearIndex],
					NearProbeTTL:   item.Rounds[nearIndex],
					FarRound:       item.Rounds[farIndex],
					FarAddr:        item.ReplySrcAddrs[farIndex],
					FarProbeTTL:    item.Rounds[farIndex],
					ProbeProtocol:  item.ProbeProtocol,
					ProbeSrcAddr:   item.ProbeSrcAddr,
					ProbeDstPrefix: item.ProbeDstPrefix,
					ProbeDstAddr:   item.ProbeDstAddr,
					ProbeSrcPort:   item.ProbeSrcPort,
					ProbeDstPort:   item.ProbeDstPort,
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case output <- forwardingDecisionRow:
				}
			}
		}
	}

	return nil
}
