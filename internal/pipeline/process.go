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
				logger.Debugln("An element is passed with success.")
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
	flowhashToIndiciesMap := make(map[uint64][]int, item.DistinctFlowhashes)
	for i := 0; i < len(item.ProbeTTLs); i++ {
		flowhashToIndiciesMap[item.FlowHashes[i]] = append(flowhashToIndiciesMap[item.FlowHashes[i]], i)
	}

	// indiciesSubset shares the same flowhash this flowid
	for _, indiciesSubset := range flowhashToIndiciesMap {
		probeTTLS := make([]uint8, 0, len(indiciesSubset))

		for _, index := range indiciesSubset {
			probeTTLS = append(probeTTLS, item.ProbeTTLs[index])
		}

		minTTL, maxTTL := slices.Min(probeTTLS), slices.Max(probeTTLS)
		ttlToIndexMap := make(map[uint8][]int, maxTTL-minTTL+1)

	outer:
		for _, index := range indiciesSubset {
			currentTTL := item.ProbeTTLs[index]
			currentAddress := item.ReplySrcAddrs[index]

			for _, j := range ttlToIndexMap[currentTTL] {
				otherAddress := item.ReplySrcAddrs[j]

				// Ensure uniqueness of the address under the same TTL
				if otherAddress.Equal(currentAddress) {
					continue outer
				}
			}

			ttlToIndexMap[currentTTL] = append(ttlToIndexMap[currentTTL], index)
		}

		for ttl := minTTL; ttl <= maxTTL; ttl++ {
			for _, nearIndex := range ttlToIndexMap[ttl] {
				for _, farIndex := range ttlToIndexMap[ttl+1] {
					// Do not add self forwarding decisions (they are most likely to be false)
					if item.ReplySrcAddrs[nearIndex].Equal(item.ReplySrcAddrs[farIndex]) {
						continue
					}

					forwardingDecisionRow := &apiv3.ForwardingDecisionRow{
						NearAddr:       item.ReplySrcAddrs[nearIndex],
						FarAddr:        item.ReplySrcAddrs[farIndex],
						NearProbeTTL:   item.ProbeTTLs[nearIndex],
						ProbeSrcAddr:   item.ProbeSrcAddr,
						ProbeDstPrefix: item.ProbeDstPrefix,
					}

					select {
					case <-ctx.Done():
						return ctx.Err()
					case output <- forwardingDecisionRow:
					}
				}
			}
		}
	}

	return nil
}
