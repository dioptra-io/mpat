package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"

	apiv3 "github.com/dioptra-io/ufuk-research/api/v3"
)

type forwardingDecisionProcessor struct {
	bufferSize int
	workers    int
	G          *errgroup.Group
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

func (p *forwardingDecisionProcessor) Start(ingestCh <-chan *apiv3.IrisGroupedResultsRow) <-chan *apiv3.ForwardingDecisionRow {
	outCh := make(chan *apiv3.ForwardingDecisionRow, p.bufferSize)

	for i := 0; i < p.workers; i++ {
		p.G.Go(func() error {
			for item := range ingestCh {
				err := processItem(item, outCh)
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

// Dummy processing function
func processItem(item *apiv3.IrisGroupedResultsRow, output chan<- *apiv3.ForwardingDecisionRow) error {
	// Add your actual processing logic here
	return nil
}
