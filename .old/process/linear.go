package process

import (
	"context"
	"sync"
)

type LinearProcess[T, K any] struct {
	InCh       chan T     // closed by previous process
	OutCh      chan K     // closed by process when it is done
	ErrCh      chan error // closed by caller, assumed to non-blocking
	NumWorkers int
	PreRun     PreRunFn    // called single time before run
	Run        RunFn[T, K] // called a single time from each worker, will not run if prerun returns an error
	PostRun    PostRunFn   // called single time after run, will run in every case
}

func (p *LinearProcess[T, K]) Start(ctx context.Context) error {
	if p.InCh == nil || p.OutCh == nil || p.ErrCh == nil {
		return ErrCommunicationChannelsNil
	}

	if p.NumWorkers < 0 {
		return ErrNumWorkersIsNegative
	}

	var wg1 sync.WaitGroup // wait for prerun to complete
	var wg2 sync.WaitGroup // wait for run to complete
	wg1.Add(1)
	wg2.Add(p.NumWorkers)

	preRunSuccessful := false

	go func() {
		defer wg1.Done()
		if p.PreRun != nil { // do not run if nil
			err := p.PreRun(ctx)
			if err != nil {
				p.ErrCh <- err
				return
			}
		}
		preRunSuccessful = true
	}()

	for i := 0; i < p.NumWorkers; i++ {
		go func() {
			wg1.Wait()             // wait for prerun to complete
			defer wg2.Done()       // defer for post run
			if !preRunSuccessful { // no race because wg1.Wait()
				return
			}

			if p.Run != nil { // do not run if nil
				if err := p.Run(ctx, p.InCh, p.OutCh); err != nil {
					p.ErrCh <- err
				}
			}
		}()
	}

	go func() {
		wg2.Wait() // wait for workers to finish
		close(p.OutCh)
		if p.PostRun != nil { // do not run if nil
			err := p.PostRun(ctx)
			if err != nil {
				p.ErrCh <- err
				return
			}
		}
	}()

	return nil
}
