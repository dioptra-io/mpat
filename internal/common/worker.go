package common

// func run(a any) error {
// 	return nil
// }
//
// func runWorkers(args []any, numWorkers int) error {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
//
// 	sem := make(chan bool, numWorkers)
// 	var wg sync.WaitGroup
//
// 	errchan := make(chan error, 1)
// 	for _, arg := range args {
// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		default:
// 		}
//
// 		wg.Add(1)
//
// 		sem <- false
//
// 		go func(argument any) {
// 			defer wg.Done()
// 			defer func() { <-sem }()
//
// 			if err := run(argument); err != nil {
// 				select {
// 				case errchan <- err:
// 				default:
// 				}
// 				cancel()
// 			}
// 		}(arg)
//
// 		go func() {
// 			wg.Wait()
// 			close(errchan)
// 		}()
//
// 		return <-errchan
//
// 	}
// 	return nil
// }
//
// func TestTestTewdwde9() {
//     ctx, cancel := context.WithTimeout(parent context.Context, timeout time.Duration)
// }
