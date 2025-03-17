package adapter

import (
	"io"
)

// This is the converter interface that takes a io.Reader and outputs another one while
// performing the operation.
type Converter interface {
	Convert(r io.Reader) (io.Reader, error)
}

// This is the converter interface that takes a io.Reader and outputs another one while
// performing the operation. Different from the Closer interface is that the resulting
// is a io.ReadCloser interface which needs to be closed when finished.
type ConvertCloser interface {
	Convert(r io.Reader) (io.ReadCloser, error)
}

// This is also a converter but instead of returning a io.Reader it retuns a generic
// readonly chan.
type ConverterChan[T any] interface {
	// Here we observe one interesting behavior, since there are two chans there is a possibility
	// that one is closed and other is not, in a select statement. Thus the caller should check both
	// channels. Here is an exmaple:
	//
	// r := strings.NewReader(str)
	//
	// objectsCh, errCh := converter.Convert(r)
	// continueLoop := true
	//
	//	for continueLoop {
	//		select {
	//		case rec, ok := <-objectsCh:
	//			if ok {
	//              // Do something with rec.
	//			} else {
	//				continueLoop = false
	//			}
	//		case err, ok := <-errCh:
	//			if ok {
	//				panic(err)
	//			} else {
	//				continueLoop = false
	//			}
	//		}
	//	}
	//
	// We cannot guarantee if the caller would be on the first or the second closed channel
	// after closing the channels.
	Convert(r io.Reader) (<-chan T, <-chan error)
}

// Similar to the Converter type interface but this takes the data directly.
type StreamerChan[T any] interface {
	// Similar to ConverterChan
	Stream() (<-chan T, <-chan error)
}

// This is a processort that essentially converts one set of buffer to another.
type ProcessorChan[T, E any] interface {
	Process(<-chan T, <-chan error) (<-chan E, <-chan error)
}

// This is an uploader with a similar structure to all other Chans.
type UploaderChan[T any] interface {
	Upload(<-chan T, <-chan error) (<-chan bool, <-chan error)
}
