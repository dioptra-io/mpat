package pipeline

import (
	"io"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/dioptra-io/ufuk-research/internal/queries"
	clientv3 "github.com/dioptra-io/ufuk-research/pkg/client/v3"
	"github.com/dioptra-io/ufuk-research/pkg/config"
)

type ClickHouseReaderStreamer struct {
	bufferSize      int
	egressChunkSize int
	client          *clientv3.HTTPSQLClient
	G               *errgroup.Group
	ctx             context.Context
}

func NewClickHouseReaderStreamer(ctx context.Context, client *clientv3.HTTPSQLClient) *ClickHouseReaderStreamer {
	g, ctx := errgroup.WithContext(ctx)

	return &ClickHouseReaderStreamer{
		bufferSize:      config.DefaultStreamBufferSize,
		egressChunkSize: config.DefaultUploadChunkSize,
		client:          client,
		G:               g,
		ctx:             ctx,
	}
}

func (s *ClickHouseReaderStreamer) Ingest(q queries.Query) (io.ReadCloser, error) {
	// var buf bytes.Buffer
	//
	// s.G.Go(func() error {
	// 	query, err := q.Query()
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	reader, err := s.client.Download(query)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	defer reader.Close()
	//
	// 	if _, err := CopyWithContext(s.ctx, &buf, reader); err != nil {
	// 		return err
	// 	}
	//
	// 	return nil
	// })
	//
	// return &buf, nil

	query, err := q.Query()
	if err != nil {
		return nil, err
	}

	reader, err := s.client.Download(query)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func (s *ClickHouseReaderStreamer) Egress(reader io.ReadCloser, q queries.Query) error {
	query, err := q.Query()
	if err != nil {
		return err
	}

	if _, err := s.client.Upload(query, reader); err != nil {
		return err
	}

	return nil
}

// func CopyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (written int64, err error) {
// 	done := make(chan struct{})
// 	pr, pw := io.Pipe()
//
// 	// Writer side: copy from src to pipe
// 	go func() {
// 		defer close(done)
// 		_, err := io.Copy(pw, src)
// 		pw.CloseWithError(err) // propagate errors to pr.Read
// 	}()
//
// 	// Reader side: monitor context
// 	buf := make([]byte, 32*1024)
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			pw.CloseWithError(ctx.Err()) // trigger error on pipe read
// 			return written, ctx.Err()
// 		default:
// 			nr, er := pr.Read(buf)
// 			if nr > 0 {
// 				nw, ew := dst.Write(buf[0:nr])
// 				if nw > 0 {
// 					written += int64(nw)
// 				}
// 				if ew != nil {
// 					return written, ew
// 				}
// 				if nr != nw {
// 					return written, io.ErrShortWrite
// 				}
// 			}
// 			if er != nil {
// 				if errors.Is(er, io.EOF) {
// 					return written, nil
// 				}
// 				return written, er
// 			}
// 		}
// 	}
// }
