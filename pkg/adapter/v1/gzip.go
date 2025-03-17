package v1

import (
	"compress/gzip"
	"io"

	"dioptra-io/ufuk-research/pkg/adapter"
)

// very simple GZip decompressor.
type GZipConverter struct {
	adapter.ConvertCloser
}

func NewGZipConverter() *GZipConverter {
	return &GZipConverter{}
}

func (p GZipConverter) Convert(r io.Reader) (io.ReadCloser, error) {
	decompressor, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return decompressor, nil
}
