package service

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"text/template"

	"github.com/dioptra-io/ufuk-research/internal/iris"
)

func renderTemplate(name, tmpl string, data any) (string, error) {
	t, err := template.New(name).Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func formatCount(n int64) string {
	s := fmt.Sprintf("%d", n)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}

// countSourceRows queries the row count of a source table on Iris.
// The where argument is an optional WHERE clause (without the WHERE keyword).
func countSourceRows(client *iris.IrisClient, sourceTable string, where string) (int64, error) {
	query := fmt.Sprintf("SELECT count() AS count FROM %s", sourceTable)
	if where != "" {
		query += " WHERE " + where
	}
	r, err := client.Query().Select(query).Json()
	if err != nil {
		return 0, err
	}
	defer r.Close()
	reader, err := decompressIfNeeded(r)
	if err != nil {
		return 0, fmt.Errorf("failed to decompress count response: %w", err)
	}
	var result struct {
		Count int64 `json:"count"`
	}
	if err := json.NewDecoder(reader).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode count response: %w", err)
	}
	return result.Count, nil
}

// decompressIfNeeded detects gzip magic bytes and wraps the reader if needed.
func decompressIfNeeded(r io.ReadCloser) (io.Reader, error) {
	buf := make([]byte, 2)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	peeked := io.MultiReader(bytes.NewReader(buf[:n]), r)
	if n == 2 && buf[0] == 0x1f && buf[1] == 0x8b {
		gz, err := gzip.NewReader(peeked)
		if err != nil {
			return nil, err
		}
		return gz, nil
	}
	return peeked, nil
}
