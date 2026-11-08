//go:build !aix

package usp_sqs_files

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ParquetToJSONLines decodes a Parquet file into newline-delimited JSON
// with one JSON object per row. Column names become JSON keys. The
// output is suitable for the usp proxy's line-scanning bundle handler.
//
// The Apache Arrow Go Parquet reader is used under the hood; Arrow's
// per-value GetOneForMarshal handles type coercion (timestamps to
// string, binary to base64, nested structs to nested JSON, etc).
//
// The AIX build uses a stub in parquet_unsupported.go because Arrow's
// thrift transitive dependency does not compile on AIX.
func parquetToJSONLines(ctx context.Context, data []byte) ([]byte, error) {
	pr, err := file.NewParquetReader(&contextParquetReader{ctx: ctx, Reader: bytes.NewReader(data)})
	if err != nil {
		return nil, fmt.Errorf("parquet.NewReader: %w", err)
	}
	defer pr.Close()

	ar, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("parquet.NewArrowReader: %w", err)
	}

	rr, err := ar.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("parquet.GetRecordReader: %w", err)
	}
	defer rr.Release()

	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	// The proxy's line scanner treats each row as a standalone event, so
	// per-row JSON must stay compact (no escaped HTML, no pretty-print).
	enc.SetEscapeHTML(false)

	for rr.Next() {
		if err := ctx.Err(); err != nil {
			return nil, context.Cause(ctx)
		}
		rec := rr.Record()
		schema := rec.Schema()
		cols := rec.Columns()
		rows := int(rec.NumRows())

		for i := 0; i < rows; i++ {
			if err := ctx.Err(); err != nil {
				return nil, context.Cause(ctx)
			}
			obj := make(map[string]interface{}, len(cols))
			for c, col := range cols {
				obj[schema.Field(c).Name] = col.GetOneForMarshal(i)
			}
			if err := enc.Encode(obj); err != nil {
				return nil, fmt.Errorf("parquet.encode row: %w", err)
			}
		}
	}
	if err := rr.Err(); err != nil && err != io.EOF {
		return nil, fmt.Errorf("parquet.read: %w", err)
	}
	return out.Bytes(), nil
}

// Parquet footer/column reads check cancellation even before Arrow's record
// reader is constructed; Arrow also receives the lease context for batch reads.
type contextParquetReader struct {
	ctx context.Context
	*bytes.Reader
}

func (r *contextParquetReader) Read(p []byte) (int, error) {
	if r.ctx.Err() != nil {
		return 0, context.Cause(r.ctx)
	}
	return r.Reader.Read(p)
}
func (r *contextParquetReader) ReadAt(p []byte, off int64) (int, error) {
	if r.ctx.Err() != nil {
		return 0, context.Cause(r.ctx)
	}
	return r.Reader.ReadAt(p, off)
}
func (r *contextParquetReader) Seek(off int64, whence int) (int64, error) {
	if r.ctx.Err() != nil {
		return 0, context.Cause(r.ctx)
	}
	return r.Reader.Seek(off, whence)
}
