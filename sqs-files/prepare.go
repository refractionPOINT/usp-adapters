package usp_sqs_files

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/refractionPOINT/usp-adapters/utils"
	"io"
	"strings"
)

// contextReader bounds cooperative decoder reads by the same source lease as
// the download and admission. It creates no detached conversion goroutine.
type contextReader struct {
	ctx context.Context
	io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, context.Cause(r.ctx)
	}
	return r.Reader.Read(p)
}
func gunzip(ctx context.Context, data []byte, limit int64) ([]byte, error) {
	r, err := gzip.NewReader(contextReader{ctx: ctx, Reader: bytes.NewReader(data)})
	if err != nil {
		return nil, err
	}
	defer r.Close()
	// Read one byte past the limit so we can distinguish "fits exactly"
	// from "would have spilled past the cap".
	out, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, Reader: r}, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) > limit {
		return nil, fmt.Errorf("decompressed size exceeds %d bytes", limit)
	}
	return out, nil
}

// PrepareBundleData transforms raw downloaded object bytes into the
// payload the proxy expects, plus a flag indicating whether the proxy
// still needs to gunzip the bundle. It exists to keep the parquet/gzip
// matrix in one place across the S3, GCS, and SQS-Files adapters.
//
//   - Plain parquet (.parquet or PAR1 magic): decoded into newline-
//     delimited JSON in-process and returned with isCompressed=false.
//   - Gzipped parquet (e.g. *.parquet.gz from Athena UNLOAD or
//     Firehose): gunzipped, then decoded, then returned uncompressed.
//   - Other gzipped objects (*.gz that aren't parquet underneath):
//     returned untouched with isCompressed=true so the proxy gunzips,
//     matching pre-parquet behaviour.
//   - Everything else: returned untouched with isCompressed=false.
//
// The name parameter is used for extension hints and for error context;
// pass the object key/path the adapter knows it by.
func prepareBundleData(ctx context.Context, name string, rawData []byte) (data []byte, isCompressed bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, false, context.Cause(ctx)
	}
	lower := strings.ToLower(name)

	if base, isGz := strings.CutSuffix(lower, ".gz"); isGz {
		// Only peel the gzip layer here when the underlying object is
		// parquet, since the adapter is the only place that can decode
		// it. Plain gzipped text files keep the existing pass-through
		// path: the proxy gunzips and routes through its line scanner.
		if strings.HasSuffix(base, ".parquet") {
			decompressed, gzErr := gunzip(ctx, rawData, utils.MaxDecompressedParquetSize)
			if gzErr != nil {
				return nil, false, fmt.Errorf("gunzip %s: %w", name, gzErr)
			}
			converted, pqErr := parquetToJSONLines(ctx, decompressed)
			if pqErr != nil {
				return nil, false, fmt.Errorf("parquet decode %s: %w", name, pqErr)
			}
			return converted, false, nil
		}
		return rawData, true, nil
	}

	if utils.IsParquetFile(name, rawData) {
		converted, pqErr := parquetToJSONLines(ctx, rawData)
		if pqErr != nil {
			return nil, false, fmt.Errorf("parquet decode %s: %w", name, pqErr)
		}
		return converted, false, nil
	}

	return rawData, false, nil
}
