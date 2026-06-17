package utils

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
)

// ParquetMagic is the 4-byte signature at the start and end of a
// well-formed Parquet file. See https://parquet.apache.org/docs/file-format/.
// Exported so adapters that detect parquet from a small magic peek
// (without holding the whole file in memory) don't redefine it.
var ParquetMagic = []byte{'P', 'A', 'R', '1'}

// IsParquetFile returns true when the name ends in .parquet or the
// provided bytes carry the Parquet magic header and trailer. Either
// hint is enough: object keys often drop the extension, and magic-only
// detection means we don't require producers to follow a naming
// convention.
func IsParquetFile(name string, data []byte) bool {
	if strings.HasSuffix(strings.ToLower(name), ".parquet") {
		return true
	}
	if len(data) < 8 {
		return false
	}
	if !bytes.Equal(data[:4], ParquetMagic) {
		return false
	}
	if !bytes.Equal(data[len(data)-4:], ParquetMagic) {
		return false
	}
	return true
}

// MaxDecompressedParquetSize bounds how many bytes the in-process
// gunzip path will accept from a compressed-parquet object before it
// errors out. A modest gzip bomb (50 MB of zeros) decompresses to many
// GB and would OOM the adapter without this cap. 1 GiB is generous for
// real workloads while still safe on small EDR hosts.
const MaxDecompressedParquetSize = 1 << 30

// gunzip decompresses gzip-encoded bytes with an upper bound on the
// output size, returning an error if the limit is exceeded. Used by
// PrepareBundleData to peel the gzip layer off compressed-parquet
// objects (e.g. Athena UNLOAD, Firehose-to-Parquet) before the parquet
// decoder sees them. The limit defends against gzip bombs.
func gunzip(data []byte, limit int64) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	// Read one byte past the limit so we can distinguish "fits exactly"
	// from "would have spilled past the cap".
	out, err := io.ReadAll(io.LimitReader(r, limit+1))
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
func PrepareBundleData(name string, rawData []byte) (data []byte, isCompressed bool, err error) {
	lower := strings.ToLower(name)

	if base, isGz := strings.CutSuffix(lower, ".gz"); isGz {
		// Only peel the gzip layer here when the underlying object is
		// parquet, since the adapter is the only place that can decode
		// it. Plain gzipped text files keep the existing pass-through
		// path: the proxy gunzips and routes through its line scanner.
		if strings.HasSuffix(base, ".parquet") {
			decompressed, gzErr := gunzip(rawData, MaxDecompressedParquetSize)
			if gzErr != nil {
				return nil, false, fmt.Errorf("gunzip %s: %w", name, gzErr)
			}
			converted, pqErr := ParquetToJSONLines(decompressed)
			if pqErr != nil {
				return nil, false, fmt.Errorf("parquet decode %s: %w", name, pqErr)
			}
			return converted, false, nil
		}
		return rawData, true, nil
	}

	if IsParquetFile(name, rawData) {
		converted, pqErr := ParquetToJSONLines(rawData)
		if pqErr != nil {
			return nil, false, fmt.Errorf("parquet decode %s: %w", name, pqErr)
		}
		return converted, false, nil
	}

	return rawData, false, nil
}
