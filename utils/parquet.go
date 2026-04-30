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

// gunzip decompresses gzip-encoded bytes. Used by PrepareBundleData to
// peel the gzip layer off compressed-parquet objects (e.g. Athena
// UNLOAD, Firehose-to-Parquet) before the parquet decoder sees them.
func gunzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
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
			decompressed, gzErr := gunzip(rawData)
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
