package utils

import (
	"bytes"
	"strings"
)

// parquetMagic is the 4-byte signature at the start and end of a
// well-formed Parquet file. See https://parquet.apache.org/docs/file-format/.
var parquetMagic = []byte{'P', 'A', 'R', '1'}

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
	if !bytes.Equal(data[:4], parquetMagic) {
		return false
	}
	if !bytes.Equal(data[len(data)-4:], parquetMagic) {
		return false
	}
	return true
}
