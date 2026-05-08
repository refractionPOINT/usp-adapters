//go:build aix

package utils

import "errors"

// ParquetToJSONLines is not available on AIX. The Apache Arrow Go
// parquet reader depends on apache/thrift, whose Unix socket code uses
// constants (syscall.MSG_DONTWAIT) that are not defined on AIX.
// Adapters will surface this error if a .parquet object is encountered.
func ParquetToJSONLines(_ []byte) ([]byte, error) {
	return nil, errors.New("parquet decoding not supported on this OS")
}
