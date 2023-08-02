//go:build aix
// +build aix

package usp_bigquery

import "errors"

// Dummy noop file to build when the platform
// is _not_ supported.

type BigQueryAdapter struct{}

func NewBigQueryAdapter(conf BigQueryConfig) (*BigQueryAdapter, chan struct{}, error) {
	return nil, nil, errors.New("bigquery collection not supported on this platform")
}

func (a *BigQueryAdapter) Close() error {
	return nil
}
