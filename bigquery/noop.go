//go:build aix
// +build aix

package usp_bigquery

import "errors"

type BigQueryAdapter struct{}

func NewBigQueryAdapter(conf BigQueryConfig) (*BigQueryAdapter, chan struct{}, error) {
	return nil, nil, errors.New("BigQuery is not supported on AIX")
}

func (a *BigQueryAdapter) Close() error {
	return nil
}
