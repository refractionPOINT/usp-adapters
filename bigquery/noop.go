//go:build aix
// +build aix

package usp_bigquery

import "errors"

type BigQueryAdapter struct{}

func NewBigQueryAdapter(conf BigQueryConfig) (*BigQueryAdapter, chan struct{}, error) {
	return nil, nil, errors.New("BigQuery is not supported on AIX")
}

func (bq *BigQueryAdapter) lookupAndSend(ctx context.Context) error {
	return nil
}

func (bq *BigQueryAdapter) Close() error {
	return nil
}

func (bq *BigQueryConfig) Validate() error {
	return nil
}

// everything past this point pulled from socket_conn

var zeroTime time.Time

type socketConn struct {
	Conn   net.Conn
	buffer [1024]byte
}

func (sc *socketConn) read0() error {
	return errors.New("function read0 is not available on AIX")
}

func (sc *socketConn) checkConn() error {
	return errors.New("function checkConn is not available on AIX")
}
