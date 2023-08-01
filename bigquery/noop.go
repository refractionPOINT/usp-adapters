//go:build aix
// +build aix

package usp_bigquery

import (
	"errors"
	"net"
	"time"
)

var zeroTime time.Time

// socketConn is a wrapped net.Conn that tries to do connectivity check.
type socketConn struct {
	net.Conn

	buffer [1]byte
	closed atomic.Int32
}

func (sc *socketConn) read0() error {
	return errors.New("function read0 is not available on AIX")
}

func (sc *socketConn) checkConn() error {
	return errors.New("function checkConn is not available on AIX")
}
