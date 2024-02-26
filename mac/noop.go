//go:build !darwin
// +build !darwin

package usp_mac

import "errors"

// Dummy noop file to build when the platform
// is _not_ MacOS since unified logging is only
// available on MacOS.

type MacAdapter struct{}

func NewMacAdapter(conf MacConfig) (*MacAdapter, chan struct{}, error) {
	return nil, nil, errors.New("mac (MacOS unified logging) collection not supported outside of MacOS")
}

func (a *MacAdapter) Close() error {
	return nil
}
