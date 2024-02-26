//go:build !darwin
// +build !darwin

package usp_mac_unified_logging

import "errors"

// Dummy noop file to build when the platform
// is _not_ MacOS since unified logging is only
// available on MacOS.

type MacUnifiedLoggingAdapter struct{}

func NewMacUnifiedLoggingAdapter(conf MacUnifiedLoggingConfig) (*MacUnifiedLoggingAdapter, chan struct{}, error) {
	return nil, nil, errors.New("mac (MacOS unified logging) collection not supported outside of MacOS")
}

func (a *MacUnifiedLoggingAdapter) Close() error {
	return nil
}
