//go:build darwin || linux || solaris || aix
// +build darwin linux solaris aix

package usp_wel

import "errors"

// Dummy noop file to build when the platform
// is _not_ Windows since the WEL API is only
// available on Windows.

type WELAdapter struct{}

func NewWELAdapter(conf WELConfig) (*WELAdapter, chan struct{}, error) {
	return nil, nil, errors.New("wel (Windows Event Logs) collection not supported outside of Windows")
}

func (a *WELAdapter) Close() error {
	return nil
}
