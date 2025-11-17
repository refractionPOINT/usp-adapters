//go:build aix
// +build aix

package usp_file

import (
	"context"
	"errors"
)

// Dummy noop file to build when the platform
// is _not_ supported.

type FileAdapter struct{}

func NewFileAdapter(ctx context.Context, conf FileConfig) (*FileAdapter, chan struct{}, error) {
	return nil, nil, errors.New("file collection not supported on this platform")
}

func (a *FileAdapter) Close() error {
	return nil
}
