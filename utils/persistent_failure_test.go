package utils

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var errCause = errors.New("HTTP 503 Service Unavailable: vendor is down")

func TestPersistentVendorFailureKeepsTextAndChain(t *testing.T) {
	err := PersistentVendorFailure(fmt.Errorf("API unavailable for a continuous retry interval: %w", errCause))
	require.ErrorIs(t, err, ErrPersistentVendorFailure)
	require.ErrorIs(t, err, errCause)
	require.Equal(t, "API unavailable for a continuous retry interval: HTTP 503 Service Unavailable: vendor is down", err.Error())
	// Still advisory after an embedder wraps it for its own context.
	require.ErrorIs(t, fmt.Errorf("adapter x: %w", err), ErrPersistentVendorFailure)
	// The sentinel is not implied by the cause alone.
	require.NotErrorIs(t, errCause, ErrPersistentVendorFailure)
	require.Nil(t, PersistentVendorFailure(nil))
}
