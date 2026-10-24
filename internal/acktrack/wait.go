package acktrack

import (
	"context"
	"time"
)

// Wait is a cancellation-aware collection delay.
func Wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
