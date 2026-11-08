package usp_sqs

import (
	"context"
	"time"
)

// Leave ten seconds of the 60-second visibility lease unused. The budget starts
// before Receive, so long polling, all admissions, and cleanup share it. On
// expiry the receiver reports OnError and exits without deleting the batch.
// Messages become visible again after their source lease; the adapter owner
// must restart collection. Already delivered members may replay on restart.
// Visibility is not renewed, so persistently slow batches can repeatedly retry.
var sourceLeaseBudget = 50 * time.Second

func (a *SQSAdapter) awaitDelivery(deadline time.Time) error {
	for {
		// Expired receipt handles must never be used for destructive cleanup.
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		confirmed, err := a.tracker.Confirm()
		if err != nil {
			return err
		}
		if _, ok := confirmed["batch"]; ok {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}
