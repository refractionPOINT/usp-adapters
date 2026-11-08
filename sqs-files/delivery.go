package usp_sqs_files

import (
	"context"
	"time"
)

// Known leases reserve ten seconds of the server's 60-second visibility window.
// The renewer extends them during long work. Uncertain renewal abandons the batch,
// reports OnError, and stops collection; the owner can restart and replay it.
var sourceLeaseBudget = 50 * time.Second

func (a *SQSFilesAdapter) awaitDelivery(deadline time.Time) error {
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
