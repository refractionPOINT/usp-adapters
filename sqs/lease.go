package usp_sqs

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"strconv"
	"sync"
	"time"
)

// batchLease owns the known receipt handles until work and renewal have joined.
// Any incomplete, late, failed, or ambiguous renewal abandons the whole batch.
// The source may then replay it; no receipt is released early on an uncertain RPC.
type batchLease struct {
	ctx                    context.Context
	cancel                 context.CancelCauseFunc
	renewCtx               context.Context
	stopRenew              context.CancelFunc
	done                   chan struct{}
	client                 *sqs.SQS
	queue                  string
	entries                []*sqs.ChangeMessageVisibilityBatchRequestEntry
	mu                     sync.Mutex
	deadline, hardDeadline time.Time
}

var leaseRPCBudget = 5 * time.Second
var leaseShipSlice = 200 * time.Millisecond
var leaseCloseBudget = 10 * time.Second
var leaseMaxLifetime = 11*time.Hour + 50*time.Minute

func newBatchLease(parent context.Context, client *sqs.SQS, queue string, messages []*sqs.Message, receivedBefore time.Time) *batchLease {
	ctx, cancel := context.WithCancelCause(parent)
	renewCtx, stop := context.WithCancel(ctx)
	l := &batchLease{ctx: ctx, cancel: cancel, renewCtx: renewCtx, stopRenew: stop, done: make(chan struct{}), client: client, queue: queue, deadline: receivedBefore.Add(sourceLeaseBudget), hardDeadline: receivedBefore.Add(leaseMaxLifetime)}
	seen := map[string]bool{}
	for i, m := range messages {
		if m == nil || m.ReceiptHandle == nil || *m.ReceiptHandle == "" || seen[*m.ReceiptHandle] {
			cancel(errors.New("invalid or duplicate SQS receipt handle"))
			break
		}
		seen[*m.ReceiptHandle] = true
		l.entries = append(l.entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: aws.Int64(60)})
	}
	if !time.Now().Before(minTime(l.deadline, l.hardDeadline)) {
		cancel(context.DeadlineExceeded)
	}
	go l.renew()
	return l
}
func (l *batchLease) renew() {
	defer close(l.done)
	for {
		l.mu.Lock()
		deadline := l.deadline
		l.mu.Unlock()
		if hard := l.hardDeadline; hard.Before(deadline) {
			deadline = hard
		}
		if !time.Now().Before(deadline) {
			l.cancel(context.DeadlineExceeded)
			return
		}
		// Renew well before expiry, with time for a bounded RPC. Budgeting from
		// request start (not response arrival) keeps server/client clock skew out.
		delay := min(sourceLeaseBudget/3, time.Until(deadline)/2)
		timer := time.NewTimer(delay)
		select {
		case <-l.renewCtx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		started := time.Now()
		if !started.Before(deadline) {
			l.cancel(context.DeadlineExceeded)
			return
		}
		rpcCtx, stop := context.WithDeadline(l.renewCtx, minTime(deadline, started.Add(leaseRPCBudget)))
		out, err := l.client.ChangeMessageVisibilityBatchWithContext(rpcCtx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: &l.queue, Entries: l.entries})
		stop()
		if err == nil {
			if out == nil || len(out.Failed) != 0 || len(out.Successful) != len(l.entries) {
				err = errors.New("incomplete SQS visibility renewal")
			} else {
				seen := map[string]bool{}
				for _, entry := range out.Successful {
					if entry == nil || entry.Id == nil {
						err = errors.New("invalid SQS visibility renewal response")
						break
					}
					id := *entry.Id
					n, e := strconv.Atoi(id)
					if e != nil || n < 0 || n >= len(l.entries) || strconv.Itoa(n) != id || seen[id] {
						err = errors.New("invalid SQS visibility renewal response")
						break
					}
					seen[id] = true
				}
			}
		}
		if err != nil {
			l.cancel(fmt.Errorf("SQS visibility ownership uncertain: %w", err))
			return
		}
		if !time.Now().Before(deadline) {
			l.cancel(context.DeadlineExceeded)
			return
		}
		l.mu.Lock()
		l.deadline = minTime(started.Add(sourceLeaseBudget), l.hardDeadline)
		l.mu.Unlock()
	}
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// stopAndJoin cancels even an in-flight renewal RPC and joins it. A canceled
// in-flight RPC is ambiguous, so renewal marks ownership uncertain and cleanup
// is forbidden. Cancellation while the renewer is idle is safe.
func (l *batchLease) stopAndJoin() error {
	l.stopRenew()
	<-l.done
	if err := context.Cause(l.ctx); err != nil {
		return err
	}
	l.mu.Lock()
	deadline := l.deadline
	l.mu.Unlock()
	if !time.Now().Before(deadline) {
		l.cancel(context.DeadlineExceeded)
		return context.DeadlineExceeded
	}
	return nil
}
func (l *batchLease) cleanupContext() (context.Context, context.CancelFunc, error) {
	if err := l.stopAndJoin(); err != nil {
		return nil, nil, err
	}
	l.mu.Lock()
	deadline := l.deadline
	l.mu.Unlock()
	ctx, cancel := context.WithDeadline(l.ctx, minTime(deadline, time.Now().Add(leaseRPCBudget)))
	return ctx, cancel, nil
}
func (l *batchLease) abort() { l.cancel(context.Canceled); l.stopRenew(); <-l.done }

func (a *SQSAdapter) collectionContext() context.Context {
	a.collectionOnce.Do(func() {
		parent := a.ctx
		if parent == nil {
			parent = context.Background()
		}
		a.collectCtx, a.stopCollection = context.WithCancel(parent)
	})
	return a.collectCtx
}
func (a *SQSAdapter) awaitDeliveryContext(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		confirmed, err := a.tracker.Confirm()
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		if _, ok := confirmed["batch"]; ok {
			return nil
		}
		if !acktrack.Wait(ctx, 20*time.Millisecond) {
			return context.Cause(ctx)
		}
	}
}
func (a *SQSAdapter) shipContext(ctx context.Context, msg *protocol.DataMessage) (acktrack.ACKDependency, error) {
	for {
		if err := ctx.Err(); err != nil {
			return acktrack.ACKDependency{}, context.Cause(ctx)
		}
		dep, err := a.tracker.Ship(msg, leaseShipSlice)
		if ctx.Err() != nil {
			return acktrack.ACKDependency{}, context.Cause(ctx)
		}
		if err != uspclient.ErrorBufferFull {
			return dep, err
		}
	}
}
