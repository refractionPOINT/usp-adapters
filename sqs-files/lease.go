package usp_sqs_files

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
	stopRequested          chan struct{}
	requestStop            sync.Once
	done                   chan struct{}
	client                 *sqs.SQS
	queue                  string
	entries                []*sqs.ChangeMessageVisibilityBatchRequestEntry
	mu                     sync.Mutex
	deadline, hardDeadline time.Time
	originalDeadline       time.Time
	compatibility          *renewalCompatibility
}

var leaseRenewInterval time.Duration

var leaseRPCBudget = 5 * time.Second
var leaseShipSlice = 200 * time.Millisecond
var leaseCloseBudget = 10 * time.Second
var leaseMaxLifetime = 11*time.Hour + 50*time.Minute

func newBatchLease(parent context.Context, client *sqs.SQS, queue string, messages []*sqs.Message, receivedBefore time.Time, policies ...*renewalCompatibility) *batchLease {
	policy := newRenewalCompatibility(nil)
	if len(policies) > 0 && policies[0] != nil {
		policy = policies[0]
	}
	ctx, cancel := context.WithCancelCause(parent)
	renewCtx, stop := context.WithCancel(ctx)
	l := &batchLease{ctx: ctx, cancel: cancel, renewCtx: renewCtx, stopRenew: stop, stopRequested: make(chan struct{}), done: make(chan struct{}), client: client, queue: queue, deadline: receivedBefore.Add(sourceLeaseBudget), hardDeadline: receivedBefore.Add(leaseMaxLifetime), originalDeadline: receivedBefore.Add(sourceLeaseBudget), compatibility: policy}
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
		l.expire()
	}
	go l.renew()
	return l
}
func (l *batchLease) renew() {
	defer close(l.done)
	for {
		if l.compatibility.isDisabled() {
			l.waitOriginalLease()
			return
		}
		deadline := l.knownDeadline()
		if !time.Now().Before(deadline) {
			l.expire()
			return
		}
		// Renew well before expiry, with time for a bounded RPC. Budgeting from
		// request start (not response arrival) keeps server/client clock skew out.
		delay := min(sourceLeaseBudget/3, time.Until(deadline)/2)
		if leaseRenewInterval > 0 {
			delay = min(leaseRenewInterval, time.Until(deadline)/2)
		}
		timer := time.NewTimer(delay)
		select {
		case <-l.compatibility.disabled:
			timer.Stop()
			l.waitOriginalLease()
			return
		case <-l.stopRequested:
			timer.Stop()
			return
		case <-l.renewCtx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		// If the timer and graceful stop became ready together, do not
		// start another RPC. A stop racing the RPC start still joins that
		// bounded request without canceling its ownership evidence.
		select {
		case <-l.compatibility.disabled:
			l.waitOriginalLease()
			return
		case <-l.stopRequested:
			return
		case <-l.renewCtx.Done():
			return
		default:
		}
		started := time.Now()
		if !started.Before(deadline) {
			l.expire()
			return
		}
		rpcCtx, stop := context.WithDeadline(l.renewCtx, minTime(deadline, started.Add(leaseRPCBudget)))
		out, err := l.client.ChangeMessageVisibilityBatchWithContext(rpcCtx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: &l.queue, Entries: l.entries})
		rpcContextErr := rpcCtx.Err()
		stop()
		if renewalPermissionDenied(out, err, len(l.entries)) {
			l.compatibility.disable()
			l.waitOriginalLease()
			return
		}
		if l.compatibilityExpired() && rpcContextErr != nil {
			l.expire()
			return
		}
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
			l.expire()
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

// stopAndJoin requests no further renewals and joins the current bounded
// request. Normal receipt completion must not cancel an RPC and manufacture
// ownership uncertainty. Abort/shutdown and RPC/lease deadlines still cancel.
func (l *batchLease) stopAndJoin() error {
	l.requestStop.Do(func() { close(l.stopRequested) })
	<-l.done
	if err := context.Cause(l.ctx); err != nil {
		return err
	}
	if !time.Now().Before(l.knownDeadline()) {
		return l.expire()
	}
	return nil
}
func (l *batchLease) cleanupContext() (context.Context, context.CancelFunc, error) {
	if err := l.stopAndJoin(); err != nil {
		return nil, nil, err
	}
	deadline := l.knownDeadline()
	ctx, cancel := context.WithDeadline(l.ctx, minTime(deadline, time.Now().Add(leaseRPCBudget)))
	return ctx, cancel, nil
}
func (l *batchLease) abort() { l.cancel(context.Canceled); l.stopRenew(); <-l.done }

func (a *SQSFilesAdapter) collectionContext() context.Context {
	a.collectionOnce.Do(func() {
		parent := a.ctx
		if parent == nil {
			parent = context.Background()
		}
		a.collectCtx, a.stopCollection = context.WithCancel(parent)
		a.intakeCtx, a.stopIntake = context.WithCancel(a.collectCtx)
	})
	return a.collectCtx
}
func (a *SQSFilesAdapter) shipContext(ctx context.Context, msg *protocol.DataMessage) (acktrack.ACKDependency, error) {
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
