package usp_sqs_files

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	uspclient "github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/refractionPOINT/usp-adapters/utils"
	"strconv"
	"sync"
	"time"
)

const maxLeasedBatches = 8
const batchObservationInterval = 20 * time.Millisecond

// Reserve ten seconds of each 60-second source lease for conservative cleanup.
var sourceLeaseBudget = 50 * time.Second

// Slots are bounded and reused only after cleanup/abandonment. Each slot has one
// ordered candidate at a time; the shared observer retains confirmations consumed
// on behalf of other waiters. No candidate coalescing can hide a batch boundary.
type batchDelivery struct {
	slots           chan int
	sourceInit      chan struct{}
	mu              sync.Mutex
	next            uint64
	confirmed       [maxLeasedBatches]uint64
	nextObservation time.Time
	observationErr  error
}

func (a *SQSFilesAdapter) ensureDelivery() *batchDelivery {
	a.deliveryOnce.Do(func() {
		d := &batchDelivery{slots: make(chan int, maxLeasedBatches), sourceInit: make(chan struct{}, 1)}
		for i := 0; i < maxLeasedBatches; i++ {
			d.slots <- i
		}
		d.sourceInit <- struct{}{}
		if a.tracker == nil {
			a.tracker, _ = acktrack.NewACKTracker(a.uspClient, maxLeasedBatches, 16)
		}
		a.delivery = d
	})
	return a.delivery
}
func (a *SQSFilesAdapter) handleBatch(ctx context.Context, messages []*sqs.Message, receivedBefore time.Time) error {
	d := a.ensureDelivery()
	select {
	case slot := <-d.slots:
		defer func() { d.slots <- slot }()
		return a.handleBatchSlot(ctx, messages, receivedBefore, slot)
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func (a *SQSFilesAdapter) handleBatchSlot(ctx context.Context, messages []*sqs.Message, receivedBefore time.Time, slot int) error {
	d := a.ensureDelivery()
	l := newBatchLease(ctx, a.sqsClient, a.conf.QueueURL, messages, receivedBefore)
	defer l.abort()
	if l.ctx.Err() != nil {
		return context.Cause(l.ctx)
	}
	// Each source batch may prepare independently. The SDK tracker orders all
	// admissions; the short registration lock orders immutable batch markers.
	eligible, dependency, err := a.prepareBatch(l.ctx, messages)
	var marker uint64
	if err == nil && len(eligible) > 0 {
		d.mu.Lock()
		d.next++
		marker = d.next
		err = a.tracker.Candidate(strconv.Itoa(slot), binary.BigEndian.AppendUint64(nil, marker), dependency)
		d.mu.Unlock()
	}
	if err != nil {
		if cause := context.Cause(l.ctx); cause != nil {
			return cause
		}
		return err
	}
	if len(eligible) == 0 {
		return nil
	}
	if err := a.awaitDeliveryContext(l.ctx, slot, marker); err != nil {
		return err
	}
	deleteCtx, cancel, err := l.cleanupContext()
	if err != nil {
		return err
	}
	defer cancel()
	req := &sqs.DeleteMessageBatchInput{QueueUrl: &a.conf.QueueURL}
	for i, m := range eligible {
		req.Entries = append(req.Entries, &sqs.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: m.ReceiptHandle})
	}
	out, err := a.sqsClient.DeleteMessageBatchWithContext(deleteCtx, req)
	if err != nil {
		if cause := context.Cause(l.ctx); cause != nil {
			return cause
		}
		return err
	}
	if out == nil || len(out.Failed) != 0 {
		return errors.New("sqsClient.DeleteMessageBatch: failed to delete some messages")
	}
	return nil
}
func (a *SQSFilesAdapter) awaitDeliveryContext(ctx context.Context, slot int, marker uint64) error {
	d := a.ensureDelivery()
	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		d.mu.Lock()
		if !time.Now().Before(d.nextObservation) {
			positions, err := a.tracker.Confirm()
			d.nextObservation = time.Now().Add(batchObservationInterval)
			d.observationErr = err
			if err == nil {
				for key, raw := range positions {
					i, e := strconv.Atoi(key)
					if e != nil || i < 0 || i >= maxLeasedBatches || len(raw) != 8 {
						d.observationErr = acktrack.ErrDependency
						break
					}
					d.confirmed[i] = binary.BigEndian.Uint64(raw)
				}
			}
		}
		err := d.observationErr
		done := d.confirmed[slot] >= marker
		d.mu.Unlock()
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if !acktrack.Wait(ctx, batchObservationInterval) {
			return context.Cause(ctx)
		}
	}
}
func (a *SQSFilesAdapter) expectedClose(err error) bool {
	return a.isStop.Load() && (errors.Is(err, context.Canceled) || errors.Is(err, uspclient.ErrorClosed))
}
func (a *SQSFilesAdapter) receiveBatches() (receiveErr error) {
	ctx := a.collectionContext()
	d := a.ensureDelivery()
	var workers sync.WaitGroup
	failures := make(chan error, 1)
	defer func() {
		workers.Wait()
		select {
		case err := <-failures:
			receiveErr = err
		default:
		}
	}()
	for !a.isStop.Load() {
		var slot int
		select {
		case slot = <-d.slots:
		case <-a.intakeCtx.Done():
			return context.Cause(a.intakeCtx)
		}
		receivedBefore := time.Now()
		rpcCtx, cancel := context.WithTimeout(a.intakeCtx, 10*time.Second)
		result, err := a.sqsClient.ReceiveMessageWithContext(rpcCtx, &sqs.ReceiveMessageInput{
			AttributeNames: []*string{}, MessageAttributeNames: []*string{}, QueueUrl: &a.conf.QueueURL,
			MaxNumberOfMessages: aws.Int64(10), VisibilityTimeout: aws.Int64(60), WaitTimeSeconds: aws.Int64(5),
		})
		cancel()
		if err != nil {
			d.slots <- slot
			if a.intakeCtx.Err() != nil {
				err = context.Cause(a.intakeCtx)
			}
			if !a.expectedClose(err) {
				a.stopCollection()
			}
			return err
		}
		if len(result.Messages) == 0 {
			d.slots <- slot
			continue
		}
		workers.Add(1)
		go func(slot int, messages []*sqs.Message, started time.Time) {
			defer workers.Done()
			defer func() { d.slots <- slot }()
			if err := a.handleBatchSlot(ctx, messages, started, slot); err != nil && !a.expectedClose(err) {
				select {
				case failures <- err:
				default:
				}
				a.stopCollection()
			}
		}(slot, result.Messages, receivedBefore)
	}
	return nil
}

// Close stops only intake first. Accepted data can confirm and source deletes
// can finish while Drain requests final USP ACKs. Cancellation ends the grace
// window, not its beginning. Both joins and the drain have bounded lifetimes.
func (a *SQSFilesAdapter) closeBatches(joined <-chan struct{}) error {
	a.isStop.Store(true)
	a.collectionContext()
	a.stopIntake()
	a.uspClient.BeginDrain()
	drained := make(chan error, 1)
	go func() { drained <- a.uspClient.Drain(leaseCloseBudget) }()
	timer := time.NewTimer(leaseCloseBudget)
	defer timer.Stop()
	var joinErr error
	select {
	case <-joined:
	case <-timer.C:
		joinErr = errors.New("SQS workers did not stop within shutdown budget")
	}
	a.stopCollection()
	if joinErr != nil {
		select {
		case <-joined:
		case <-time.After(leaseRPCBudget + time.Second):
		}
	}
	_, closeErr := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
	// Closing wakes a real SDK Drain if its budget has not already elapsed.
	select {
	case err := <-drained:
		return errors.Join(joinErr, err, closeErr)
	case <-time.After(leaseCloseBudget):
		return errors.Join(joinErr, closeErr, errors.New("SQS drain did not stop"))
	}
}
