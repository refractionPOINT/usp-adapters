package usp_sqs

import (
	"context"
	"errors"
	"fmt"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultWriteTimeout = 60 * 10
)

type SQSAdapter struct {
	conf           SQSConfig
	uspClient      acktrack.SDKClient
	tracker        *acktrack.ACKTracker
	receiveStopped <-chan struct{}
	collectionOnce sync.Once
	collectCtx     context.Context
	stopCollection context.CancelFunc

	awsConfig  *aws.Config
	awsSession *session.Session
	sqsClient  *sqs.SQS

	ctx    context.Context
	isStop atomic.Bool
}

type SQSConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	AccessKey     string                  `json:"access_key" yaml:"access_key"`
	SecretKey     string                  `json:"secret_key,omitempty" yaml:"secret_key,omitempty"`
	QueueURL      string                  `json:"queue_url" yaml:"queue_url"`
	Region        string                  `json:"region" yaml:"region"`
}

func (c *SQSConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.AccessKey == "" {
		return errors.New("missing access_key")
	}
	if c.SecretKey == "" {
		return errors.New("missing secret_key")
	}
	if c.Region == "" {
		return errors.New("missing region")
	}
	if c.QueueURL == "" {
		return errors.New("missing queue_url")
	}
	return nil
}

func NewSQSAdapter(ctx context.Context, conf SQSConfig) (*SQSAdapter, chan struct{}, error) {
	a := &SQSAdapter{
		conf: conf,
		ctx:  context.Background(),
	}

	var err error
	a.awsConfig = &aws.Config{
		Region:      aws.String(conf.Region),
		Credentials: credentials.NewStaticCredentials(conf.AccessKey, conf.SecretKey, ""),
	}

	if a.awsSession, err = session.NewSession(a.awsConfig); err != nil {
		return nil, nil, err
	}

	a.sqsClient = sqs.New(a.awsSession)

	a.uspClient, err = acktrack.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	var subErr error
	var subErrMu sync.Mutex
	chStopped := make(chan struct{})
	a.receiveStopped = chStopped
	go func() {
		defer close(chStopped)
		err := a.receiveEvents()
		subErrMu.Lock()
		subErr = err
		subErrMu.Unlock()
	}()
	// Give it a second to start the subscriber to check it's
	// working without any errors.
	time.Sleep(2 * time.Second)
	subErrMu.Lock()
	startErr := subErr
	subErrMu.Unlock()
	if startErr != nil {
		a.uspClient.Close()
		return nil, nil, startErr
	}

	return a, chStopped, nil
}

func (a *SQSAdapter) Close() error {
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")
	a.isStop.Store(true)
	a.collectionContext()
	a.stopCollection()
	if a.receiveStopped != nil {
		select {
		case <-a.receiveStopped:
		case <-time.After(leaseCloseBudget):
			a.uspClient.BeginDrain()
			_, err := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
			return errors.Join(errors.New("SQS workers did not stop within shutdown budget"), err)
		}
	}
	if _, err := utils.DrainCloseUSP(a.uspClient, a.conf.ClientOptions); err != nil {
		return err
	}
	return nil
}

func (a *SQSAdapter) receiveEvents() (receiveErr error) {
	// All receiver failures, including worker and late cleanup errors, reach
	// the existing error callback before the stopped channel is closed.
	defer func() {
		if receiveErr != nil && !errors.Is(receiveErr, uspclient.ErrorClosed) && a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("SQS receive stopped: %w", receiveErr))
		}
	}()

	collectCtx := a.collectionContext()
	for !a.isStop.Load() {
		receivedBefore := time.Now()
		deadline := receivedBefore.Add(min(sourceLeaseBudget, 10*time.Second))
		receiveCtx, receiveCancel := context.WithDeadline(collectCtx, deadline)
		result, err := a.sqsClient.ReceiveMessageWithContext(receiveCtx, &sqs.ReceiveMessageInput{
			AttributeNames:        []*string{},
			MessageAttributeNames: []*string{},
			QueueUrl:              &a.conf.QueueURL,
			MaxNumberOfMessages:   aws.Int64(10),
			VisibilityTimeout:     aws.Int64(60), // 60 seconds
			WaitTimeSeconds:       aws.Int64(5),
		})
		receiveCancel()
		if err != nil {
			return err
		}

		if len(result.Messages) == 0 {
			continue
		}
		if err := a.handleBatch(collectCtx, result.Messages, receivedBefore); err != nil {
			return err
		}

	}
	return nil
}

func (a *SQSAdapter) processEvent(message *string) (acktrack.ACKDependency, error) {
	return a.processEventContext(context.Background(), message)
}
func (a *SQSAdapter) processEventContext(ctx context.Context, message *string) (acktrack.ACKDependency, error) {
	if message == nil {
		return acktrack.ACKDependency{}, errors.New("missing message body")
	}
	msg := &protocol.DataMessage{
		TextPayload: *message,
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	return a.shipContext(ctx, msg)
}

func (a *SQSAdapter) handleBatch(ctx context.Context, messages []*sqs.Message, receivedBefore time.Time) error {
	if a.tracker == nil {
		a.tracker, _ = acktrack.NewACKTracker(a.uspClient, 1, 16)
	}
	l := newBatchLease(ctx, a.sqsClient, a.conf.QueueURL, messages, receivedBefore)
	defer l.abort()
	if l.ctx.Err() != nil {
		return context.Cause(l.ctx)
	}
	var dependency acktrack.ACKDependency
	for _, message := range messages {
		dep, err := a.processEventContext(l.ctx, message.Body)
		if err != nil {
			return err
		}
		dependency, err = dependency.Include(dep)
		if err != nil {
			return err
		}
	}
	if err := a.tracker.Candidate("batch", []byte{1}, dependency); err != nil {
		return err
	}
	if err := a.awaitDeliveryContext(l.ctx); err != nil {
		return err
	}
	deleteCtx, cancel, err := l.cleanupContext()
	if err != nil {
		return err
	}
	defer cancel()
	req := &sqs.DeleteMessageBatchInput{QueueUrl: &a.conf.QueueURL}
	for i, m := range messages {
		req.Entries = append(req.Entries, &sqs.DeleteMessageBatchRequestEntry{Id: aws.String(fmt.Sprint(i)), ReceiptHandle: m.ReceiptHandle})
	}
	out, err := a.sqsClient.DeleteMessageBatchWithContext(deleteCtx, req)
	if err != nil {
		return err
	}
	if out == nil || len(out.Failed) != 0 {
		return errors.New("sqsClient.DeleteMessageBatch: failed to delete some messages")
	}
	return nil
}
