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
	if a.receiveStopped != nil {
		<-a.receiveStopped
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

	for !a.isStop.Load() {
		deadline := time.Now().Add(sourceLeaseBudget)
		receiveCtx, receiveCancel := context.WithDeadline(a.ctx, deadline)
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
		if a.tracker == nil {
			a.tracker, _ = acktrack.NewACKTracker(a.uspClient, 1, 16)
		}
		delRequest := &sqs.DeleteMessageBatchInput{QueueUrl: &a.conf.QueueURL}
		var dependency acktrack.ACKDependency
		for _, msg := range result.Messages {
			dep, err := a.processEvent(msg.Body)
			if err != nil {
				return err
			}
			dependency, err = dependency.Include(dep)
			if err != nil {
				return err
			}
			delRequest.Entries = append(delRequest.Entries, &sqs.DeleteMessageBatchRequestEntry{Id: msg.MessageId, ReceiptHandle: msg.ReceiptHandle})
		}
		if err := a.tracker.Candidate("batch", []byte{1}, dependency); err != nil {
			return err
		}
		if err := a.awaitDelivery(deadline); err != nil {
			return err
		}
		deleteCtx, cancel := context.WithDeadline(a.ctx, deadline)
		delRes, err := a.sqsClient.DeleteMessageBatchWithContext(deleteCtx, delRequest)
		cancel()
		if err != nil {
			return err
		}
		if len(delRes.Failed) != 0 {
			return errors.New("sqsClient.DeleteMessageBatch: failed to delete some messages")
		}

	}
	return nil
}

func (a *SQSAdapter) processEvent(message *string) (acktrack.ACKDependency, error) {
	if message == nil {
		return acktrack.ACKDependency{}, errors.New("missing message body")
	}
	msg := &protocol.DataMessage{
		TextPayload: *message,
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	dep, err := a.tracker.Ship(msg, 2*time.Second)
	if err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.DebugLog("stream falling behind")
			dep, err = a.tracker.Ship(msg, 2*time.Second)
		}
		if err != nil {
			return dep, err
		}
	}
	return dep, nil
}
