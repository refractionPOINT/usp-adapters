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
	intakeCtx      context.Context
	stopIntake     context.CancelFunc
	deliveryOnce   sync.Once
	delivery       *batchDelivery

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
	joined := a.receiveStopped
	if joined == nil {
		empty := make(chan struct{})
		close(empty)
		joined = empty
	}
	return a.closeBatches(joined)
}
func (a *SQSAdapter) receiveEvents() (receiveErr error) {
	defer func() {
		if a.expectedClose(receiveErr) {
			receiveErr = nil
		}
		if receiveErr != nil && !a.expectedClose(receiveErr) && !errors.Is(receiveErr, uspclient.ErrorClosed) && a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("SQS receive stopped: %w", receiveErr))
		}
	}()

	return a.receiveBatches()
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

func (a *SQSAdapter) prepareBatch(ctx context.Context, messages []*sqs.Message) ([]*sqs.Message, acktrack.ACKDependency, error) {
	var dep acktrack.ACKDependency
	var eligible []*sqs.Message
	for _, m := range messages {
		if m.Body == nil {
			if a.conf.ClientOptions.OnWarning != nil {
				a.conf.ClientOptions.OnWarning("retaining SQS notification missing body for redrive/DLQ")
			}
			continue
		}
		next, err := a.processEventContext(ctx, m.Body)
		if err != nil {
			return nil, dep, err
		}
		dep, err = dep.Include(next)
		if err != nil {
			return nil, dep, err
		}
		eligible = append(eligible, m)
	}
	return eligible, dep, nil
}
