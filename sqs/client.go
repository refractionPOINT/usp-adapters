package usp_sqs

import (
	"context"
	"errors"
	"fmt"
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
	conf      SQSConfig
	uspClient *uspclient.Client

	awsConfig  *aws.Config
	awsSession *session.Session
	sqsClient  *sqs.SQS

	ctx    context.Context
	isStop bool
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

func NewSQSAdapter(conf SQSConfig) (*SQSAdapter, chan struct{}, error) {
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

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	var subErr error
	chStopped := make(chan struct{})
	go func() {
		defer close(chStopped)
		subErr = a.receiveEvents()
	}()
	// Give it a second to start the subscriber to check it's
	// working without any errors.
	time.Sleep(2 * time.Second)
	if subErr != nil {
		a.uspClient.Close()
		return nil, nil, subErr
	}

	return a, chStopped, nil
}

func (a *SQSAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.isStop = true
	if _, err := a.uspClient.Close(); err != nil {
		return err
	}
	return nil
}

func (a *SQSAdapter) receiveEvents() error {
	for !a.isStop {
		result, err := a.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames:        []*string{},
			MessageAttributeNames: []*string{},
			QueueUrl:              &a.conf.QueueURL,
			MaxNumberOfMessages:   aws.Int64(10),
			VisibilityTimeout:     aws.Int64(60), // 60 seconds
			WaitTimeSeconds:       aws.Int64(5),
		})
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("sqsClient.ReceiveMessage: %v", err))
			return err
		}
		delRequest := &sqs.DeleteMessageBatchInput{
			Entries:  make([]*sqs.DeleteMessageBatchRequestEntry, len(result.Messages)),
			QueueUrl: &a.conf.QueueURL,
		}
		if len(result.Messages) == 0 {
			continue
		}
		for i, msg := range result.Messages {
			if err := a.processEvent(msg.Body); err != nil {
				return err
			}
			delRequest.Entries[i] = &sqs.DeleteMessageBatchRequestEntry{
				Id:            msg.MessageId,
				ReceiptHandle: msg.ReceiptHandle,
			}
		}
		delRes, err := a.sqsClient.DeleteMessageBatch(delRequest)
		if err != nil {
			return err
		}
		if len(delRes.Failed) != 0 {
			return errors.New("sqsClient.DeleteMessageBatch: failed to delete some messages")
		}
	}
	return nil
}

func (a *SQSAdapter) processEvent(message *string) error {
	msg := &protocol.DataMessage{
		TextPayload: *message,
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.DebugLog("stream falling behind")
			err = a.uspClient.Ship(msg, 0)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			return err
		}
	}
	return nil
}
