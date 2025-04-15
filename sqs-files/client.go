package usp_sqs_files

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	defaultWriteTimeout = 60 * 10
)

type SQSFilesAdapter struct {
	conf      SQSFilesConfig
	uspClient *uspclient.Client

	chFiles chan fileInfo

	// SQS
	awsConfig  *aws.Config
	awsSession *session.Session
	sqsClient  *sqs.SQS

	// S3
	isS3Inited    bool
	awsS3Config   *aws.Config
	awsS3Session  *session.Session
	awsS3         *s3.S3
	awsDownloader *s3manager.Downloader

	ctx    context.Context
	isStop bool
	wg     sync.WaitGroup
}

type SQSFilesConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// SQS specific
	AccessKey string `json:"access_key" yaml:"access_key"`
	SecretKey string `json:"secret_key,omitempty" yaml:"secret_key,omitempty"`
	QueueURL  string `json:"queue_url" yaml:"queue_url"`
	Region    string `json:"region" yaml:"region"`

	// S3 specific
	ParallelFetch int    `json:"parallel_fetch" yaml:"parallel_fetch"`
	BucketPath    string `json:"bucket_path,omitempty" yaml:"bucket_path,omitempty"`
	FilePath      string `json:"file_path,omitempty" yaml:"file_path,omitempty"`
	// Optional: alternative to BucketPath
	Bucket string `json:"bucket,omitempty" yaml:"bucket,omitempty"`
}

type fileInfo struct {
	bucket string
	path   string
}

func (c *SQSFilesConfig) Validate() error {
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

func NewSQSFilesAdapter(conf SQSFilesConfig) (*SQSFilesAdapter, chan struct{}, error) {
	if err := conf.Validate(); err != nil {
		return nil, nil, err
	}

	if conf.ParallelFetch <= 0 {
		conf.ParallelFetch = 1
	}
	if conf.BucketPath == "" {
		conf.BucketPath = "bucket"
	}
	if conf.FilePath == "" {
		conf.FilePath = "files/path"
	}

	a := &SQSFilesAdapter{
		conf: conf,
		ctx:  context.Background(),
	}

	var err error

	// SQS
	a.awsConfig = &aws.Config{
		Region:      aws.String(conf.Region),
		Credentials: credentials.NewStaticCredentials(conf.AccessKey, conf.SecretKey, ""),
	}

	if a.awsSession, err = session.NewSession(a.awsConfig); err != nil {
		return nil, nil, err
	}

	a.sqsClient = sqs.New(a.awsSession)

	// The S3 SDK will be initialized at run-time
	// once we get the first file from an SQS event.
	a.isS3Inited = false

	a.chFiles = make(chan fileInfo)

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	// Start the processors.
	for i := 0; i < a.conf.ParallelFetch; i++ {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.processFiles()
		}()
	}

	var subErr error
	chStopped := make(chan struct{})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
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

func (a *SQSFilesAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	a.isStop = true
	a.wg.Wait()
	if _, err := a.uspClient.Close(); err != nil {
		return err
	}
	return nil
}

func (a *SQSFilesAdapter) initS3SDKs(bucket string) error {
	if a.isS3Inited {
		return nil
	}
	region, err := a.getBucketRegion(bucket)
	if err != nil {
		return fmt.Errorf("s3.Region: %v", err)
	}
	a.awsS3Config = &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(a.conf.AccessKey, a.conf.SecretKey, ""),
	}

	if a.awsS3Session, err = session.NewSession(a.awsS3Config); err != nil {
		return fmt.Errorf("s3.NewSession(): %v", err)
	}

	a.awsS3 = s3.New(a.awsSession)
	a.awsDownloader = s3manager.NewDownloader(a.awsS3Session)
	return nil
}

func (a *SQSFilesAdapter) getBucketRegion(bucket string) (string, error) {
	return s3manager.GetBucketRegion(a.ctx, session.Must(session.NewSession(&aws.Config{})), bucket, "us-east-1")
}

func (a *SQSFilesAdapter) receiveEvents() error {
	defer close(a.chFiles)

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
			Entries:  make([]*sqs.DeleteMessageBatchRequestEntry, 0, len(result.Messages)),
			QueueUrl: &a.conf.QueueURL,
		}
		if len(result.Messages) == 0 {
			continue
		}
		for _, msg := range result.Messages {
			delRequest.Entries = append(delRequest.Entries, &sqs.DeleteMessageBatchRequestEntry{
				Id:            msg.MessageId,
				ReceiptHandle: msg.ReceiptHandle,
			})

			d := utils.Dict{}
			if err := json.Unmarshal([]byte(*msg.Body), &d); err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("sqsClient.Message.Json: %v", err))
				continue
			}

			bucket := a.conf.Bucket
			if bucket == "" {
				bucket = d.ExpandableFindOneString(a.conf.BucketPath)
			}
			filePaths := d.ExpandableFindString(a.conf.FilePath)

			if bucket == "" {
				a.conf.ClientOptions.OnError(errors.New("sqsClient.Message: missing bucket"))
				continue
			}
			if len(filePaths) == 0 {
				continue
			}
			if err := a.initS3SDKs(bucket); err != nil {
				a.conf.ClientOptions.OnError(err)
				return err
			}

			for _, p := range filePaths {
				a.chFiles <- fileInfo{
					bucket: bucket,
					path:   p,
				}
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

func (a *SQSFilesAdapter) processFiles() error {
	for f := range a.chFiles {
		startTime := time.Now().UTC()
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("downloading file %s", f.path))

		writerAt := aws.NewWriteAtBuffer([]byte{})

		if _, err := a.awsDownloader.Download(writerAt, &s3.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.path),
		}); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("s3.Download(): %v", err))
			return err
		}
		isCompressed := false

		if strings.HasSuffix(f.path, ".gz") {
			isCompressed = true
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("file %s downloaded in %v", f.path, time.Since(startTime)))

		a.processEvent(writerAt.Bytes(), isCompressed)
	}
	return nil
}

func (a *SQSFilesAdapter) processEvent(data []byte, isCompressed bool) bool {
	// Since we're dealing with files, we use the
	// bundle payloads to avoid having to go through
	// the whole unmarshal+marshal roundtrip.
	var msg *protocol.DataMessage
	if isCompressed {
		msg = &protocol.DataMessage{
			CompressedBundlePayload: data,
			TimestampMs:             uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
	} else {
		msg = &protocol.DataMessage{
			BundlePayload: data,
			TimestampMs:   uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
	}

	if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
		if err == uspclient.ErrorBufferFull {
			a.conf.ClientOptions.OnWarning("stream falling behind")
			err = a.uspClient.Ship(msg, 1*time.Hour)
		}
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
			return false
		}
	}
	return true
}
