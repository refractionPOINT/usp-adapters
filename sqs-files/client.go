package usp_sqs_files

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"net/url"
	"sync"
	"sync/atomic"
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
	conf           SQSFilesConfig
	uspClient      acktrack.SDKClient
	tracker        *acktrack.ACKTracker
	receiveStopped <-chan struct{}
	collectionOnce sync.Once
	collectCtx     context.Context
	stopCollection context.CancelFunc

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
	isStop atomic.Bool
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
	ParallelFetch     int    `json:"parallel_fetch" yaml:"parallel_fetch"`
	BucketPath        string `json:"bucket_path,omitempty" yaml:"bucket_path,omitempty"`
	FilePath          string `json:"file_path,omitempty" yaml:"file_path,omitempty"`
	IsDecodeObjectKey bool   `json:"is_decode_object_key,omitempty" yaml:"is_decode_object_key,omitempty"`
	// Optional: alternative to BucketPath
	Bucket string `json:"bucket,omitempty" yaml:"bucket,omitempty"`
}

type fileResult struct {
	dep acktrack.ACKDependency
	err error
}

type fileInfo struct {
	bucket string
	path   string
	done   chan fileResult
	ctx    context.Context
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

func NewSQSFilesAdapter(ctx context.Context, conf SQSFilesConfig) (*SQSFilesAdapter, chan struct{}, error) {
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

	a.uspClient, err = acktrack.NewClient(ctx, conf.ClientOptions)
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
	var subErrMu sync.Mutex
	chStopped := make(chan struct{})
	a.receiveStopped = chStopped
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
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

func (a *SQSFilesAdapter) Close() error {
	defer utils.StopBlockedProducers(a.uspClient)()
	a.conf.ClientOptions.DebugLog("closing")
	a.isStop.Store(true)
	a.collectionContext()
	a.stopCollection()
	joined := make(chan struct{})
	go func() { a.wg.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(leaseCloseBudget):
		a.uspClient.BeginDrain()
		_, err := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
		return errors.Join(errors.New("SQS file workers did not stop within shutdown budget"), err)
	}
	if _, err := utils.DrainCloseUSP(a.uspClient, a.conf.ClientOptions); err != nil {
		return err
	}
	return nil
}

func (a *SQSFilesAdapter) initS3SDKs(ctx context.Context, bucket string) error {
	if a.isS3Inited {
		return nil
	}
	region, err := a.getBucketRegion(ctx, bucket)
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

	a.awsS3 = s3.New(a.awsS3Session)
	a.awsDownloader = s3manager.NewDownloader(a.awsS3Session)
	a.isS3Inited = true
	return nil
}

func (a *SQSFilesAdapter) getBucketRegion(ctx context.Context, bucket string) (string, error) {
	return s3manager.GetBucketRegion(ctx, session.Must(session.NewSession(&aws.Config{})), bucket, "us-east-1")
}

func (a *SQSFilesAdapter) receiveEvents() (receiveErr error) {
	// All receiver failures, including worker and late cleanup errors, reach
	// the existing error callback before the stopped channel is closed.
	defer func() {
		if receiveErr != nil && !errors.Is(receiveErr, uspclient.ErrorClosed) && a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("SQS receive stopped: %w", receiveErr))
		}
	}()

	defer close(a.chFiles)

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

func (a *SQSFilesAdapter) processBatch(ctx context.Context, messages []*sqs.Message) error {
	var dependency acktrack.ACKDependency
	for _, message := range messages {
		d := utils.Dict{}
		if message.Body == nil {
			return errors.New("missing notification body")
		}
		if err := json.Unmarshal([]byte(*message.Body), &d); err != nil {
			return err
		}
		bucket := a.conf.Bucket
		if bucket == "" {
			bucket = d.ExpandableFindOneString(a.conf.BucketPath)
		}
		paths := d.ExpandableFindString(a.conf.FilePath)
		if bucket == "" || len(paths) == 0 {
			return errors.New("notification missing bucket or files")
		}
		if err := a.initS3SDKs(ctx, bucket); err != nil {
			return err
		}
		// Buffer one result per file so workers never get stranded on errors.
		results := make(chan fileResult, len(paths))
		for _, path := range paths {
			select {
			case a.chFiles <- fileInfo{bucket: bucket, path: path, done: results, ctx: ctx}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		for range paths {
			select {
			case r := <-results:
				if r.err != nil {
					return r.err
				}
				var err error
				dependency, err = dependency.Include(r.dep)
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return a.tracker.Candidate("batch", []byte{1}, dependency)
}

func (a *SQSFilesAdapter) processFiles() error {
	for f := range a.chFiles {
		dep, err := a.processFile(f)
		f.done <- fileResult{dep: dep, err: err}
	}
	return nil
}

func (a *SQSFilesAdapter) processFile(f fileInfo) (acktrack.ACKDependency, error) {
	var empty acktrack.ACKDependency
	if err := f.ctx.Err(); err != nil {
		return empty, err
	}
	path := f.path
	if a.conf.IsDecodeObjectKey {
		var err error
		path, err = url.QueryUnescape(path)
		if err != nil {
			return empty, err
		}
	}
	writerAt := aws.NewWriteAtBuffer([]byte{})
	if _, err := a.awsDownloader.DownloadWithContext(f.ctx, writerAt, &s3.GetObjectInput{Bucket: aws.String(f.bucket), Key: aws.String(path)}); err != nil {
		return empty, err
	}
	if err := f.ctx.Err(); err != nil {
		return empty, context.Cause(f.ctx)
	}
	data, compressed, err := prepareBundleData(f.ctx, path, writerAt.Bytes())
	if err != nil {
		return empty, err
	}
	if err := f.ctx.Err(); err != nil {
		return empty, context.Cause(f.ctx)
	}
	return a.shipEventContext(f.ctx, data, compressed)
}

func (a *SQSFilesAdapter) shipEvent(data []byte, isCompressed bool) (acktrack.ACKDependency, error) {
	return a.shipEventContext(context.Background(), data, isCompressed)
}
func (a *SQSFilesAdapter) shipEventContext(ctx context.Context, data []byte, isCompressed bool) (acktrack.ACKDependency, error) {
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

	return a.shipContext(ctx, msg)
}

func (a *SQSFilesAdapter) handleBatch(ctx context.Context, messages []*sqs.Message, receivedBefore time.Time) error {
	if a.tracker == nil {
		a.tracker, _ = acktrack.NewACKTracker(a.uspClient, 1, 16)
	}
	l := newBatchLease(ctx, a.sqsClient, a.conf.QueueURL, messages, receivedBefore)
	defer l.abort()
	if l.ctx.Err() != nil {
		return context.Cause(l.ctx)
	}
	if err := a.processBatch(l.ctx, messages); err != nil {
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
