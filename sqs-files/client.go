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
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	intakeCtx      context.Context
	stopIntake     context.CancelFunc
	deliveryOnce   sync.Once
	delivery       *batchDelivery

	chFiles chan fileInfo

	// SQS
	awsConfig  *aws.Config
	awsSession *session.Session
	sqsClient  *sqs.SQS

	// Captured downloaders are immutable and safe for concurrent workers.
	bucketDownloaders map[string]*s3manager.Downloader

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
	bucket     string
	path       string
	done       chan fileResult
	ctx        context.Context
	downloader *s3manager.Downloader
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
	joined := make(chan struct{})
	go func() { a.wg.Wait(); close(joined) }()
	return a.closeBatches(joined)
}

const maxBucketDownloaders = 64

func (a *SQSFilesAdapter) downloaderForBucket(ctx context.Context, bucket string) (*s3manager.Downloader, error) {
	state := a.ensureDelivery()
	select {
	case <-state.sourceInit:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	defer func() { state.sourceInit <- struct{}{} }()
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if cached := a.bucketDownloaders[bucket]; cached != nil {
		return cached, nil
	}
	base := a.awsConfig
	if base == nil {
		base = &a.sqsClient.Config
	}
	sess, err := session.NewSession(base.Copy(&aws.Config{Region: aws.String("us-east-1")}))
	if err != nil {
		return nil, fmt.Errorf("s3.NewSession: %w", err)
	}
	lookupCtx, cancel := context.WithTimeout(ctx, leaseRPCBudget)
	region, err := s3manager.GetBucketRegionWithClient(lookupCtx, s3.New(sess), bucket)
	lookupErr := lookupCtx.Err()
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if lookupErr != nil {
			return nil, fmt.Errorf("s3.Region: %w", lookupErr)
		}
		// Bucket lookup failures commonly mean a wrong configuration. They
		// must retain notifications and reach the visible error callback.
		return nil, fmt.Errorf("s3.Region for bucket %q: %w", bucket, err)
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	regional, err := session.NewSession(base.Copy(&aws.Config{Region: aws.String(region)}))
	if err != nil {
		return nil, fmt.Errorf("s3.NewSession: %w", err)
	}
	downloader := s3manager.NewDownloader(regional)
	if a.bucketDownloaders == nil {
		a.bucketDownloaders = make(map[string]*s3manager.Downloader)
	}
	if len(a.bucketDownloaders) >= maxBucketDownloaders {
		// Eviction drops only this cache reference. In-flight workers retain their
		// exact immutable downloader; never rewrite a client used by another bucket.
		for key := range a.bucketDownloaders {
			delete(a.bucketDownloaders, key)
			break
		}
	}
	a.bucketDownloaders[bucket] = downloader
	return downloader, nil
}

func (a *SQSFilesAdapter) receiveEvents() (receiveErr error) {
	defer func() {
		if a.expectedClose(receiveErr) {
			receiveErr = nil
		}
		if receiveErr != nil && !a.expectedClose(receiveErr) && !errors.Is(receiveErr, uspclient.ErrorClosed) && a.conf.ClientOptions.OnError != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("SQS receive stopped: %w", receiveErr))
		}
	}()
	defer close(a.chFiles)
	return a.receiveBatches()
}

// Permanent notification errors are retained for the queue's redrive/DLQ policy.
// OnWarning keeps hosted owners running; OnError is reserved for fatal source,
// transport, admission, observation, or lease failures. These retained failures
// never authorize deletion. Source redrive/DLQ policy is required to quarantine
// them; without it they can replay until the source retention window expires.
type permanentNotificationError struct{ error }

// Only a successful download followed by a decode failure gets this outcome.
// It completes that file without touching the object; malformed notification
// paths/configuration remain retained notification errors.
type undecodableFileError struct {
	error
	bucket string
	key    string
}

// S3 sends this control notice when notification delivery is configured. It has
// no telemetry to admit. Retain it for redrive/DLQ without raising a fatal error.
var errS3TestEvent = errors.New("S3 TestEvent control notice retained for queue redrive/DLQ")

func permanent(err error) error { return &permanentNotificationError{err} }

func (a *SQSFilesAdapter) prepareBatch(ctx context.Context, messages []*sqs.Message) ([]*sqs.Message, acktrack.ACKDependency, bool, error) {
	var eligible []*sqs.Message
	var dependency acktrack.ACKDependency
	var hasDependency bool
	for _, m := range messages {
		dep, hasDep, err := a.processNotification(ctx, m)
		if err != nil {
			if errors.Is(err, errS3TestEvent) && ctx.Err() == nil {
				if a.conf.ClientOptions.OnWarning != nil {
					a.conf.ClientOptions.OnWarning(err.Error())
				}
				continue
			}
			var bad *permanentNotificationError
			if errors.As(err, &bad) && ctx.Err() == nil {
				if a.conf.ClientOptions.OnWarning != nil {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("retaining unprocessable SQS notification for redrive/DLQ: %v", bad.error))
				}
				continue
			}
			return nil, dependency, hasDependency, err
		}
		if hasDep {
			dependency, err = dependency.Include(dep)
			if err != nil {
				return nil, dependency, hasDependency, err
			}
			hasDependency = true
		}
		eligible = append(eligible, m)
	}
	return eligible, dependency, hasDependency, nil
}
func (a *SQSFilesAdapter) processNotification(ctx context.Context, message *sqs.Message) (acktrack.ACKDependency, bool, error) {
	var dependency acktrack.ACKDependency
	var hasDependency bool
	d := utils.Dict{}
	if message.Body == nil {
		return dependency, hasDependency, permanent(errors.New("missing notification body"))
	}
	if err := json.Unmarshal([]byte(*message.Body), &d); err != nil {
		return dependency, hasDependency, permanent(err)
	}
	_, hasRecords := d["Records"]
	if !hasRecords && d.ExpandableFindOneString("Event") == "s3:TestEvent" && d.ExpandableFindOneString("Service") == "Amazon S3" {
		return dependency, hasDependency, errS3TestEvent
	}
	bucket := a.conf.Bucket
	if bucket == "" {
		bucket = d.ExpandableFindOneString(a.conf.BucketPath)
	}
	paths := d.ExpandableFindString(a.conf.FilePath)
	if bucket == "" || len(paths) == 0 {
		return dependency, hasDependency, permanent(errors.New("notification missing bucket or files"))
	}
	downloader, err := a.downloaderForBucket(ctx, bucket)
	if err != nil {
		return dependency, hasDependency, err
	}
	fileCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan fileResult, len(paths))
	for _, path := range paths {
		select {
		case a.chFiles <- fileInfo{bucket: bucket, path: path, done: results, ctx: fileCtx, downloader: downloader}:
		case <-ctx.Done():
			return dependency, hasDependency, context.Cause(ctx)
		}
	}
	var permanentErr error
	for range paths {
		select {
		case r := <-results:
			if r.err != nil {
				var absent *absentSourceError
				if errors.As(r.err, &absent) && ctx.Err() == nil {
					a.warnAbsent(absent)
					continue
				}
				var skipped *undecodableFileError
				if errors.As(r.err, &skipped) && ctx.Err() == nil {
					if a.conf.ClientOptions.OnWarning != nil {
						a.conf.ClientOptions.OnWarning(fmt.Sprintf("skipped_undecodable_files=1 bucket=%q object_key=%q: %v", skipped.bucket, skipped.key, skipped.error))
					}
					continue
				}
				var bad *permanentNotificationError
				if errors.As(r.err, &bad) && ctx.Err() == nil {
					permanentErr = bad
					continue
				}
				return dependency, hasDependency, r.err
			}
			var err error
			dependency, err = dependency.Include(r.dep)
			if err != nil {
				return dependency, hasDependency, err
			}
			hasDependency = true
		case <-ctx.Done():
			return dependency, hasDependency, context.Cause(ctx)
		}
	}
	return dependency, hasDependency, permanentErr
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
			return empty, permanent(err)
		}
	}
	writerAt := aws.NewWriteAtBuffer([]byte{})
	if _, err := f.downloader.DownloadWithContext(f.ctx, writerAt, &s3.GetObjectInput{Bucket: aws.String(f.bucket), Key: aws.String(path)}); err != nil {
		if f.ctx.Err() != nil {
			return empty, context.Cause(f.ctx)
		}
		if sourceIsAbsent(err, "NoSuchKey", "NotFound") {
			return empty, &absentSourceError{err}
		}
		return empty, err
	}
	if err := f.ctx.Err(); err != nil {
		return empty, context.Cause(f.ctx)
	}
	data, compressed, err := prepareBundleData(f.ctx, path, writerAt.Bytes())
	if err != nil {
		if f.ctx.Err() != nil {
			return empty, context.Cause(f.ctx)
		}
		return empty, &undecodableFileError{error: err, bucket: f.bucket, key: path}
	}
	if err := f.ctx.Err(); err != nil {
		return empty, context.Cause(f.ctx)
	}
	return a.shipEventContext(f.ctx, data, compressed)
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

// An absent object in an existing bucket has no bytes to admit. Unlike
// malformed data, it is complete rather than retained for DLQ. Authorization, server, transport,
// and cancellation failures must never be mistaken for absence.
type absentSourceError struct{ error }

func sourceIsAbsent(err error, codes ...string) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var api awserr.Error
	if errors.As(err, &api) && (api.Code() == "AccessDenied" || api.Code() == "Forbidden" || api.Code() == "NoSuchBucket") {
		return false
	}
	var failure awserr.RequestFailure
	if errors.As(err, &failure) && failure.StatusCode() != 0 {
		return failure.StatusCode() == 404
	}
	if errors.As(err, &api) {
		for _, code := range codes {
			if api.Code() == code {
				return true
			}
		}
	}
	return false
}
func (a *SQSFilesAdapter) warnAbsent(err error) {
	if a.conf.ClientOptions.OnWarning != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("S3 source absent; completing file without admission: %v", err))
	}
}
