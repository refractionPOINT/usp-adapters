package usp_sqs_files

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"

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

	// S3. Cached per bucket: one queue can carry events for buckets in
	// different regions and a downloader is only valid for its bucket's
	// region. Guarded by s3Mutex because receiveEvents populates the map
	// while the processFiles goroutines read from it.
	s3Mutex     sync.Mutex
	downloaders map[string]*s3manager.Downloader

	ctx    context.Context
	isStop bool
	wg     sync.WaitGroup
}

type SQSFilesConfig struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`

	// SQS specific
	AccessKey  string `json:"access_key" yaml:"access_key"`
	SecretKey  string `json:"secret_key,omitempty" yaml:"secret_key,omitempty"`
	QueueURL   string `json:"queue_url" yaml:"queue_url"`
	Region     string `json:"region" yaml:"region"`
	RoleArn    string `json:"role_arn,omitempty" yaml:"role_arn,omitempty"`
	ExternalId string `json:"external_id,omitempty" yaml:"external_id,omitempty"`

	// S3 specific
	ParallelFetch          int    `json:"parallel_fetch" yaml:"parallel_fetch"`
	BucketPath             string `json:"bucket_path,omitempty" yaml:"bucket_path,omitempty"`
	FilePath               string `json:"file_path,omitempty" yaml:"file_path,omitempty"`
	IsDecodeObjectKey      bool   `json:"is_decode_object_key,omitempty" yaml:"is_decode_object_key,omitempty"`
	SplitCloudTrailRecords bool   `json:"split_cloudtrail_records,omitempty" yaml:"split_cloudtrail_records,omitempty"`
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

	// If RoleArn is provided, assume the role
	if conf.RoleArn != "" {
		creds, err := a.assumeRole(conf.RoleArn, conf.ExternalId)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to assume role: %v", err)
		}
		a.awsConfig.Credentials = creds
		if a.awsSession, err = session.NewSession(a.awsConfig); err != nil {
			return nil, nil, err
		}
	}

	a.sqsClient = sqs.New(a.awsSession)

	// The S3 SDKs are initialized at run-time, per bucket, as SQS events
	// name them.
	a.downloaders = map[string]*s3manager.Downloader{}

	a.chFiles = make(chan fileInfo)

	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	// Log the CloudTrail splitting configuration
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("CloudTrail splitting config: SplitCloudTrailRecords=%v", a.conf.SplitCloudTrailRecords))

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

// initS3SDKs makes sure a downloader exists for bucket, resolving the
// bucket's region once and caching the result.
//
// Caching per bucket rather than with a single "already initialized" flag
// matters twice over: a flag either re-resolves the region on every message
// (an API round trip each time, and no connection reuse) or pins every
// bucket to the first one's region. A queue fed by buckets in more than one
// region needs a downloader per bucket.
func (a *SQSFilesAdapter) initS3SDKs(bucket string) error {
	a.s3Mutex.Lock()
	defer a.s3Mutex.Unlock()

	if _, ok := a.downloaders[bucket]; ok {
		return nil
	}

	region, err := a.getBucketRegion(bucket)
	if err != nil {
		return fmt.Errorf("s3.Region: %v", err)
	}

	// Use the same credentials as SQS (which may already be assumed role credentials)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: a.awsConfig.Credentials,
	})
	if err != nil {
		return fmt.Errorf("s3.NewSession(): %v", err)
	}

	a.downloaders[bucket] = s3manager.NewDownloader(sess)
	return nil
}

// getDownloader returns the cached downloader for bucket. receiveEvents always
// calls initS3SDKs before queueing a file, so a miss means the file was queued
// without initialization.
func (a *SQSFilesAdapter) getDownloader(bucket string) (*s3manager.Downloader, error) {
	a.s3Mutex.Lock()
	defer a.s3Mutex.Unlock()

	d, ok := a.downloaders[bucket]
	if !ok {
		return nil, fmt.Errorf("no S3 downloader for bucket %s", bucket)
	}
	return d, nil
}

// getBucketRegion resolves which region a bucket lives in so the downloader
// talks to the right endpoint.
//
// The lookup itself is unsigned: s3manager.GetBucketRegion sends a HeadBucket
// with credentials.AnonymousCredentials and reads the x-amz-bucket-region
// response header, so it needs no IAM permission and never calls STS. The
// session is still built from the adapter's configuration rather than an
// empty aws.Config so that it does not load the host's ambient AWS config
// (environment, shared config, instance profile), which session.Must would
// panic on if it were invalid.
//
// A lookup failure (network, proxy, or a missing bucket) is not fatal: the
// configured region is a reasonable fallback and keeps the adapter running.
// A wrong fallback surfaces as a per-file S3 error rather than the adapter
// refusing to start.
func (a *SQSFilesAdapter) getBucketRegion(bucket string) (string, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(a.conf.Region),
		Credentials: a.awsConfig.Credentials,
	})
	if err != nil {
		return "", fmt.Errorf("s3.NewSession(): %v", err)
	}

	region, err := s3manager.GetBucketRegion(a.ctx, sess, bucket, a.conf.Region)
	if err != nil {
		a.conf.ClientOptions.OnWarning(fmt.Sprintf("s3.GetBucketRegion(%s): %v, falling back to configured region %s", bucket, err, a.conf.Region))
		return a.conf.Region, nil
	}
	return region, nil
}

func (a *SQSFilesAdapter) assumeRole(roleArn, externalId string) (*credentials.Credentials, error) {
	// Create STS client with base credentials
	stsClient := sts.New(a.awsSession)

	// Build assume role input
	assumeRoleInput := &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String(fmt.Sprintf("sqs-files-adapter-%d", time.Now().Unix())),
	}

	// Add external ID if provided (required for cross-account access)
	if externalId != "" {
		assumeRoleInput.ExternalId = aws.String(externalId)
	}

	// Return credentials that will automatically refresh
	return stscreds.NewCredentialsWithClient(stsClient, roleArn, func(p *stscreds.AssumeRoleProvider) {
		p.ExternalID = assumeRoleInput.ExternalId
		p.RoleSessionName = *assumeRoleInput.RoleSessionName
	}), nil
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
		path := f.path
		if a.conf.IsDecodeObjectKey {
			// URL Decode the path
			var err error
			path, err = url.QueryUnescape(path)
			if err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("url.QueryUnescape(): %v", err))
				continue
			}
		}
		downloader, err := a.getDownloader(f.bucket)
		if err != nil {
			a.conf.ClientOptions.OnError(err)
			continue
		}

		startTime := time.Now().UTC()
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("downloading file %s", path))

		writerAt := aws.NewWriteAtBuffer([]byte{})

		if _, err := downloader.Download(writerAt, &s3.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(path),
		}); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("s3.Download(): %v", err))
			return err
		}
		isCompressed := false

		if strings.HasSuffix(path, ".gz") {
			isCompressed = true
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("file %s downloaded in %v", path, time.Since(startTime)))

		// If CloudTrail record splitting is enabled, attempt to try to split the records
		if a.conf.SplitCloudTrailRecords {
			a.conf.ClientOptions.DebugLog(fmt.Sprintf("CloudTrail splitting enabled for %s", path))
			records, err := a.splitCloudTrailRecords(writerAt.Bytes())
			if err != nil {
				// Warn rather than debug: splitting was asked for and did not
				// happen, and the fall-through ships the file as one line.
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("failed to split CloudTrail records from %s: %v", path, err))
			} else if len(records) > 0 {
				a.conf.ClientOptions.DebugLog(fmt.Sprintf("split %d CloudTrail records from %s", len(records), path))
				for i, record := range records {
					if !a.processEvent(record, false) {
						a.conf.ClientOptions.OnError(fmt.Errorf("failed to process record %d from %s", i+1, path))
					}
				}
				continue
			} else {
				a.conf.ClientOptions.OnWarning(fmt.Sprintf("no CloudTrail records found in %s, processing as regular file", path))
			}
			// If splitting failed or returned no records, fall through to process as normal file
		}

		// Whatever reaches here is shipped as a single bundle payload, so a
		// CloudTrail file that was not split is one enormous line. The proxy
		// rejects an oversized line by dropping the connection before acking,
		// which makes the uspclient retransmit it on every reconnect and stalls
		// the adapter for good. Skipping the one file keeps ingestion alive.
		if err := utils.CheckMaxLineSize(path, writerAt.Bytes(), isCompressed); err != nil {
			a.conf.ClientOptions.OnError(err)
			continue
		}

		a.processEvent(writerAt.Bytes(), isCompressed)
	}
	return nil
}

// splitCloudTrailRecords attempts to parse the data as a CloudTrail event
// and split it into individual records. Returns nil error and empty slice if not a CloudTrail event.
// Handles both raw JSON and gzip-compressed JSON files.
func (a *SQSFilesAdapter) splitCloudTrailRecords(data []byte) ([][]byte, error) {
	// Check if data is gzipped (starts with gzip magic number 0x1f 0x8b)
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		// Decompress the gzipped data
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer gr.Close()

		// Bounded: this materializes the decompressed object, and CloudTrail
		// files of a flood of near-identical events compress hundreds-to-one,
		// so an unbounded read here is what turns a small object into a
		// multi-hundred-MB allocation (and a gzip bomb into an OOM).
		decompressed, err := io.ReadAll(io.LimitReader(gr, utils.MaxDecompressedSize+1))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress gzip data: %v", err)
		}
		if int64(len(decompressed)) > utils.MaxDecompressedSize {
			return nil, fmt.Errorf("decompressed size exceeds %d bytes", int64(utils.MaxDecompressedSize))
		}
		data = decompressed
	}

	var result [][]byte

	// Try to parse as CloudTrail event with wrapper
	var wrapper map[string]interface{}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return nil, err
	}

	// Check for event.Records structure (with routing wrapper)
	var records []interface{}
	if eventMap, ok := wrapper["event"].(map[string]interface{}); ok {
		if recordsArray, ok := eventMap["Records"].([]interface{}); ok {
			records = recordsArray
		}
	} else if recordsArray, ok := wrapper["Records"].([]interface{}); ok {
		// Direct Records array (no wrapper)
		records = recordsArray
	}

	// If no records found, return empty (not an error, just not a CloudTrail event)
	if len(records) == 0 {
		return nil, nil
	}

	// Extract routing and timestamp from wrapper if present
	routing := wrapper["routing"]
	ts := wrapper["ts"]

	// Split each record into individual messages
	for _, record := range records {
		var message map[string]interface{}
		if routing != nil || ts != nil {
			// Preserve routing and timestamp
			message = map[string]interface{}{
				"event": record,
			}
			if routing != nil {
				message["routing"] = routing
			}
			if ts != nil {
				message["ts"] = ts
			}
		} else {
			// Just the record itself
			message = map[string]interface{}{
				"event": record,
			}
		}

		// Marshal back to JSON
		recordData, err := json.Marshal(message)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("failed to marshal record: %v", err))
			continue
		}
		result = append(result, recordData)
	}

	return result, nil
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
