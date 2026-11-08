package usp_s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/refractionPOINT/usp-adapters/utils"
)

// connResetRetryer extends the default AWS SDK retryer so that
// transient transport errors like "connection reset by peer"
// (or the related unexpected EOF that follows an RST) are retried
// transparently.
//
// S3, NAT gateways or load‑balancers will silently drop idle TLS
// connections. When the SDK tries to reuse that socket it receives
// an RST on the first read and surfaces a syscall.ECONNRESET which
// is *not* retried by default in v1 of the AWS SDK.
// Adding the pattern match here lets us keep the rest of the default
// exponential‑back‑off semantics intact.

type connResetRetryer struct {
	client.DefaultRetryer
}

func (r connResetRetryer) ShouldRetry(req *request.Request) bool {
	// honour the built‑in rules first
	if r.DefaultRetryer.ShouldRetry(req) {
		return true
	}
	// fallback: look for TCP‑level resets that are safe to retry
	if req.Error != nil {
		msg := req.Error.Error()
		if strings.Contains(msg, "connection reset by peer") ||
			strings.Contains(msg, "unexpected EOF") {
			return true
		}
	}
	return false
}

const maxObjectSize = 1024 * 1024 * 100 // 100 MB

type S3Adapter struct {
	conf           S3Config
	uspClient      acktrack.SDKClient
	tracker        *acktrack.ACKTracker
	pendingDeletes []pendingObjectDelete
	admitted       uint64
	lastDependency acktrack.ACKDependency
	ackView        *objectACKView
	deleteErr      error
	closeOnce      sync.Once
	closeErr       error

	ctx context.Context

	awsConfig     *aws.Config
	awsSession    *session.Session
	awsS3         *s3.S3
	awsDownloader *s3manager.Downloader

	isStop uint32
	wg     sync.WaitGroup

	region string
}

type S3Config struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	BucketName    string                  `json:"bucket_name" yaml:"bucket_name"`
	AccessKey     string                  `json:"access_key" yaml:"access_key"`
	SecretKey     string                  `json:"secret_key,omitempty" yaml:"secret_key,omitempty"`
	IsOneTimeLoad bool                    `json:"single_load" yaml:"single_load"`
	Prefix        string                  `json:"prefix" yaml:"prefix"`
	ParallelFetch int                     `json:"parallel_fetch" yaml:"parallel_fetch"`
	Region        string                  `json:"region" yaml:"region"`
}

func (c *S3Config) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.BucketName == "" {
		return errors.New("missing bucket_name")
	}
	if c.AccessKey == "" {
		return errors.New("missing access_key")
	}
	if c.SecretKey == "" {
		return errors.New("missing secret_key")
	}
	return nil
}

type s3LocalFile struct {
	Identity     s3ObjectIdentity
	Obj          *s3Record
	Data         []byte
	IsCompressed bool
	Err          error
}

type s3Record struct {
	Key  string
	Size int64
}

func NewS3Adapter(ctx context.Context, conf S3Config) (*S3Adapter, chan struct{}, error) {
	if conf.ParallelFetch <= 0 {
		conf.ParallelFetch = 1
	}
	a := &S3Adapter{
		conf: conf,
		ctx:  ctx,
	}

	var err error
	var region string

	if conf.Region != "" {
		region = conf.Region
	} else {
		if region, err = a.getRegion(); err != nil {
			// Check if context was cancelled/timed out during region detection
			if ctx.Err() != nil {
				return nil, nil, fmt.Errorf("s3.GetBucketRegion() context cancelled: %w", ctx.Err())
			}
			return nil, nil, fmt.Errorf("s3.GetBucketRegion(): %v", err)
		}
	}
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("s3 region for %q: %s", a.conf.BucketName, region))

	a.region = region

	// ---------------------------
	// HTTP client tuned for S3
	// ---------------------------

	tr := &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		// 25 s is comfortably below the ~60 s keep‑alive employed by S3 ELBs
		// and WAY below the 350 s idle‑flow timeout of NAT Gateways.
		IdleConnTimeout: 25 * time.Second,
	}

	httpClient := &http.Client{
		Transport: tr,
		// no global timeout – we stream large objects, per‑request
		// context deadlines should be used instead.
	}

	a.awsConfig = &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(conf.AccessKey, conf.SecretKey, ""),
		HTTPClient:  httpClient,
		Retryer: connResetRetryer{
			DefaultRetryer: client.DefaultRetryer{NumMaxRetries: 8},
		},
	}

	if a.awsSession, err = session.NewSession(a.awsConfig); err != nil {
		return nil, nil, fmt.Errorf("s3.NewSession(): %v", err)
	}

	a.awsS3 = s3.New(a.awsSession)
	a.awsDownloader = s3manager.NewDownloader(a.awsSession)

	a.uspClient, err = acktrack.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	chStopped := make(chan struct{})

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(chStopped)

		for {
			isFilesFound := false
			if atomic.LoadUint32(&a.isStop) == 1 {
				break
			}

			isFilesFound, err = a.lookForFiles()
			if err != nil || a.conf.IsOneTimeLoad {
				break
			}

			if !isFilesFound {
				time.Sleep(5 * time.Second)
			}
		}

		if err != nil {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("s3 stoppped with error: %v", err))
		}
	}()

	return a, chStopped, nil
}

func (a *S3Adapter) getRegion() (string, error) {
	// Try commercial partition first
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Credentials:      credentials.NewStaticCredentials(a.conf.AccessKey, a.conf.SecretKey, ""),
		S3ForcePathStyle: aws.Bool(false),
	})
	if err != nil {
		return "", err
	}

	region, err := s3manager.GetBucketRegion(a.ctx, sess, a.conf.BucketName, "us-east-1")
	if err == nil {
		if region == "" {
			region = "us-east-1"
		}
		return region, nil
	}

	// If commercial failed and error suggests access issues, try GovCloud
	if !isNotFound(err) {
		// Try GovCloud partition
		govSess, govErr := session.NewSession(&aws.Config{
			Region:           aws.String("us-gov-west-1"),
			Credentials:      credentials.NewStaticCredentials(a.conf.AccessKey, a.conf.SecretKey, ""),
			S3ForcePathStyle: aws.Bool(false),
		})
		if govErr != nil {
			return "", govErr
		}

		govRegion, govRegionErr := s3manager.GetBucketRegion(a.ctx, govSess, a.conf.BucketName, "us-gov-west-1")
		if govRegionErr == nil {
			if govRegion == "" {
				govRegion = "us-gov-west-1"
			}
			return govRegion, nil
		}
		err = fmt.Errorf("govcloud region: %v, commercial region: %v", govRegionErr, err)
	}

	return "", fmt.Errorf("bucket %q not found in commercial or GovCloud partitions: %w", a.conf.BucketName, err)
}

func isNotFound(err error) bool {
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case "NoSuchBucket", "NotFound":
			return true
		}
	}
	return false
}

func (a *S3Adapter) Close() error {
	a.closeOnce.Do(func() {
		atomic.StoreUint32(&a.isStop, 1)
		a.uspClient.BeginDrain()
		a.wg.Wait()
		drainErr := a.uspClient.Drain(time.Minute)
		// Observe while the SDK is still live. Close is never delivery evidence.
		deleteErr := a.confirmDelete()
		_, closeErr := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)

		a.closeErr = errors.Join(drainErr, deleteErr, closeErr)
	})
	return a.closeErr
}

func (a *S3Adapter) lookForFiles() (bool, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(a.conf.BucketName),
		Prefix: &a.conf.Prefix,
	}
	found := false
	for {
		if atomic.LoadUint32(&a.isStop) == 1 {
			return found, nil
		}
		resp, err := a.awsS3.ListObjectsV2(input)
		if err != nil {
			err = fmt.Errorf("s3.ListObjectsV2() @ %s: %w", a.region, err)
			a.conf.ClientOptions.OnError(err)
			if a.conf.IsOneTimeLoad {
				return found, err
			}
			// Continuous collection retries retained objects on the next scan.
			return found, nil
		}
		pageFound, err := a.processListing(resp)
		found = found || pageFound
		if err != nil {
			return found, err
		}
		if !aws.BoolValue(resp.IsTruncated) {
			return found, nil
		}
		next := aws.StringValue(resp.NextContinuationToken)
		if next == "" || next == aws.StringValue(input.ContinuationToken) {
			err := errors.New("s3.ListObjectsV2(): truncated listing has no advancing continuation token")
			a.conf.ClientOptions.OnError(err)
			if !a.conf.IsOneTimeLoad {
				return found, nil
			}
			return found, err
		}
		input.ContinuationToken = aws.String(next)
	}
}

func (a *S3Adapter) processListing(resp *s3.ListObjectsV2Output) (bool, error) {
	var err error

	// We pipeline the downloading of files from S3 to support
	// high throughputs. It is important we keep file ordering
	// but beyond that we can download in parallel.
	nextFileIndex := 0
	filesMutex := sync.Mutex{}
	genNewFile, close, err := utils.Pipeliner(func() (utils.Element, error) {
		if atomic.LoadUint32(&a.isStop) == 1 {
			return nil, nil
		}
		filesMutex.Lock()
		defer filesMutex.Unlock()
		if nextFileIndex >= len(resp.Contents) {
			return nil, nil
		}
		e := resp.Contents[nextFileIndex]
		nextFileIndex++
		return &s3Record{Key: *e.Key, Size: *e.Size}, nil
	}, a.conf.ParallelFetch, func(e utils.Element) utils.Element {
		item := e.(*s3Record)

		if item.Size > maxObjectSize {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("file %s too large (%d)", item.Key, item.Size))
			return &s3LocalFile{
				Obj:  item,
				Data: nil,
				Err:  fmt.Errorf("file too large"),
			}
		}

		startTime := time.Now().UTC()
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("downloading file %s (%d)", item.Key, item.Size))

		writerAt := aws.NewWriteAtBuffer([]byte{})
		// The downloader can issue parallel ranged GETs. Validate that every
		// response belongs to the same object before admitting the bundle.
		var identity s3ObjectIdentity
		var identityMu sync.Mutex
		observeIdentity := func(r *request.Request) {
			r.Handlers.Unmarshal.PushBack(func(r *request.Request) {
				output, ok := r.Data.(*s3.GetObjectOutput)
				if !ok || r.Error != nil {
					return
				}
				current := s3ObjectIdentity{ETag: aws.StringValue(output.ETag), VersionID: aws.StringValue(output.VersionId)}
				identityMu.Lock()
				defer identityMu.Unlock()
				if current.ETag == "" || (identity.ETag != "" && identity != current) {
					r.Error = errors.New("downloaded object identity missing or changed")
					return
				}
				identity = current
			})
		}

		if _, err := a.awsDownloader.Download(writerAt, &s3.GetObjectInput{
			Bucket: aws.String(a.conf.BucketName),
			Key:    aws.String(item.Key),
		}, s3manager.WithDownloaderRequestOptions(observeIdentity)); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("s3.Download() @ %s: %v", a.region, err))
			return &s3LocalFile{
				Obj:  item,
				Data: nil,
				Err:  err,
			}
		}

		// PrepareBundleData routes through the parquet decoder when
		// needed (including gzipped parquet) and otherwise returns the
		// bytes untouched. Anything that fails here surfaces as the
		// download error so the caller skips the object.
		rawData, isCompressed, err := utils.PrepareBundleData(item.Key, writerAt.Bytes())
		if err != nil {
			a.conf.ClientOptions.OnError(err)
			return &s3LocalFile{
				Obj:  item,
				Data: nil,
				Err:  err,
			}
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("file %s downloaded in %v (%d)", item.Key, time.Since(startTime), item.Size))

		return &s3LocalFile{
			Identity:     identity,
			Obj:          item,
			Data:         rawData,
			IsCompressed: isCompressed,
		}
	})

	if err != nil {
		return false, err
	}
	defer close()

	isDataFound := false

	for {
		if err = a.waitForDelivery(maxPendingObjectDeletes); err != nil || atomic.LoadUint32(&a.isStop) == 1 {
			break
		}
		var newFile interface{}
		newFile, err = genNewFile()
		if err != nil {
			break
		}
		if newFile == nil {
			break
		}
		if atomic.LoadUint32(&a.isStop) == 1 {
			break
		}
		localFile := newFile.(*s3LocalFile)

		if localFile.Err != nil {
			// We failed downloading this file
			// but we logged this fact earlier
			// so we can just skip it.
			continue
		}

		startTime := time.Now().UTC()

		if !a.processEvent(localFile.Data, localFile.IsCompressed) {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("file %s NOT processed in %v (%d)", localFile.Obj.Key, time.Since(startTime), localFile.Obj.Size))
			continue
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("file %s processed in %v (%d)", localFile.Obj.Key, time.Since(startTime), localFile.Obj.Size))

		if a.conf.IsOneTimeLoad {
			// In one time loads we don't delete the contents.
			continue
		}

		key, identity := localFile.Obj.Key, localFile.Identity
		a.pendingDeletes = append(a.pendingDeletes, pendingObjectDelete{marker: a.admitted, dependency: a.lastDependency, remove: func() error { return a.deleteDeliveredObject(key, identity) }})
		isDataFound = true
	}

	// Flush this listing before another pass can list pending objects again.
	if err == nil {
		err = a.waitForDelivery(1)
	}
	return isDataFound, err
}

func (a *S3Adapter) processEvent(data []byte, isCompressed bool) bool {
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

	// Every object is one bundle frame, including all decoded parquet rows.
	// Keep its admission dependency until the entire frame is acknowledged.
	if a.tracker == nil {
		var err error
		a.ackView = &objectACKView{client: a.uspClient}
		a.tracker, err = acktrack.NewACKTracker(a.ackView, 1, 16)
		if err != nil {
			a.conf.ClientOptions.OnError(err)
			return false
		}
	}
	dep, err := a.tracker.Ship(msg, 10*time.Second)
	if errors.Is(err, uspclient.ErrorBufferFull) {
		a.conf.ClientOptions.OnWarning("stream falling behind")
		dep, err = a.tracker.Ship(msg, time.Hour)
	}
	if err != nil {
		if !errors.Is(err, uspclient.ErrorClosed) {
			a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %w", err))
		}
		return false
	}
	a.admitted++
	a.lastDependency = dep

	return true
}

// AWS SDK v1 predates conditional DeleteObject inputs. Its request option adds
// If-Match before signing; never emulate this with a racy HEAD-then-DELETE.
// Always delete the key (creating a delete marker on versioned buckets), never
// an individual version. Identical-ETag replacements cannot be distinguished.
type s3ObjectIdentity struct {
	ETag      string
	VersionID string
}

func (a *S3Adapter) deleteDeliveredObject(key string, identity s3ObjectIdentity) error {
	input := &s3.DeleteObjectInput{Bucket: aws.String(a.conf.BucketName), Key: aws.String(key)}
	if identity.ETag == "" {
		return errors.New("cannot delete object without downloaded identity")
	}
	_, err := a.awsS3.DeleteObjectWithContext(a.ctx, input, request.WithSetRequestHeaders(map[string]string{"If-Match": identity.ETag}))
	return err
}
