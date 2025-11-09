package usp_s3

import (
	"context"
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
	"github.com/refractionPOINT/usp-adapters/adaptertypes"
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
	conf      adaptertypes.S3Config
	uspClient *uspclient.Client

	ctx context.Context

	awsConfig     *aws.Config
	awsSession    *session.Session
	awsS3         *s3.S3
	awsDownloader *s3manager.Downloader

	isStop uint32
	wg     sync.WaitGroup

	region string
}

type s3LocalFile struct {
	Obj          *s3Record
	Data         []byte
	IsCompressed bool
	Err          error
}

type s3Record struct {
	Key  string
	Size int64
}

func NewS3Adapter(conf adaptertypes.S3Config) (*S3Adapter, chan struct{}, error) {
	if conf.ParallelFetch <= 0 {
		conf.ParallelFetch = 1
	}
	a := &S3Adapter{
		conf: conf,
		ctx:  context.Background(),
	}

	var err error
	var region string

	if conf.Region != "" {
		region = conf.Region
	} else {
		if region, err = a.getRegion(); err != nil {
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

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
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
	a.conf.ClientOptions.DebugLog("closing")
	atomic.StoreUint32(&a.isStop, 1)
	a.wg.Wait()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *S3Adapter) lookForFiles() (bool, error) {
	resp, err := a.awsS3.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(a.conf.BucketName),
		Prefix: &a.conf.Prefix,
	})
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("s3.ListObjectsV2() @ %s: %v", a.region, err))
		// Ignore the error upstream so that we just keep retrying.
		return false, nil
	}

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

		if _, err := a.awsDownloader.Download(writerAt, &s3.GetObjectInput{
			Bucket: aws.String(a.conf.BucketName),
			Key:    aws.String(item.Key),
		}); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("s3.Download() @ %s: %v", a.region, err))
			return &s3LocalFile{
				Obj:  item,
				Data: nil,
				Err:  err,
			}
		}

		isCompressed := false

		if strings.HasSuffix(item.Key, ".gz") {
			isCompressed = true
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("file %s downloaded in %v (%d)", item.Key, time.Since(startTime), item.Size))

		return &s3LocalFile{
			Obj:          item,
			Data:         writerAt.Bytes(),
			IsCompressed: isCompressed,
		}
	})

	if err != nil {
		return false, err
	}
	defer close()

	isDataFound := false

	// We will delete the files as
	// we go asynchronously so that
	// we can get higher throughput.
	errMutex := sync.Mutex{}
	var delErr error
	delWg := sync.WaitGroup{}

	for {
		var newFile interface{}
		newFile, err = genNewFile()
		if err != nil {
			break
		}
		if newFile == nil {
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

		delWg.Add(1)
		go func() {
			defer delWg.Done()
			if _, err = a.awsS3.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(a.conf.BucketName),
				Key:    aws.String(localFile.Obj.Key),
			}); err != nil {
				a.conf.ClientOptions.OnError(fmt.Errorf("s3.DeleteObject(): %v", err))
				// Since we rely on object deletion to prevent re-ingesting
				// the same files over and over again, we need to abort
				// if we cannot delete a file.
				a.conf.ClientOptions.OnWarning("aborting because files cannot be deleted")
				errMutex.Lock()
				delErr = err
				errMutex.Unlock()
			}
		}()

		// Take the opportunity to check if a delete failed.
		errMutex.Lock()
		if delErr != nil {
			err = delErr
		}
		errMutex.Unlock()

		if err != nil {
			break
		}

		isDataFound = true
	}

	// Wait for all file deletes and confirm they did not error.
	delWg.Wait()
	errMutex.Lock()
	if delErr != nil {
		err = delErr
	}
	errMutex.Unlock()

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
