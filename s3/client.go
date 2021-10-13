package usp_s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
)

type S3Adapter struct {
	conf      S3Config
	dbgLog    func(string)
	uspClient *uspclient.Client

	ctx context.Context

	awsConfig     *aws.Config
	awsSession    *session.Session
	awsS3         *s3.S3
	awsDownloader *s3manager.Downloader

	isStop uint32
	wg     sync.WaitGroup
}

type S3Config struct {
	ClientOptions uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	BucketName    string                  `json:"bucket_name" yaml:"bucket_name"`
	AccessKey     string                  `json:"access_key" yaml:"access_key"`
	SecretKey     string                  `json:"secret_key,omitempty" yaml:"secret_key,omitempty"`
	IsOneTimeLoad bool                    `json:"single_load" yaml:"single_load"`
}

func NewS3Adapter(conf S3Config) (*S3Adapter, chan struct{}, error) {
	a := &S3Adapter{
		conf: conf,
		dbgLog: func(s string) {
			if conf.ClientOptions.DebugLog == nil {
				return
			}
			conf.ClientOptions.DebugLog(s)
		},
		ctx: context.Background(),
	}

	var err error
	var region string

	if region, err = a.getRegion(); err != nil {
		return nil, nil, err
	}

	a.awsConfig = &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(conf.AccessKey, conf.SecretKey, ""),
	}

	if a.awsSession, err = session.NewSession(a.awsConfig); err != nil {
		return nil, nil, err
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
	}()

	return a, chStopped, nil
}

func (a *S3Adapter) getRegion() (string, error) {
	return s3manager.GetBucketRegion(a.ctx, session.Must(session.NewSession(&aws.Config{})), a.conf.BucketName, "us-east-1")
}

func (a *S3Adapter) Close() error {
	a.dbgLog("closing")
	atomic.StoreUint32(&a.isStop, 1)
	a.wg.Wait()
	return nil
}

func (a *S3Adapter) lookForFiles() (bool, error) {
	resp, err := a.awsS3.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(a.conf.BucketName)})
	if err != nil {
		a.dbgLog(fmt.Sprintf("s3.ListObjectsV2(): %v", err))
		// Ignore the error upstream so that we just keep retrying.
		return false, nil
	}

	isDataFound := false
	for _, item := range resp.Contents {
		a.dbgLog(fmt.Sprintf("processing file %s (%d)", *item.Key, *item.Size))

		writerAt := aws.NewWriteAtBuffer([]byte{})

		if _, err := a.awsDownloader.Download(writerAt, &s3.GetObjectInput{
			Bucket: aws.String(a.conf.BucketName),
			Key:    aws.String(*item.Key),
		}); err != nil {
			a.dbgLog(fmt.Sprintf("s3.Download(): %v", err))
			continue
		}

		var reader io.Reader
		reader = bytes.NewBuffer(writerAt.Bytes())

		if strings.HasSuffix(*item.Key, ".gz") {
			if reader, err = gzip.NewReader(reader); err != nil {
				a.dbgLog(fmt.Sprintf("gzip.NewReader(): %v", err))
				continue
			}
		}

		event, err := ioutil.ReadAll(reader)
		if err != nil {
			a.dbgLog(fmt.Sprintf("ioutil.ReadAll(): %v", err))
			continue
		}
		isDataFound = true
		if !a.processEvent(event) {
			continue
		}
		if _, err := a.awsS3.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(a.conf.BucketName),
			Key:    aws.String(*item.Key),
		}); err != nil {
			a.dbgLog(fmt.Sprintf("s3.DeleteObject(): %v", err))
			// Since we rely on object deletion to prevent re-ingesting
			// the same files over and over again, we need to abort
			// if we cannot delete a file.
			a.dbgLog("aborting because files cannot be deleted")
			return true, err
		}
	}

	return isDataFound, nil
}

func (a *S3Adapter) processEvent(data []byte) bool {
	for _, line := range strings.Split(string(data), "\n") {
		msg := &protocol.DataMessage{
			TextPayload: line,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}
		if err := a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if err == uspclient.ErrorBufferFull {
				a.dbgLog("stream falling behind")
				err = a.uspClient.Ship(msg, 0)
			}
			if err != nil {
				a.dbgLog(fmt.Sprintf("Ship(): %v", err))
				return false
			}
		}
	}
	return true
}
