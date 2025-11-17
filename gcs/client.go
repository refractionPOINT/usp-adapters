package usp_gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const maxObjectSize = 1024 * 1024 * 100 // 100 MB

type GCSAdapter struct {
	conf      GCSConfig
	uspClient *uspclient.Client

	ctx context.Context

	client *storage.Client
	bucket *storage.BucketHandle

	isStop uint32
	wg     sync.WaitGroup
}

type GCSConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	BucketName          string                  `json:"bucket_name" yaml:"bucket_name"`
	ServiceAccountCreds string                  `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty"`
	IsOneTimeLoad       bool                    `json:"single_load" yaml:"single_load"`
	Prefix              string                  `json:"prefix" yaml:"prefix"`
	ParallelFetch       int                     `json:"parallel_fetch" yaml:"parallel_fetch"`
}

func (c *GCSConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.BucketName == "" {
		return errors.New("missing bucket_name")
	}
	return nil
}

type gcsLocalFile struct {
	Obj          *storage.ObjectHandle
	Attrs        *storage.ObjectAttrs
	Data         []byte
	IsCompressed bool
	Err          error
}

func NewGCSAdapter(ctx context.Context, conf GCSConfig) (*GCSAdapter, chan struct{}, error) {
	if conf.ParallelFetch <= 0 {
		conf.ParallelFetch = 1
	}
	a := &GCSAdapter{
		conf: conf,
		ctx:  context.Background(),
	}

	var err error

	if a.conf.ServiceAccountCreds == "" {
		if a.client, err = storage.NewClient(a.ctx); err != nil {
			return nil, nil, err
		}
	} else if a.conf.ServiceAccountCreds == "-" {
		if a.client, err = storage.NewClient(a.ctx, option.WithoutAuthentication()); err != nil {
			return nil, nil, err
		}
	} else if !strings.HasPrefix(a.conf.ServiceAccountCreds, "{") {
		if a.client, err = storage.NewClient(a.ctx, option.WithCredentialsFile(conf.ServiceAccountCreds)); err != nil {
			return nil, nil, err
		}
	} else {
		if a.client, err = storage.NewClient(a.ctx, option.WithCredentialsJSON([]byte(conf.ServiceAccountCreds))); err != nil {
			return nil, nil, err
		}
	}

	a.bucket = a.client.Bucket(conf.BucketName)

	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
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
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("gcs stoppped with error: %v", err))
		}
	}()

	return a, chStopped, nil
}

func (a *GCSAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")
	atomic.StoreUint32(&a.isStop, 1)
	a.wg.Wait()
	a.client.Close()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

func (a *GCSAdapter) lookForFiles() (bool, error) {
	query := &storage.Query{Prefix: a.conf.Prefix}

	it := a.bucket.Objects(a.ctx, query)

	// We pipeline the downloading of files from GCS to support
	// high throughputs. It is important we keep file ordering
	// but beyond that we can download in parallel.
	filesMutex := sync.Mutex{}
	genNewFile, close, err := utils.Pipeliner(func() (utils.Element, error) {
		if atomic.LoadUint32(&a.isStop) == 1 {
			return nil, nil
		}
		filesMutex.Lock()
		defer filesMutex.Unlock()

		attrs, err := it.Next()
		if err == iterator.Done {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		return attrs, nil
	}, a.conf.ParallelFetch, func(e utils.Element) utils.Element {
		attrs := e.(*storage.ObjectAttrs)
		obj := a.bucket.Object(attrs.Name).ReadCompressed(true)

		if attrs.Size > maxObjectSize {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("file %s too large (%d)", attrs.Name, attrs.Size))
			return &gcsLocalFile{
				Obj:  obj,
				Data: nil,
				Err:  fmt.Errorf("file too large"),
			}
		}

		startTime := time.Now().UTC()
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("downloading file %s (%d)", attrs.Name, attrs.Size))

		r, err := obj.NewReader(a.ctx)
		if err != nil {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("gcs.NewReader(): %v", err))
			return &gcsLocalFile{
				Obj:  obj,
				Data: nil,
				Err:  err,
			}
		}

		objData, err := io.ReadAll(r)
		r.Close()

		if err != nil {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("gcs.Download(): %v", err))
			return &gcsLocalFile{
				Obj:  obj,
				Data: nil,
				Err:  err,
			}
		}

		isCompressed := false

		if strings.HasSuffix(attrs.Name, ".gz") {
			isCompressed = true
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("file %s downloaded in %v (%d)", attrs.Name, time.Since(startTime), attrs.Size))

		return &gcsLocalFile{
			Obj:          obj,
			Attrs:        attrs,
			Data:         objData,
			IsCompressed: isCompressed,
		}
	})

	if err != nil {
		return false, err
	}
	defer close()

	isDataFound := false
	for {
		var newFile interface{}
		newFile, err = genNewFile()
		if err != nil {
			break
		}
		if newFile == nil {
			break
		}
		localFile := newFile.(*gcsLocalFile)

		if localFile.Err != nil {
			// We failed downloading this file
			// but we logged this fact earlier
			// so we can just skip it.
			continue
		}

		startTime := time.Now().UTC()

		if !a.processEvent(localFile.Data, localFile.IsCompressed) {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("file %s NOT processed in %v (%d)", localFile.Attrs.Name, time.Since(startTime), localFile.Attrs.Size))
			continue
		}

		a.conf.ClientOptions.DebugLog(fmt.Sprintf("file %s processed in %v (%d)", localFile.Attrs.Name, time.Since(startTime), localFile.Attrs.Size))

		if a.conf.IsOneTimeLoad {
			// In one time loads we don't delete the contents.
			continue
		}

		if err := localFile.Obj.Delete(a.ctx); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("gcs.DeleteObject(): %v", err))
			// Since we rely on object deletion to prevent re-ingesting
			// the same files over and over again, we need to abort
			// if we cannot delete a file.
			a.conf.ClientOptions.OnWarning("aborting because files cannot be deleted")
			break
		}

		isDataFound = true
	}

	return isDataFound, err
}

func (a *GCSAdapter) processEvent(data []byte, isCompressed bool) bool {
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
