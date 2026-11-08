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
	"github.com/refractionPOINT/usp-adapters/credguard"
	"github.com/refractionPOINT/usp-adapters/internal/acktrack"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const maxObjectSize = 1024 * 1024 * 100 // 100 MB

type GCSAdapter struct {
	conf           GCSConfig
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
	c.ServiceAccountCreds = strings.TrimSpace(c.ServiceAccountCreds)
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
	} else {
		credOpt, cerr := credguard.CredentialOption(conf.ServiceAccountCreds)
		if cerr != nil {
			return nil, nil, cerr
		}
		if a.client, err = storage.NewClient(a.ctx, credOpt); err != nil {
			return nil, nil, err
		}
	}

	a.bucket = a.client.Bucket(conf.BucketName)

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
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("gcs stoppped with error: %v", err))
		}
	}()

	return a, chStopped, nil
}

func (a *GCSAdapter) Close() error {
	a.closeOnce.Do(func() {
		atomic.StoreUint32(&a.isStop, 1)
		a.uspClient.BeginDrain()
		a.wg.Wait()
		drainErr := a.uspClient.Drain(time.Minute)
		// Observe while the SDK is still live. Close is never delivery evidence.
		deleteErr := a.confirmDelete()
		_, closeErr := utils.CloseUSP(a.uspClient, a.conf.ClientOptions)
		if a.client != nil {
			_ = a.client.Close()
		}
		a.closeErr = errors.Join(drainErr, deleteErr, closeErr)
	})
	return a.closeErr
}

func (a *GCSAdapter) lookForFiles() (bool, error) {

	it := a.bucket.Objects(a.ctx, &storage.Query{Prefix: a.conf.Prefix})
	found := false
	for atomic.LoadUint32(&a.isStop) == 0 {
		attrs, err := it.Next()
		if err == iterator.Done {
			return found, nil
		}
		if err != nil {
			return found, err
		}
		page := []*storage.ObjectAttrs{attrs}
		// Consume only the already-buffered SDK page. Fetching the next page
		// waits until this page's admitted objects have been confirmed/deleted.
		for it.PageInfo().Remaining() > 0 {
			attrs, err = it.Next()
			if err != nil {
				return found, err
			}
			page = append(page, attrs)
		}
		pageFound, err := a.processListing(page)
		found = found || pageFound
		if err != nil {
			return found, err
		}
	}
	return found, nil
}

func (a *GCSAdapter) processListing(page []*storage.ObjectAttrs) (bool, error) {
	next := 0

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

		if next >= len(page) {
			return nil, nil
		}
		attrs := page[next]
		next++

		return attrs, nil
	}, a.conf.ParallelFetch, func(e utils.Element) utils.Element {
		attrs := e.(*storage.ObjectAttrs)
		obj := a.bucket.Object(attrs.Name).Generation(attrs.Generation).ReadCompressed(true)

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
		err = errors.Join(err, r.Close())

		if err != nil {
			a.conf.ClientOptions.OnWarning(fmt.Sprintf("gcs.Download(): %v", err))
			return &gcsLocalFile{
				Obj:  obj,
				Data: nil,
				Err:  err,
			}
		}

		// PrepareBundleData routes through the parquet decoder when
		// needed (including gzipped parquet) and otherwise returns the
		// bytes untouched.
		objData, isCompressed, err := utils.PrepareBundleData(attrs.Name, objData)
		if err != nil {
			a.conf.ClientOptions.OnError(err)
			return &gcsLocalFile{
				Obj:  obj,
				Data: nil,
				Err:  err,
			}
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

		key, generation := localFile.Attrs.Name, localFile.Attrs.Generation
		a.pendingDeletes = append(a.pendingDeletes, pendingObjectDelete{marker: a.admitted, dependency: a.lastDependency, remove: func() error { return a.bucket.Object(key).Generation(generation).Delete(a.ctx) }})
		isDataFound = true
	}

	// Flush this listing before another pass can list pending objects again.
	if err == nil {
		err = a.waitForDelivery(1)
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
