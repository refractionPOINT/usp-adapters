package usp_bigquery

import (
	"context"
	"errors"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type BigQueryAdapter struct {
	conf      BigQueryConfig
	client    *bigquery.Client
	dataset   *bigquery.Dataset
	table     *bigquery.Table
	isStop    uint32
	wg        sync.WaitGroup
	chStopped chan struct{}
	uspClient *uspclient.Client
	ctx       context.Context
}

type BigQueryConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ProjectId           string                  `json:"project_id" yaml:"project_id"`
	DatasetName         string                  `json:"dataset_name" yaml:"dataset_name"`
	TableName           string                  `json:"table_name" yaml:"table_name"`
	ServiceAccountCreds string                  `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty"`
	SqlQuery            string                  `json:"sql_query" yaml:"sql_query"`
	IsOneTimeLoad       bool                    `json:"is_one_time_load" yaml:"is_one_time_load"`
}

func (bq *BigQueryConfig) Validate() error {
	if bq.ProjectId == "" {
		return errors.New("missing project_id")
	}
	if bq.DatasetName == "" {
		return errors.New("missing dataset_name")
	}
	if bq.TableName == "" {
		return errors.New("missing table_name")
	}
	return nil
}

func NewBigQueryAdapter(conf BigQueryConfig) (*BigQueryAdapter, chan struct{}, error) {
	a := &BigQueryAdapter{
		conf:      conf,
		chStopped: make(chan struct{}),
		ctx:       context.Background(),
	}

	var err error
	if a.conf.ServiceAccountCreds == "" {
		if a.client, err = bigquery.NewClient(context.Background(), a.conf.ProjectId); err != nil {
			return nil, nil, err
		}
	} else if a.conf.ServiceAccountCreds == "-" {
		if a.client, err = bigquery.NewClient(context.Background(), a.conf.ProjectId, option.WithoutAuthentication()); err != nil {
			return nil, nil, err
		}
	} else if !strings.HasPrefix(a.conf.ServiceAccountCreds, "{") {
		if a.client, err = bigquery.NewClient(context.Background(), a.conf.ProjectId, option.WithCredentialsFile(a.conf.ServiceAccountCreds)); err != nil {
			return nil, nil, err
		}
	} else {
		if a.client, err = bigquery.NewClient(context.Background(), a.conf.ProjectId, option.WithCredentialsJSON([]byte(a.conf.ServiceAccountCreds))); err != nil {
			return nil, nil, err
		}
	}

	a.dataset = a.client.Dataset(a.conf.DatasetName)
	a.table = a.dataset.Table(a.conf.TableName)

	a.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(a.chStopped)

		for {
			if atomic.LoadUint32(&a.isStop) == 1 {
				return
			}
			err = a.lookupAndSend()
			if err != nil || a.conf.IsOneTimeLoad {
				atomic.StoreUint32(&a.isStop, 1)
				return
			}

			time.Sleep(5 * time.Second)
		}
	}()

	return a, a.chStopped, nil
}

func (a *BigQueryAdapter) lookupAndSend() error {
	q := a.client.Query(a.conf.SqlQuery)
	it, err := q.Read(a.ctx)
	if err != nil {
		return err
	}

	for {
		var row []bigquery.Value
		err = it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}

		// Convert []bigquery.Value to map[string]interface{} for USP payload
		rowMap := make(map[string]interface{})
		for i, col := range row {
			rowMap["col"+strconv.Itoa(i)] = col
		}

		msg := &protocol.DataMessage{
			JsonPayload: rowMap,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}

		if err = a.uspClient.Ship(msg, 10*time.Second); err != nil {
			if errors.Is(err, uspclient.ErrorBufferFull) {
				a.conf.ClientOptions.OnWarning("stream falling behind")
				err = a.uspClient.Ship(msg, 1*time.Hour)
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *BigQueryAdapter) Close() error {
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
