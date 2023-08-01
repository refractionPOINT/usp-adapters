package usp_bigquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"strings"
	"sync"
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
	uspClient *uspclient.Client
	ctx       context.Context
	cancel    context.CancelFunc
}

type BigQueryConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ProjectId           string                  `json:"project_id" yaml:"project_id"`
	DatasetName         string                  `json:"dataset_name" yaml:"dataset_name"`
	TableName           string                  `json:"table_name" yaml:"table_name"`
	ServiceAccountCreds string                  `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty"`
	SqlQuery            string                  `json:"sql_query" yaml:"sql_query"`
	QueryInterval       string                  `json:"query_interval" yaml:"query_interval"`
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
	if bq.SqlQuery == "" {
		return errors.New("missing sql query")
	}
	return nil
}

func NewBigQueryAdapter(conf BigQueryConfig) (*BigQueryAdapter, chan struct{}, error) {
	// Create bq cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	bq := &BigQueryAdapter{
		conf:   conf,
		ctx:    ctx,
		cancel: cancel,
	}

	var err error
	if bq.conf.ServiceAccountCreds == "" {
		if bq.client, err = bigquery.NewClient(context.Background(), bq.conf.ProjectId); err != nil {
			return nil, nil, err
		}
	} else if bq.conf.ServiceAccountCreds == "-" {
		if bq.client, err = bigquery.NewClient(context.Background(), bq.conf.ProjectId, option.WithoutAuthentication()); err != nil {
			return nil, nil, err
		}
	} else if !strings.HasPrefix(bq.conf.ServiceAccountCreds, "{") {
		if bq.client, err = bigquery.NewClient(context.Background(), bq.conf.ProjectId, option.WithCredentialsFile(bq.conf.ServiceAccountCreds)); err != nil {
			return nil, nil, err
		}
	} else {
		if bq.client, err = bigquery.NewClient(context.Background(), bq.conf.ProjectId, option.WithCredentialsJSON([]byte(bq.conf.ServiceAccountCreds))); err != nil {
			return nil, nil, err
		}
	}

	bq.dataset = bq.client.Dataset(bq.conf.DatasetName)
	bq.table = bq.dataset.Table(bq.conf.TableName)

	bq.uspClient, err = uspclient.NewClient(conf.ClientOptions)
	if err != nil {
		return nil, nil, err
	}

	// Parse the query interval
	queryInterval, err := time.ParseDuration(conf.QueryInterval)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid query interval: %w", err)
	}

	chStopped := make(chan struct{})
	bq.wg.Add(1)
	go func() {
		defer bq.wg.Done()
		defer close(chStopped)

		for {
			err = bq.lookupAndSend(bq.ctx)
			if err != nil || bq.conf.IsOneTimeLoad {
				return
			}

			select {
			case <-time.After(queryInterval):
			case <-bq.ctx.Done():
				return
			}
		}
	}()

	return bq, chStopped, nil
}

func (bq *BigQueryAdapter) lookupAndSend(ctx context.Context) error {
	q := bq.client.Query(bq.conf.SqlQuery)

	// Pass the context to the Read method
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	tableRef := bq.client.DatasetInProject(bq.conf.ProjectId, bq.conf.DatasetName).Table(bq.conf.TableName)

	// Pass the context to the Metadata method
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}
	schema := meta.Schema

	for {
		var row []bigquery.Value

		// Check the context's Done channel before calling Next
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err = it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}

		// Convert response to json format
		rowMap := make(map[string]interface{})
		for i, col := range row {
			rowMap[schema[i].Name] = col // use column name to form json object
		}

		msg := &protocol.DataMessage{
			JsonPayload: rowMap,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
		}

		// Check the context's Done channel before calling Ship
		select {
		case <-ctx.Done():
			return ctx.Err()
		default: // continue
		}

		if err = bq.uspClient.Ship(msg, 10*time.Second); err != nil {
			if errors.Is(err, uspclient.ErrorBufferFull) {
				bq.conf.ClientOptions.OnWarning("stream falling behind")
				err = bq.uspClient.Ship(msg, 1*time.Hour)
			}
			if err != nil {
				bq.conf.ClientOptions.OnError(fmt.Errorf("ship(): %v", err))
				return err
			}
		}
	}

	return nil
}

func (bq *BigQueryAdapter) Close() error {
	bq.conf.ClientOptions.DebugLog("closing")
	bq.cancel() // cancel the context
	bq.wg.Wait()
	bq.client.Close()
	err1 := bq.uspClient.Drain(1 * time.Minute)
	_, err2 := bq.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}
