package usp_bigquery

import (
	"context"
	"errors"
	"fmt"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
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
	TimestampColumnName string                  `json:"timestamp_column_name" yaml:"timestamp_column_name"`
	LastQueryTime       time.Time               `json:"last_query_time" yaml:"last_query_time"`
	QueryInterval       time.Duration           `json:"query_interval" yaml:"query_interval"`
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
	a := &BigQueryAdapter{
		conf: conf,
		ctx:  context.Background(),
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

	chStopped := make(chan struct{})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(chStopped)

		for {
			if atomic.LoadUint32(&a.isStop) == 1 {
				return
			}
			err = a.lookupAndSend()
			if err != nil || a.conf.IsOneTimeLoad {
				atomic.StoreUint32(&a.isStop, 1)
				return
			}

			time.Sleep(a.conf.QueryInterval)
		}
	}()

	return a, chStopped, nil
}

func (bq *BigQueryAdapter) lookupAndSend() error {
	query := bq.conf.SqlQuery
	if bq.conf.TimestampColumnName != "" && !bq.conf.LastQueryTime.IsZero() {
		// Create the WHERE clause
		whereClause := fmt.Sprintf(" WHERE %s > TIMESTAMP \"%s\"",
			bq.conf.TimestampColumnName,
			bq.conf.LastQueryTime.Format(time.RFC3339))

		// Check if the query contains an ORDER BY clause
		if strings.Contains(strings.ToUpper(query), "ORDER BY") {
			// Split the query into two parts: the SELECT part and the ORDER BY part
			queryParts := strings.SplitN(query, " ORDER BY ", 2)

			// Insert the WHERE clause between the two parts
			query = fmt.Sprintf("%s%s ORDER BY %s", queryParts[0], whereClause, queryParts[1])
		} else {
			// If there's no ORDER BY clause, simply append the WHERE clause at the end
			query += whereClause
		}
	}

	q := bq.client.Query(query)
	it, err := q.Read(bq.ctx)
	if err != nil {
		return err
	}

	tableRef := bq.client.DatasetInProject(bq.conf.ProjectId, bq.conf.DatasetName).Table(bq.conf.TableName)
	meta, err := tableRef.Metadata(bq.ctx)

	if err != nil {
		return err
	}
	schema := meta.Schema

	for {
		var row []bigquery.Value
		err = it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}

		// convert response to json format
		rowMap := make(map[string]interface{})
		for i, col := range row {
			rowMap[schema[i].Name] = col // use column name to form json object
		}

		msg := &protocol.DataMessage{
			JsonPayload: rowMap,
			TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
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

	// Update the time of the last query
	bq.conf.LastQueryTime = time.Now()

	return nil
}

func (bq *BigQueryAdapter) Close() error {
	bq.conf.ClientOptions.DebugLog("closing")
	atomic.StoreUint32(&bq.isStop, 1)
	bq.wg.Wait()
	bq.client.Close()
	err1 := bq.uspClient.Drain(1 * time.Minute)
	_, err2 := bq.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}
