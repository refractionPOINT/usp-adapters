package usp_file

import (
	"context"
	"errors"
	"log"
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
	isStop    chan bool
	wg        sync.WaitGroup
	chStopped chan struct{}
}

type BigQueryConfig struct {
	ProjectId           string `json:"project_id" yaml:"project_id"`
	DatasetName         string `json:"dataset_name" yaml:"dataset_name"`
	TableName           string `json:"table_name" yaml:"table_name"`
	ServiceAccountCreds string `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty"`
	SqlQuery            string `json:"sql_query" yaml:"sql_query"`
	IsOneTimeLoad       bool   `json:"is_one_time_load" yaml:"is_one_time_load"`
}

func (c *BigQueryConfig) Validate() error {
	if c.ProjectId == "" {
		return errors.New("missing project_id")
	}
	if c.DatasetName == "" {
		return errors.New("missing dataset_name")
	}
	if c.TableName == "" {
		return errors.New("missing table_name")
	}
	return nil
}

func NewBigQueryAdapter(conf BigQueryConfig) (*BigQueryAdapter, chan struct{}, error) {
	a := &BigQueryAdapter{
		conf:      conf,
		chStopped: make(chan struct{}),
		isStop:    make(chan bool),
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

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(a.chStopped)

		for {
			select {
			case <-a.isStop:
				return
			default:
				_, err := a.Lookup()
				if err != nil || a.conf.IsOneTimeLoad {
					a.isStop <- true
					return
				}
				// figure out what to do with results

				time.Sleep(5 * time.Second)
			}
		}
	}()

	return a, a.chStopped, nil
}

func (a *BigQueryAdapter) Lookup() ([]bigquery.Value, error) {
	ctx := context.Background()
	q := a.client.Query(a.conf.SqlQuery)
	it, err := q.Read(ctx)
	if err != nil {
		log.Printf("Error during Lookup(): %v", err)
		return nil, err
	}

	var results []bigquery.Value
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			log.Printf("Error during Lookup(): %v", err)
			return nil, err
		}

		results = append(results, row...)
	}

	return results, nil
}
