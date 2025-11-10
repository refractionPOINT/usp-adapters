package usp_bigquery

import (
	"github.com/refractionPOINT/go-uspclient"
)

type BigQueryConfig struct {
	ClientOptions       uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ProjectId           string                  `json:"project_id" yaml:"project_id"`
	BigQueryProject     string                  `json:"bigquery_project" yaml:"bigquery_project"`
	DatasetName         string                  `json:"dataset_name" yaml:"dataset_name"`
	TableName           string                  `json:"table_name" yaml:"table_name"`
	ServiceAccountCreds string                  `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty"`
	SqlQuery            string                  `json:"sql_query" yaml:"sql_query"`
	QueryInterval       string                  `json:"query_interval" yaml:"query_interval"`
	IsOneTimeLoad       bool                    `json:"is_one_time_load" yaml:"is_one_time_load"`
	Filters             []string                `json:"filters,omitempty" yaml:"filters,omitempty"`
}
