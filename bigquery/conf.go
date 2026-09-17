package usp_bigquery

import (
	"errors"

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
}

// Validate validates the BigQuery adapter configuration.
//
// Parameters:
//
//	None
//
// Returns:
//
//	error - Returns nil if validation passes, or an error describing the validation failure.
func (c *BigQueryConfig) Validate() error {
	if c.ProjectId == "" {
		return errors.New("missing project_id")
	}
	// BigQueryProject will usually be the same as ProjectId but could be different
	// if using outside project dataset such as a public data set.
	if c.BigQueryProject == "" {
		return errors.New("missing bigquery_project")
	}
	if c.DatasetName == "" {
		return errors.New("missing dataset_name")
	}
	if c.TableName == "" {
		return errors.New("missing table_name")
	}
	if c.SqlQuery == "" {
		return errors.New("missing sql_query")
	}
	return nil
}
