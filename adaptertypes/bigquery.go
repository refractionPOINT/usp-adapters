package adaptertypes

// BigQueryConfig defines the configuration for the Google BigQuery adapter
type BigQueryConfig struct {
	ClientOptions       ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ProjectId           string        `json:"project_id" yaml:"project_id" description:"GCP project ID for billing/quota" category:"source" example:"my-project-123"`
	BigQueryProject     string        `json:"bigquery_project" yaml:"bigquery_project" description:"GCP project containing the BigQuery dataset" category:"source" example:"analytics-project"`
	DatasetName         string        `json:"dataset_name" yaml:"dataset_name" description:"BigQuery dataset name" category:"source" example:"logs_dataset"`
	TableName           string        `json:"table_name" yaml:"table_name" description:"BigQuery table name" category:"source" example:"security_events"`
	ServiceAccountCreds string        `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty" description:"GCP service account JSON credentials" category:"auth" sensitive:"true"`
	SqlQuery            string        `json:"sql_query" yaml:"sql_query" description:"SQL query to execute" category:"source" llmguidance:"Use WHERE clause to filter data. Include timestamp column for incremental queries"`
	QueryInterval       string        `json:"query_interval" yaml:"query_interval" description:"Interval between query executions" category:"behavior" example:"1h" default:"1h" llmguidance:"Format: duration string like '1h', '30m', '24h'"`
	IsOneTimeLoad       bool          `json:"is_one_time_load" yaml:"is_one_time_load" description:"If true, run query once and exit. If false, run continuously at intervals" category:"behavior" default:"false"`
}
