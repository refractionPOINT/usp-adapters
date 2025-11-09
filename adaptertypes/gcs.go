package adaptertypes

// GCSConfig defines the configuration for the Google Cloud Storage adapter
type GCSConfig struct {
	ClientOptions       ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	BucketName          string        `json:"bucket_name" yaml:"bucket_name" description:"Name of the GCS bucket containing log files" category:"source" example:"my-logs-bucket" llmguidance:"Provide just the bucket name, not gs:// URI. Example: 'my-company-logs'"`
	ServiceAccountCreds string        `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty" description:"GCP service account JSON credentials" category:"auth" sensitive:"true" llmguidance:"Provide the full JSON key file contents as a string. Service account needs roles/storage.objectViewer and roles/storage.objectAdmin (for deletion) permissions"`
	IsOneTimeLoad       bool          `json:"single_load" yaml:"single_load" description:"If true, loads all files once without deletion. If false, continuously monitors bucket and deletes files after successful ingestion" category:"behavior" default:"false" llmguidance:"Set to true for one-time historical data imports. Set to false for continuous live monitoring. When false, files are deleted after processing"`
	Prefix              string        `json:"prefix" yaml:"prefix" description:"Object prefix to filter which objects to process" category:"source" example:"logs/application/" llmguidance:"Optional. Use to limit processing to specific paths within the bucket. Leave empty to process all objects"`
	ParallelFetch       int           `json:"parallel_fetch" yaml:"parallel_fetch" description:"Number of parallel file downloads from GCS" category:"performance" default:"1" llmguidance:"Increase for better throughput. Recommended: 5-10 for most use cases. Higher values use more memory"`
}
