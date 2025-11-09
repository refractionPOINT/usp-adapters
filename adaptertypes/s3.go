package adaptertypes

// S3Config defines the configuration for the Amazon S3 adapter
type S3Config struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`

	BucketName string `json:"bucket_name" yaml:"bucket_name" description:"Name of the S3 bucket containing log files" category:"source" example:"my-cloudtrail-logs" llmguidance:"Provide just the bucket name, not the full ARN or path. Example: 'my-logs-bucket'"`

	AccessKey string `json:"access_key" yaml:"access_key" description:"AWS access key ID for S3 authentication" category:"auth" sensitive:"true" llmguidance:"AWS access key typically starts with 'AKIA'. Requires IAM permissions: s3:ListBucket, s3:GetObject, and s3:DeleteObject (unless single_load is true)"`

	SecretKey string `json:"secret_key,omitempty" yaml:"secret_key,omitempty" description:"AWS secret access key corresponding to the access key" category:"auth" sensitive:"true" llmguidance:"Keep this value secret. Never log or expose in plain text"`

	IsOneTimeLoad bool `json:"single_load" yaml:"single_load" description:"If true, loads all files once without deletion. If false, continuously monitors bucket and deletes files after successful ingestion" category:"behavior" default:"false" llmguidance:"Set to true for one-time historical data imports. Set to false for continuous live monitoring. When false, files are deleted after processing to prevent re-ingestion"`

	Prefix string `json:"prefix" yaml:"prefix" description:"S3 key prefix to filter which objects to process" category:"source" example:"AWSLogs/123456789012/CloudTrail/" llmguidance:"Optional. Use to limit processing to specific paths within the bucket. Leave empty to process all objects in the bucket"`

	ParallelFetch int `json:"parallel_fetch" yaml:"parallel_fetch" description:"Number of parallel file downloads from S3" category:"performance" default:"1" llmguidance:"Increase for better throughput on large buckets. Recommended: 5-10 for most use cases. Higher values use more memory"`

	Region string `json:"region" yaml:"region" description:"AWS region where the bucket is located" category:"source" example:"us-east-1" llmguidance:"Optional. If not specified, the region will be auto-detected. Specify to avoid the auto-detection API call"`
}
