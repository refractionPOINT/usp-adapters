package adaptertypes

// SQSFilesConfig defines the configuration for the AWS SQS+S3 files adapter
type SQSFilesConfig struct {
	ClientOptions     ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	AccessKey         string        `json:"access_key" yaml:"access_key" description:"AWS access key ID" category:"auth" sensitive:"true"`
	SecretKey         string        `json:"secret_key,omitempty" yaml:"secret_key,omitempty" description:"AWS secret access key" category:"auth" sensitive:"true"`
	QueueURL          string        `json:"queue_url" yaml:"queue_url" description:"SQS queue URL receiving S3 notifications" category:"source" example:"https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"`
	Region            string        `json:"region" yaml:"region" description:"AWS region" category:"source" example:"us-east-1"`
	ParallelFetch     int           `json:"parallel_fetch" yaml:"parallel_fetch" description:"Number of parallel file downloads" category:"performance" default:"1"`
	BucketPath        string        `json:"bucket_path,omitempty" yaml:"bucket_path,omitempty" description:"S3 bucket path from SQS message" category:"parsing" llmguidance:"JSON path to bucket name in SQS message"`
	FilePath          string        `json:"file_path,omitempty" yaml:"file_path,omitempty" description:"S3 file key path from SQS message" category:"parsing" llmguidance:"JSON path to object key in SQS message"`
	IsDecodeObjectKey bool          `json:"is_decode_object_key,omitempty" yaml:"is_decode_object_key,omitempty" description:"URL decode the object key from SQS message" category:"parsing" default:"false"`
	Bucket            string        `json:"bucket,omitempty" yaml:"bucket,omitempty" description:"Override bucket name from SQS message" category:"source" llmguidance:"Optional. Use if all files are in same bucket"`
}
