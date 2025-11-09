package adaptertypes

import (
	"errors"
	"fmt"
)

// SQSConfig defines the configuration for the AWS SQS adapter
type SQSConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	AccessKey     string        `json:"access_key" yaml:"access_key" description:"AWS access key ID" category:"auth" sensitive:"true"`
	SecretKey     string        `json:"secret_key,omitempty" yaml:"secret_key,omitempty" description:"AWS secret access key" category:"auth" sensitive:"true"`
	QueueURL      string        `json:"queue_url" yaml:"queue_url" description:"SQS queue URL" category:"source" example:"https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"`
	Region        string        `json:"region" yaml:"region" description:"AWS region" category:"source" example:"us-east-1"`
}

func (c *SQSConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.AccessKey == "" {
		return errors.New("missing access_key")
	}
	if c.SecretKey == "" {
		return errors.New("missing secret_key")
	}
	if c.Region == "" {
		return errors.New("missing region")
	}
	if c.QueueURL == "" {
		return errors.New("missing queue_url")
	}
	return nil
}
