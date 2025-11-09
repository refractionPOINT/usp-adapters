package adaptertypes

import (
	"errors"
	"fmt"
)

// PubSubConfig defines the configuration for the Google Cloud Pub/Sub adapter
type PubSubConfig struct {
	ClientOptions       ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	SubscriptionName    string        `json:"sub_name" yaml:"sub_name" description:"Name of the Pub/Sub subscription to consume from" category:"source" example:"my-logs-subscription" llmguidance:"The subscription must already exist. Format: 'subscription-name' or 'projects/PROJECT_ID/subscriptions/SUBSCRIPTION_NAME'"`
	ProjectName         string        `json:"project_name" yaml:"project_name" description:"GCP project ID" category:"source" example:"my-gcp-project" llmguidance:"The GCP project ID (not project name or number)"`
	ServiceAccountCreds string        `json:"service_account_creds,omitempty" yaml:"service_account_creds,omitempty" description:"GCP service account JSON credentials" category:"auth" sensitive:"true" llmguidance:"Provide full JSON key file contents. Requires pubsub.subscriber role. Leave empty to use default service account"`
	MaxPSBuffer         int           `json:"max_ps_buffer,omitempty" yaml:"max_ps_buffer,omitempty" description:"Maximum number of messages to buffer" category:"performance" default:"1000"`
}

func (c *PubSubConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.SubscriptionName == "" {
		return errors.New("missing sub_name")
	}
	if c.ProjectName == "" {
		return errors.New("missing project_name")
	}
	if c.ServiceAccountCreds == "" {
		return errors.New("missing service_account_creds")
	}
	return nil
}
