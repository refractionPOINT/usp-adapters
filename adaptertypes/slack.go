package adaptertypes

import (
	"errors"
	"fmt"
)

// SlackConfig defines the configuration for the Slack audit logs adapter
type SlackConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Token         string        `json:"token" yaml:"token" description:"Slack API token for audit log access" category:"auth" sensitive:"true" llmguidance:"Requires an Enterprise Grid organization and audit logs API access"`
}

func (c *SlackConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Token == "" {
		return errors.New("missing token")
	}
	return nil
}
