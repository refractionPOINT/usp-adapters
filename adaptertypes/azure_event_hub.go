package adaptertypes

import (
	"errors"
	"fmt"
)

// EventHubConfig defines the configuration for the Azure Event Hub adapter
type EventHubConfig struct {
	ClientOptions    ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ConnectionString string        `json:"connection_string" yaml:"connection_string" description:"Azure Event Hub connection string" category:"auth" sensitive:"true" llmguidance:"Found in Azure Portal under Event Hubs > Shared access policies. Requires 'Listen' permission"`
}

func (c *EventHubConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ConnectionString == "" {
		return errors.New("missing connection_string")
	}
	return nil
}
