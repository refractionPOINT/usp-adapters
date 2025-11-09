package adaptertypes

import (
	"errors"
	"fmt"
)

// HubSpotConfig defines the configuration for the HubSpot adapter
type HubSpotConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	AccessToken   string        `json:"access_token" yaml:"access_token" description:"HubSpot private app access token" category:"auth" sensitive:"true" llmguidance:"Generate in HubSpot Settings > Integrations > Private Apps"`
}

func (c *HubSpotConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.AccessToken == "" {
		return errors.New("missing access token")
	}
	return nil
}
