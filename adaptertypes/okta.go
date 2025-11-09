package adaptertypes

import (
	"errors"
	"fmt"
)

// OktaConfig defines the configuration for the Okta adapter
type OktaConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`

	ApiKey string `json:"apikey" yaml:"apikey" description:"Okta API token for authentication" category:"auth" sensitive:"true" llmguidance:"Generate an API token in Okta Admin Console > Security > API > Tokens. Requires read permissions for system log events"`

	URL string `json:"url" yaml:"url" description:"Okta organization URL" category:"source" example:"https://your-org.okta.com" llmguidance:"Your Okta organization domain. Format: https://<your-domain>.okta.com (no trailing slash)"`
}

func (c *OktaConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	if c.ApiKey == "" {
		return errors.New("missing api key")
	}
	return nil
}
