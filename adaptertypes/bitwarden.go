package adaptertypes

import (
	"errors"
	"fmt"
)

// BitwardenConfig defines the configuration for the Bitwarden adapter
type BitwardenConfig struct {
	ClientOptions    ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ClientID         string        `json:"client_id" yaml:"client_id" description:"Bitwarden API client ID" category:"auth"`
	ClientSecret     string        `json:"client_secret" yaml:"client_secret" description:"Bitwarden API client secret" category:"auth" sensitive:"true"`
	Region           string        `json:"region" yaml:"region" description:"Bitwarden region" category:"source" example:"US" llmguidance:"Options: 'US' or 'EU'"`
	TokenEndpointURL string        `json:"token_endpoint_url" yaml:"token_endpoint_url" description:"Custom token endpoint URL" category:"source" llmguidance:"Optional. Override default token endpoint"`
	EventsBaseURL    string        `json:"events_base_url" yaml:"events_base_url" description:"Custom events API base URL" category:"source" llmguidance:"Optional. Override default events endpoint"`
}

func (c *BitwardenConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}

	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}

	// Check if custom URLs are provided
	hasCustomURLs := c.TokenEndpointURL != "" || c.EventsBaseURL != ""

	if hasCustomURLs {
		// If custom URLs are provided, both must be set and region must be empty
		if c.TokenEndpointURL == "" {
			return errors.New("token_endpoint_url must be set when events_base_url is provided")
		}
		if c.EventsBaseURL == "" {
			return errors.New("events_base_url must be set when token_endpoint_url is provided")
		}
		if c.Region != "" {
			return errors.New("region cannot be set when using custom URLs (token_endpoint_url and events_base_url)")
		}
	} else {
		// If custom URLs are not provided, use region-based configuration
		if c.Region == "" {
			c.Region = "us"
		}
		if c.Region != "us" && c.Region != "eu" {
			return fmt.Errorf("invalid region: %s (must be 'us' or 'eu')", c.Region)
		}
	}

	return nil
}
