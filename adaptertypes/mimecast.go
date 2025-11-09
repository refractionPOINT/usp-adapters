package adaptertypes

import (
	"errors"
	"fmt"
)

// MimecastConfig defines the configuration for the Mimecast adapter
type MimecastConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ClientId      string        `json:"client_id" yaml:"client_id" description:"Mimecast API client ID" category:"auth"`
	ClientSecret  string        `json:"client_secret" yaml:"client_secret" description:"Mimecast API client secret" category:"auth" sensitive:"true"`
}

func (c *MimecastConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ClientId == "" {
		return errors.New("missing client id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client secret")
	}

	return nil
}
