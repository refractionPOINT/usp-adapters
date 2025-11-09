package adaptertypes

import (
	"errors"
	"fmt"
)

// ITGlueConfig defines the configuration for the IT Glue adapter
type ITGlueConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Token         string        `json:"token" yaml:"token" description:"IT Glue API token" category:"auth" sensitive:"true"`
}

func (c *ITGlueConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Token == "" {
		return errors.New("missing token")
	}
	return nil
}
