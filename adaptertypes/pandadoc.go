package adaptertypes

import (
	"errors"
	"fmt"
)

// PandaDocConfig defines the configuration for the PandaDoc adapter
type PandaDocConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ApiKey        string        `json:"api_key" yaml:"api_key" description:"PandaDoc API key" category:"auth" sensitive:"true"`
}

func (c *PandaDocConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ApiKey == "" {
		return errors.New("missing api key")
	}

	return nil
}
