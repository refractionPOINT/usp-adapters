package adaptertypes

import (
	"errors"
	"fmt"
)

// SophosConfig defines the configuration for the Sophos Central adapter
type SophosConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ClientId      string        `json:"clientid" yaml:"clientid" description:"Sophos Central API client ID" category:"auth"`
	ClientSecret  string        `json:"clientsecret" yaml:"clientsecret" description:"Sophos Central API client secret" category:"auth" sensitive:"true"`
	TenantId      string        `json:"tenantid" yaml:"tenantid" description:"Sophos Central tenant ID" category:"auth"`
	URL           string        `json:"url" yaml:"url" description:"Sophos Central API URL" category:"source" example:"https://api-us01.central.sophos.com"`
}

func (c *SophosConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	if c.ClientId == "" {
		return errors.New("missing client id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client secret")
	}
	if c.TenantId == "" {
		return errors.New("missing tenant id")
	}
	return nil
}
