package adaptertypes

import (
	"errors"
	"fmt"
)

var regionalDomains = map[string]string{
	"us": "https://api.xdr.trendmicro.com",
	"eu": "https://api.eu.xdr.trendmicro.com",
	"sg": "https://api.sg.xdr.trendmicro.com",
	"jp": "https://api.xdr.trendmicro.co.jp",
	"in": "https://api.in.xdr.trendmicro.com",
	"au": "https://api.au.xdr.trendmicro.com",
}

// TrendMicroConfig defines the configuration for the Trend Micro adapter
type TrendMicroConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	APIToken      string        `json:"api_token" yaml:"api_token" description:"Trend Micro API token" category:"auth" sensitive:"true"`
	Region        string        `json:"region" yaml:"region" description:"Trend Micro region" category:"source" example:"us" llmguidance:"Options: 'us', 'eu', 'sg', 'au', 'in', 'jp'"`
}

func (c *TrendMicroConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}

	if c.APIToken == "" {
		return errors.New("missing api_token")
	}
	if c.Region == "" {
		c.Region = "us"
	}
	if _, ok := regionalDomains[c.Region]; !ok {
		return fmt.Errorf("invalid region: %s (must be one of: us, eu, sg, jp, in, au)", c.Region)
	}
	return nil
}
