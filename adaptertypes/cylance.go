package adaptertypes

import (
	"errors"
	"fmt"
)

const defaultLoggingBaseURL = "https://protectapi.cylance.com"

// CylanceConfig defines the configuration for the Cylance adapter
type CylanceConfig struct {
	ClientOptions  ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	TenantID       string        `json:"tenant_id" yaml:"tenant_id" description:"Cylance tenant ID" category:"auth"`
	AppID          string        `json:"app_id" yaml:"app_id" description:"Cylance application ID" category:"auth"`
	AppSecret      string        `json:"app_secret" yaml:"app_secret" description:"Cylance application secret" category:"auth" sensitive:"true"`
	LoggingBaseURL string        `json:"logging_base_url" yaml:"logging_base_url" description:"Cylance logging API base URL" category:"source" example:"https://protectapi.cylance.com"`
}

func (c *CylanceConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.TenantID == "" {
		return errors.New("missing tenant id")
	}
	if c.AppID == "" {
		return errors.New("missing app id")
	}
	if c.AppSecret == "" {
		return errors.New("missing app secret")
	}
	if c.LoggingBaseURL == "" {
		c.LoggingBaseURL = defaultLoggingBaseURL
	}
	return nil
}
