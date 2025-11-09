package adaptertypes

import (
	"errors"
	"fmt"
)

// MsGraphConfig defines the configuration for the Microsoft Graph adapter
type MsGraphConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	TenantID      string        `json:"tenant_id" yaml:"tenant_id" description:"Azure AD tenant ID" category:"auth"`
	ClientID      string        `json:"client_id" yaml:"client_id" description:"Azure AD application (client) ID" category:"auth"`
	ClientSecret  string        `json:"client_secret" yaml:"client_secret" description:"Azure AD application client secret" category:"auth" sensitive:"true"`
	URL           string        `json:"url" yaml:"url" description:"Microsoft Graph API URL to query" category:"source" example:"https://graph.microsoft.com/v1.0/security/alerts" llmguidance:"Full Graph API endpoint URL"`
}

func (c *MsGraphConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.TenantID == "" {
		return errors.New("missing tenant_id")
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	return nil
}
