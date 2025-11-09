package adaptertypes

import (
	"errors"
	"fmt"
)

// DefenderConfig defines the configuration for the Microsoft Defender adapter
type DefenderConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	TenantID      string        `json:"tenant_id" yaml:"tenant_id" description:"Azure AD tenant ID" category:"auth" example:"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`
	ClientID      string        `json:"client_id" yaml:"client_id" description:"Azure AD application (client) ID" category:"auth" example:"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`
	ClientSecret  string        `json:"client_secret" yaml:"client_secret" description:"Azure AD application client secret" category:"auth" sensitive:"true"`
}

func (c *DefenderConfig) Validate() error {
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
	return nil
}
