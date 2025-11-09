package adaptertypes

import (
	"errors"
	"fmt"
	"strings"
)

// Office365Config defines the configuration for the Office 365 adapter
type Office365Config struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Domain        string        `json:"domain" yaml:"domain" description:"Office 365 domain" category:"source" example:"contoso.com"`
	TenantID      string        `json:"tenant_id" yaml:"tenant_id" description:"Azure AD tenant ID" category:"auth"`
	PublisherID   string        `json:"publisher_id" yaml:"publisher_id" description:"Publisher ID for Office 365 Management API" category:"auth"`
	ClientID      string        `json:"client_id" yaml:"client_id" description:"Azure AD application (client) ID" category:"auth"`
	ClientSecret  string        `json:"client_secret" yaml:"client_secret" description:"Azure AD application client secret" category:"auth" sensitive:"true"`
	Endpoint      string        `json:"endpoint" yaml:"endpoint" description:"Office 365 endpoint" category:"source" example:"commercial" llmguidance:"Options: 'commercial', 'gcc', 'gcchigh', 'dod'"`
	ContentTypes  string        `json:"content_types" yaml:"content_types" description:"Comma-separated list of content types to fetch" category:"source" example:"Audit.AzureActiveDirectory,Audit.Exchange" llmguidance:"Common: Audit.AzureActiveDirectory, Audit.Exchange, Audit.SharePoint, Audit.General"`
	StartTime     string        `json:"start_time" yaml:"start_time" description:"Start time for initial fetch" category:"behavior" llmguidance:"RFC3339 format. Leave empty to start from current time"`

	Deduper Deduper `json:"-" yaml:"-"`
}

// Office 365 Management API endpoints for different cloud environments
var office365URL = map[string]string{
	"enterprise":   "https://manage.office.com/api/v1.0/",
	"gcc-gov":      "https://manage-gcc.office.com/api/v1.0/",
	"gcc-high-gov": "https://manage.office365.us/api/v1.0/",
	"dod-gov":      "https://manage.protection.apps.mil/api/v1.0/",
}

func (c *Office365Config) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Domain == "" {
		return errors.New("missing domain")
	}
	if c.TenantID == "" {
		return errors.New("missing tenant_id")
	}
	if c.PublisherID == "" {
		return errors.New("missing publisher_id")
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.Endpoint == "" {
		return errors.New("missing endpoint")
	}
	_, ok := office365URL[c.Endpoint]
	if !strings.HasPrefix(c.Endpoint, "https://") && !ok {
		return fmt.Errorf("invalid endpoint, not https or in %v", office365URL)
	}
	return nil
}
