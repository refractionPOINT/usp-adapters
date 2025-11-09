package adaptertypes

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
