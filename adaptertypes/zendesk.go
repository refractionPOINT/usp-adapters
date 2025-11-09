package adaptertypes

// ZendeskConfig defines the configuration for the Zendesk adapter
type ZendeskConfig struct {
	ClientOptions  ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ApiToken       string        `json:"api_token" yaml:"api_token" description:"Zendesk API token" category:"auth" sensitive:"true"`
	ZendeskDomain  string        `json:"zendesk_domain" yaml:"zendesk_domain" description:"Zendesk domain" category:"source" example:"mycompany.zendesk.com" llmguidance:"Your Zendesk subdomain"`
	ZendeskEmail   string        `json:"zendesk_email" yaml:"zendesk_email" description:"Zendesk admin email address" category:"auth" llmguidance:"Email of the admin user who owns the API token"`
}
