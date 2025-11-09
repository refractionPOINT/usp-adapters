package adaptertypes

// BitwardenConfig defines the configuration for the Bitwarden adapter
type BitwardenConfig struct {
	ClientOptions    ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ClientID         string        `json:"client_id" yaml:"client_id" description:"Bitwarden API client ID" category:"auth"`
	ClientSecret     string        `json:"client_secret" yaml:"client_secret" description:"Bitwarden API client secret" category:"auth" sensitive:"true"`
	Region           string        `json:"region" yaml:"region" description:"Bitwarden region" category:"source" example:"US" llmguidance:"Options: 'US' or 'EU'"`
	TokenEndpointURL string        `json:"token_endpoint_url" yaml:"token_endpoint_url" description:"Custom token endpoint URL" category:"source" llmguidance:"Optional. Override default token endpoint"`
	EventsBaseURL    string        `json:"events_base_url" yaml:"events_base_url" description:"Custom events API base URL" category:"source" llmguidance:"Optional. Override default events endpoint"`
}
