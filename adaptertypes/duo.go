package adaptertypes

// DuoConfig defines the configuration for the Duo Security adapter
type DuoConfig struct {
	ClientOptions  ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	IntegrationKey string        `json:"integration_key" yaml:"integration_key" description:"Duo Admin API integration key" category:"auth" sensitive:"true" llmguidance:"Generate in Duo Admin Panel > Applications > Admin API"`
	SecretKey      string        `json:"secret_key" yaml:"secret_key" description:"Duo Admin API secret key" category:"auth" sensitive:"true"`
	APIHostname    string        `json:"api_hostname" yaml:"api_hostname" description:"Duo API hostname" category:"source" example:"api-xxxxx.duosecurity.com" llmguidance:"Found in Duo Admin Panel under your Admin API application"`
}
