package adaptertypes

// OnePasswordConfig defines the configuration for the 1Password adapter
type OnePasswordConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Token         string        `json:"token" yaml:"token" description:"1Password Events API bearer token" category:"auth" sensitive:"true" llmguidance:"Generate token in 1Password admin console under Integrations > Events Reporting"`
	Endpoint      string        `json:"endpoint" yaml:"endpoint" description:"1Password API endpoint" category:"source" example:"business" llmguidance:"Options: 'business', 'enterprise', 'ca' (Canada), 'eu' (Europe). Or provide full HTTPS URL"`
}
