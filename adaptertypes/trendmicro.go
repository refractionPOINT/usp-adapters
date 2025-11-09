package adaptertypes

// TrendMicroConfig defines the configuration for the Trend Micro adapter
type TrendMicroConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	APIToken      string        `json:"api_token" yaml:"api_token" description:"Trend Micro API token" category:"auth" sensitive:"true"`
	Region        string        `json:"region" yaml:"region" description:"Trend Micro region" category:"source" example:"us" llmguidance:"Options: 'us', 'eu', 'sg', 'au', 'in', 'jp'"`
}
