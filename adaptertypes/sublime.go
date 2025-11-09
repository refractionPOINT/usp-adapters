package adaptertypes

// SublimeConfig defines the configuration for the Sublime Security adapter
type SublimeConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ApiKey        string        `json:"api_key" yaml:"api_key" description:"Sublime Security API key" category:"auth" sensitive:"true"`
	BaseURL       string        `json:"base_url" yaml:"base_url" description:"Sublime Security API base URL" category:"source" example:"https://api.sublime.security"`
}
