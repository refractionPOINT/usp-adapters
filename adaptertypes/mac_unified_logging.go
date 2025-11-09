package adaptertypes

// MacUnifiedLoggingConfig defines the configuration for the macOS Unified Logging adapter
type MacUnifiedLoggingConfig struct {
	ClientOptions   ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	WriteTimeoutSec uint64        `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty" description:"Timeout in seconds for writing data to USP" category:"performance" default:"600"`
	Predicate       string        `json:"predicate,omitempty" yaml:"predicate,omitempty" description:"macOS log predicate filter" category:"source" example:"subsystem == 'com.apple.securityd'" llmguidance:"Optional. Filter logs using NSPredicate syntax. Leave empty for all logs"`
}

// Validate validates the MacUnifiedLoggingConfig
// Note: This config doesn't have required fields beyond ClientOptions
func (c *MacUnifiedLoggingConfig) Validate() error {
	return c.ClientOptions.Validate()
}
