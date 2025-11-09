package adaptertypes

// StdinConfig defines the configuration for the stdin adapter
type StdinConfig struct {
	ClientOptions   ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	WriteTimeoutSec uint64        `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty" description:"Timeout in seconds for writing data to USP" category:"performance" default:"600"`
}
