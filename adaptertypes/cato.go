package adaptertypes

// CatoConfig defines the configuration for the Cato Networks adapter
type CatoConfig struct {
	ClientOptions   ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	WriteTimeoutSec uint64        `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty" description:"Timeout in seconds for writing data to USP" category:"performance" default:"600"`
	ApiKey          string        `json:"apikey" yaml:"apikey" description:"Cato Networks API key" category:"auth" sensitive:"true"`
	AccountId       int           `json:"accountid" yaml:"accountid" description:"Cato Networks account ID" category:"source"`
}
