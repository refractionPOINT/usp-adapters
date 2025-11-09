package adaptertypes

// ITGlueConfig defines the configuration for the IT Glue adapter
type ITGlueConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Token         string        `json:"token" yaml:"token" description:"IT Glue API token" category:"auth" sensitive:"true"`
}
