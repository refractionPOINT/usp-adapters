package adaptertypes

// ProofpointTapConfig defines the configuration for the Proofpoint TAP adapter
type ProofpointTapConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Principal     string        `json:"principal" yaml:"principal" description:"Proofpoint TAP service principal" category:"auth"`
	Secret        string        `json:"secret" yaml:"secret" description:"Proofpoint TAP service secret" category:"auth" sensitive:"true"`
}
