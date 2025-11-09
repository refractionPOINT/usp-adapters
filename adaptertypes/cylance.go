package adaptertypes

// CylanceConfig defines the configuration for the Cylance adapter
type CylanceConfig struct {
	ClientOptions  ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	TenantID       string        `json:"tenant_id" yaml:"tenant_id" description:"Cylance tenant ID" category:"auth"`
	AppID          string        `json:"app_id" yaml:"app_id" description:"Cylance application ID" category:"auth"`
	AppSecret      string        `json:"app_secret" yaml:"app_secret" description:"Cylance application secret" category:"auth" sensitive:"true"`
	LoggingBaseURL string        `json:"logging_base_url" yaml:"logging_base_url" description:"Cylance logging API base URL" category:"source" example:"https://protectapi.cylance.com"`
}
