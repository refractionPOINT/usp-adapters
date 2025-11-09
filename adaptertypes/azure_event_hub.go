package adaptertypes

// EventHubConfig defines the configuration for the Azure Event Hub adapter
type EventHubConfig struct {
	ClientOptions    ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ConnectionString string        `json:"connection_string" yaml:"connection_string" description:"Azure Event Hub connection string" category:"auth" sensitive:"true" llmguidance:"Found in Azure Portal under Event Hubs > Shared access policies. Requires 'Listen' permission"`
}
