package adaptertypes

import "time"

type SentinelOneConfig struct {
	ClientOptions       ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Domain              string        `json:"domain" yaml:"domain" description:"SentinelOne console domain" category:"source" example:"mycompany.sentinelone.net" llmguidance:"Your SentinelOne console domain (without https://)"`
	APIKey              string        `json:"api_key" yaml:"api_key" description:"SentinelOne API key" category:"auth" sensitive:"true" llmguidance:"Generate in SentinelOne console under Settings > Users > Service Users"`
	URLs                string        `json:"urls" yaml:"urls" description:"Comma-separated list of SentinelOne API endpoints to query" category:"source" example:"/web/api/v2.1/activities" llmguidance:"API endpoints to poll for data. Common: /web/api/v2.1/activities, /web/api/v2.1/threats"`
	StartTime           string        `json:"start_time" yaml:"start_time" description:"Start time for data collection" category:"behavior" example:"2024-01-01T00:00:00Z" llmguidance:"ISO 8601 format timestamp. Events before this time are ignored"`
	TimeBetweenRequests time.Duration `json:"time_between_requests" yaml:"time_between_requests" description:"Duration to wait between API requests" category:"performance" default:"60s" llmguidance:"Go duration format (e.g., '60s', '5m'). Prevents API rate limiting"`
}
