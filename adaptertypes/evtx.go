package adaptertypes

// EVTXConfig defines the configuration for the EVTX file parser adapter
type EVTXConfig struct {
	ClientOptions   ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	WriteTimeoutSec uint64        `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty" description:"Timeout in seconds for writing data to USP" category:"performance" default:"600"`
	FilePath        string        `json:"file_path" yaml:"file_path" description:"Path to the EVTX file to parse" category:"source" example:"C:\\Windows\\System32\\winevt\\Logs\\Security.evtx" llmguidance:"For Windows Event Log files (.evtx format). Use for one-time parsing of archived event logs"`
}
