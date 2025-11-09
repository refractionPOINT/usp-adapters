package adaptertypes

import (
	"errors"
	"fmt"
)

// WELConfig defines the configuration for the Windows Event Log adapter
type WELConfig struct {
	ClientOptions   ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	EvtSources      string        `json:"evt_sources,omitempty" yaml:"evt_sources,omitempty" description:"Comma-separated list of Windows Event Log sources to monitor" category:"source" example:"Application,Security,System" llmguidance:"Common sources: Application, Security, System, Microsoft-Windows-Sysmon/Operational"`
	WriteTimeoutSec uint64        `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty" description:"Timeout in seconds for writing data to USP" category:"performance" default:"600"`
}

func (c *WELConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.EvtSources == "" {
		return errors.New("missing evt_sources, a csv of SOURCE-NAME:FILTER")
	}
	return nil
}
