package usp_wel

import (
	"fmt"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
)

type WELConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	EvtSources      string                  `json:"evt_sources,omitempty" yaml:"evt_sources,omitempty"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	Filters    []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
	FilterMode utils.FilterMode       `json:"filter_mode,omitempty" yaml:"filter_mode,omitempty"`
}

// Validate validates the WEL adapter configuration.
//
// Parameters:
//
//	None
//
// Returns:
//
//	error - Returns nil if validation passes, or an error describing the validation failure.
func (c *WELConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	return nil
}
