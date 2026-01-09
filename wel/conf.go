package usp_wel

import (
	"fmt"

	"github.com/refractionPOINT/go-uspclient"
)

type WELConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	EvtSources      string                  `json:"evt_sources,omitempty" yaml:"evt_sources,omitempty"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
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
