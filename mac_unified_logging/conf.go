package usp_mac_unified_logging

import (
	"fmt"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/utils"
)

type MacUnifiedLoggingConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	Predicate       string                  `json:"predicate,omitempty" yaml:"predicate,omitempty"`
	Filters    []utils.FilterPattern `json:"filters,omitempty" yaml:"filters,omitempty"`
	FilterMode utils.FilterMode       `json:"filter_mode,omitempty" yaml:"filter_mode,omitempty"`
}

// Validate validates the Mac Unified Logging adapter configuration.
//
// Parameters:
//
//	None
//
// Returns:
//
//	error - Returns nil if validation passes, or an error describing the validation failure.
func (c *MacUnifiedLoggingConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	return nil
}
