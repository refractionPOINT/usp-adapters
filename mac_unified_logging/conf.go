package usp_mac_unified_logging

import (
	"fmt"

	"github.com/refractionPOINT/go-uspclient"
)

type MacUnifiedLoggingConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	Predicate       string                  `json:"predicate,omitempty" yaml:"predicate,omitempty"`
}

func (c *MacUnifiedLoggingConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	return nil
}
