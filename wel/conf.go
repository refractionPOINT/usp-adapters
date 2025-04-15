package usp_wel

import (
	"errors"
	"fmt"

	"github.com/refractionPOINT/go-uspclient"
)

type WELConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	EvtSources      string                  `json:"evt_sources,omitempty" yaml:"evt_sources,omitempty"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
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
