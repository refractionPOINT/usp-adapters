package usp_wel

import (
	"github.com/refractionPOINT/go-uspclient"
)

type WELConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	EvtSources      string                  `json:"evt_sources,omitempty" yaml:"evt_sources,omitempty"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
}
