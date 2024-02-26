package usp_mac_unified_logging

import (
	"github.com/refractionPOINT/go-uspclient"
)

type MacUnifiedLoggingConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
}
