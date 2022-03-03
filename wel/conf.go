package usp_wel

import (
	"github.com/refractionPOINT/go-uspclient"
)

type WELConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	ChannelPath     string                  `json:"channel_path,omitempty" yaml:"channel_path,omitempty"`
	Query           string                  `json:"query,omitempty" yaml:"query,omitempty"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
}
