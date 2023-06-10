package usp_k8s_pods

import (
	"github.com/refractionPOINT/go-uspclient"
)

type K8sPodsConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	Root            string                  `json:"root" yaml:"root"`
}
