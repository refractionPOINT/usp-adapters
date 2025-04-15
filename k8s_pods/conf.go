package usp_k8s_pods

import (
	"errors"
	"fmt"

	"github.com/refractionPOINT/go-uspclient"
)

type K8sPodsConfig struct {
	ClientOptions   uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	Root            string                  `json:"root" yaml:"root"`
	IncludePodsRE   string                  `json:"include_pods_re" yaml:"include_pods_re"`
	ExcludePodsRE   string                  `json:"exclude_pods_re" yaml:"exclude_pods_re"`
}

func (c *K8sPodsConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Root == "" {
		return errors.New("file_path missing")
	}
	return nil
}
