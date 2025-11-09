package adaptertypes

import (
	"errors"
	"fmt"
)

// K8sPodsConfig defines the configuration for the Kubernetes pods log adapter
type K8sPodsConfig struct {
	ClientOptions   ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	WriteTimeoutSec uint64        `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty" description:"Timeout in seconds for writing data to USP" category:"performance" default:"600"`
	Root            string        `json:"root" yaml:"root" description:"Root directory containing pod logs" category:"source" example:"/var/log/pods" llmguidance:"Usually /var/log/pods on the node filesystem"`
	IncludePodsRE   string        `json:"include_pods_re" yaml:"include_pods_re" description:"Regular expression to include specific pods" category:"filter" llmguidance:"Optional. Include only pods matching this regex"`
	ExcludePodsRE   string        `json:"exclude_pods_re" yaml:"exclude_pods_re" description:"Regular expression to exclude specific pods" category:"filter" llmguidance:"Optional. Exclude pods matching this regex"`
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
