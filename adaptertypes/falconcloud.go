package adaptertypes

import (
	"errors"
	"fmt"
	"time"
)

// FalconCloudConfig defines the configuration for the CrowdStrike Falcon adapter
type FalconCloudConfig struct {
	WriteTimeoutSec uint64        `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty" description:"Timeout in seconds for writing data to USP" category:"performance" default:"600"`
	ClientOptions   ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ClientId        string        `json:"client_id" yaml:"client_id" description:"CrowdStrike API client ID" category:"auth"`
	ClientSecret    string        `json:"client_secret" yaml:"client_secret" description:"CrowdStrike API client secret" category:"auth" sensitive:"true"`
	IsUsingOffset   bool          `json:"is_using_offset" yaml:"is_using_offset" description:"Use offset-based pagination instead of time-based" category:"behavior" default:"false"`
	Offset          uint64        `json:"offset" yaml:"offset" description:"Starting offset for event stream" category:"behavior" default:"0"`
	NotBefore       *time.Time    `json:"not_before,omitempty" yaml:"not_before,omitempty" description:"Only fetch events after this timestamp" category:"behavior"`
}

func (c *FalconCloudConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ClientId == "" {
		return errors.New("missing client id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client secret")
	}
	return nil
}
