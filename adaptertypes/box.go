package adaptertypes

import (
	"errors"
	"fmt"
)

// BoxConfig defines the configuration for the Box adapter
type BoxConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ClientID      string        `json:"client_id" yaml:"client_id" description:"Box OAuth2 client ID" category:"auth"`
	ClientSecret  string        `json:"client_secret" yaml:"client_secret" description:"Box OAuth2 client secret" category:"auth" sensitive:"true"`
	SubjectID     string        `json:"subject_id" yaml:"subject_id" description:"Box enterprise or user ID" category:"auth" llmguidance:"Enterprise ID for enterprise events, or user ID for user events"`
}

func (c *BoxConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}

	if c.ClientID == "" || c.ClientSecret == "" || c.SubjectID == "" {
		return errors.New("missing Box client ID, secret, or subject ID")
	}
	return nil
}
