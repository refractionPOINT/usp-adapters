package adaptertypes

import (
	"errors"
	"fmt"
	"strings"
)

var OnePasswordURL = map[string]string{
	"business":   "https://events.1password.com",
	"enterprise": "https://events.ent.1password.com",
	"ca":         "https://events.1password.ca",
	"eu":         "https://events.1password.eu",
}

// OnePasswordConfig defines the configuration for the 1Password adapter
type OnePasswordConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	Token         string        `json:"token" yaml:"token" description:"1Password Events API bearer token" category:"auth" sensitive:"true" llmguidance:"Generate token in 1Password admin console under Integrations > Events Reporting"`
	Endpoint      string        `json:"endpoint" yaml:"endpoint" description:"1Password API endpoint" category:"source" example:"business" llmguidance:"Options: 'business', 'enterprise', 'ca' (Canada), 'eu' (Europe). Or provide full HTTPS URL"`
}

func (c *OnePasswordConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.Token == "" {
		return errors.New("missing token")
	}
	if c.Endpoint == "" {
		return errors.New("missing endpoint")
	}
	_, ok := OnePasswordURL[c.Endpoint]
	if !strings.HasPrefix(c.Endpoint, "https://") && !ok {
		return fmt.Errorf("invalid endpoint, not https or in %v", OnePasswordURL)
	}
	return nil
}
