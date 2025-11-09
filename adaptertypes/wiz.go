package adaptertypes

import (
	"errors"
	"fmt"
)

// WizConfig defines the configuration for the Wiz security platform adapter
type WizConfig struct {
	ClientOptions ClientOptions          `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`
	ClientID      string                 `json:"client_id" yaml:"client_id" description:"Wiz API client ID" category:"auth"`
	ClientSecret  string                 `json:"client_secret" yaml:"client_secret" description:"Wiz API client secret" category:"auth" sensitive:"true"`
	URL           string                 `json:"url" yaml:"url" description:"Wiz API URL" category:"source" example:"https://api.us1.app.wiz.io/graphql"`
	Query         string                 `json:"query" yaml:"query" description:"GraphQL query to execute" category:"source" llmguidance:"Custom GraphQL query for fetching data"`
	Variables     map[string]interface{} `json:"variables" yaml:"variables" description:"Variables for the GraphQL query" category:"source"`
	TimeField     string                 `json:"time_field" yaml:"time_field" description:"Field name containing timestamp" category:"parsing" example:"createdAt"`
	DataPath      []string               `json:"data_path" yaml:"data_path" description:"Path to data in GraphQL response" category:"parsing" llmguidance:"Array of keys to navigate to the data array in response"`
	IDField       string                 `json:"id_field" yaml:"id_field" description:"Field name containing unique ID for deduplication" category:"parsing" example:"id"`
}

func (c *WizConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.ClientID == "" {
		return errors.New("missing client_id")
	}
	if c.ClientSecret == "" {
		return errors.New("missing client_secret")
	}
	if c.URL == "" {
		return errors.New("missing url")
	}
	if c.Query == "" {
		return errors.New("missing query")
	}
	if c.TimeField == "" {
		return errors.New("missing time_field")
	}
	if len(c.DataPath) == 0 {
		return errors.New("missing data_path")
	}
	if c.IDField == "" {
		return errors.New("missing id_field")
	}
	return nil
}
