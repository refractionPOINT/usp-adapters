package usp_file

import (
	"errors"
	"fmt"

	"github.com/refractionPOINT/go-uspclient"
)

type FileConfig struct {
	ClientOptions         uspclient.ClientOptions `json:"client_options" yaml:"client_options"`
	WriteTimeoutSec       uint64                  `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty"`
	FilePath              string                  `json:"file_path" yaml:"file_path"`
	NoFollow              bool                    `json:"no_follow" yaml:"no_follow"`
	InactivityThreshold   int                     `json:"inactivity_threshold" yaml:"inactivity_threshold"`
	ReactivationThreshold int                     `json:"reactivation_threshold" yaml:"reactivation_threshold"`
	Backfill              bool                    `json:"backfill" yaml:"backfill"`
	SerializeFiles        bool                    `json:"serialize_files" yaml:"serialize_files"`
	Poll                  bool                    `json:"poll" yaml:"poll"`
	MultiLineJSON         bool                    `json:"multi_line_json" yaml:"multi_line_json"`
}

// Validate validates the File adapter configuration.
//
// Parameters:
//
//	None
//
// Returns:
//
//	error - Returns nil if validation passes, or an error describing the validation failure.
func (c *FileConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.FilePath == "" {
		return errors.New("file_path missing")
	}
	return nil
}
