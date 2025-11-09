package adaptertypes

import (
	"errors"
	"fmt"
)

// FileConfig defines the configuration for the file adapter
type FileConfig struct {
	ClientOptions ClientOptions `json:"client_options" yaml:"client_options" description:"USP client configuration for data ingestion" category:"client"`

	WriteTimeoutSec uint64 `json:"write_timeout_sec,omitempty" yaml:"write_timeout_sec,omitempty" description:"Timeout in seconds for writing data to USP" category:"performance" default:"120" llmguidance:"Increase if processing very large files or slow network connections"`

	FilePath string `json:"file_path" yaml:"file_path" description:"Path to the log file or directory to monitor" category:"source" example:"/var/log/application/*.log" llmguidance:"Supports wildcards (*). For directories, specify pattern like '/var/log/*.log'. For single file, specify exact path"`

	NoFollow bool `json:"no_follow" yaml:"no_follow" description:"If true, do not follow file as it grows (read once and exit)" category:"behavior" default:"false" llmguidance:"Set to true for one-time file reads. Set to false to continuously tail the file (like 'tail -f')"`

	InactivityThreshold int `json:"inactivity_threshold" yaml:"inactivity_threshold" description:"Seconds of inactivity before marking file as inactive" category:"behavior" default:"300" llmguidance:"Used to detect when log files have been rotated or are no longer being written to"`

	ReactivationThreshold int `json:"reactivation_threshold" yaml:"reactivation_threshold" description:"Minimum number of new bytes before reactivating an inactive file" category:"behavior" default:"100" llmguidance:"Prevents reactivation on small writes. Set lower if logs are written infrequently"`

	Backfill bool `json:"backfill" yaml:"backfill" description:"If true, read existing file contents before tailing. If false, only read new data" category:"behavior" default:"false" llmguidance:"Set to true to ingest historical data from existing files. Set to false to only capture new log entries written after adapter starts"`

	SerializeFiles bool `json:"serialize_files" yaml:"serialize_files" description:"If true, process files one at a time. If false, process multiple files concurrently" category:"performance" default:"false" llmguidance:"Set to true if file processing order matters or to reduce resource usage"`

	Poll bool `json:"poll" yaml:"poll" description:"If true, use polling instead of inotify/fsevents for file changes" category:"behavior" default:"false" llmguidance:"Enable if running in environments where file system notifications don't work (NFS, some containers)"`

	MultiLineJSON bool `json:"multi_line_json" yaml:"multi_line_json" description:"If true, parse multi-line formatted JSON objects" category:"parsing" default:"false" llmguidance:"Enable if JSON objects span multiple lines (pretty-printed JSON). Disable for line-delimited JSON (JSONL/NDJSON)"`
}

func (c *FileConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.FilePath == "" {
		return errors.New("file_path missing")
	}
	return nil
}
