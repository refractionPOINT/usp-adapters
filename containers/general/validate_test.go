package main

import (
	"strings"
	"testing"

	"github.com/refractionPOINT/go-limacharlie/limacharlie"
	"github.com/refractionPOINT/go-uspclient"
	usp_bigquery "github.com/refractionPOINT/usp-adapters/bigquery"
	usp_file "github.com/refractionPOINT/usp-adapters/file"
	usp_k8s_pods "github.com/refractionPOINT/usp-adapters/k8s_pods"
	usp_syslog "github.com/refractionPOINT/usp-adapters/syslog"
	usp_wel "github.com/refractionPOINT/usp-adapters/wel"
)

func TestValidateConfig(t *testing.T) {
	t.Run("ValidSyslogConfig", func(t *testing.T) {
		config := &Configuration{}
		config.Syslog = usp_syslog.SyslogConfig{
			ClientOptions: uspclient.ClientOptions{
				Identity: uspclient.Identity{
					Oid:             "test-oid",
					InstallationKey: "test-key",
				},
				Platform: "text",
			},
			Port: 514,
		}

		err := validateConfig("syslog", config)
		if err != nil {
			t.Errorf("expected valid syslog config to pass validation, got error: %v", err)
		}
	})

	t.Run("InvalidSyslogConfigMissingIdentity", func(t *testing.T) {
		config := &Configuration{}
		config.Syslog = usp_syslog.SyslogConfig{
			ClientOptions: uspclient.ClientOptions{},
			Port:          514,
		}

		err := validateConfig("syslog", config)
		if err == nil {
			t.Error("expected syslog config with missing identity to fail validation")
		}
	})

	t.Run("ValidWELConfig", func(t *testing.T) {
		config := &Configuration{}
		config.Wel = usp_wel.WELConfig{
			ClientOptions: uspclient.ClientOptions{
				Identity: uspclient.Identity{
					Oid:             "test-oid",
					InstallationKey: "test-key",
				},
				Platform: "json",
			},
			EvtSources: "Security,System",
		}

		err := validateConfig("wel", config)
		if err != nil {
			t.Errorf("expected valid wel config to pass validation, got error: %v", err)
		}
	})

	t.Run("InvalidWELConfigMissingIdentity", func(t *testing.T) {
		config := &Configuration{}
		config.Wel = usp_wel.WELConfig{
			ClientOptions: uspclient.ClientOptions{},
			EvtSources:    "Security",
		}

		err := validateConfig("wel", config)
		if err == nil {
			t.Error("expected wel config with missing identity to fail validation")
		}
	})

	t.Run("ValidFileConfig", func(t *testing.T) {
		config := &Configuration{}
		config.File = usp_file.FileConfig{
			ClientOptions: uspclient.ClientOptions{
				Identity: uspclient.Identity{
					Oid:             "test-oid",
					InstallationKey: "test-key",
				},
				Platform: "text",
			},
			FilePath: "/var/log/test.log",
		}

		err := validateConfig("file", config)
		if err != nil {
			t.Errorf("expected valid file config to pass validation, got error: %v", err)
		}
	})

	t.Run("InvalidFileConfigMissingFilePath", func(t *testing.T) {
		config := &Configuration{}
		config.File = usp_file.FileConfig{
			ClientOptions: uspclient.ClientOptions{
				Identity: uspclient.Identity{
					Oid:             "test-oid",
					InstallationKey: "test-key",
				},
				Platform: "text",
			},
			// FilePath is missing
		}

		err := validateConfig("file", config)
		if err == nil {
			t.Error("expected file config with missing file_path to fail validation")
		}
	})

	t.Run("InvalidFileConfigMissingIdentity", func(t *testing.T) {
		config := &Configuration{}
		config.File = usp_file.FileConfig{
			ClientOptions: uspclient.ClientOptions{},
			FilePath:      "/var/log/test.log",
		}

		err := validateConfig("file", config)
		if err == nil {
			t.Error("expected file config with missing identity to fail validation")
		}
	})

	t.Run("ValidK8sPodsConfig", func(t *testing.T) {
		config := &Configuration{}
		config.K8sPods = usp_k8s_pods.K8sPodsConfig{
			ClientOptions: uspclient.ClientOptions{
				Identity: uspclient.Identity{
					Oid:             "test-oid",
					InstallationKey: "test-key",
				},
				Platform: "json",
			},
			Root: "/var/log/pods",
		}

		err := validateConfig("k8s_pods", config)
		if err != nil {
			t.Errorf("expected valid k8s_pods config to pass validation, got error: %v", err)
		}
	})

	t.Run("InvalidK8sPodsConfigMissingRoot", func(t *testing.T) {
		config := &Configuration{}
		config.K8sPods = usp_k8s_pods.K8sPodsConfig{
			ClientOptions: uspclient.ClientOptions{
				Identity: uspclient.Identity{
					Oid:             "test-oid",
					InstallationKey: "test-key",
				},
				Platform: "json",
			},
			// Root is missing
		}

		err := validateConfig("k8s_pods", config)
		if err == nil {
			t.Error("expected k8s_pods config with missing root to fail validation")
		}
	})

	t.Run("InvalidK8sPodsConfigMissingIdentity", func(t *testing.T) {
		config := &Configuration{}
		config.K8sPods = usp_k8s_pods.K8sPodsConfig{
			ClientOptions: uspclient.ClientOptions{},
			Root:          "/var/log/pods",
		}

		err := validateConfig("k8s_pods", config)
		if err == nil {
			t.Error("expected k8s_pods config with missing identity to fail validation")
		}
	})

	t.Run("ValidBigQueryConfig", func(t *testing.T) {
		config := &Configuration{}
		config.BigQuery = usp_bigquery.BigQueryConfig{
			ClientOptions: uspclient.ClientOptions{
				Identity: uspclient.Identity{
					Oid:             "test-oid",
					InstallationKey: "test-key",
				},
				Platform: "json",
			},
			ProjectId:       "my-project",
			BigQueryProject: "my-project",
			DatasetName:     "my-dataset",
			TableName:       "my-table",
			SqlQuery:        "SELECT * FROM my-table",
		}

		err := validateConfig("bigquery", config)
		if err != nil {
			t.Errorf("expected valid bigquery config to pass validation, got error: %v", err)
		}
	})

	t.Run("InvalidBigQueryConfigMissingProjectId", func(t *testing.T) {
		config := &Configuration{}
		config.BigQuery = usp_bigquery.BigQueryConfig{
			ClientOptions: uspclient.ClientOptions{
				Identity: uspclient.Identity{
					Oid:             "test-oid",
					InstallationKey: "test-key",
				},
				Platform: "json",
			},
			// ProjectId is missing
			BigQueryProject: "my-project",
			DatasetName:     "my-dataset",
			TableName:       "my-table",
			SqlQuery:        "SELECT * FROM my-table",
		}

		err := validateConfig("bigquery", config)
		if err == nil {
			t.Error("expected bigquery config with missing project_id to fail validation")
		}
	})

	t.Run("InvalidBigQueryConfigMissingSqlQuery", func(t *testing.T) {
		config := &Configuration{}
		config.BigQuery = usp_bigquery.BigQueryConfig{
			ClientOptions: uspclient.ClientOptions{
				Identity: uspclient.Identity{
					Oid:             "test-oid",
					InstallationKey: "test-key",
				},
				Platform: "json",
			},
			ProjectId:       "my-project",
			BigQueryProject: "my-project",
			DatasetName:     "my-dataset",
			TableName:       "my-table",
			// SqlQuery is missing
		}

		err := validateConfig("bigquery", config)
		if err == nil {
			t.Error("expected bigquery config with missing sql_query to fail validation")
		}
	})

	t.Run("UnknownAdapterType", func(t *testing.T) {
		config := &Configuration{}
		err := validateConfig("unknown_adapter", config)
		if err == nil {
			t.Error("expected unknown adapter type to fail validation")
		}
		if err.Error() != "unknown adapter type: unknown_adapter" {
			t.Errorf("expected specific error message, got: %v", err)
		}
	})

	t.Run("AllKnownAdapterTypesSupported", func(t *testing.T) {
		// List of all adapter types that should be supported
		adapterTypes := []string{
			"syslog", "pubsub", "gcs", "s3", "stdin", "1password", "bitwarden",
			"itglue", "sophos", "okta", "office365", "wiz", "wel", "mac_unified_logging",
			"azure_event_hub", "duo", "cato", "cylance", "entraid", "defender",
			"slack", "sqs", "sqs-files", "simulator", "file", "evtx", "k8s_pods",
			"bigquery", "imap", "hubspot", "falconcloud", "mimecast", "ms_graph",
			"zendesk", "pandadoc", "proofpoint_tap", "box", "sublime",
			"sentinel_one", "trendmicro",
		}

		config := &Configuration{}
		for _, adapterType := range adapterTypes {
			err := validateConfig(adapterType, config)
			// We expect errors due to missing identity, but NOT "unknown adapter type"
			if err != nil && err.Error() == "unknown adapter type: "+adapterType {
				t.Errorf("adapter type %q should be supported but got unknown adapter error", adapterType)
			}
		}
	})
}

// TestGetClientOptions tests the reflection-based getClientOptions function
// that extracts ClientOptions from adapter configs based on json tags.
func TestGetClientOptions(t *testing.T) {
	t.Run("Syslog", func(t *testing.T) {
		config := &Configuration{}
		config.Syslog.ClientOptions = uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "test-oid-syslog"},
		}

		opts, err := getClientOptions("syslog", config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts == nil {
			t.Fatal("expected non-nil ClientOptions")
		}
		if opts.Identity.Oid != "test-oid-syslog" {
			t.Errorf("expected Oid 'test-oid-syslog', got %q", opts.Identity.Oid)
		}
	})

	t.Run("WEL", func(t *testing.T) {
		config := &Configuration{}
		config.Wel.ClientOptions = uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "test-oid-wel"},
		}

		opts, err := getClientOptions("wel", config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.Identity.Oid != "test-oid-wel" {
			t.Errorf("expected Oid 'test-oid-wel', got %q", opts.Identity.Oid)
		}
	})

	t.Run("S3", func(t *testing.T) {
		config := &Configuration{}
		config.S3.ClientOptions = uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "test-oid-s3"},
		}

		opts, err := getClientOptions("s3", config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.Identity.Oid != "test-oid-s3" {
			t.Errorf("expected Oid 'test-oid-s3', got %q", opts.Identity.Oid)
		}
	})

	t.Run("SqsFiles", func(t *testing.T) {
		// Test adapter with hyphen in name (sqs-files)
		config := &Configuration{}
		config.SqsFiles.ClientOptions = uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "test-oid-sqs-files"},
		}

		opts, err := getClientOptions("sqs-files", config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.Identity.Oid != "test-oid-sqs-files" {
			t.Errorf("expected Oid 'test-oid-sqs-files', got %q", opts.Identity.Oid)
		}
	})

	t.Run("OnePassword", func(t *testing.T) {
		// Test adapter with numeric prefix (1password)
		config := &Configuration{}
		config.OnePassword.ClientOptions = uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "test-oid-1password"},
		}

		opts, err := getClientOptions("1password", config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.Identity.Oid != "test-oid-1password" {
			t.Errorf("expected Oid 'test-oid-1password', got %q", opts.Identity.Oid)
		}
	})

	t.Run("UnknownAdapter", func(t *testing.T) {
		config := &Configuration{}
		_, err := getClientOptions("unknown_adapter", config)
		if err == nil {
			t.Fatal("expected error for unknown adapter")
		}
		if err.Error() != "unknown adapter type: unknown_adapter" {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("ReturnsPointerToActualField", func(t *testing.T) {
		// Verify that modifying the returned pointer modifies the original config
		config := &Configuration{}
		config.Syslog.ClientOptions = uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "original"},
		}

		opts, err := getClientOptions("syslog", config)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Modify via the returned pointer
		opts.Identity.Oid = "modified"

		// Verify the original config was modified
		if config.Syslog.ClientOptions.Identity.Oid != "modified" {
			t.Error("expected modification via returned pointer to affect original config")
		}
	})

	t.Run("AllKnownAdapterTypes", func(t *testing.T) {
		// Comprehensive test that all adapter types work with reflection
		adapterTypes := []string{
			"syslog", "pubsub", "gcs", "s3", "stdin", "1password", "bitwarden",
			"itglue", "sophos", "okta", "office365", "wiz", "wel", "mac_unified_logging",
			"azure_event_hub", "duo", "cato", "cylance", "entraid", "defender",
			"slack", "sqs", "sqs-files", "simulator", "file", "evtx", "k8s_pods",
			"bigquery", "imap", "hubspot", "falconcloud", "mimecast", "ms_graph",
			"zendesk", "pandadoc", "proofpoint_tap", "box", "sublime",
			"sentinel_one", "trendmicro",
		}

		config := &Configuration{}
		for _, adapterType := range adapterTypes {
			opts, err := getClientOptions(adapterType, config)
			if err != nil {
				t.Errorf("adapter %q: unexpected error: %v", adapterType, err)
				continue
			}
			if opts == nil {
				t.Errorf("adapter %q: expected non-nil ClientOptions", adapterType)
			}
		}
	})

	t.Run("MatchesJsonTagNotFieldName", func(t *testing.T) {
		// Verify reflection uses json tag, not Go field name
		// Field name is "SentinelOne" but json tag is "sentinel_one"
		config := &Configuration{}
		config.SentinelOne.ClientOptions = uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "test-sentinel"},
		}

		// Should work with json tag name
		opts, err := getClientOptions("sentinel_one", config)
		if err != nil {
			t.Fatalf("expected json tag 'sentinel_one' to work: %v", err)
		}
		if opts.Identity.Oid != "test-sentinel" {
			t.Errorf("expected Oid 'test-sentinel', got %q", opts.Identity.Oid)
		}

		// Should NOT work with Go field name
		_, err = getClientOptions("SentinelOne", config)
		if err == nil {
			t.Error("expected Go field name 'SentinelOne' to fail (should use json tag)")
		}
	})

	t.Run("MultipleConfigsIndependent", func(t *testing.T) {
		// Verify getting options for one adapter doesn't affect another
		config := &Configuration{}
		config.Syslog.ClientOptions = uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "syslog-oid"},
		}
		config.S3.ClientOptions = uspclient.ClientOptions{
			Identity: uspclient.Identity{Oid: "s3-oid"},
		}

		syslogOpts, _ := getClientOptions("syslog", config)
		s3Opts, _ := getClientOptions("s3", config)

		if syslogOpts.Identity.Oid != "syslog-oid" {
			t.Errorf("syslog Oid changed unexpectedly: %q", syslogOpts.Identity.Oid)
		}
		if s3Opts.Identity.Oid != "s3-oid" {
			t.Errorf("s3 Oid changed unexpectedly: %q", s3Opts.Identity.Oid)
		}
	})
}

func TestConfigValidatorInterface(t *testing.T) {
	t.Run("SyslogConfigImplementsInterface", func(t *testing.T) {
		var _ ConfigValidator = &usp_syslog.SyslogConfig{}
	})

	t.Run("WELConfigImplementsInterface", func(t *testing.T) {
		var _ ConfigValidator = &usp_wel.WELConfig{}
	})

	t.Run("FileConfigImplementsInterface", func(t *testing.T) {
		var _ ConfigValidator = &usp_file.FileConfig{}
	})

	t.Run("K8sPodsConfigImplementsInterface", func(t *testing.T) {
		var _ ConfigValidator = &usp_k8s_pods.K8sPodsConfig{}
	})

	t.Run("BigQueryConfigImplementsInterface", func(t *testing.T) {
		var _ ConfigValidator = &usp_bigquery.BigQueryConfig{}
	})
}

// TestCheckParsingResults tests the checkParsingResults function which validates
// the response from the LimaCharlie USP parsing API.
func TestCheckParsingResults(t *testing.T) {
	t.Run("SuccessfulParsingWithResults", func(t *testing.T) {
		// Successful parsing returns at least one event
		result := &limacharlie.USPMappingValidationResponse{
			Results: []limacharlie.Dict{
				{"event_type": "INFO", "message": "test event 1"},
				{"event_type": "WARN", "message": "test event 2"},
			},
			Errors: []string{},
		}

		err := checkParsingResults(result)
		if err != nil {
			t.Errorf("expected successful parsing to return nil, got error: %v", err)
		}
	})

	t.Run("ParsingWithAPIErrors", func(t *testing.T) {
		// API returns errors indicating parsing failure
		result := &limacharlie.USPMappingValidationResponse{
			Results: []limacharlie.Dict{},
			Errors:  []string{"regex pattern did not match", "invalid mapping configuration"},
		}

		err := checkParsingResults(result)
		if err == nil {
			t.Error("expected parsing with API errors to return error")
		}
		if !strings.Contains(err.Error(), "parsing validation failed") {
			t.Errorf("expected error message to contain 'parsing validation failed', got: %v", err)
		}
	})

	t.Run("EmptyResultsNoErrors", func(t *testing.T) {
		// Empty results with no API errors - indicates regex doesn't match
		// or wrong platform type
		result := &limacharlie.USPMappingValidationResponse{
			Results: []limacharlie.Dict{},
			Errors:  []string{},
		}

		err := checkParsingResults(result)
		if err == nil {
			t.Error("expected empty results to return error")
		}
		if !strings.Contains(err.Error(), "no events parsed") {
			t.Errorf("expected error message to contain 'no events parsed', got: %v", err)
		}
	})

	t.Run("NilResults", func(t *testing.T) {
		// Nil results should be treated as empty results
		result := &limacharlie.USPMappingValidationResponse{
			Results: nil,
			Errors:  nil,
		}

		err := checkParsingResults(result)
		if err == nil {
			t.Error("expected nil results to return error")
		}
		if !strings.Contains(err.Error(), "no events parsed") {
			t.Errorf("expected error message to contain 'no events parsed', got: %v", err)
		}
	})

	t.Run("PartialParsingWithErrors", func(t *testing.T) {
		// Some results but also some errors - errors take precedence
		result := &limacharlie.USPMappingValidationResponse{
			Results: []limacharlie.Dict{
				{"event_type": "INFO", "message": "partial event"},
			},
			Errors: []string{"some lines failed to parse"},
		}

		err := checkParsingResults(result)
		if err == nil {
			t.Error("expected partial parsing with errors to return error")
		}
		if !strings.Contains(err.Error(), "parsing validation failed") {
			t.Errorf("expected error message to contain 'parsing validation failed', got: %v", err)
		}
	})

	t.Run("SingleEventSuccess", func(t *testing.T) {
		// Single event is still a success
		result := &limacharlie.USPMappingValidationResponse{
			Results: []limacharlie.Dict{
				{"event_type": "INFO"},
			},
			Errors: []string{},
		}

		err := checkParsingResults(result)
		if err != nil {
			t.Errorf("expected single event parsing to return nil, got error: %v", err)
		}
	})
}
