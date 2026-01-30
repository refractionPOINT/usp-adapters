package main

import (
	"testing"

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
