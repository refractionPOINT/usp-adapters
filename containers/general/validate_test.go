package main

import (
	"testing"

	"github.com/refractionPOINT/go-uspclient"
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
}
