package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Exercise the adapter executable's loaders, rather than only the SDK struct.
func TestSensorIdentityFileConfig(t *testing.T) {
	for _, identity := range []string{"", "email", "username", "github_login", "device"} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(format+"/"+identity, func(t *testing.T) {
				var input []byte
				if format == "json" {
					input = []byte(fmt.Sprintf(`{"stdin":{"client_options":{"identity":{"oid":"test-org","installation_key":"test-key"},"platform":"json","mapping":{"sensor_key_path":"actor/login","sensor_hostname_path":"actor/name","sensor_identity_type":%q},"mappings":[{"sensor_key_path":"device/id","sensor_identity_type":%q}]}}}`, identity, identity))
				} else {
					input = []byte(fmt.Sprintf("stdin:\n  client_options:\n    identity:\n      oid: test-org\n      installation_key: test-key\n    platform: json\n    mapping:\n      sensor_key_path: actor/login\n      sensor_hostname_path: actor/name\n      sensor_identity_type: %q\n    mappings:\n      - sensor_key_path: device/id\n        sensor_identity_type: %q\n", identity, identity))
				}
				path := filepath.Join(t.TempDir(), "config."+format)
				if err := os.WriteFile(path, input, 0600); err != nil {
					t.Fatal(err)
				}
				configs, err := parseConfigsFromFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if len(configs) == 0 {
					t.Fatal("loader returned no configurations")
				}
				for _, config := range configs {
					assertIdentityConfig(t, config, identity)
					var encoded []byte
					var restored Configuration
					if format == "json" {
						encoded, err = json.Marshal(config)
						if err == nil {
							err = json.Unmarshal(encoded, &restored)
						}
					} else {
						encoded, err = yaml.Marshal(config)
						if err == nil {
							err = yaml.Unmarshal(encoded, &restored)
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					assertIdentityConfig(t, &restored, identity)
				}
			})
		}
	}
}

func assertIdentityConfig(t *testing.T, config *Configuration, identity string) {
	t.Helper()
	opts := config.Stdin.ClientOptions
	if opts.Mapping.SensorIdentityType != identity || opts.Mapping.SensorKeyPath != "actor/login" || opts.Mapping.SensorHostnamePath != "actor/name" {
		t.Fatalf("mapping declaration was not preserved: %+v", opts.Mapping)
	}
	if len(opts.Mappings) != 1 || opts.Mappings[0].SensorIdentityType != identity || opts.Mappings[0].SensorKeyPath != "device/id" {
		t.Fatalf("mapping list declaration was not preserved: %+v", opts.Mappings)
	}
	if err := config.Stdin.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSensorIdentityEnvironmentConfig(t *testing.T) {
	// parseConfigs uses the same parser for CLI parameters and os.Environ.
	t.Setenv("client_options.identity.oid", "test-org")
	t.Setenv("client_options.identity.installation_key", "test-key")
	t.Setenv("client_options.platform", "json")
	t.Setenv("client_options.mapping.sensor_key_path", "actor/login")
	t.Setenv("client_options.mapping.sensor_hostname_path", "actor/name")
	t.Setenv("client_options.mapping.sensor_identity_type", "github_login")
	t.Setenv("client_options.mappings[0].sensor_key_path", "device/id")
	t.Setenv("client_options.mappings[0].sensor_identity_type", "github_login")
	method, configs, err := parseConfigs([]string{"stdin", "ignored=value", "another=value"})
	if err != nil {
		t.Fatal(err)
	}
	if method != "stdin" || len(configs) != 1 {
		t.Fatalf("unexpected configs: %q %d", method, len(configs))
	}
	assertIdentityConfig(t, configs[0], "github_login")
}

func TestSensorIdentityInvalidConfig(t *testing.T) {
	for _, field := range []string{"mapping.sensor_identity_type", "mappings[0].sensor_identity_type"} {
		for _, value := range []string{"host", "EMAIL", " email", "email "} {
			t.Run(field+"/"+value, func(t *testing.T) {
				var config Configuration
				err := parseConfigsFromParams("stdin", []string{
					"client_options.identity.oid=test-org", "client_options.identity.installation_key=test-key", "client_options.platform=json", "client_options." + field + "=" + value,
				}, &config)
				if err != nil {
					t.Fatal(err)
				}
				// Invalid declarations fail even without a sensor_key_path.
				if err := config.Stdin.Validate(); err == nil || !strings.Contains(err.Error(), "sensor_identity_type") {
					t.Fatalf("invalid declaration accepted or unrelated error: %v", err)
				}
			})
		}
	}
}
