package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const testEventsPath = "params/events"

func assertEventsPathConfig(t *testing.T, config *Configuration) {
	t.Helper()
	opts := config.Stdin.ClientOptions
	if opts.Mapping.EventsPath != testEventsPath {
		t.Fatalf("mapping events_path was not preserved: %+v", opts.Mapping)
	}
	if len(opts.Mappings) != 1 || opts.Mappings[0].EventsPath != "other/batch" {
		t.Fatalf("mappings events_path was not preserved: %+v", opts.Mappings)
	}
	if err := config.Stdin.Validate(); err != nil {
		t.Fatal(err)
	}
}

// Exercise the adapter executable's loaders, rather than only the SDK struct.
func TestEventsPathFileConfig(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var input []byte
			if format == "json" {
				input = []byte(fmt.Sprintf(`{"stdin":{"client_options":{"identity":{"oid":"test-org","installation_key":"test-key"},"platform":"json","mapping":{"events_path":%q},"mappings":[{"events_path":"other/batch"}]}}}`, testEventsPath))
			} else {
				input = []byte(fmt.Sprintf("stdin:\n  client_options:\n    identity:\n      oid: test-org\n      installation_key: test-key\n    platform: json\n    mapping:\n      events_path: %q\n    mappings:\n      - events_path: other/batch\n", testEventsPath))
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
				assertEventsPathConfig(t, config)
				// The field must survive re-encoding too: it is what travels
				// to the ingestion proxy.
				var restored Configuration
				if format == "json" {
					encoded, err := json.Marshal(config)
					if err == nil {
						err = json.Unmarshal(encoded, &restored)
					}
					if err != nil {
						t.Fatal(err)
					}
				} else {
					encoded, err := yaml.Marshal(config)
					if err == nil {
						err = yaml.Unmarshal(encoded, &restored)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				assertEventsPathConfig(t, &restored)
			}
		})
	}
}

func TestEventsPathEnvironmentConfig(t *testing.T) {
	// parseConfigs uses the same parser for CLI parameters and os.Environ.
	t.Setenv("client_options.identity.oid", "test-org")
	t.Setenv("client_options.identity.installation_key", "test-key")
	t.Setenv("client_options.platform", "json")
	t.Setenv("client_options.mapping.events_path", testEventsPath)
	t.Setenv("client_options.mappings[0].events_path", "other/batch")
	method, configs, err := parseConfigs([]string{"stdin", "ignored=value", "another=value"})
	if err != nil {
		t.Fatal(err)
	}
	if method != "stdin" || len(configs) != 1 {
		t.Fatalf("unexpected configs: %q %d", method, len(configs))
	}
	assertEventsPathConfig(t, configs[0])
}

func TestEventsPathInvalidConfig(t *testing.T) {
	for _, field := range []string{"mapping.events_path", "mappings[0].events_path"} {
		for _, value := range []string{"/params/events", "params//events", "params/events/", "params/ events", strings.Repeat("k/", 10) + "k"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				var config Configuration
				err := parseConfigsFromParams("stdin", []string{
					"client_options.identity.oid=test-org", "client_options.identity.installation_key=test-key", "client_options.platform=json", "client_options." + field + "=" + value,
				}, &config)
				if err != nil {
					t.Fatal(err)
				}
				if err := config.Stdin.Validate(); err == nil || !strings.Contains(err.Error(), "events_path") {
					t.Fatalf("invalid events_path accepted or unrelated error: %v", err)
				}
			})
		}
	}
}

// Constructors use the ingestion client directly, so an invalid events_path
// must be refused on the actual executable entry path, before input/network work.
func TestEventsPathAdapterConstructorRefusesInvalidConfig(t *testing.T) {
	for _, field := range []string{"mapping", "mappings[0]"} {
		t.Run(field, func(t *testing.T) {
			var config Configuration
			if err := parseConfigsFromParams("stdin", []string{
				"client_options.identity.oid=test-org", "client_options.identity.installation_key=test-key", "client_options.platform=json",
				"client_options." + field + ".events_path=/bad",
				"client_options." + field + ".parsing_re=(?P<actor>.+)",
			}, &config); err != nil {
				t.Fatal(err)
			}
			config.Stdin.ClientOptions.TestSinkMode = true
			client, _, err := runAdapter(context.Background(), "stdin", config, false)
			if client != nil {
				_ = client.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "events_path") {
				t.Fatalf("constructor accepted invalid events_path or unrelated error: %v", err)
			}
		})
	}
}
