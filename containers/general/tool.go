package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/refractionPOINT/go-limacharlie/limacharlie"
	"github.com/refractionPOINT/usp-adapters/1password"
	"github.com/refractionPOINT/usp-adapters/azure_event_hub"
	usp_bigquery "github.com/refractionPOINT/usp-adapters/bigquery"
	"github.com/refractionPOINT/usp-adapters/bitwarden"
	"github.com/refractionPOINT/usp-adapters/box"
	"github.com/refractionPOINT/usp-adapters/cato"
	"github.com/refractionPOINT/usp-adapters/cylance"
	"github.com/refractionPOINT/usp-adapters/defender"
	"github.com/refractionPOINT/usp-adapters/duo"
	"github.com/refractionPOINT/usp-adapters/entraid"
	"github.com/refractionPOINT/usp-adapters/evtx"
	"github.com/refractionPOINT/usp-adapters/falconcloud"
	"github.com/refractionPOINT/usp-adapters/file"
	"github.com/refractionPOINT/usp-adapters/gcs"
	"github.com/refractionPOINT/usp-adapters/hubspot"
	"github.com/refractionPOINT/usp-adapters/imap"
	"github.com/refractionPOINT/usp-adapters/itglue"
	"github.com/refractionPOINT/usp-adapters/k8s_pods"
	"github.com/refractionPOINT/usp-adapters/mac_unified_logging"
	"github.com/refractionPOINT/usp-adapters/mimecast"
	"github.com/refractionPOINT/usp-adapters/ms_graph"
	"github.com/refractionPOINT/usp-adapters/o365"
	"github.com/refractionPOINT/usp-adapters/okta"
	"github.com/refractionPOINT/usp-adapters/pandadoc"
	"github.com/refractionPOINT/usp-adapters/proofpoint_tap"
	"github.com/refractionPOINT/usp-adapters/pubsub"
	"github.com/refractionPOINT/usp-adapters/s3"
	"github.com/refractionPOINT/usp-adapters/sentinelone"
	"github.com/refractionPOINT/usp-adapters/simulator"
	"github.com/refractionPOINT/usp-adapters/slack"
	"github.com/refractionPOINT/usp-adapters/sophos"
	"github.com/refractionPOINT/usp-adapters/sqs"
	"github.com/refractionPOINT/usp-adapters/sqs-files"
	"github.com/refractionPOINT/usp-adapters/stdin"
	"github.com/refractionPOINT/usp-adapters/sublime"
	"github.com/refractionPOINT/usp-adapters/syslog"
	"github.com/refractionPOINT/usp-adapters/trendmicro"
	"github.com/refractionPOINT/usp-adapters/wel"
	"github.com/refractionPOINT/usp-adapters/wiz"
	"github.com/refractionPOINT/usp-adapters/zendesk"

	"github.com/refractionPOINT/usp-adapters/utils"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/containers/conf"
	confupdateclient "github.com/refractionPOINT/usp-adapters/containers/general/conf_update_client"
	"gopkg.in/yaml.v3"
)

type USPClient interface {
	Close() error
}

// ConfigValidator is implemented by adapter configs that support validation.
type ConfigValidator interface {
	Validate() error
}

type AdapterStats struct {
	m                sync.Mutex
	lastAck          time.Time
	lastBackPressure time.Time
}

type Configuration struct {
	conf.GeneralConfigs `json:",inline" yaml:",inline"`

	SensorType string `json:"sensor_type" yaml:"sensor_type"`

	// If a literal config is not specified, the OID and GUID
	// will be used to fetch the config from Limacharlie
	// and update the config in real time.
	Cloud struct {
		OID        string `json:"oid" yaml:"oid"`
		ConfGUID   string `json:"conf_guid" yaml:"conf_guid"`
		ShowConfig bool   `json:"show_config" yaml:"show_config"`
	} `json:"cloud" yaml:"cloud"`
}

func logError(format string, elems ...interface{}) string {
	s := fmt.Sprintf(format+"\n", elems...)
	os.Stderr.Write([]byte(s))
	return s
}

func log(format string, elems ...interface{}) {
	fmt.Printf(format+"\n", elems...)
}

func printStruct(prefix string, s interface{}, isTop bool) {
	val := reflect.ValueOf(s)
	for i := 0; i < val.Type().NumField(); i++ {
		// logError("%#v", val.Type().Field(i))
		tag := val.Type().Field(i).Tag.Get("json")
		if tag == "-" {
			continue
		}
		components := strings.Split(tag, ",")
		p := components[0]
		if prefix != "" {
			p = fmt.Sprintf("%s.%s", prefix, p)
		}
		if isTop {
			logError("\nFor %s\n----------------------------------", p)
			p = ""
		}
		e := val.Field(i)
		if e.Kind() == reflect.Struct {
			printStruct(p, e.Interface(), false)
		} else {
			logError(p)
		}
	}
}

func printUsage() {
	logError("Usage: ./adapter [--validate] [--test-parsing <sample_file>] adapter_type [config_file.yaml | <param>...]")
	logError("")
	logError("Flags:")
	logError("  --validate                   Validate configuration without running the adapter")
	logError("  --test-parsing <sample_file> Test parsing with sample data file (requires OID and API key)")
	logError("")
	logError("Available configs:\n")
	printStruct("", Configuration{}, true)
}

func printConfig(method string, c interface{}) {
	b, _ := yaml.Marshal(c)
	log("Configs in use (%s):\n----------------------------------\n%s----------------------------------\n", method, string(b))
}

func main() {
	log("starting")

	// Check for --validate and --test-parsing flags before other processing
	validateOnly := false
	testParsingFile := ""
	args := os.Args[1:]

	for len(args) > 0 {
		if args[0] == "--validate" {
			validateOnly = true
			args = args[1:]
		} else if args[0] == "--test-parsing" {
			if len(args) < 2 {
				logError("--test-parsing requires a sample file path")
				os.Exit(1)
			}
			testParsingFile = args[1]
			args = args[2:]
		} else {
			break
		}
	}

	if len(args) > 0 && strings.HasPrefix(args[0], "-") {
		if err := serviceMode(os.Args[0], args[0], args[1:]); err != nil {
			logError("service: %v", err)
			os.Exit(1)
		}
		return
	}

	method, configsToRun, err := parseConfigs(args)
	if err != nil {
		printUsage()
		logError("\nerror: %s", err)
		os.Exit(1)
	}
	if len(configsToRun) == 0 {
		logError("no configs to run")
		os.Exit(1)
		return
	}

	// Handle --validate flag: validate config and exit without running the adapter
	if validateOnly {
		hasError := false
		for i, config := range configsToRun {
			log("validating config %d for adapter: %s", i+1, method)
			if err := validateConfig(method, config); err != nil {
				logError("config %d validation failed: %v", i+1, err)
				hasError = true
			} else {
				log("config %d validation passed", i+1)
			}
		}
		if hasError {
			os.Exit(1)
		}
		log("all configs validated successfully")
		return
	}

	// Handle --test-parsing flag: test parsing with sample data via API
	if testParsingFile != "" {
		if len(configsToRun) == 0 {
			logError("no configs to test parsing with")
			os.Exit(1)
		}
		config := configsToRun[0]
		if err := testParsing(method, config, testParsingFile); err != nil {
			logError("parsing test failed: %v", err)
			os.Exit(1)
		}
		return
	}

	mCurrentlyRunning := sync.Mutex{}
	clients := []USPClient{}
	chRunnings := make(chan struct{})
	healthCheckPortRequested := 0
	for _, config := range configsToRun {
		// If an OID and GUID are specified, we will start a conf update client
		// to update the config in real time.
		showConfig := true
		var confUpdateClient *confupdateclient.ConfUpdateClient
		if config.Cloud.ConfGUID != "" && config.Cloud.OID != "" {
			var confData map[string]interface{}
			confUpdateClient, confData, err = confupdateclient.NewConfUpdateClient(config.Cloud.OID, config.Cloud.ConfGUID, &limacharlie.LCLoggerZerolog{})
			if err != nil {
				logError("error creating conf update client: %v", err)
				os.Exit(1)
			}
			// Use this new config to run the adapter.
			if err := limacharlie.Dict(confData).UnMarshalToStruct(config); err != nil {
				logError("error unmarshalling conf update: %v", err)
				os.Exit(1)
			}
			method = config.SensorType
			showConfig = config.Cloud.ShowConfig
		}

		log("starting adapter: %s", method)
		client, chRunning, err := runAdapter(context.Background(), method, *config, showConfig)
		if err != nil {
			logError("error running adapter: %v", err)
			os.Exit(1)
		}
		mCurrentlyRunning.Lock()
		clients = append(clients, client)
		mCurrentlyRunning.Unlock()
		go func() {
			<-chRunning
			// Check if the client is still in the list.
			// If it is not, it means that we are stopping
			// the client in a controlled way and want to
			// keep going.
			isFound := false
			mCurrentlyRunning.Lock()
			for _, c := range clients {
				if c == client {
					isFound = true
				}
			}
			mCurrentlyRunning.Unlock()
			if !isFound {
				return
			}
			chRunnings <- struct{}{}
		}()
		if config.Healthcheck != 0 {
			healthCheckPortRequested = config.Healthcheck
		}
		if confUpdateClient != nil {
			log("watching for conf updates")
			go func() {
				defer confUpdateClient.Close()

				newConfig := Configuration{}
				if err := confUpdateClient.WatchForChanges(1*time.Minute, func(data map[string]interface{}) {
					if err := limacharlie.Dict(data).UnMarshalToStruct(&newConfig); err != nil {
						logError("error unmarshalling conf update: %v", err)
					}
					log("stopping previous adapter")
					// Remove the previous client from the list.
					mCurrentlyRunning.Lock()
					for i, c := range clients {
						if c == client {
							clients = append(clients[:i], clients[i+1:]...)
						}
					}
					mCurrentlyRunning.Unlock()

					if err := client.Close(); err != nil {
						logError("error closing client: %v", err)
					}

					// Wait for the client to be closed.
					<-chRunning

					log("starting new adapter")
					client, chRunning, err = runAdapter(context.Background(), method, newConfig, showConfig)
				}); err != nil {
					logError("error watching for conf updates: %v", err)
				}
			}()
		}
	}

	// If healthchecks were requested, start it.
	if healthCheckPortRequested != 0 {
		if err := startHealthChecks(healthCheckPortRequested); err != nil {
			logError("error starting healthchecks: %v", err)
			os.Exit(1)
		}
	}

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)

	select {
	case <-osSignals:
		log("received signal to exit")
		break
	case <-chRunnings:
		log("client stopped")
		break
	}
	for _, client := range clients {
		if err := client.Close(); err != nil {
			logError("error closing client: %v", err)
			os.Exit(1)
		}
	}
	log("exited")
}

// testParsing tests the parsing configuration with sample data via the LimaCharlie API.
// It reads sample data from the specified file and sends it to the validation API
// to verify that the parsing rules correctly transform the data.
//
// Parameters:
//
//	method - The adapter type name (e.g., "syslog", "wel", "s3").
//	configs - The configuration containing parsing/mapping settings.
//	sampleFile - Path to a file containing sample data to test.
//
// Returns:
//
//	error - Returns nil if parsing succeeds, or an error describing the failure.
func testParsing(method string, configs *Configuration, sampleFile string) error {
	// Read sample data from file
	sampleData, err := os.ReadFile(sampleFile)
	if err != nil {
		return fmt.Errorf("failed to read sample file %s: %v", sampleFile, err)
	}

	// Get the client options for the adapter to extract OID, API key, platform, and mapping
	clientOpts, err := getClientOptions(method, configs)
	if err != nil {
		return fmt.Errorf("failed to get client options: %v", err)
	}

	// Validate we have required credentials
	if clientOpts.Identity.Oid == "" {
		return errors.New("missing OID in client_options.identity.oid (required for API validation)")
	}
	if clientOpts.Identity.InstallationKey == "" {
		return errors.New("missing API key in client_options.identity.installation_key (required for API validation)")
	}

	// Create LimaCharlie client and organization for API authentication
	lcClient, err := limacharlie.NewClient(limacharlie.ClientOptions{
		OID:    clientOpts.Identity.Oid,
		APIKey: clientOpts.Identity.InstallationKey,
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to create LimaCharlie client: %v", err)
	}

	org, err := limacharlie.NewOrganization(lcClient)
	if err != nil {
		return fmt.Errorf("failed to authenticate with LimaCharlie: %v", err)
	}

	// Exchange API key for JWT (required for API calls)
	if _, err := lcClient.RefreshJWT(time.Hour); err != nil {
		return fmt.Errorf("failed to authenticate (check API key): %v", err)
	}

	// Build the validation request
	req := limacharlie.USPMappingValidationRequest{
		Platform:  clientOpts.Platform,
		TextInput: string(sampleData),
	}


	// Add mapping if configured (convert to Dict format for API)
	if clientOpts.Mapping.ParsingRE != "" || clientOpts.Mapping.EventTypePath != "" || clientOpts.Mapping.Transform != nil {
		mappingDict := limacharlie.Dict{}

		if clientOpts.Mapping.ParsingRE != "" {
			mappingDict["parsing_re"] = clientOpts.Mapping.ParsingRE
		}
		if clientOpts.Mapping.EventTypePath != "" {
			mappingDict["event_type_path"] = clientOpts.Mapping.EventTypePath
		}
		if clientOpts.Mapping.EventTimePath != "" {
			mappingDict["event_time_path"] = clientOpts.Mapping.EventTimePath
		}
		if clientOpts.Mapping.SensorHostnamePath != "" {
			mappingDict["sensor_hostname_path"] = clientOpts.Mapping.SensorHostnamePath
		}
		if clientOpts.Mapping.SensorKeyPath != "" {
			mappingDict["sensor_key_path"] = clientOpts.Mapping.SensorKeyPath
		}
		if clientOpts.Mapping.Transform != nil {
			mappingDict["transform"] = clientOpts.Mapping.Transform
		}
		if len(clientOpts.Mapping.Mappings) > 0 {
			mappingDict["mappings"] = clientOpts.Mapping.Mappings
		}

		req.Mapping = mappingDict
	}

	// Add mappings array if configured (for multi-mapping selection)
	if len(clientOpts.Mappings) > 0 {
		mappingsArray := make([]limacharlie.Dict, 0, len(clientOpts.Mappings))
		for _, m := range clientOpts.Mappings {
			md := limacharlie.Dict{}
			if m.ParsingRE != "" {
				md["parsing_re"] = m.ParsingRE
			}
			if m.EventTypePath != "" {
				md["event_type_path"] = m.EventTypePath
			}
			if m.EventTimePath != "" {
				md["event_time_path"] = m.EventTimePath
			}
			if m.Transform != nil {
				md["transform"] = m.Transform
			}
			mappingsArray = append(mappingsArray, md)
		}
		req.Mappings = mappingsArray
	}

	log("testing parsing with platform=%s", clientOpts.Platform)

	// Call the validation API via SDK
	result, err := org.ValidateUSPMapping(req)
	if err != nil {
		return fmt.Errorf("API call failed: %v", err)
	}

	// Check for errors
	if len(result.Errors) > 0 {
		log("PARSING FAILED")
		log("")
		log("Errors:")
		for _, e := range result.Errors {
			log("  - %s", e)
		}
		return errors.New("parsing validation failed")
	}

	// Display results
	log("PARSING SUCCESSFUL")
	log("")
	log("Parsed %d event(s):", len(result.Results))
	log("")

	for i, event := range result.Results {
		log("Event %d:", i+1)
		eventJSON, _ := json.MarshalIndent(event, "  ", "  ")
		log("  %s", string(eventJSON))
		log("")
	}

	return nil
}

// getClientOptions extracts the ClientOptions from the config based on adapter type.
//
// Parameters:
//
//	method - The adapter type name.
//	configs - The configuration struct.
//
// Returns:
//
//	*uspclient.ClientOptions - The client options for the adapter.
//	error - An error if the adapter type is unknown.
func getClientOptions(method string, configs *Configuration) (*uspclient.ClientOptions, error) {
	switch method {
	case "syslog":
		return &configs.Syslog.ClientOptions, nil
	case "pubsub":
		return &configs.PubSub.ClientOptions, nil
	case "gcs":
		return &configs.Gcs.ClientOptions, nil
	case "s3":
		return &configs.S3.ClientOptions, nil
	case "stdin":
		return &configs.Stdin.ClientOptions, nil
	case "1password":
		return &configs.OnePassword.ClientOptions, nil
	case "bitwarden":
		return &configs.Bitwarden.ClientOptions, nil
	case "itglue":
		return &configs.ITGlue.ClientOptions, nil
	case "sophos":
		return &configs.Sophos.ClientOptions, nil
	case "okta":
		return &configs.Okta.ClientOptions, nil
	case "office365":
		return &configs.Office365.ClientOptions, nil
	case "wiz":
		return &configs.Wiz.ClientOptions, nil
	case "wel":
		return &configs.Wel.ClientOptions, nil
	case "mac_unified_logging":
		return &configs.MacUnifiedLogging.ClientOptions, nil
	case "azure_event_hub":
		return &configs.AzureEventHub.ClientOptions, nil
	case "duo":
		return &configs.Duo.ClientOptions, nil
	case "cato":
		return &configs.Cato.ClientOptions, nil
	case "cylance":
		return &configs.Cylance.ClientOptions, nil
	case "entraid":
		return &configs.EntraID.ClientOptions, nil
	case "defender":
		return &configs.Defender.ClientOptions, nil
	case "slack":
		return &configs.Slack.ClientOptions, nil
	case "sqs":
		return &configs.Sqs.ClientOptions, nil
	case "sqs-files":
		return &configs.SqsFiles.ClientOptions, nil
	case "simulator":
		return &configs.Simulator.ClientOptions, nil
	case "file":
		return &configs.File.ClientOptions, nil
	case "evtx":
		return &configs.Evtx.ClientOptions, nil
	case "k8s_pods":
		return &configs.K8sPods.ClientOptions, nil
	case "bigquery":
		return &configs.BigQuery.ClientOptions, nil
	case "imap":
		return &configs.Imap.ClientOptions, nil
	case "hubspot":
		return &configs.HubSpot.ClientOptions, nil
	case "falconcloud":
		return &configs.FalconCloud.ClientOptions, nil
	case "mimecast":
		return &configs.Mimecast.ClientOptions, nil
	case "ms_graph":
		return &configs.MsGraph.ClientOptions, nil
	case "zendesk":
		return &configs.Zendesk.ClientOptions, nil
	case "pandadoc":
		return &configs.PandaDoc.ClientOptions, nil
	case "proofpoint_tap":
		return &configs.ProofpointTap.ClientOptions, nil
	case "box":
		return &configs.Box.ClientOptions, nil
	case "sublime":
		return &configs.Sublime.ClientOptions, nil
	case "sentinel_one":
		return &configs.SentinelOne.ClientOptions, nil
	case "trendmicro":
		return &configs.TrendMicro.ClientOptions, nil
	default:
		return nil, fmt.Errorf("unknown adapter type: %s", method)
	}
}

// validateConfig validates the configuration for the specified adapter type.
// It checks that all required fields are present and properly formatted
// without actually starting the adapter.
//
// Parameters:
//
//	method - The adapter type name (e.g., "syslog", "wel", "s3").
//	configs - The configuration to validate.
//
// Returns:
//
//	error - Returns nil if validation passes, or an error describing the validation failure.
func validateConfig(method string, configs *Configuration) error {
	var validator ConfigValidator

	switch method {
	case "syslog":
		validator = &configs.Syslog
	case "pubsub":
		validator = &configs.PubSub
	case "gcs":
		validator = &configs.Gcs
	case "s3":
		validator = &configs.S3
	case "stdin":
		validator = &configs.Stdin
	case "1password":
		validator = &configs.OnePassword
	case "bitwarden":
		validator = &configs.Bitwarden
	case "itglue":
		validator = &configs.ITGlue
	case "sophos":
		validator = &configs.Sophos
	case "okta":
		validator = &configs.Okta
	case "office365":
		validator = &configs.Office365
	case "wiz":
		validator = &configs.Wiz
	case "wel":
		validator = &configs.Wel
	case "mac_unified_logging":
		validator = &configs.MacUnifiedLogging
	case "azure_event_hub":
		validator = &configs.AzureEventHub
	case "duo":
		validator = &configs.Duo
	case "cato":
		validator = &configs.Cato
	case "cylance":
		validator = &configs.Cylance
	case "entraid":
		validator = &configs.EntraID
	case "defender":
		validator = &configs.Defender
	case "slack":
		validator = &configs.Slack
	case "sqs":
		validator = &configs.Sqs
	case "sqs-files":
		validator = &configs.SqsFiles
	case "simulator":
		validator = &configs.Simulator
	case "file":
		validator = &configs.File
	case "evtx":
		validator = &configs.Evtx
	case "k8s_pods":
		validator = &configs.K8sPods
	case "bigquery":
		validator = &configs.BigQuery
	case "imap":
		validator = &configs.Imap
	case "hubspot":
		validator = &configs.HubSpot
	case "falconcloud":
		validator = &configs.FalconCloud
	case "mimecast":
		validator = &configs.Mimecast
	case "ms_graph":
		validator = &configs.MsGraph
	case "zendesk":
		validator = &configs.Zendesk
	case "pandadoc":
		validator = &configs.PandaDoc
	case "proofpoint_tap":
		validator = &configs.ProofpointTap
	case "box":
		validator = &configs.Box
	case "sublime":
		validator = &configs.Sublime
	case "sentinel_one":
		validator = &configs.SentinelOne
	case "trendmicro":
		validator = &configs.TrendMicro
	default:
		return fmt.Errorf("unknown adapter type: %s", method)
	}

	return validator.Validate()
}

func runAdapter(ctx context.Context, method string, configs Configuration, showConfig bool) (USPClient, chan struct{}, error) {
	var client USPClient
	var chRunning chan struct{}
	var err error

	var configToShow interface{}

	if method == "syslog" {
		configs.Syslog.ClientOptions = applyLogging(configs.Syslog.ClientOptions)
		configs.Syslog.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Syslog
		client, chRunning, err = usp_syslog.NewSyslogAdapter(ctx, configs.Syslog)
	} else if method == "pubsub" {
		configs.PubSub.ClientOptions = applyLogging(configs.PubSub.ClientOptions)
		configs.PubSub.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.PubSub
		client, chRunning, err = usp_pubsub.NewPubSubAdapter(ctx, configs.PubSub)
	} else if method == "gcs" {
		configs.Gcs.ClientOptions = applyLogging(configs.Gcs.ClientOptions)
		configs.Gcs.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Gcs
		client, chRunning, err = usp_gcs.NewGCSAdapter(ctx, configs.Gcs)
	} else if method == "s3" {
		configs.S3.ClientOptions = applyLogging(configs.S3.ClientOptions)
		configs.S3.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.S3
		client, chRunning, err = usp_s3.NewS3Adapter(ctx, configs.S3)
	} else if method == "stdin" {
		configs.Stdin.ClientOptions = applyLogging(configs.Stdin.ClientOptions)
		configs.Stdin.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Stdin
		client, chRunning, err = usp_stdin.NewStdinAdapter(ctx, configs.Stdin)
	} else if method == "1password" {
		configs.OnePassword.ClientOptions = applyLogging(configs.OnePassword.ClientOptions)
		configs.OnePassword.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.OnePassword
		client, chRunning, err = usp_1password.NewOnePasswordpAdapter(ctx, configs.OnePassword)
	} else if method == "bitwarden" {
		configs.Bitwarden.ClientOptions = applyLogging(configs.Bitwarden.ClientOptions)
		configs.Bitwarden.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Bitwarden
		client, chRunning, err = usp_bitwarden.NewBitwardenAdapter(ctx, configs.Bitwarden)
	} else if method == "itglue" {
		configs.ITGlue.ClientOptions = applyLogging(configs.ITGlue.ClientOptions)
		configs.ITGlue.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.ITGlue
		client, chRunning, err = usp_itglue.NewITGlueAdapter(ctx, configs.ITGlue)
	} else if method == "sophos" {
		configs.Sophos.ClientOptions = applyLogging(configs.Sophos.ClientOptions)
		configs.Sophos.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Sophos
		client, chRunning, err = usp_sophos.NewSophosAdapter(ctx, configs.Sophos)
	} else if method == "okta" {
		configs.Okta.ClientOptions = applyLogging(configs.Okta.ClientOptions)
		configs.Okta.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Okta
		client, chRunning, err = usp_okta.NewOktaAdapter(ctx, configs.Okta)
	} else if method == "office365" {
		configs.Office365.ClientOptions = applyLogging(configs.Office365.ClientOptions)
		configs.Office365.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Office365
		client, chRunning, err = usp_o365.NewOffice365Adapter(ctx, configs.Office365)
	} else if method == "wiz" {
		configs.Wiz.ClientOptions = applyLogging(configs.Wiz.ClientOptions)
		configs.Wiz.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Wiz
		client, chRunning, err = usp_wiz.NewWizAdapter(ctx, configs.Wiz)
	} else if method == "wel" {
		configs.Wel.ClientOptions = applyLogging(configs.Wel.ClientOptions)
		configs.Wel.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Wel
		client, chRunning, err = usp_wel.NewWELAdapter(ctx, configs.Wel)
	} else if method == "mac_unified_logging" {
		configs.MacUnifiedLogging.ClientOptions = applyLogging(configs.MacUnifiedLogging.ClientOptions)
		configs.MacUnifiedLogging.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.MacUnifiedLogging
		client, chRunning, err = usp_mac_unified_logging.NewMacUnifiedLoggingAdapter(ctx, configs.MacUnifiedLogging)
	} else if method == "azure_event_hub" {
		configs.AzureEventHub.ClientOptions = applyLogging(configs.AzureEventHub.ClientOptions)
		configs.AzureEventHub.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.AzureEventHub
		client, chRunning, err = usp_azure_event_hub.NewEventHubAdapter(ctx, configs.AzureEventHub)
	} else if method == "duo" {
		configs.Duo.ClientOptions = applyLogging(configs.Duo.ClientOptions)
		configs.Duo.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Duo
		client, chRunning, err = usp_duo.NewDuoAdapter(ctx, configs.Duo)
	} else if method == "cato" {
		configs.Cato.ClientOptions = applyLogging(configs.Cato.ClientOptions)
		configs.Cato.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Cato
		client, chRunning, err = usp_cato.NewCatoAdapter(ctx, configs.Cato)
	} else if method == "cylance" {
		configs.Cylance.ClientOptions = applyLogging(configs.Cylance.ClientOptions)
		configs.Cylance.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Cylance
		client, chRunning, err = usp_cylance.NewCylanceAdapter(ctx, configs.Cylance)
	} else if method == "entraid" {
		configs.EntraID.ClientOptions = applyLogging(configs.EntraID.ClientOptions)
		configs.EntraID.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.EntraID
		client, chRunning, err = usp_entraid.NewEntraIDAdapter(ctx, configs.EntraID)
	} else if method == "defender" {
		configs.Defender.ClientOptions = applyLogging(configs.Defender.ClientOptions)
		configs.Defender.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Defender
		client, chRunning, err = usp_defender.NewDefenderAdapter(ctx, configs.Defender)
	} else if method == "slack" {
		configs.Slack.ClientOptions = applyLogging(configs.Slack.ClientOptions)
		configs.Slack.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Slack
		client, chRunning, err = usp_slack.NewSlackAdapter(ctx, configs.Slack)
	} else if method == "sqs" {
		configs.Sqs.ClientOptions = applyLogging(configs.Sqs.ClientOptions)
		configs.Sqs.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Sqs
		client, chRunning, err = usp_sqs.NewSQSAdapter(ctx, configs.Sqs)
	} else if method == "sqs-files" {
		configs.SqsFiles.ClientOptions = applyLogging(configs.SqsFiles.ClientOptions)
		configs.SqsFiles.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.SqsFiles
		client, chRunning, err = usp_sqs_files.NewSQSFilesAdapter(ctx, configs.SqsFiles)
	} else if method == "simulator" {
		configs.Simulator.ClientOptions = applyLogging(configs.Simulator.ClientOptions)
		configs.Simulator.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Simulator
		client, chRunning, err = usp_simulator.NewSimulatorAdapter(ctx, configs.Simulator)
	} else if method == "file" {
		configs.File.ClientOptions = applyLogging(configs.File.ClientOptions)
		configs.File.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.File
		client, chRunning, err = usp_file.NewFileAdapter(ctx, configs.File)
	} else if method == "evtx" {
		configs.Evtx.ClientOptions = applyLogging(configs.Evtx.ClientOptions)
		configs.Evtx.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Evtx
		client, chRunning, err = usp_evtx.NewEVTXAdapter(ctx, configs.Evtx)
	} else if method == "k8s_pods" {
		configs.K8sPods.ClientOptions = applyLogging(configs.K8sPods.ClientOptions)
		configs.K8sPods.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.K8sPods
		client, chRunning, err = usp_k8s_pods.NewK8sPodsAdapter(ctx, configs.K8sPods)
	} else if method == "bigquery" {
		configs.BigQuery.ClientOptions = applyLogging(configs.BigQuery.ClientOptions)
		configs.BigQuery.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.BigQuery
		client, chRunning, err = usp_bigquery.NewBigQueryAdapter(ctx, configs.BigQuery)
	} else if method == "imap" {
		configs.Imap.ClientOptions = applyLogging(configs.Imap.ClientOptions)
		configs.Imap.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Imap
		client, chRunning, err = usp_imap.NewImapAdapter(ctx, configs.Imap)
	} else if method == "hubspot" {
		configs.HubSpot.ClientOptions = applyLogging(configs.HubSpot.ClientOptions)
		configs.HubSpot.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.HubSpot
		client, chRunning, err = usp_hubspot.NewHubSpotAdapter(ctx, configs.HubSpot)
	} else if method == "falconcloud" {
		configs.FalconCloud.ClientOptions = applyLogging(configs.FalconCloud.ClientOptions)
		configs.FalconCloud.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.FalconCloud
		client, chRunning, err = usp_falconcloud.NewFalconCloudAdapter(ctx, configs.FalconCloud)
	} else if method == "mimecast" {
		configs.Mimecast.ClientOptions = applyLogging(configs.Mimecast.ClientOptions)
		configs.Mimecast.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Mimecast
		client, chRunning, err = usp_mimecast.NewMimecastAdapter(ctx, configs.Mimecast)
	} else if method == "ms_graph" {
		configs.MsGraph.ClientOptions = applyLogging(configs.MsGraph.ClientOptions)
		configs.MsGraph.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.MsGraph
		client, chRunning, err = usp_ms_graph.NewMsGraphAdapter(ctx, configs.MsGraph)
	} else if method == "zendesk" {
		configs.Zendesk.ClientOptions = applyLogging(configs.Zendesk.ClientOptions)
		configs.Zendesk.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Zendesk
		client, chRunning, err = usp_zendesk.NewZendeskAdapter(ctx, configs.Zendesk)
	} else if method == "pandadoc" {
		configs.PandaDoc.ClientOptions = applyLogging(configs.PandaDoc.ClientOptions)
		configs.PandaDoc.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.PandaDoc
		client, chRunning, err = usp_pandadoc.NewPandaDocAdapter(ctx, configs.PandaDoc)
	} else if method == "proofpoint_tap" {
		configs.ProofpointTap.ClientOptions = applyLogging(configs.ProofpointTap.ClientOptions)
		configs.ProofpointTap.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.ProofpointTap
		client, chRunning, err = usp_proofpoint_tap.NewProofpointTapAdapter(ctx, configs.ProofpointTap)
	} else if method == "box" {
		configs.Box.ClientOptions = applyLogging(configs.Box.ClientOptions)
		configs.Box.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Box
		client, chRunning, err = usp_box.NewBoxAdapter(ctx, configs.Box)
	} else if method == "sublime" {
		configs.Sublime.ClientOptions = applyLogging(configs.Sublime.ClientOptions)
		configs.Sublime.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.Sublime
		client, chRunning, err = usp_sublime.NewSublimeAdapter(ctx, configs.Sublime)
	} else if method == "sentinel_one" {
		configs.SentinelOne.ClientOptions = applyLogging(configs.SentinelOne.ClientOptions)
		configs.SentinelOne.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.SentinelOne
		client, chRunning, err = usp_sentinelone.NewSentinelOneAdapter(ctx, configs.SentinelOne)
	} else if method == "trendmicro" {
		configs.TrendMicro.ClientOptions = applyLogging(configs.TrendMicro.ClientOptions)
		configs.TrendMicro.ClientOptions.Architecture = "usp_adapter"
		configToShow = configs.TrendMicro
		client, chRunning, err = usp_trendmicro.NewTrendMicroAdapter(ctx, configs.TrendMicro)
	} else {
		return nil, nil, errors.New(logError("unknown adapter_type: %s", method))
	}

	if showConfig {
		printConfig(method, configToShow)
	}

	if err != nil {
		return nil, nil, errors.New(logError("error instantiating client: %v", err))
	}

	return client, chRunning, nil
}

func parseConfigs(args []string) (string, []*Configuration, error) {
	configsToRun := []*Configuration{}
	var err error
	if len(args) < 2 {
		return "", nil, errors.New("not enough arguments")
	}

	method := args[0]
	args = args[1:]
	if len(args) == 1 {
		log("loading config from file: %s", args[0])
		if configsToRun, err = parseConfigsFromFile(args[0]); err != nil {
			return "", nil, err
		}
		log("found %d configs to run", len(configsToRun))
	} else {
		configs := &Configuration{}
		// Read the config from the CLI.
		if err = parseConfigsFromParams(method, args, configs); err != nil {
			return "", nil, err
		}
		// Read the config from the Env.
		if err = parseConfigsFromParams(method, os.Environ(), configs); err != nil {
			return "", nil, err
		}
		configsToRun = append(configsToRun, configs)
	}
	return method, configsToRun, nil
}

func parseConfigsFromFile(filePath string) ([]*Configuration, error) {
	f, err := os.Open(filePath)
	if err != nil {
		printUsage()
		return nil, errors.New(logError("os.Open(): %v", err))
	}
	b, err := io.ReadAll(f)
	if err != nil {
		printUsage()
		return nil, errors.New(logError("io.ReadAll(): %v", err))
	}
	yamlDecoder := yaml.NewDecoder(bytes.NewBuffer(b))
	jsonDecoder := json.NewDecoder(bytes.NewBuffer(b))
	var jsonErr error
	var yamlErr error

	configsToRun := []*Configuration{}
	for {
		configs := &Configuration{}

		if jsonErr = jsonDecoder.Decode(configs); jsonErr != nil {
			if jsonErr == io.EOF {
				jsonErr = nil
			}
			break
		}
		configsToRun = append(configsToRun, configs)
	}

	for {
		configs := &Configuration{}

		if yamlErr = yamlDecoder.Decode(configs); yamlErr != nil {
			if yamlErr == io.EOF {
				yamlErr = nil
			}
			break
		}

		configsToRun = append(configsToRun, configs)
	}

	if jsonErr != nil && yamlErr != nil {
		printUsage()
		return nil, errors.New(logError("decoding error: json=%v yaml=%v", jsonErr, yamlErr))
	}

	return configsToRun, nil
}

func parseConfigsFromParams(prefix string, params []string, configs *Configuration) error {
	// Read the config from the CLI.
	if err := utils.ParseCLI(prefix, params, configs); err != nil {
		printUsage()
		return errors.New(logError("ParseCLI(): %v", err))
	}

	return nil
}

func applyLogging(o uspclient.ClientOptions) uspclient.ClientOptions {
	stats := &AdapterStats{
		lastAck:          time.Now(),
		lastBackPressure: time.Now(),
	}

	o.DebugLog = func(msg string) {
		log("DBG %s: %s", time.Now().Format(time.Stamp), msg)
	}
	o.OnWarning = func(msg string) {
		log("WRN %s: %s", time.Now().Format(time.Stamp), msg)
	}
	o.OnError = func(err error) {
		logError("ERR %s: %s", time.Now().Format(time.Stamp), err.Error())
	}
	o.BufferOptions.OnBackPressure = func() {
		stats.m.Lock()
		defer stats.m.Unlock()
		stats.lastBackPressure = time.Now()
	}
	o.BufferOptions.OnAck = func() {
		stats.m.Lock()
		defer stats.m.Unlock()
		stats.lastAck = time.Now()
	}

	go func() {
		for {
			time.Sleep(10 * time.Second)
			stats.m.Lock()
			log("FLO %s: last_ack=%s last_pressure=%s", time.Now().Format(time.Stamp), stats.lastAck.Format(time.Stamp), stats.lastBackPressure.Format(time.Stamp))
			stats.m.Unlock()
		}
	}()

	return o
}
