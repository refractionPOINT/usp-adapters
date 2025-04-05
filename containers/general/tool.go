package main

import (
	"bytes"
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
	"github.com/refractionPOINT/usp-adapters/cato"
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
	"github.com/refractionPOINT/usp-adapters/pubsub"
	"github.com/refractionPOINT/usp-adapters/s3"
	"github.com/refractionPOINT/usp-adapters/sentinelone"
	"github.com/refractionPOINT/usp-adapters/simulator"
	"github.com/refractionPOINT/usp-adapters/slack"
	"github.com/refractionPOINT/usp-adapters/sophos"
	"github.com/refractionPOINT/usp-adapters/sqs"
	"github.com/refractionPOINT/usp-adapters/sqs-files"
	"github.com/refractionPOINT/usp-adapters/stdin"
	"github.com/refractionPOINT/usp-adapters/syslog"
	"github.com/refractionPOINT/usp-adapters/wel"
	"github.com/refractionPOINT/usp-adapters/zendesk"

	"github.com/refractionPOINT/usp-adapters/utils"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/usp-adapters/containers/conf"
	confupdateclient "github.com/refractionPOINT/usp-adapters/containers/general/conf_update_client"
	"gopkg.in/yaml.v2"
)

type USPClient interface {
	Close() error
}

type AdapterStats struct {
	m                sync.Mutex
	lastAck          time.Time
	lastBackPressure time.Time
}

type Configuration struct {
	conf.GeneralConfigs

	// If a literal config is not specified, the OID and GUID
	// will be used to fetch the config from Limacharlie
	// and update the config in real time.
	OID      string `json:"oid" yaml:"oid"`
	ConfGUID string `json:"conf_guid" yaml:"conf_guid"`
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
	logError("Usage: ./adapter adapter_type [config_file.yaml | <param>...]")
	logError("Available configs:\n")
	printStruct("", Configuration{}, true)
}

func printConfig(method string, c interface{}) {
	b, _ := yaml.Marshal(c)
	log("Configs in use (%s):\n----------------------------------\n%s----------------------------------\n", method, string(b))
}

func main() {
	log("starting")

	if len(os.Args) > 1 && strings.HasPrefix(os.Args[1], "-") {
		if err := serviceMode(os.Args[0], os.Args[1], os.Args[2:]); err != nil {
			logError("service: %v", err)
			os.Exit(1)
		}
		return
	}

	method, configsToRun, err := parseConfigs(os.Args[1:])
	if err != nil {
		logError("error: %s", err)
		printUsage()
		os.Exit(1)
	}
	if len(configsToRun) == 0 {
		logError("no configs to run")
		os.Exit(1)
		return
	}
	clients := []USPClient{}
	chRunnings := make(chan struct{})
	healthCheckPortRequested := 0
	for _, config := range configsToRun {
		// If an OID and GUID are specified, we will start a conf update client
		// to update the config in real time.
		var confUpdateClient *confupdateclient.ConfUpdateClient
		if config.OID != "" && config.ConfGUID != "" {
			var confData map[string]interface{}
			confUpdateClient, confData, err = confupdateclient.NewConfUpdateClient(config.OID, config.ConfGUID, &limacharlie.LCLoggerZerolog{})
			if err != nil {
				logError("error creating conf update client: %v", err)
				os.Exit(1)
			}
			// Use this new config to run the adapter.
			if err := limacharlie.Dict(confData).UnMarshalToStruct(config); err != nil {
				logError("error unmarshalling conf update: %v", err)
				os.Exit(1)
			}
		}

		log("starting adapter: %s", method)
		client, chRunning, err := runAdapter(method, *config)
		if err != nil {
			os.Exit(1)
		}
		clients = append(clients, client)
		go func() {
			<-chRunning
			chRunnings <- struct{}{}
		}()
		if config.Healthcheck != 0 {
			healthCheckPortRequested = config.Healthcheck
		}
		if confUpdateClient != nil {
			log("watching for conf updates")
			go func() {
				newConfig := Configuration{}
				if err := confUpdateClient.WatchForChanges(10*time.Second, func(data map[string]interface{}) {
					if err := limacharlie.Dict(data).UnMarshalToStruct(&newConfig); err != nil {
						logError("error unmarshalling conf update: %v", err)
					}
					log("stopping previous adapter")
					if err := client.Close(); err != nil {
						logError("error closing client: %v", err)
					}
					log("starting new adapter")
					client, chRunning, err = runAdapter(method, newConfig)
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

func runAdapter(method string, configs Configuration) (USPClient, chan struct{}, error) {
	var client USPClient
	var chRunning chan struct{}
	var err error

	if method == "syslog" {
		configs.Syslog.ClientOptions = applyLogging(configs.Syslog.ClientOptions)
		configs.Syslog.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Syslog)
		client, chRunning, err = usp_syslog.NewSyslogAdapter(configs.Syslog)
	} else if method == "pubsub" {
		configs.PubSub.ClientOptions = applyLogging(configs.PubSub.ClientOptions)
		configs.PubSub.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.PubSub)
		client, chRunning, err = usp_pubsub.NewPubSubAdapter(configs.PubSub)
	} else if method == "gcs" {
		configs.Gcs.ClientOptions = applyLogging(configs.Gcs.ClientOptions)
		configs.Gcs.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Gcs)
		client, chRunning, err = usp_gcs.NewGCSAdapter(configs.Gcs)
	} else if method == "s3" {
		configs.S3.ClientOptions = applyLogging(configs.S3.ClientOptions)
		configs.S3.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.S3)
		client, chRunning, err = usp_s3.NewS3Adapter(configs.S3)
	} else if method == "stdin" {
		configs.Stdin.ClientOptions = applyLogging(configs.Stdin.ClientOptions)
		configs.Stdin.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Stdin)
		client, chRunning, err = usp_stdin.NewStdinAdapter(configs.Stdin)
	} else if method == "1password" {
		configs.OnePassword.ClientOptions = applyLogging(configs.OnePassword.ClientOptions)
		configs.OnePassword.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.OnePassword)
		client, chRunning, err = usp_1password.NewOnePasswordpAdapter(configs.OnePassword)
	} else if method == "itglue" {
		configs.ITGlue.ClientOptions = applyLogging(configs.ITGlue.ClientOptions)
		configs.ITGlue.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.ITGlue)
		client, chRunning, err = usp_itglue.NewITGlueAdapter(configs.ITGlue)
	} else if method == "sophos" {
		configs.Sophos.ClientOptions = applyLogging(configs.Sophos.ClientOptions)
		configs.Sophos.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Sophos)
		client, chRunning, err = usp_sophos.NewSophosAdapter(configs.Sophos)
	} else if method == "okta" {
		configs.Okta.ClientOptions = applyLogging(configs.Okta.ClientOptions)
		configs.Okta.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Okta)
		client, chRunning, err = usp_okta.NewOktaAdapter(configs.Okta)
	} else if method == "office365" {
		configs.Office365.ClientOptions = applyLogging(configs.Office365.ClientOptions)
		configs.Office365.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Office365)
		client, chRunning, err = usp_o365.NewOffice365Adapter(configs.Office365)
	} else if method == "wel" {
		configs.Wel.ClientOptions = applyLogging(configs.Wel.ClientOptions)
		configs.Wel.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Wel)
		client, chRunning, err = usp_wel.NewWELAdapter(configs.Wel)
	} else if method == "mac_unified_logging" {
		configs.MacUnifiedLogging.ClientOptions = applyLogging(configs.MacUnifiedLogging.ClientOptions)
		configs.MacUnifiedLogging.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.MacUnifiedLogging)
		client, chRunning, err = usp_mac_unified_logging.NewMacUnifiedLoggingAdapter(configs.MacUnifiedLogging)
	} else if method == "azure_event_hub" {
		configs.AzureEventHub.ClientOptions = applyLogging(configs.AzureEventHub.ClientOptions)
		configs.AzureEventHub.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.AzureEventHub)
		client, chRunning, err = usp_azure_event_hub.NewEventHubAdapter(configs.AzureEventHub)
	} else if method == "duo" {
		configs.Duo.ClientOptions = applyLogging(configs.Duo.ClientOptions)
		configs.Duo.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Duo)
		client, chRunning, err = usp_duo.NewDuoAdapter(configs.Duo)
	} else if method == "cato" {
		configs.Cato.ClientOptions = applyLogging(configs.Cato.ClientOptions)
		configs.Cato.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Cato)
		client, chRunning, err = usp_cato.NewCatoAdapter(configs.Cato)
	} else if method == "entraid" {
		configs.EntraID.ClientOptions = applyLogging(configs.EntraID.ClientOptions)
		configs.EntraID.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.EntraID)
		client, chRunning, err = usp_entraid.NewEntraIDAdapter(configs.EntraID)
	} else if method == "defender" {
		configs.Defender.ClientOptions = applyLogging(configs.Defender.ClientOptions)
		configs.Defender.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Defender)
		client, chRunning, err = usp_defender.NewDefenderAdapter(configs.Defender)
	} else if method == "slack" {
		configs.Slack.ClientOptions = applyLogging(configs.Slack.ClientOptions)
		configs.Slack.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Slack)
		client, chRunning, err = usp_slack.NewSlackAdapter(configs.Slack)
	} else if method == "sqs" {
		configs.Sqs.ClientOptions = applyLogging(configs.Sqs.ClientOptions)
		configs.Sqs.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Sqs)
		client, chRunning, err = usp_sqs.NewSQSAdapter(configs.Sqs)
	} else if method == "sqs-files" {
		configs.SqsFiles.ClientOptions = applyLogging(configs.SqsFiles.ClientOptions)
		configs.SqsFiles.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.SqsFiles)
		client, chRunning, err = usp_sqs_files.NewSQSFilesAdapter(configs.SqsFiles)
	} else if method == "simulator" {
		configs.Simulator.ClientOptions = applyLogging(configs.Simulator.ClientOptions)
		configs.Simulator.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Simulator)
		client, chRunning, err = usp_simulator.NewSimulatorAdapter(configs.Simulator)
	} else if method == "file" {
		configs.File.ClientOptions = applyLogging(configs.File.ClientOptions)
		configs.File.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.File)
		client, chRunning, err = usp_file.NewFileAdapter(configs.File)
	} else if method == "evtx" {
		configs.Evtx.ClientOptions = applyLogging(configs.Evtx.ClientOptions)
		configs.Evtx.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Evtx)
		client, chRunning, err = usp_evtx.NewEVTXAdapter(configs.Evtx)
	} else if method == "k8s_pods" {
		configs.K8sPods.ClientOptions = applyLogging(configs.K8sPods.ClientOptions)
		configs.K8sPods.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.K8sPods)
		client, chRunning, err = usp_k8s_pods.NewK8sPodsAdapter(configs.K8sPods)
	} else if method == "bigquery" {
		configs.BigQuery.ClientOptions = applyLogging(configs.BigQuery.ClientOptions)
		configs.BigQuery.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.BigQuery)
		client, chRunning, err = usp_bigquery.NewBigQueryAdapter(configs.BigQuery)
	} else if method == "imap" {
		configs.Imap.ClientOptions = applyLogging(configs.Imap.ClientOptions)
		configs.Imap.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Imap)
		client, chRunning, err = usp_imap.NewImapAdapter(configs.Imap)
	} else if method == "hubspot" {
		configs.HubSpot.ClientOptions = applyLogging(configs.HubSpot.ClientOptions)
		configs.HubSpot.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.HubSpot)
		client, chRunning, err = usp_hubspot.NewHubSpotAdapter(configs.HubSpot)
	} else if method == "falconcloud" {
		configs.FalconCloud.ClientOptions = applyLogging(configs.FalconCloud.ClientOptions)
		configs.FalconCloud.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.FalconCloud)
		client, chRunning, err = usp_falconcloud.NewFalconCloudAdapter(configs.FalconCloud)
	} else if method == "mimecast" {
		configs.Mimecast.ClientOptions = applyLogging(configs.Mimecast.ClientOptions)
		configs.Mimecast.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Mimecast)
		client, chRunning, err = usp_mimecast.NewMimecastAdapter(configs.Mimecast)
	} else if method == "ms_graph" {
		configs.MsGraph.ClientOptions = applyLogging(configs.MsGraph.ClientOptions)
		configs.MsGraph.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.MsGraph)
		client, chRunning, err = usp_ms_graph.NewMsGraphAdapter(configs.MsGraph)
	} else if method == "zendesk" {
		configs.Zendesk.ClientOptions = applyLogging(configs.Zendesk.ClientOptions)
		configs.Zendesk.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.Zendesk)
		client, chRunning, err = usp_zendesk.NewZendeskAdapter(configs.Zendesk)
	} else if method == "pandadoc" {
		configs.PandaDoc.ClientOptions = applyLogging(configs.PandaDoc.ClientOptions)
		configs.PandaDoc.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.PandaDoc)
		client, chRunning, err = usp_pandadoc.NewPandaDocAdapter(configs.PandaDoc)
	} else if method == "sentinel_one" {
		configs.SentinelOne.ClientOptions = applyLogging(configs.SentinelOne.ClientOptions)
		configs.SentinelOne.ClientOptions.Architecture = "usp_adapter"
		printConfig(method, configs.SentinelOne)
		client, chRunning, err = usp_sentinelone.NewSentinelOneAdapter(configs.SentinelOne)
	} else {
		return nil, nil, errors.New(logError("unknown adapter_type: %s", method))
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
